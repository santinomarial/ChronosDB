#include "chronos/common/uuid_generator.hpp"
#include "chronos/common/version.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/reactor.hpp"
#include "chronos/network/spsc_queue.hpp"
#include "chronos/service/native_protocol_service.hpp"
#include "chronos/service/single_node_database.hpp"

#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace {

using chronos::network::MessageType;
using chronos::network::NetworkTask;
using chronos::network::ProtocolErrorCode;
using chronos::network::Reactor;
using chronos::network::ReactorBackend;
using chronos::network::SpscNetworkTaskQueue;
using chronos::service::NativeProtocolService;
using chronos::service::SingleNodeDatabase;

volatile std::sig_atomic_t stop_requested = 0;

extern "C" void request_stop(const int) {
  stop_requested = 1;
}

struct Options {
  ReactorBackend backend{ReactorBackend::kEpoll};
  std::uint16_t port{8812U};
  std::size_t queue_capacity{1024U};
  std::string data_directory;
  bool help{};
  bool version{};
};

void print_usage(const std::string_view program, std::ostream& stream) {
  stream << "Usage: " << program
         << " [--listen 127.0.0.1] [--port PORT] [--backend epoll|io_uring]"
            " [--queue-capacity COUNT] [--data-dir PATH]\n"
            "       "
         << program << " --help\n"
         << "       " << program << " --version\n";
}

template <typename Integer>
[[nodiscard]] bool parse_integer(const std::string_view text, Integer& value) noexcept {
  if (text.empty())
    return false;
  Integer parsed{};
  const auto result = std::from_chars(text.data(), text.data() + text.size(), parsed);
  if (result.ec != std::errc{} || result.ptr != text.data() + text.size())
    return false;
  value = parsed;
  return true;
}

// C++ process entry points supply the conventional C argv array.
// NOLINTNEXTLINE(modernize-avoid-c-arrays)
[[nodiscard]] std::optional<Options> parse_options(const int argc, const char* const argv[]) {
  Options options;
  for (int index = 1; index < argc; ++index) {
    const std::string_view argument{argv[index]};
    if (argument == "--help") {
      options.help = true;
      continue;
    }
    if (argument == "--version") {
      options.version = true;
      continue;
    }
    if (index + 1 >= argc) {
      std::cerr << "chronosd: missing value for " << argument << '\n';
      return std::nullopt;
    }
    const std::string_view value{argv[++index]};
    if (argument == "--listen") {
      if (value != "127.0.0.1") {
        std::cerr << "chronosd: plaintext service is restricted to 127.0.0.1\n";
        return std::nullopt;
      }
    } else if (argument == "--port") {
      if (!parse_integer(value, options.port)) {
        std::cerr << "chronosd: port must be an integer from 0 through 65535\n";
        return std::nullopt;
      }
    } else if (argument == "--queue-capacity") {
      if (!parse_integer(value, options.queue_capacity) || options.queue_capacity == 0U ||
          options.queue_capacity > 1'048'576U) {
        std::cerr << "chronosd: queue capacity must be from 1 through 1048576\n";
        return std::nullopt;
      }
    } else if (argument == "--backend") {
      if (value == "epoll")
        options.backend = ReactorBackend::kEpoll;
      else if (value == "io_uring")
        options.backend = ReactorBackend::kIoUring;
      else {
        std::cerr << "chronosd: backend must be epoll or io_uring\n";
        return std::nullopt;
      }
    } else if (argument == "--data-dir") {
      if (value.empty()) {
        std::cerr << "chronosd: data directory must be nonempty\n";
        return std::nullopt;
      }
      options.data_directory = value;
    } else {
      std::cerr << "chronosd: unknown option " << argument << '\n';
      return std::nullopt;
    }
  }
  if ((options.help || options.version) && argc != 2) {
    std::cerr << "chronosd: --help and --version must be used alone\n";
    return std::nullopt;
  }
  return options;
}

[[nodiscard]] chronos::common::Result<chronos::runtime::DatabaseBootstrapDescriptor>
new_database_descriptor(chronos::common::UuidGenerator& identities) {
  auto database_id = identities.generate();
  if (!database_id.has_value())
    return chronos::common::make_unexpected(database_id.error());
  chronos::common::Result<chronos::common::Uuid> metadata_group = identities.generate();
  if (!metadata_group.has_value())
    return chronos::common::make_unexpected(metadata_group.error());
  for (std::size_t attempt = 0U; *metadata_group == *database_id && attempt < 8U; ++attempt) {
    metadata_group = identities.generate();
    if (!metadata_group.has_value())
      return chronos::common::make_unexpected(metadata_group.error());
  }
  if (*metadata_group == *database_id) {
    return chronos::common::make_unexpected(
        chronos::common::Status{chronos::common::StatusCode::kUnavailable,
                                "secure UUID source repeated the database identity"});
  }
  return chronos::runtime::DatabaseBootstrapDescriptor{
      .database_id = *database_id,
      .metadata_group_id = *metadata_group,
      .local_node_id = 1U,
      .mutable_head_rows = 65'536U,
      .maximum_sealed_generations = 8U,
      .variable_column_bytes = 1U * 1024U * 1024U,
      .maximum_retry_entries = 65'536U,
      .wal_segment_target_bytes = chronos::wal::kSegmentSizeLimit,
      .raft_segment_target_bytes = 64U * 1024U * 1024U};
}

class DataPlaneWorker {
public:
  DataPlaneWorker(SpscNetworkTaskQueue& requests, SpscNetworkTaskQueue& responses, Reactor& reactor,
                  NativeProtocolService* service) noexcept
      : requests_(&requests), responses_(&responses), reactor_(&reactor), service_(service) {}

  DataPlaneWorker(const DataPlaneWorker&) = delete;
  DataPlaneWorker& operator=(const DataPlaneWorker&) = delete;

  [[nodiscard]] bool start() noexcept {
    try {
      thread_ = std::thread{[this] { run(); }};
      return true;
    } catch (const std::bad_alloc&) {
      return false;
    } catch (const std::system_error&) {
      return false;
    }
  }

  void stop() noexcept {
    stopping_.store(true, std::memory_order_release);
    if (thread_.joinable())
      thread_.join();
  }

  [[nodiscard]] bool failed() const noexcept {
    return failed_.load(std::memory_order_acquire);
  }

private:
  [[nodiscard]] std::optional<NetworkTask> unconfigured(NetworkTask request) {
    auto payload = chronos::network::encode_error_message(ProtocolErrorCode::kExecutionFailure,
                                                          "chronosd data plane is not configured");
    if (!payload.has_value())
      return std::nullopt;
    request.frame = {.header = {.message_type = MessageType::kError,
                                .request_id = request.frame.header.request_id,
                                .payload_size = static_cast<std::uint32_t>(payload->size())},
                     .payload = std::move(*payload)};
    return request;
  }

  [[nodiscard]] std::optional<NetworkTask> unsupported(NetworkTask request) {
    auto payload = chronos::network::encode_error_message(
        ProtocolErrorCode::kExecutionFailure, "native subscriptions are not configured");
    if (!payload.has_value())
      return std::nullopt;
    request.frame = {.header = {.message_type = MessageType::kError,
                                .request_id = request.frame.header.request_id,
                                .payload_size = static_cast<std::uint32_t>(payload->size())},
                     .payload = std::move(*payload)};
    return request;
  }

  void run() noexcept {
    std::optional<NetworkTask> pending;
    std::vector<NetworkTask> sequence;
    std::size_t next_response{};
    while (!stopping_.load(std::memory_order_acquire)) {
      if (pending.has_value()) {
        if (!responses_->try_push_preserving(*pending)) {
          std::this_thread::sleep_for(std::chrono::milliseconds{1});
          continue;
        }
        pending.reset();
        if (!reactor_->notify_response_ready().is_ok()) {
          failed_.store(true, std::memory_order_release);
          return;
        }
        continue;
      }

      if (next_response < sequence.size()) {
        pending.emplace(std::move(sequence[next_response++]));
        if (next_response == sequence.size()) {
          sequence.clear();
          next_response = 0U;
        }
        continue;
      }

      auto request = requests_->try_pop();
      if (!request.has_value()) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
        continue;
      }
      if (request->frame.header.message_type == MessageType::kCancel)
        continue;
      if (service_ == nullptr) {
        pending = unconfigured(std::move(*request));
      } else if (request->frame.header.message_type == MessageType::kIngestRequest) {
        auto response = service_->execute_ingest(std::move(*request));
        if (response.has_value())
          pending.emplace(std::move(*response));
      } else if (request->frame.header.message_type == MessageType::kQueryRequest) {
        auto responses = service_->execute_query(std::move(*request));
        if (responses.has_value()) {
          sequence = std::move(responses->responses);
          next_response = 0U;
        }
      } else {
        pending = unsupported(std::move(*request));
      }
      if (!pending.has_value() && sequence.empty()) {
        failed_.store(true, std::memory_order_release);
        return;
      }
    }
  }

  SpscNetworkTaskQueue* requests_;
  SpscNetworkTaskQueue* responses_;
  Reactor* reactor_;
  NativeProtocolService* service_;
  std::atomic<bool> stopping_{false};
  std::atomic<bool> failed_{false};
  std::thread thread_;
};

} // namespace

// NOLINTNEXTLINE(modernize-avoid-c-arrays)
int main(const int argc, const char* const argv[]) {
  const std::string_view program = argc > 0 ? std::string_view{argv[0]} : "chronosd";
  const auto options = parse_options(argc, argv);
  if (!options.has_value()) {
    print_usage(program, std::cerr);
    return 2;
  }
  if (options->help) {
    print_usage(program, std::cout);
    return 0;
  }
  if (options->version) {
    std::cout << chronos::common::version_text() << '\n';
    return 0;
  }

  chronos::common::SystemUuidGenerator identities;
  std::optional<SingleNodeDatabase> database;
  std::optional<NativeProtocolService> service;
  if (!options->data_directory.empty()) {
    auto descriptor = new_database_descriptor(identities);
    if (!descriptor.has_value()) {
      std::cerr << "chronosd: secure identity setup failed: " << descriptor.error().to_string()
                << '\n';
      return 1;
    }
    chronos::service::SingleNodeDatabaseConfig database_config{
        .bootstrap = {.database_root = options->data_directory, .new_database = *descriptor},
        .wal_recovery = {.repair_incomplete_final_tail = false},
        .raft_recovery = {.repair_incomplete_final_tail = false}};
    auto opened = SingleNodeDatabase::open_or_create(std::move(database_config));
    if (!opened.has_value()) {
      std::cerr << "chronosd: database start failed: " << opened.error().to_string() << '\n';
      return 1;
    }
    database.emplace(std::move(*opened));
    service.emplace(*database, identities);
  }

  auto requests = SpscNetworkTaskQueue::create(options->queue_capacity);
  auto responses = SpscNetworkTaskQueue::create(options->queue_capacity);
  if (!requests.has_value() || !responses.has_value()) {
    const auto& error = !requests.has_value() ? requests.error() : responses.error();
    std::cerr << "chronosd: queue creation failed: " << error.to_string() << '\n';
    return 1;
  }

  chronos::network::EpollServerConfig config;
  config.port = options->port;
  auto reactor =
      Reactor::start(options->backend, config, {.requests = &*requests, .responses = &*responses});
  if (!reactor.has_value()) {
    std::cerr << "chronosd: server start failed: " << reactor.error().to_string() << '\n';
    return 1;
  }

  DataPlaneWorker worker{*requests, *responses, *reactor,
                         service.has_value() ? std::addressof(*service) : nullptr};
  if (!worker.start()) {
    static_cast<void>(reactor->shutdown());
    if (database.has_value())
      static_cast<void>(database->shutdown());
    std::cerr << "chronosd: worker start failed\n";
    return 1;
  }

  stop_requested = 0;
  if (std::signal(SIGINT, request_stop) == SIG_ERR ||
      std::signal(SIGTERM, request_stop) == SIG_ERR) {
    worker.stop();
    static_cast<void>(reactor->shutdown());
    if (database.has_value())
      static_cast<void>(database->shutdown());
    std::cerr << "chronosd: signal handler installation failed\n";
    return 1;
  }

  std::cout << "chronosd listening on 127.0.0.1:" << reactor->bound_port()
            << " backend=" << chronos::network::reactor_backend_name(options->backend)
            << " data_plane=" << (service.has_value() ? "configured" : "unconfigured") << '\n'
            << std::flush;

  int exit_code = 0;
  while (stop_requested == 0 && !worker.failed()) {
    const auto status = reactor->poll_once(std::chrono::milliseconds{10});
    if (!status.is_ok()) {
      std::cerr << "chronosd: reactor failure: " << status.to_string() << '\n';
      exit_code = 1;
      break;
    }
  }
  if (worker.failed()) {
    std::cerr << "chronosd: data-plane worker failed\n";
    exit_code = 1;
  }

  worker.stop();
  const auto shutdown = reactor->shutdown();
  if (!shutdown.is_ok()) {
    std::cerr << "chronosd: shutdown failed: " << shutdown.to_string() << '\n';
    exit_code = 1;
  }
  if (database.has_value()) {
    const auto database_shutdown = database->shutdown();
    if (!database_shutdown.is_ok()) {
      std::cerr << "chronosd: database shutdown failed: " << database_shutdown.to_string() << '\n';
      exit_code = 1;
    }
  }
  return exit_code;
}
