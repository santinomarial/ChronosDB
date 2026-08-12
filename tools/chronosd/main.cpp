#include "chronos/common/version.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/reactor.hpp"
#include "chronos/network/spsc_queue.hpp"

#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <new>
#include <optional>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>

namespace {

using chronos::network::MessageType;
using chronos::network::NetworkTask;
using chronos::network::ProtocolErrorCode;
using chronos::network::Reactor;
using chronos::network::ReactorBackend;
using chronos::network::SpscNetworkTaskQueue;

volatile std::sig_atomic_t stop_requested = 0;

extern "C" void request_stop(const int) {
  stop_requested = 1;
}

struct Options {
  ReactorBackend backend{ReactorBackend::kEpoll};
  std::uint16_t port{8812U};
  std::size_t queue_capacity{1024U};
  bool help{};
  bool version{};
};

void print_usage(const std::string_view program, std::ostream& stream) {
  stream << "Usage: " << program
         << " [--listen 127.0.0.1] [--port PORT] [--backend epoll|io_uring]"
            " [--queue-capacity COUNT]\n"
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

class UnconfiguredDataPlaneWorker {
public:
  UnconfiguredDataPlaneWorker(SpscNetworkTaskQueue& requests, SpscNetworkTaskQueue& responses,
                              Reactor& reactor) noexcept
      : requests_(&requests), responses_(&responses), reactor_(&reactor) {}

  UnconfiguredDataPlaneWorker(const UnconfiguredDataPlaneWorker&) = delete;
  UnconfiguredDataPlaneWorker& operator=(const UnconfiguredDataPlaneWorker&) = delete;

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
  void run() noexcept {
    std::optional<NetworkTask> pending;
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

      auto request = requests_->try_pop();
      if (!request.has_value()) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
        continue;
      }
      if (request->frame.header.message_type == MessageType::kCancel)
        continue;

      auto payload = chronos::network::encode_error_message(
          ProtocolErrorCode::kExecutionFailure, "chronosd data plane is not configured");
      if (!payload.has_value()) {
        failed_.store(true, std::memory_order_release);
        return;
      }
      pending.emplace(NetworkTask{
          .connection_id = request->connection_id,
          .principal_id = request->principal_id,
          .frame = {.header = {.message_type = MessageType::kError,
                               .request_id = request->frame.header.request_id,
                               .payload_size = static_cast<std::uint32_t>(payload->size())},
                    .payload = std::move(*payload)}});
    }
  }

  SpscNetworkTaskQueue* requests_;
  SpscNetworkTaskQueue* responses_;
  Reactor* reactor_;
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

  UnconfiguredDataPlaneWorker worker{*requests, *responses, *reactor};
  if (!worker.start()) {
    static_cast<void>(reactor->shutdown());
    std::cerr << "chronosd: worker start failed\n";
    return 1;
  }

  stop_requested = 0;
  if (std::signal(SIGINT, request_stop) == SIG_ERR ||
      std::signal(SIGTERM, request_stop) == SIG_ERR) {
    worker.stop();
    static_cast<void>(reactor->shutdown());
    std::cerr << "chronosd: signal handler installation failed\n";
    return 1;
  }

  std::cout << "chronosd listening on 127.0.0.1:" << reactor->bound_port()
            << " backend=" << chronos::network::reactor_backend_name(options->backend)
            << " data_plane=unconfigured\n"
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
  return exit_code;
}
