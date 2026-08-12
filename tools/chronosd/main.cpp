#include "chronos/common/uuid_generator.hpp"
#include "chronos/common/version.hpp"
#include "chronos/io/posix_io.hpp"
#include "chronos/live/subscription_plan_storage.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/reactor.hpp"
#include "chronos/network/spsc_queue.hpp"
#include "chronos/service/native_protocol_service.hpp"
#include "chronos/service/single_node_committed_append_router.hpp"
#include "chronos/service/single_node_database.hpp"
#include "chronos/service/single_node_subscription_runtime.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <system_error>
#include <thread>
#include <unistd.h>
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
using chronos::service::SingleNodeCommittedAppendRouter;
using chronos::service::SingleNodeDatabase;
using chronos::service::SingleNodeSubscriptionRuntime;

volatile std::sig_atomic_t stop_requested = 0;

extern "C" void request_stop(const int) {
  stop_requested = 1;
}

struct Options {
  ReactorBackend backend{ReactorBackend::kEpoll};
  std::uint16_t port{8812U};
  std::size_t queue_capacity{1024U};
  std::string data_directory;
  std::string subscription_sql;
  std::string subscription_key_file;
  bool help{};
  bool version{};
};

void print_usage(const std::string_view program, std::ostream& stream) {
  stream << "Usage: " << program
         << " [--listen 127.0.0.1] [--port PORT] [--backend epoll|io_uring]"
            " [--queue-capacity COUNT] [--data-dir PATH]"
            " [--subscription-sql SQL --subscription-key-file PATH]\n"
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
    } else if (argument == "--subscription-sql") {
      if (value.empty()) {
        std::cerr << "chronosd: subscription SQL must be nonempty\n";
        return std::nullopt;
      }
      options.subscription_sql = value;
    } else if (argument == "--subscription-key-file") {
      if (value.empty()) {
        std::cerr << "chronosd: subscription key file must be nonempty\n";
        return std::nullopt;
      }
      options.subscription_key_file = value;
    } else {
      std::cerr << "chronosd: unknown option " << argument << '\n';
      return std::nullopt;
    }
  }
  if ((options.help || options.version) && argc != 2) {
    std::cerr << "chronosd: --help and --version must be used alone\n";
    return std::nullopt;
  }
  if (options.subscription_sql.empty() != options.subscription_key_file.empty()) {
    std::cerr << "chronosd: subscription SQL and key file must be configured together\n";
    return std::nullopt;
  }
  if (!options.subscription_sql.empty() && options.data_directory.empty()) {
    std::cerr << "chronosd: subscriptions require a configured data directory\n";
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

[[nodiscard]] chronos::common::Status invalid(std::string message) {
  return {chronos::common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] chronos::common::Status io_error(std::string message) {
  return {chronos::common::StatusCode::kIoError, std::move(message)};
}

[[nodiscard]] chronos::common::Result<chronos::live::ResumeTokenMacKey>
load_subscription_key(const std::string& path) {
  const int descriptor = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (descriptor < 0)
    return chronos::common::make_unexpected(io_error("cannot open subscription key file"));
  const auto close_descriptor = [&]() noexcept { static_cast<void>(::close(descriptor)); };
  struct stat metadata {};
  if (::fstat(descriptor, &metadata) != 0 || !S_ISREG(metadata.st_mode) ||
      (metadata.st_mode & 0077) != 0 || metadata.st_size != 32) {
    close_descriptor();
    return chronos::common::make_unexpected(
        invalid("subscription key must be a 32-byte regular file inaccessible to group/other"));
  }
  chronos::live::ResumeTokenMacKey key{};
  std::size_t offset = 0U;
  while (offset < key.size()) {
    const ssize_t count = ::read(descriptor, key.data() + offset, key.size() - offset);
    if (count < 0 && errno == EINTR)
      continue;
    if (count <= 0) {
      close_descriptor();
      return chronos::common::make_unexpected(io_error("cannot read subscription key file"));
    }
    offset += static_cast<std::size_t>(count);
  }
  close_descriptor();
  if (std::ranges::all_of(key, [](const std::byte value) { return value == std::byte{0}; }))
    return chronos::common::make_unexpected(invalid("subscription key must be nonzero"));
  return key;
}

[[nodiscard]] chronos::common::Result<bool>
ensure_directory(const std::filesystem::path& parent_path, const std::string_view name) {
  auto parent = chronos::io::PosixDirectory::open(parent_path.string());
  if (!parent.has_value())
    return chronos::common::make_unexpected(parent.error());
  chronos::common::Status created = parent->create_exclusive_directory(name, 0700U);
  if (created.is_ok()) {
    chronos::common::Status synced = parent->sync();
    if (!synced.is_ok())
      return chronos::common::make_unexpected(std::move(synced));
    return true;
  }
  if (created.code() != chronos::common::StatusCode::kAlreadyExists)
    return chronos::common::make_unexpected(std::move(created));
  auto existing = parent->open_directory(name);
  if (!existing.has_value())
    return chronos::common::make_unexpected(existing.error());
  return false;
}

[[nodiscard]] chronos::common::Result<bool> directory_empty(const std::filesystem::path& path) {
  auto directory = chronos::io::PosixDirectory::open(path.string());
  if (!directory.has_value())
    return chronos::common::make_unexpected(directory.error());
  auto entries = directory->list_entries();
  if (!entries.has_value())
    return chronos::common::make_unexpected(entries.error());
  return entries->empty();
}

[[nodiscard]] chronos::common::Result<std::vector<chronos::live::MultiTabletSubscriptionMember>>
current_subscription_members(const chronos::manifest::DatabaseStorageSnapshot& snapshot,
                             const chronos::schema::TableId& table_id) {
  try {
    std::vector<chronos::live::MultiTabletSubscriptionMember> members;
    for (const chronos::manifest::PublishedTabletStorage& tablet : snapshot.tablets()) {
      if (tablet.table_id() != table_id)
        continue;
      std::uint64_t sequence = 0U;
      if (tablet.applied_position().has_value()) {
        const chronos::head::HeadCommitPosition& position = *tablet.applied_position();
        if (position.source != chronos::head::CommitSource::kWal ||
            position.wal_id != snapshot.wal_id())
          return chronos::common::make_unexpected(
              invalid("single-node subscription tablet is not WAL-backed"));
        sequence = position.record_sequence;
      } else {
        const auto durable = std::ranges::find_if(
            snapshot.durable_tablets(), [&](const chronos::manifest::TabletDescriptor& candidate) {
              return candidate.tablet_id == tablet.tablet_id();
            });
        if (durable == snapshot.durable_tablets().end() || durable->table_id != table_id)
          return chronos::common::make_unexpected(
              invalid("single-node subscription tablet has no durable boundary"));
        sequence = durable->durable_record_sequence;
      }
      members.push_back({tablet.tablet_id(), snapshot.wal_id(), sequence});
    }
    if (members.empty())
      return chronos::common::make_unexpected(
          invalid("subscription plan table has no local tablets"));
    return members;
  } catch (const std::bad_alloc&) {
    return chronos::common::make_unexpected(
        chronos::common::Status{chronos::common::StatusCode::kResourceExhausted,
                                "subscription source discovery allocation failed"});
  }
}

struct DaemonSubscription {
  chronos::live::PreparedSubscriptionPlan plan;
  chronos::live::DurableMultiTabletSubscription coordinator;
  chronos::query::QueryResourceContext resources;
  SpscNetworkTaskQueue requests;
  SpscNetworkTaskQueue responses;
  std::optional<SingleNodeSubscriptionRuntime> runtime;

  DaemonSubscription(chronos::live::PreparedSubscriptionPlan configured_plan,
                     chronos::live::DurableMultiTabletSubscription configured_coordinator,
                     chronos::query::QueryResourceContext configured_resources,
                     SpscNetworkTaskQueue configured_requests,
                     SpscNetworkTaskQueue configured_responses) noexcept
      : plan(std::move(configured_plan)), coordinator(std::move(configured_coordinator)),
        resources(std::move(configured_resources)), requests(std::move(configured_requests)),
        responses(std::move(configured_responses)) {}
};

[[nodiscard]] chronos::common::Result<std::unique_ptr<DaemonSubscription>>
configure_subscription(SingleNodeDatabase& database, SingleNodeCommittedAppendRouter& router,
                       const Options& options, const chronos::live::ResumeTokenMacKey& key) {
  const std::filesystem::path root{options.data_directory};
  auto subscriptions_created = ensure_directory(root, "subscriptions");
  if (!subscriptions_created.has_value())
    return chronos::common::make_unexpected(subscriptions_created.error());
  const std::filesystem::path subscriptions = root / "subscriptions";
  auto plans_created = ensure_directory(subscriptions, "plans");
  if (!plans_created.has_value())
    return chronos::common::make_unexpected(plans_created.error());
  auto checkpoints_created = ensure_directory(subscriptions, "checkpoints");
  if (!checkpoints_created.has_value())
    return chronos::common::make_unexpected(checkpoints_created.error());
  const std::filesystem::path plans_path = subscriptions / "plans";
  auto plans_empty = directory_empty(plans_path);
  if (!plans_empty.has_value())
    return chronos::common::make_unexpected(plans_empty.error());
  chronos::live::SubscriptionPlanStorageConfig plan_storage_config{
      .directory_path = plans_path.string(), .database_id = database.bootstrap().database_id};
  auto plan_storage =
      (*plans_created || *plans_empty)
          ? chronos::live::SubscriptionPlanStorage::create(plan_storage_config)
          : chronos::live::SubscriptionPlanStorage::open_existing(plan_storage_config);
  if (!plan_storage.has_value())
    return chronos::common::make_unexpected(plan_storage.error());
  auto installed = plan_storage->install(options.subscription_sql, database.query_catalog());
  if (!installed.has_value())
    return chronos::common::make_unexpected(installed.error());
  auto plan = plan_storage->load(installed->plan_fingerprint, database.query_catalog());
  if (!plan.has_value())
    return chronos::common::make_unexpected(plan.error());

  const std::string state_name =
      chronos::live::subscription_plan_file_name(plan->fingerprint()) + ".state";
  const std::filesystem::path checkpoints_path = subscriptions / "checkpoints";
  auto state_created = ensure_directory(checkpoints_path, state_name);
  if (!state_created.has_value())
    return chronos::common::make_unexpected(state_created.error());
  const std::filesystem::path state_path = checkpoints_path / state_name;
  auto state_empty = directory_empty(state_path);
  if (!state_empty.has_value())
    return chronos::common::make_unexpected(state_empty.error());
  auto snapshot = database.storage_snapshot();
  if (!snapshot.has_value())
    return chronos::common::make_unexpected(snapshot.error());
  auto members = current_subscription_members(*snapshot, plan->schema_ptr()->table_id());
  if (!members.has_value())
    return chronos::common::make_unexpected(members.error());
  std::vector<chronos::live::MultiTabletSubscriptionCheckpointSourceIdentity> identities;
  identities.reserve(members->size());
  for (const auto& member : *members)
    identities.push_back({member.tablet_id, member.wal_id});
  chronos::live::DurableMultiTabletSubscriptionConfig coordinator_config{
      .storage = {.directory_path = state_path.string(),
                  .identity = {database.bootstrap().database_id, plan->schema_ptr()->table_id(),
                               plan->fingerprint(), plan->schema_ptr()->schema_id(),
                               plan->schema_ptr()->version(), identities}},
      .source = {.database_id = database.bootstrap().database_id,
                 .table_id = plan->schema_ptr()->table_id(),
                 .plan_fingerprint = plan->fingerprint(),
                 .schema_id = plan->schema_ptr()->schema_id(),
                 .schema_version = plan->schema_ptr()->version(),
                 .members = *members,
                 .token_key = key}};
  auto coordinator =
      (*state_created || *state_empty)
          ? chronos::live::DurableMultiTabletSubscription::create_new(std::move(coordinator_config))
          : chronos::live::DurableMultiTabletSubscription::open_existing(
                std::move(coordinator_config));
  if (!coordinator.has_value())
    return chronos::common::make_unexpected(coordinator.error());

  std::vector<chronos::live::SourcePosition> current;
  current.reserve(coordinator->source().members.size());
  for (const auto& source : coordinator->source().members) {
    const auto found = std::ranges::find_if(*members, [&](const auto& member) {
      return member.tablet_id == source.tablet_id && member.wal_id == source.wal_id;
    });
    if (found == members->end())
      return chronos::common::make_unexpected(
          invalid("subscription checkpoint source is no longer local"));
    current.push_back({found->tablet_id, found->wal_id, found->committed_record_sequence});
  }
  auto restored = coordinator->latest_positions();
  if (!restored.has_value())
    return chronos::common::make_unexpected(restored.error());
  if (*restored != current) {
    chronos::common::Status rebased = coordinator->mark_replay_unavailable_through(current);
    if (!rebased.is_ok())
      return chronos::common::make_unexpected(std::move(rebased));
  }
  if (coordinator->has_uncheckpointed_changes()) {
    auto checkpoint = coordinator->checkpoint();
    if (!checkpoint.has_value())
      return chronos::common::make_unexpected(checkpoint.error());
  }

  auto context = database.subscription_snapshot_context(plan->schema_ptr()->table_id());
  if (!context.has_value())
    return chronos::common::make_unexpected(context.error());
  auto resources = chronos::query::QueryResourceContext::create(128U * 1024U * 1024U);
  auto requests = SpscNetworkTaskQueue::create(options.queue_capacity);
  auto responses = SpscNetworkTaskQueue::create(options.queue_capacity);
  if (!resources.has_value() || !requests.has_value() || !responses.has_value()) {
    if (!resources.has_value())
      return chronos::common::make_unexpected(resources.error());
    return chronos::common::make_unexpected(!requests.has_value() ? requests.error()
                                                                  : responses.error());
  }
  try {
    auto owner = std::make_unique<DaemonSubscription>(std::move(*plan), std::move(*coordinator),
                                                      std::move(*resources), std::move(*requests),
                                                      std::move(*responses));
    auto runtime = SingleNodeSubscriptionRuntime::create({.observer_router = &router,
                                                          .plan = &owner->plan,
                                                          .coordinator = &owner->coordinator,
                                                          .catalog = database.query_catalog(),
                                                          .resources = &owner->resources,
                                                          .storage = context->storage,
                                                          .publisher = context->publisher,
                                                          .lineage = context->lineage,
                                                          .requests = &owner->requests,
                                                          .responses = &owner->responses});
    if (!runtime.has_value())
      return chronos::common::make_unexpected(runtime.error());
    owner->runtime.emplace(std::move(*runtime));
    return owner;
  } catch (const std::bad_alloc&) {
    return chronos::common::make_unexpected(
        chronos::common::Status{chronos::common::StatusCode::kResourceExhausted,
                                "subscription runtime ownership allocation failed"});
  }
}

class DataPlaneWorker {
public:
  DataPlaneWorker(SpscNetworkTaskQueue& requests, SpscNetworkTaskQueue& responses, Reactor& reactor,
                  NativeProtocolService* service, SingleNodeSubscriptionRuntime* subscriptions,
                  SpscNetworkTaskQueue* subscription_requests,
                  SpscNetworkTaskQueue* subscription_responses) noexcept
      : requests_(&requests), responses_(&responses), reactor_(&reactor), service_(service),
        subscriptions_(subscriptions), subscription_requests_(subscription_requests),
        subscription_responses_(subscription_responses) {}

  DataPlaneWorker(const DataPlaneWorker&) = delete;
  DataPlaneWorker& operator=(const DataPlaneWorker&) = delete;

  [[nodiscard]] bool start() noexcept {
    try {
      thread_ = std::thread{[this] {
        run();
        stopped_.store(true, std::memory_order_release);
      }};
      return true;
    } catch (const std::bad_alloc&) {
      return false;
    } catch (const std::system_error&) {
      return false;
    }
  }

  void request_stop() noexcept {
    stopping_.store(true, std::memory_order_release);
  }

  void join() noexcept {
    if (thread_.joinable())
      thread_.join();
  }

  [[nodiscard]] bool stopped() const noexcept {
    return stopped_.load(std::memory_order_acquire);
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
    bool shutdown_started = false;
    for (;;) {
      if (stopping_.load(std::memory_order_acquire) && !shutdown_started) {
        shutdown_started = true;
        if (subscriptions_ != nullptr)
          subscriptions_->begin_shutdown();
      }
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

      if (subscription_responses_ != nullptr) {
        auto response = subscription_responses_->try_pop();
        if (response.has_value()) {
          pending.emplace(std::move(*response));
          continue;
        }
      }

      if (subscriptions_ != nullptr) {
        const auto status = subscriptions_->poll_once();
        if (!status.is_ok()) {
          failed_.store(true, std::memory_order_release);
          return;
        }
      }

      if (shutdown_started && (subscriptions_ == nullptr || subscriptions_->drained()) &&
          (subscription_responses_ == nullptr || subscription_responses_->empty()))
        return;

      if (shutdown_started) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
        continue;
      }

      auto request = requests_->try_pop();
      if (!request.has_value()) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
        continue;
      }
      if (service_ == nullptr) {
        pending = unconfigured(std::move(*request));
      } else if (request->frame.header.message_type == MessageType::kSubscribeRequest ||
                 request->frame.header.message_type == MessageType::kSubscriptionAcknowledge ||
                 (request->frame.header.message_type == MessageType::kCancel &&
                  subscriptions_ != nullptr &&
                  subscriptions_->owns(request->connection_id, request->frame.header.request_id))) {
        if (subscriptions_ == nullptr) {
          pending = unsupported(std::move(*request));
        } else if (subscription_requests_ == nullptr ||
                   !subscription_requests_->try_push(std::move(*request))) {
          failed_.store(true, std::memory_order_release);
          return;
        }
        continue;
      } else if (request->frame.header.message_type == MessageType::kCancel) {
        continue;
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
  SingleNodeSubscriptionRuntime* subscriptions_;
  SpscNetworkTaskQueue* subscription_requests_;
  SpscNetworkTaskQueue* subscription_responses_;
  std::atomic<bool> stopping_{false};
  std::atomic<bool> stopped_{false};
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
  SingleNodeCommittedAppendRouter append_router;
  std::optional<chronos::live::ResumeTokenMacKey> subscription_key;
  if (!options->subscription_sql.empty()) {
    auto loaded = load_subscription_key(options->subscription_key_file);
    if (!loaded.has_value()) {
      std::cerr << "chronosd: subscription key setup failed: " << loaded.error().to_string()
                << '\n';
      return 1;
    }
    subscription_key.emplace(*loaded);
  }
  std::optional<SingleNodeDatabase> database;
  std::optional<NativeProtocolService> service;
  std::unique_ptr<DaemonSubscription> subscription;
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
        .raft_recovery = {.repair_incomplete_final_tail = false},
        .committed_append_observer = subscription_key.has_value() ? &append_router : nullptr};
    auto opened = SingleNodeDatabase::open_or_create(std::move(database_config));
    if (!opened.has_value()) {
      std::cerr << "chronosd: database start failed: " << opened.error().to_string() << '\n';
      return 1;
    }
    database.emplace(std::move(*opened));
    service.emplace(*database, identities);
    if (subscription_key.has_value()) {
      auto configured =
          configure_subscription(*database, append_router, *options, *subscription_key);
      if (!configured.has_value()) {
        static_cast<void>(database->shutdown());
        std::cerr << "chronosd: subscription runtime start failed: "
                  << configured.error().to_string() << '\n';
        return 1;
      }
      subscription = std::move(*configured);
    }
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

  DataPlaneWorker worker{*requests,
                         *responses,
                         *reactor,
                         service.has_value() ? std::addressof(*service) : nullptr,
                         subscription != nullptr ? std::addressof(*subscription->runtime) : nullptr,
                         subscription != nullptr ? std::addressof(subscription->requests) : nullptr,
                         subscription != nullptr ? std::addressof(subscription->responses)
                                                 : nullptr};
  if (!worker.start()) {
    static_cast<void>(reactor->shutdown());
    subscription.reset();
    if (database.has_value())
      static_cast<void>(database->shutdown());
    std::cerr << "chronosd: worker start failed\n";
    return 1;
  }

  stop_requested = 0;
  if (std::signal(SIGINT, request_stop) == SIG_ERR ||
      std::signal(SIGTERM, request_stop) == SIG_ERR) {
    worker.request_stop();
    while (!worker.stopped())
      static_cast<void>(reactor->poll_once(std::chrono::milliseconds{10}));
    worker.join();
    static_cast<void>(reactor->shutdown());
    subscription.reset();
    if (database.has_value())
      static_cast<void>(database->shutdown());
    std::cerr << "chronosd: signal handler installation failed\n";
    return 1;
  }

  std::cout << "chronosd listening on 127.0.0.1:" << reactor->bound_port()
            << " backend=" << chronos::network::reactor_backend_name(options->backend)
            << " data_plane=" << (service.has_value() ? "configured" : "unconfigured")
            << " subscriptions=" << (subscription != nullptr ? "configured" : "disabled") << '\n'
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

  worker.request_stop();
  bool reactor_drain_failed = false;
  while (!worker.stopped()) {
    if (!reactor_drain_failed) {
      const auto status = reactor->poll_once(std::chrono::milliseconds{10});
      if (!status.is_ok()) {
        std::cerr << "chronosd: shutdown reactor drain failed: " << status.to_string() << '\n';
        exit_code = 1;
        reactor_drain_failed = true;
      }
    } else {
      static_cast<void>(responses->try_pop());
      std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
  }
  worker.join();
  const auto shutdown = reactor->shutdown();
  if (!shutdown.is_ok()) {
    std::cerr << "chronosd: shutdown failed: " << shutdown.to_string() << '\n';
    exit_code = 1;
  }
  subscription.reset();
  if (database.has_value()) {
    const auto database_shutdown = database->shutdown();
    if (!database_shutdown.is_ok()) {
      std::cerr << "chronosd: database shutdown failed: " << database_shutdown.to_string() << '\n';
      exit_code = 1;
    }
  }
  return exit_code;
}
