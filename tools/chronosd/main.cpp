#include "chronos/common/uuid_generator.hpp"
#include "chronos/common/version.hpp"
#include "chronos/io/posix_io.hpp"
#include "chronos/live/subscription_plan_storage.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/reactor.hpp"
#include "chronos/network/spsc_queue.hpp"
#include "chronos/service/native_protocol_service.hpp"
#include "chronos/service/replicated_group_config.hpp"
#include "chronos/service/replicated_ingest_database.hpp"
#include "chronos/service/replicated_ingest_service.hpp"
#include "chronos/service/replicated_peer_config.hpp"
#include "chronos/service/replicated_raft_transport_runtime.hpp"
#include "chronos/service/replicated_read_barrier.hpp"
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
#include <cstdio>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
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
using chronos::service::ReplicatedIngestDatabase;
using chronos::service::ReplicatedIngestService;
using chronos::service::ReplicatedRaftTransportRuntime;
using chronos::service::ReplicatedReadBarrier;
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
  std::string replicated_groups_file;
  std::string replicated_peers_file;
  std::string raft_tls_certificate_file;
  std::string raft_tls_private_key_file;
  std::string raft_tls_trust_store_file;
  bool help{};
  bool version{};
};

void print_usage(const std::string_view program, std::ostream& stream) {
  stream << "Usage: " << program
         << " [--listen 127.0.0.1] [--port PORT] [--backend epoll|io_uring]"
            " [--queue-capacity COUNT] [--data-dir PATH]"
            " [--replicated-groups FILE]"
            " [--replicated-peers FILE --raft-tls-cert FILE --raft-tls-key FILE"
            " --raft-tls-ca FILE]"
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
    } else if (argument == "--replicated-groups") {
      if (value.empty()) {
        std::cerr << "chronosd: replicated group configuration path must be nonempty\n";
        return std::nullopt;
      }
      options.replicated_groups_file = value;
    } else if (argument == "--replicated-peers") {
      if (value.empty()) {
        std::cerr << "chronosd: replicated peer configuration path must be nonempty\n";
        return std::nullopt;
      }
      options.replicated_peers_file = value;
    } else if (argument == "--raft-tls-cert") {
      options.raft_tls_certificate_file = value;
    } else if (argument == "--raft-tls-key") {
      options.raft_tls_private_key_file = value;
    } else if (argument == "--raft-tls-ca") {
      options.raft_tls_trust_store_file = value;
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
  if (!options.replicated_groups_file.empty() && options.data_directory.empty()) {
    std::cerr << "chronosd: replicated group configuration requires a data directory\n";
    return std::nullopt;
  }
  if (!options.replicated_groups_file.empty() && !options.subscription_sql.empty()) {
    std::cerr
        << "chronosd: replicated ingest and single-node subscriptions are mutually exclusive\n";
    return std::nullopt;
  }
  const std::array<bool, 4U> transport_fields{
      !options.replicated_peers_file.empty(), !options.raft_tls_certificate_file.empty(),
      !options.raft_tls_private_key_file.empty(), !options.raft_tls_trust_store_file.empty()};
  const std::size_t transport_field_count =
      static_cast<std::size_t>(std::ranges::count(transport_fields, true));
  if (transport_field_count != 0U && transport_field_count != transport_fields.size()) {
    std::cerr << "chronosd: replicated peers and all Raft TLS files must be configured together\n";
    return std::nullopt;
  }
  if (transport_field_count != 0U && options.replicated_groups_file.empty()) {
    std::cerr << "chronosd: Raft peer transport requires replicated group configuration\n";
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
      .variable_column_bytes = std::uint64_t{1U} * 1024U * 1024U,
      .maximum_retry_entries = 65'536U,
      .wal_segment_target_bytes = chronos::wal::kSegmentSizeLimit,
      .raft_segment_target_bytes = std::uint64_t{64U} * 1024U * 1024U};
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

[[nodiscard]] chronos::common::Result<std::string>
load_bounded_config(const std::string& path, const std::size_t maximum_bytes,
                    const std::string_view kind) {
  const int descriptor = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (descriptor < 0)
    return chronos::common::make_unexpected(io_error("cannot open " + std::string{kind}));
  const auto close_descriptor = [&]() noexcept { static_cast<void>(::close(descriptor)); };
  struct stat metadata {};
  if (::fstat(descriptor, &metadata) != 0 || !S_ISREG(metadata.st_mode) || metadata.st_size <= 0 ||
      static_cast<std::uintmax_t>(metadata.st_size) > maximum_bytes) {
    close_descriptor();
    return chronos::common::make_unexpected(
        invalid(std::string{kind} + " must be a bounded nonempty regular file"));
  }
  try {
    std::string bytes(static_cast<std::size_t>(metadata.st_size), '\0');
    std::size_t offset = 0U;
    while (offset < bytes.size()) {
      const ssize_t count = ::read(descriptor, bytes.data() + offset, bytes.size() - offset);
      if (count < 0 && errno == EINTR)
        continue;
      if (count <= 0) {
        close_descriptor();
        return chronos::common::make_unexpected(
            io_error("cannot read complete " + std::string{kind}));
      }
      offset += static_cast<std::size_t>(count);
    }
    char extra{};
    ssize_t trailing{};
    do {
      trailing = ::read(descriptor, &extra, 1U);
    } while (trailing < 0 && errno == EINTR);
    close_descriptor();
    if (trailing < 0)
      return chronos::common::make_unexpected(io_error("cannot finish " + std::string{kind}));
    if (trailing != 0)
      return chronos::common::make_unexpected(
          invalid(std::string{kind} + " changed while being read"));
    return bytes;
  } catch (const std::bad_alloc&) {
    close_descriptor();
    return chronos::common::make_unexpected(chronos::common::Status{
        chronos::common::StatusCode::kResourceExhausted, std::string{kind} + " allocation failed"});
  } catch (const std::length_error&) {
    close_descriptor();
    return chronos::common::make_unexpected(
        invalid(std::string{kind} + " size exceeds process limits"));
  }
}

[[nodiscard]] chronos::common::Status validate_tls_file(const std::string& path,
                                                        const bool private_key) {
  const int descriptor = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (descriptor < 0)
    return io_error("cannot open Raft TLS file");
  struct stat metadata {};
  constexpr std::uintmax_t maximum_bytes = std::uintmax_t{16U} * 1024U * 1024U;
  const bool valid = ::fstat(descriptor, &metadata) == 0 && S_ISREG(metadata.st_mode) &&
                     metadata.st_size > 0 &&
                     static_cast<std::uintmax_t>(metadata.st_size) <= maximum_bytes &&
                     (!private_key || (metadata.st_mode & 0077) == 0);
  static_cast<void>(::close(descriptor));
  return valid ? chronos::common::Status::ok()
               : invalid(
                     private_key
                         ? "Raft TLS key must be a bounded regular file inaccessible to group/other"
                         : "Raft TLS certificate authority must be a bounded regular file");
}

[[nodiscard]] chronos::common::Status
validate_transport_membership(const chronos::raft::NodeId local_node_id,
                              const std::vector<chronos::raft::RaftGroupConfiguration>& groups,
                              const std::vector<chronos::service::ReplicatedPeer>& peers) {
  for (const auto& group : groups) {
    if (!std::ranges::contains(group.voters, local_node_id))
      return invalid("resident Raft group does not include the local node as a voter");
    for (const chronos::raft::NodeId voter : group.voters) {
      if (std::ranges::none_of(peers, [voter](const auto& peer) { return peer.node_id == voter; }))
        return invalid("Raft group voter has no authenticated peer route");
    }
  }
  return chronos::common::Status::ok();
}

[[nodiscard]] chronos::common::Status
elect_single_node_groups(ReplicatedIngestDatabase& database,
                         const std::vector<chronos::raft::RaftGroupConfiguration>& groups) {
  for (const chronos::raft::RaftGroupConfiguration& group : groups) {
    if (group.voters.size() != 1U || group.voters.front() != database.bootstrap().local_node_id)
      continue;
    auto election = database.ingest_runtime()->runtime()->try_submit(
        {{group.group_id, chronos::raft::StartElectionOperation{}},
         {group.group_id, chronos::raft::CommitCurrentTermOperation{}}});
    if (!election.has_value())
      return election.error();
    auto completed = election->wait();
    if (!completed.has_value())
      return completed.error();
    if (completed->size() != 2U)
      return invalid("single-node Raft election returned an invalid result count");
    if (!completed->front().status.is_ok())
      return completed->front().status;
    if (!completed->back().status.is_ok())
      return completed->back().status;
    auto observation = database.ingest_runtime()->runtime()->try_observe_group(group.group_id);
    if (!observation.has_value())
      return observation.error();
    auto observed = observation->wait();
    if (!observed.has_value())
      return observed.error();
    if (observed->size() != 1U)
      return invalid("single-node Raft observation returned an invalid result count");
    const auto& group_observation = observed->front();
    const auto& state = group_observation.observation;
    if (!group_observation.status.is_ok() || !state.has_value()) {
      return {chronos::common::StatusCode::kUnavailable,
              "single-node Raft group did not become local leader"};
    }
    const auto& value = *state;
    if (value.role != chronos::raft::Role::kLeader ||
        value.leader_id != database.bootstrap().local_node_id) {
      return {chronos::common::StatusCode::kUnavailable,
              "single-node Raft group did not become local leader"};
    }
  }
  return chronos::common::Status::ok();
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
      const auto& applied_position = tablet.applied_position();
      if (applied_position.has_value()) {
        const chronos::head::HeadCommitPosition& position = *applied_position;
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

// Field order keeps the runtime ahead of every owner it borrows during reverse destruction.
// NOLINTNEXTLINE(clang-analyzer-optin.performance.Padding)
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
  auto resources = chronos::query::QueryResourceContext::create(std::size_t{128U} * 1024U * 1024U);
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

struct DataPlaneWorkerConfig {
  SpscNetworkTaskQueue* requests{};
  SpscNetworkTaskQueue* responses{};
  Reactor* reactor{};
  NativeProtocolService* service{};
  ReplicatedIngestService* replicated{};
  SingleNodeSubscriptionRuntime* subscriptions{};
  SpscNetworkTaskQueue* subscription_requests{};
  SpscNetworkTaskQueue* subscription_responses{};
};

class DataPlaneWorker {
public:
  explicit DataPlaneWorker(const DataPlaneWorkerConfig& config) noexcept
      : requests_(config.requests), responses_(config.responses), reactor_(config.reactor),
        service_(config.service), replicated_(config.replicated),
        subscriptions_(config.subscriptions), subscription_requests_(config.subscription_requests),
        subscription_responses_(config.subscription_responses) {}

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
  [[nodiscard]] static std::optional<NetworkTask> unconfigured(NetworkTask request) {
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

  [[nodiscard]] static std::optional<NetworkTask> unsupported(NetworkTask request) {
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
        if (replicated_ != nullptr)
          replicated_->begin_shutdown();
        if (subscriptions_ != nullptr)
          subscriptions_->begin_shutdown();
      }
      if (replicated_ != nullptr) {
        auto polled = replicated_->poll_once();
        if (!polled.has_value()) {
          failed_.store(true, std::memory_order_release);
          return;
        }
        if (polled->response_enqueued && !reactor_->notify_response_ready().is_ok()) {
          failed_.store(true, std::memory_order_release);
          return;
        }
        if (shutdown_started && replicated_->drained() && responses_->empty())
          return;
        if (!polled->response_enqueued)
          std::this_thread::sleep_for(std::chrono::milliseconds{1});
        continue;
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
  ReplicatedIngestService* replicated_;
  SingleNodeSubscriptionRuntime* subscriptions_;
  SpscNetworkTaskQueue* subscription_requests_;
  SpscNetworkTaskQueue* subscription_responses_;
  std::atomic<bool> stopping_{false};
  std::atomic<bool> stopped_{false};
  std::atomic<bool> failed_{false};
  std::thread thread_;
};

class RaftTransportWorker {
public:
  RaftTransportWorker(ReplicatedRaftTransportRuntime& runtime,
                      ReplicatedReadBarrier& read_barrier) noexcept
      : runtime_(&runtime), read_barrier_(&read_barrier) {}
  RaftTransportWorker(const RaftTransportWorker&) = delete;
  RaftTransportWorker& operator=(const RaftTransportWorker&) = delete;

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

  void request_stop() noexcept {
    stopping_.store(true, std::memory_order_release);
  }
  void join() noexcept {
    if (thread_.joinable())
      thread_.join();
  }
  [[nodiscard]] bool failed() const noexcept {
    return failed_.load(std::memory_order_acquire);
  }

private:
  void run() noexcept {
    while (!stopping_.load(std::memory_order_acquire)) {
      const chronos::common::Status driven = read_barrier_->poll_owner_drive(*runtime_);
      if (!driven.is_ok()) {
        failed_.store(true, std::memory_order_release);
        return;
      }
      const chronos::common::Status polled = runtime_->poll_once(std::chrono::milliseconds{10});
      if (!polled.is_ok()) {
        failed_.store(true, std::memory_order_release);
        return;
      }
      for (std::size_t drained = 0U; drained < 256U; ++drained) {
        auto completed = runtime_->take_completed();
        if (!completed.has_value()) {
          if (completed.error().code() != chronos::common::StatusCode::kUnavailable) {
            failed_.store(true, std::memory_order_release);
            return;
          }
          break;
        }
        const chronos::common::Status observed = read_barrier_->poll_owner_observe(*completed);
        if (!observed.is_ok()) {
          failed_.store(true, std::memory_order_release);
          return;
        }
        const auto& transition = completed->result.transition;
        if (!completed->result.status.is_ok() || !transition.has_value())
          continue;
        if (transition->snapshot_install.has_value()) {
          failed_.store(true, std::memory_order_release);
          return;
        }
      }
    }
  }

  ReplicatedRaftTransportRuntime* runtime_{};
  ReplicatedReadBarrier* read_barrier_{};
  std::atomic<bool> stopping_{false};
  std::atomic<bool> failed_{false};
  std::thread thread_;
};

// NOLINTNEXTLINE(modernize-avoid-c-arrays)
int run_daemon(const int argc, const char* const argv[]) {
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
  std::optional<std::vector<chronos::raft::RaftGroupConfiguration>> replicated_groups;
  if (!options->replicated_groups_file.empty()) {
    auto loaded = load_bounded_config(options->replicated_groups_file,
                                      chronos::service::ReplicatedGroupConfigLimits{}.maximum_bytes,
                                      "replicated group config");
    if (!loaded.has_value()) {
      std::cerr << "chronosd: replicated group config read failed: " << loaded.error().to_string()
                << '\n';
      return 1;
    }
    auto parsed = chronos::service::parse_replicated_group_config(*loaded);
    if (!parsed.has_value()) {
      std::cerr << "chronosd: replicated group config is invalid: " << parsed.error().to_string()
                << '\n';
      return 1;
    }
    replicated_groups.emplace(std::move(*parsed));
  }
  std::optional<std::vector<chronos::service::ReplicatedPeer>> replicated_peers;
  if (!options->replicated_peers_file.empty()) {
    auto loaded = load_bounded_config(options->replicated_peers_file,
                                      chronos::service::ReplicatedPeerConfigLimits{}.maximum_bytes,
                                      "replicated peer config");
    if (!loaded.has_value()) {
      std::cerr << "chronosd: replicated peer config read failed: " << loaded.error().to_string()
                << '\n';
      return 1;
    }
    auto parsed = chronos::service::parse_replicated_peer_config(*loaded);
    if (!parsed.has_value()) {
      std::cerr << "chronosd: replicated peer config is invalid: " << parsed.error().to_string()
                << '\n';
      return 1;
    }
    for (const auto& [path, private_key] : std::array<std::pair<std::string_view, bool>, 3U>{
             std::pair<std::string_view, bool>{options->raft_tls_certificate_file, false},
             {options->raft_tls_private_key_file, true},
             {options->raft_tls_trust_store_file, false}}) {
      const chronos::common::Status valid = validate_tls_file(std::string{path}, private_key);
      if (!valid.is_ok()) {
        std::cerr << "chronosd: Raft TLS file validation failed: " << valid.to_string() << '\n';
        return 1;
      }
    }
    replicated_peers.emplace(std::move(*parsed));
  }
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
  std::optional<ReplicatedIngestDatabase> replicated_database;
  std::optional<ReplicatedRaftTransportRuntime> raft_transport;
  std::optional<ReplicatedReadBarrier> replicated_read_barrier;
  std::optional<NativeProtocolService> service;
  std::unique_ptr<DaemonSubscription> subscription;
  if (!options->data_directory.empty()) {
    if (replicated_groups.has_value()) {
      auto opened = ReplicatedIngestDatabase::open_existing(
          {.bootstrap = {.database_root = options->data_directory}, .groups = *replicated_groups});
      if (!opened.has_value()) {
        std::cerr << "chronosd: replicated database start failed: " << opened.error().to_string()
                  << '\n';
        return 1;
      }
      replicated_database.emplace(std::move(*opened));
      if (replicated_peers.has_value()) {
        const chronos::common::Status membership = validate_transport_membership(
            replicated_database->bootstrap().local_node_id, *replicated_groups, *replicated_peers);
        if (!membership.is_ok()) {
          static_cast<void>(replicated_database->shutdown());
          std::cerr << "chronosd: replicated peer membership is invalid: " << membership.to_string()
                    << '\n';
          return 1;
        }
        std::vector<chronos::raft::GroupId> resident_groups;
        resident_groups.reserve(replicated_groups->size());
        for (const auto& group : *replicated_groups)
          resident_groups.push_back(group.group_id);
        auto transport = ReplicatedRaftTransportRuntime::create(
            {.local_node_id = replicated_database->bootstrap().local_node_id,
             .durable_runtime = replicated_database->ingest_runtime()->runtime(),
             .peers = *replicated_peers,
             .resident_groups = std::move(resident_groups),
             .tls = {.certificate_chain_file = options->raft_tls_certificate_file,
                     .private_key_file = options->raft_tls_private_key_file,
                     .trust_store_file = options->raft_tls_trust_store_file}});
        if (!transport.has_value()) {
          static_cast<void>(replicated_database->shutdown());
          std::cerr << "chronosd: Raft peer transport start failed: "
                    << transport.error().to_string() << '\n';
          return 1;
        }
        raft_transport.emplace(std::move(*transport));
        auto read_barrier = ReplicatedReadBarrier::create_transported(
            {replicated_database->query_barrier_groups().begin(),
             replicated_database->query_barrier_groups().end()});
        if (!read_barrier.has_value()) {
          static_cast<void>(raft_transport->shutdown());
          static_cast<void>(replicated_database->shutdown());
          std::cerr << "chronosd: replicated read barrier start failed: "
                    << read_barrier.error().to_string() << '\n';
          return 1;
        }
        replicated_read_barrier.emplace(std::move(*read_barrier));
      } else {
        const bool all_local = std::ranges::all_of(*replicated_groups, [&](const auto& group) {
          return group.voters.size() == 1U &&
                 group.voters.front() == replicated_database->bootstrap().local_node_id;
        });
        if (!all_local) {
          static_cast<void>(replicated_database->shutdown());
          std::cerr << "chronosd: multi-voter groups require configured Raft peer transport\n";
          return 1;
        }
        const chronos::common::Status elected =
            elect_single_node_groups(*replicated_database, *replicated_groups);
        if (!elected.is_ok()) {
          static_cast<void>(replicated_database->shutdown());
          std::cerr << "chronosd: replicated single-node election failed: " << elected.to_string()
                    << '\n';
          return 1;
        }
        auto read_barrier = ReplicatedReadBarrier::create_local(
            replicated_database->ingest_runtime()->runtime(),
            {replicated_database->query_barrier_groups().begin(),
             replicated_database->query_barrier_groups().end()});
        if (!read_barrier.has_value()) {
          static_cast<void>(replicated_database->shutdown());
          std::cerr << "chronosd: replicated read barrier start failed: "
                    << read_barrier.error().to_string() << '\n';
          return 1;
        }
        replicated_read_barrier.emplace(std::move(*read_barrier));
      }
      service.emplace(*replicated_database, *replicated_read_barrier);
    } else {
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
      auto opened = SingleNodeDatabase::open_or_create(database_config);
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
  }

  auto requests = SpscNetworkTaskQueue::create(options->queue_capacity);
  auto responses = SpscNetworkTaskQueue::create(options->queue_capacity);
  if (!requests.has_value() || !responses.has_value()) {
    const auto& error = !requests.has_value() ? requests.error() : responses.error();
    std::cerr << "chronosd: queue creation failed: " << error.to_string() << '\n';
    if (raft_transport.has_value())
      static_cast<void>(raft_transport->shutdown());
    if (replicated_database.has_value())
      static_cast<void>(replicated_database->shutdown());
    return 1;
  }

  std::optional<ReplicatedIngestService> replicated_service;
  if (replicated_database.has_value()) {
    auto created = ReplicatedIngestService::create(
        {.coordinator = replicated_database->ingest_runtime()->coordinator(),
         .queries = service.has_value() ? std::addressof(*service) : nullptr,
         .requests = std::addressof(*requests),
         .responses = std::addressof(*responses)});
    if (!created.has_value()) {
      if (raft_transport.has_value())
        static_cast<void>(raft_transport->shutdown());
      static_cast<void>(replicated_database->shutdown());
      std::cerr << "chronosd: replicated ingest service start failed: "
                << created.error().to_string() << '\n';
      return 1;
    }
    replicated_service.emplace(std::move(*created));
  }

  chronos::network::EpollServerConfig config;
  config.port = options->port;
  if (replicated_service.has_value()) {
    config.state.maximum_protocol_major = chronos::network::kProtocolV2Major;
    config.state.supported_feature_bits = chronos::network::kProtocolV2QuorumSyncFeature |
                                          chronos::network::kProtocolV2LeaderRedirectFeature;
  }
  auto reactor =
      Reactor::start(options->backend, config, {.requests = &*requests, .responses = &*responses});
  if (!reactor.has_value()) {
    std::cerr << "chronosd: server start failed: " << reactor.error().to_string() << '\n';
    replicated_service.reset();
    if (raft_transport.has_value())
      static_cast<void>(raft_transport->shutdown());
    if (replicated_database.has_value())
      static_cast<void>(replicated_database->shutdown());
    return 1;
  }

  std::optional<RaftTransportWorker> raft_worker;
  if (raft_transport.has_value()) {
    raft_worker.emplace(*raft_transport, *replicated_read_barrier);
    if (!raft_worker->start()) {
      static_cast<void>(reactor->shutdown());
      replicated_service.reset();
      static_cast<void>(raft_transport->shutdown());
      static_cast<void>(replicated_database->shutdown());
      std::cerr << "chronosd: Raft transport worker start failed\n";
      return 1;
    }
  }

  DataPlaneWorker worker{
      {.requests = std::addressof(*requests),
       .responses = std::addressof(*responses),
       .reactor = std::addressof(*reactor),
       .service = service.has_value() ? std::addressof(*service) : nullptr,
       .replicated = replicated_service.has_value() ? std::addressof(*replicated_service) : nullptr,
       .subscriptions = subscription != nullptr ? std::addressof(*subscription->runtime) : nullptr,
       .subscription_requests =
           subscription != nullptr ? std::addressof(subscription->requests) : nullptr,
       .subscription_responses =
           subscription != nullptr ? std::addressof(subscription->responses) : nullptr}};
  if (!worker.start()) {
    if (raft_worker.has_value()) {
      raft_worker->request_stop();
      raft_worker->join();
      static_cast<void>(raft_transport->shutdown());
    }
    static_cast<void>(reactor->shutdown());
    subscription.reset();
    replicated_service.reset();
    if (database.has_value())
      static_cast<void>(database->shutdown());
    if (replicated_database.has_value())
      static_cast<void>(replicated_database->shutdown());
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
    if (replicated_read_barrier.has_value())
      static_cast<void>(replicated_read_barrier->shutdown());
    if (raft_worker.has_value()) {
      raft_worker->request_stop();
      raft_worker->join();
      static_cast<void>(raft_transport->shutdown());
    }
    static_cast<void>(reactor->shutdown());
    subscription.reset();
    replicated_service.reset();
    if (database.has_value())
      static_cast<void>(database->shutdown());
    if (replicated_database.has_value())
      static_cast<void>(replicated_database->shutdown());
    std::cerr << "chronosd: signal handler installation failed\n";
    return 1;
  }

  std::cout << "chronosd listening on 127.0.0.1:" << reactor->bound_port()
            << " backend=" << chronos::network::reactor_backend_name(options->backend)
            << " data_plane="
            << (replicated_service.has_value()
                    ? "replicated"
                    : (service.has_value() ? "configured" : "unconfigured"))
            << " subscriptions=" << (subscription != nullptr ? "configured" : "disabled")
            << " raft_transport="
            << (raft_transport.has_value()
                    ? "configured"
                    : (replicated_service.has_value() ? "local" : "disabled"))
            << '\n'
            << std::flush;

  int exit_code = 0;
  while (stop_requested == 0 && !worker.failed() &&
         (!raft_worker.has_value() || !raft_worker->failed())) {
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
  if (raft_worker.has_value() && raft_worker->failed()) {
    std::cerr << "chronosd: Raft transport worker failed\n";
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
  replicated_service.reset();
  if (replicated_read_barrier.has_value()) {
    const auto barrier_shutdown = replicated_read_barrier->shutdown();
    if (!barrier_shutdown.is_ok()) {
      std::cerr << "chronosd: replicated read barrier shutdown failed: "
                << barrier_shutdown.to_string() << '\n';
      exit_code = 1;
    }
  }
  if (raft_worker.has_value()) {
    raft_worker->request_stop();
    raft_worker->join();
    const auto transport_shutdown = raft_transport->shutdown();
    if (!transport_shutdown.is_ok()) {
      std::cerr << "chronosd: Raft transport shutdown failed: " << transport_shutdown.to_string()
                << '\n';
      exit_code = 1;
    }
  }
  if (database.has_value()) {
    const auto database_shutdown = database->shutdown();
    if (!database_shutdown.is_ok()) {
      std::cerr << "chronosd: database shutdown failed: " << database_shutdown.to_string() << '\n';
      exit_code = 1;
    }
  }
  if (replicated_database.has_value()) {
    const auto database_shutdown = replicated_database->shutdown();
    if (!database_shutdown.is_ok()) {
      std::cerr << "chronosd: replicated database shutdown failed: "
                << database_shutdown.to_string() << '\n';
      exit_code = 1;
    }
  }
  return exit_code;
}

} // namespace

// C++ process entry points supply the conventional C argv array.
// NOLINTNEXTLINE(modernize-avoid-c-arrays)
int main(const int argc, const char* const argv[]) {
  try {
    return run_daemon(argc, argv);
  } catch (const std::exception& error) {
    std::fprintf(stderr, "chronosd: unhandled exception: %s\n", error.what());
  } catch (...) {
    std::fputs("chronosd: unhandled non-standard exception\n", stderr);
  }
  return 1;
}
