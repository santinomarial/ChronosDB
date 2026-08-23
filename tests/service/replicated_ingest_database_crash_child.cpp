#include "chronos/ingest/async_raft_tablet_application.hpp"
#include "chronos/ingest/raft_tablet_state_machine.hpp"
#include "chronos/raft/async_durable_runtime.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/metadata_runtime.hpp"
#include "chronos/service/replicated_ingest_database.hpp"
#include "chronos/service/replicated_ingest_runtime.hpp"
#include "service/replicated_ingest_database_crash_fixture.hpp"

#include <exception>
#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::service::test {
namespace {

struct ChildConfig {
  std::filesystem::path root;
  bool use_snapshots{false};
};

[[nodiscard]] ChildConfig parse_config(const int count, char** const values) {
  ChildConfig config;
  for (int index = 1; index + 1 < count; index += 2) {
    const std::string_view key{values[index]};
    const std::string_view value{values[index + 1]};
    if (key == "--directory")
      config.root = value;
    else if (key == "--operation")
      config.use_snapshots = value == "compaction";
  }
  return config;
}

[[nodiscard]] common::Status submit(ReplicatedIngestRuntime& runtime,
                                    std::vector<raft::DurableRaftRequest> requests) {
  auto queued = runtime.runtime()->try_submit(std::move(requests));
  if (!queued.has_value())
    return queued.error();
  auto completed = queued->wait();
  return completed.has_value() ? common::Status::ok() : completed.error();
}

[[nodiscard]] common::Status prepare_retained_history(const std::filesystem::path& root) {
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(new_crash_bootstrap_config(root));
  if (!bootstrap.has_value())
    return bootstrap.error();
  auto initial = ReplicatedIngestRuntime::create_new(crash_runtime_config(*bootstrap));
  if (!initial.has_value())
    return initial.error();
  common::Status status =
      submit(*initial, {{crash_metadata_group(), raft::StartElectionOperation{}}});
  if (status.is_ok())
    status = submit(*initial, {{crash_tablet_group(), raft::StartElectionOperation{}}});
  if (status.is_ok())
    status = submit(*initial, crash_metadata_requests());
  if (status.is_ok()) {
    status =
        submit(*initial,
               {{crash_tablet_group(),
                 raft::ProposeOperation{ingest::kRaftColumnarAppendEntryType, crash_command()}}});
  }
  if (status.is_ok()) {
    auto publication = initial->tablet_application()->snapshot(crash_tablet_group());
    if (!publication.has_value())
      status = publication.error();
    else if (publication->visible_row_count() != 2U || publication->retry_entry_count() != 1U)
      status = {common::StatusCode::kInternal,
                "crash child initial tablet publication is incomplete"};
  }
  const common::Status initial_shutdown = initial->shutdown();
  if (status.is_ok())
    status = initial_shutdown;
  const common::Status bootstrap_close = bootstrap->close();
  if (status.is_ok())
    status = bootstrap_close;
  return status;
}

[[nodiscard]] common::Status execute(raft::DurableMultiRaftRuntime& runtime,
                                     std::vector<raft::DurableRaftRequest> requests) {
  auto result = runtime.execute_batch(std::move(requests));
  return result.has_value() ? common::Status::ok() : result.error();
}

[[nodiscard]] common::Status prepare_snapshot_history(const std::filesystem::path& root) {
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(new_crash_bootstrap_config(root));
  if (!bootstrap.has_value())
    return bootstrap.error();
  const auto metadata_snapshot = crash_metadata_snapshot_config(root);
  const auto tablet_snapshot = crash_tablet_snapshot_config(root);
  std::error_code directory_error;
  static_cast<void>(
      std::filesystem::create_directories(metadata_snapshot.directory_path, directory_error));
  if (directory_error)
    return {common::StatusCode::kIoError, "create metadata crash snapshot directory failed"};
  static_cast<void>(
      std::filesystem::create_directories(tablet_snapshot.directory_path, directory_error));
  if (directory_error)
    return {common::StatusCode::kIoError, "create tablet crash snapshot directory failed"};

  auto configured = crash_runtime_config(*bootstrap);
  auto durable = raft::DurableMultiRaftRuntime::create_new(configured.local_node_id, configured.log,
                                                           configured.groups);
  if (!durable.has_value())
    return durable.error();
  common::Status status = common::Status::ok();
  {
    auto metadata_storage = raft::MetadataSnapshotStorage::create(metadata_snapshot);
    if (!metadata_storage.has_value())
      return metadata_storage.error();
    auto metadata = raft::DurableMetadataStateMachine::recover(crash_metadata_group(), *durable,
                                                               std::move(*metadata_storage));
    if (!metadata.has_value())
      return metadata.error();
    auto tablet_storage = ingest::RaftTabletSnapshotStorage::create(tablet_snapshot);
    if (!tablet_storage.has_value())
      return tablet_storage.error();
    if (configured.tablets.size() != 1U)
      return {common::StatusCode::kInternal, "crash snapshot fixture has the wrong tablet count"};
    auto& tablet_config = configured.tablets.front();
    auto tablet = ingest::RaftTabletStateMachine::recover(
        crash_tablet_group(), *durable, std::move(*tablet_storage),
        std::move(tablet_config.retry_directory), std::move(tablet_config.tablet),
        std::move(tablet_config.retained_schemas), tablet_config.decode_limits);
    if (!tablet.has_value())
      return tablet.error();

    status = execute(*durable, {{crash_metadata_group(), raft::StartElectionOperation{}}});
    if (status.is_ok())
      status = execute(*durable, {{crash_tablet_group(), raft::StartElectionOperation{}}});
    if (status.is_ok())
      status = execute(*durable, crash_metadata_requests());
    if (status.is_ok()) {
      status = execute(*durable, {{crash_tablet_group(),
                                   raft::ProposeOperation{ingest::kRaftColumnarAppendEntryType,
                                                          crash_command()}}});
    }
    if (!status.is_ok())
      return status;
    auto metadata_applied = metadata->apply_committed();
    if (!metadata_applied.has_value())
      return metadata_applied.error();
    auto tablet_applied = tablet->apply_committed();
    if (!tablet_applied.has_value())
      return tablet_applied.error();
    auto metadata_compacted = metadata->compact_applied_prefix(4U);
    if (!metadata_compacted.has_value())
      return metadata_compacted.error();
    auto tablet_compacted = tablet->compact_applied_prefix(1U, 1U, {});
    if (!tablet_compacted.has_value())
      return tablet_compacted.error();

    const raft::ProposeOperation node{
        raft::kRaftMetadataCommandEntryType,
        raft::encode_metadata_command_v1(raft::ClusterNodeMetadata{1U, "node-1.example:7000"})
            .value()};
    status = execute(*durable, {{crash_metadata_group(), node}});
    if (status.is_ok()) {
      status = execute(*durable, {{crash_tablet_group(),
                                   raft::ProposeOperation{ingest::kRaftColumnarAppendEntryType,
                                                          crash_suffix_command()}}});
    }
    if (!status.is_ok())
      return status;
    const raft::RaftNode* const metadata_node = durable->find_group(crash_metadata_group());
    const raft::RaftNode* const tablet_node = durable->find_group(crash_tablet_group());
    if (metadata_node == nullptr || tablet_node == nullptr || metadata_node->commit_index() != 5U ||
        metadata_node->applied_index() != 4U || tablet_node->commit_index() != 2U ||
        tablet_node->applied_index() != 1U) {
      return {common::StatusCode::kInternal,
              "crash snapshot fixture did not retain the expected committed suffixes"};
    }
  }
  status = durable->close();
  const common::Status bootstrap_close = bootstrap->close();
  return status.is_ok() ? bootstrap_close : status;
}

[[nodiscard]] common::Status prepare_and_reopen(const ChildConfig& config) {
  common::Status prepared = config.use_snapshots ? prepare_snapshot_history(config.root)
                                                 : prepare_retained_history(config.root);
  if (!prepared.is_ok())
    return prepared;

  auto database = ReplicatedIngestDatabase::open_existing(
      crash_database_config(config.root, config.use_snapshots));
  if (!database.has_value())
    return database.error();
  auto catalog = database->ingest_runtime()->metadata_application()->catalog_snapshot();
  if (!catalog.has_value())
    return catalog.error();
  auto recovered = database->ingest_runtime()->tablet_application()->snapshot(crash_tablet_group());
  if (!recovered.has_value())
    return recovered.error();
  const std::size_t expected_rows = config.use_snapshots ? 4U : 2U;
  const std::size_t expected_retries = config.use_snapshots ? 2U : 1U;
  const raft::LogIndex expected_tablet_index = config.use_snapshots ? 2U : 1U;
  const raft::LogIndex expected_metadata_index = config.use_snapshots ? 5U : 4U;
  if ((*catalog)->applied_index != expected_metadata_index ||
      (config.use_snapshots &&
       ((*catalog)->cluster_nodes.size() != 1U ||
        (*catalog)->cluster_nodes.front().endpoint != "node-1.example:7000")) ||
      recovered->visible_row_count() != expected_rows ||
      recovered->retry_entry_count() != expected_retries ||
      recovered->applied_position() !=
          head::HeadCommitPosition::raft(crash_tablet_group(), expected_tablet_index)) {
    return {common::StatusCode::kInternal,
            "crash child packaged tablet publication disagrees with durable state"};
  }

  std::cout << "READY " << recovered->visible_row_count() << ' ' << recovered->retry_entry_count()
            << '\n'
            << std::flush;
  for (;;)
    static_cast<void>(::pause());
}

} // namespace
} // namespace chronos::service::test

int main(const int count, char** const values) {
  try {
    const chronos::service::test::ChildConfig config =
        chronos::service::test::parse_config(count, values);
    if (config.root.empty()) {
      std::cerr << "replicated database crash child requires --directory\n";
      return 2;
    }
    const chronos::common::Status status = chronos::service::test::prepare_and_reopen(config);
    if (!status.is_ok()) {
      std::cerr << status.to_string() << '\n';
      return 3;
    }
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 4;
  } catch (...) {
    std::cerr << "unknown replicated database crash-child failure\n";
    return 5;
  }
}
