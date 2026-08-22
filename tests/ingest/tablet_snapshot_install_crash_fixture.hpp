#ifndef CHRONOS_TESTS_INGEST_TABLET_SNAPSHOT_INSTALL_CRASH_FIXTURE_HPP_
#define CHRONOS_TESTS_INGEST_TABLET_SNAPSHOT_INSTALL_CRASH_FIXTURE_HPP_

#include "chronos/common/crc32c.hpp"
#include "chronos/ingest/raft_tablet_snapshot.hpp"
#include "chronos/ingest/raft_tablet_snapshot_storage.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/rebalancing.hpp"
#include "chronos/raft/tablet_movement_checkpoint_recovery.hpp"

#include <cstddef>
#include <filesystem>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::ingest::test {

template <typename Identifier> [[nodiscard]] inline Identifier crash_id(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] inline raft::GroupId crash_group_id() {
  common::Uuid::Bytes bytes{};
  bytes.fill(std::byte{3U});
  return raft::GroupId{bytes};
}

[[nodiscard]] inline schema::TableId crash_table_id() {
  return crash_id<schema::TableId>(std::byte{4U});
}

[[nodiscard]] inline schema::TabletId crash_tablet_id() {
  return crash_id<schema::TabletId>(std::byte{5U});
}

[[nodiscard]] inline RaftTabletApplicationSnapshot crash_application_snapshot() {
  raft::SnapshotMetadata metadata{.last_included_index = 9U,
                                  .last_included_term = 4U,
                                  .manifest_generation = 7U,
                                  .part_set_checksum = {},
                                  .configuration_index = 6U,
                                  .voters = {1U, 2U, 3U}};
  metadata.part_set_checksum.front() = std::byte{0xA5U};
  return {.group_id = crash_group_id(),
          .table_id = crash_table_id(),
          .tablet_id = crash_tablet_id(),
          .raft_snapshot = std::move(metadata),
          .entries = {}};
}

[[nodiscard]] inline common::Result<raft::RecoveredTabletMovementGeneration>
crash_recovered_movement(const std::vector<std::byte>& bytes) {
  auto movement = raft::TabletMovement::begin(crash_tablet_id(), 10U, 1U, 4U, {1U, 2U, 3U});
  if (!movement.has_value())
    return common::make_unexpected(movement.error());
  common::Status status =
      movement->begin_snapshot({7U, 9U, 4U, bytes.size(), common::crc32c(bytes)});
  if (!status.is_ok())
    return common::make_unexpected(std::move(status));
  status = movement->accept_snapshot_chunk(0U, bytes, common::crc32c(bytes));
  if (!status.is_ok())
    return common::make_unexpected(std::move(status));
  status = movement->finish_snapshot();
  if (!status.is_ok())
    return common::make_unexpected(std::move(status));
  return raft::RecoveredTabletMovementGeneration{2U, true, std::move(*movement)};
}

[[nodiscard]] inline std::vector<raft::RaftGroupConfiguration> crash_groups() {
  return {{crash_group_id(), {1U, 2U, 3U}}};
}

[[nodiscard]] inline raft::RaftPersistentLogConfig
crash_log_config(const std::filesystem::path& root) {
  return {.directory_path = (root / "raft").string()};
}

[[nodiscard]] inline RaftTabletSnapshotStorageConfig
crash_snapshot_config(const std::filesystem::path& root) {
  return {.directory_path = (root / "snapshots").string(), .group_id = crash_group_id()};
}

[[nodiscard]] inline common::Result<raft::GroupSnapshotInstall>
request_crash_snapshot(raft::DurableMultiRaftRuntime& runtime,
                       const raft::SnapshotMetadata& metadata) {
  auto requested = runtime.execute_batch(
      {{crash_group_id(),
        raft::ReceiveOperation{1U, raft::InstallSnapshotRequest{4U, 1U, metadata}}}});
  if (!requested.has_value())
    return common::make_unexpected(requested.error());
  if (requested->size() != 1U || !requested->front().status.is_ok()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInternal, "crash fixture snapshot request did not become pending"});
  }
  std::optional<raft::MultiRaftTransition> transition = std::move(requested->front().transition);
  if (!transition.has_value()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInternal, "crash fixture snapshot request did not transition"});
  }
  std::optional<raft::GroupSnapshotInstall> install =
      std::move(transition.value().snapshot_install);
  if (!install.has_value()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInternal, "crash fixture snapshot request did not become pending"});
  }
  return std::move(install).value();
}

} // namespace chronos::ingest::test

#endif // CHRONOS_TESTS_INGEST_TABLET_SNAPSHOT_INSTALL_CRASH_FIXTURE_HPP_
