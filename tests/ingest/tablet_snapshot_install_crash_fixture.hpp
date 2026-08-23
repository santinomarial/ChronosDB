#ifndef CHRONOS_TESTS_INGEST_TABLET_SNAPSHOT_INSTALL_CRASH_FIXTURE_HPP_
#define CHRONOS_TESTS_INGEST_TABLET_SNAPSHOT_INSTALL_CRASH_FIXTURE_HPP_

#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/ingest/raft_tablet_snapshot.hpp"
#include "chronos/ingest/raft_tablet_snapshot_storage.hpp"
#include "chronos/ingest/raft_tablet_state_machine.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/rebalancing.hpp"
#include "chronos/raft/tablet_movement_checkpoint_recovery.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include <cstddef>
#include <filesystem>
#include <memory>
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

[[nodiscard]] inline schema::TabletId crash_compaction_tablet_id() {
  return columnar::test::id<schema::TabletId>(70U);
}

[[nodiscard]] inline TabletState crash_compaction_tablet() {
  return TabletState::create(
             columnar::test::batch_schema(), crash_compaction_tablet_id(),
             TabletStateConfig{
                 .head_capacity = {.row_capacity = 8U, .variable_value_bytes = {0U, 8U, 0U}},
                 .maximum_schema_versions = 1U,
                 .maximum_sealed_generations = 2U,
                 .maximum_retry_entries = 8U})
      .value();
}

[[nodiscard]] inline RetryDirectory crash_compaction_retry_directory() {
  return RetryDirectory::create({.maximum_entries = 8U}).value();
}

[[nodiscard]] inline std::vector<std::shared_ptr<const schema::TableSchema>>
crash_compaction_schemas() {
  return {columnar::test::batch_schema()};
}

[[nodiscard]] inline std::vector<std::byte> crash_compaction_command() {
  auto batch = columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                                    columnar::test::batch_columns())
                   .value();
  const auto encoded_batch = columnar::encode_columnar_batch_v1(batch).value();
  const auto encoded = encode_columnar_append_v1({.client_id = request_id<ClientId>(1U),
                                                  .client_batch_id = request_id<ClientBatchId>(33U),
                                                  .tablet_id = crash_compaction_tablet_id()},
                                                 encoded_batch)
                           .value();
  return {encoded.bytes().begin(), encoded.bytes().end()};
}

[[nodiscard]] inline RaftTabletApplicationSnapshot
crash_application_snapshot(const raft::LogIndex included = 9U) {
  raft::SnapshotMetadata metadata{.last_included_index = included,
                                  .last_included_term = included == 9U ? 4U : 5U,
                                  .manifest_generation = included == 9U ? 7U : included,
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

[[nodiscard]] inline std::vector<raft::RaftGroupConfiguration> crash_compaction_groups() {
  return {{crash_group_id(), {4U}}};
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
