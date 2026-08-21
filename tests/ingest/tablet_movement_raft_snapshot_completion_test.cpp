#include "chronos/common/crc32c.hpp"
#include "chronos/ingest/tablet_movement_raft_snapshot_completion.hpp"

#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

class TemporaryDirectory {
public:
  explicit TemporaryDirectory(const char* name) {
    std::string pattern =
        (std::filesystem::temp_directory_path() / (std::string{name} + "-XXXXXX")).string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }

  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

template <typename Identifier> [[nodiscard]] Identifier id(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] raft::GroupId group_id(const std::byte seed = std::byte{3U}) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return raft::GroupId{bytes};
}

[[nodiscard]] schema::TableId table_id() {
  return id<schema::TableId>(std::byte{4U});
}

[[nodiscard]] schema::TabletId tablet_id() {
  return id<schema::TabletId>(std::byte{5U});
}

[[nodiscard]] RaftTabletApplicationSnapshot application_snapshot() {
  raft::SnapshotMetadata metadata{.last_included_index = 9U,
                                  .last_included_term = 4U,
                                  .manifest_generation = 7U,
                                  .part_set_checksum = {},
                                  .configuration_index = 6U,
                                  .voters = {1U, 2U, 3U}};
  metadata.part_set_checksum.front() = std::byte{0xA5U};
  return {.group_id = group_id(),
          .table_id = table_id(),
          .tablet_id = tablet_id(),
          .raft_snapshot = std::move(metadata),
          .entries = {}};
}

[[nodiscard]] common::Result<raft::RecoveredTabletMovementGeneration>
recovered_movement(const std::vector<std::byte>& bytes,
                   const raft::TabletMovementPhase phase = raft::TabletMovementPhase::kCatchingUp) {
  auto movement = raft::TabletMovement::begin(tablet_id(), 10U, 1U, 4U, {1U, 2U, 3U});
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
  if (phase == raft::TabletMovementPhase::kReady) {
    status = movement->mark_caught_up(9U);
    if (!status.is_ok())
      return common::make_unexpected(std::move(status));
  }
  return raft::RecoveredTabletMovementGeneration{2U, true, std::move(*movement)};
}

[[nodiscard]] common::Result<raft::GroupSnapshotInstall>
request_snapshot(raft::DurableMultiRaftRuntime& runtime, raft::SnapshotMetadata metadata,
                 const raft::NodeId source = 1U) {
  const raft::Term request_term = metadata.last_included_term;
  auto requested = runtime.execute_batch(
      {{group_id(),
        raft::ReceiveOperation{
            source, raft::InstallSnapshotRequest{request_term, source, std::move(metadata)}}}});
  if (!requested.has_value())
    return common::make_unexpected(requested.error());
  if (requested->size() != 1U) {
    return common::make_unexpected(common::Status{common::StatusCode::kInternal,
                                                  "test snapshot request did not become pending"});
  }
  if (!requested->front().status.is_ok())
    return common::make_unexpected(requested->front().status);
  std::optional<raft::MultiRaftTransition> transition = std::move(requested->front().transition);
  if (!transition.has_value()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "test snapshot request did not transition"});
  }
  std::optional<raft::GroupSnapshotInstall> snapshot_install =
      std::move(transition.value().snapshot_install);
  if (!snapshot_install.has_value()) {
    return common::make_unexpected(common::Status{common::StatusCode::kInternal,
                                                  "test snapshot request did not become pending"});
  }
  return std::move(snapshot_install).value();
}

[[nodiscard]] raft::RaftPersistentLogConfig log_config(const TemporaryDirectory& directory) {
  return {.directory_path = directory.path().string()};
}

[[nodiscard]] RaftTabletSnapshotStorageConfig snapshot_config(const TemporaryDirectory& directory) {
  return {.directory_path = directory.path().string(), .group_id = group_id()};
}

TEST(TabletMovementRaftSnapshotCompletionTest, PersistsMetadataBeforeReleasingExactSuccess) {
  TemporaryDirectory log_directory{"chronos-movement-raft-complete-log"};
  TemporaryDirectory snapshot_directory{"chronos-movement-raft-complete-rtas"};
  ASSERT_FALSE(log_directory.path().empty());
  ASSERT_FALSE(snapshot_directory.path().empty());
  const auto expected = application_snapshot();
  auto bytes = encode_raft_tablet_application_snapshot_v1(expected);
  ASSERT_TRUE(bytes.has_value()) << bytes.error().to_string();
  auto recovered = recovered_movement(*bytes);
  const std::vector<raft::RaftGroupConfiguration> groups{{group_id(), {1U, 2U, 3U}}};
  auto runtime = raft::DurableMultiRaftRuntime::create_new(4U, log_config(log_directory), groups);
  auto snapshot_storage = RaftTabletSnapshotStorage::create(snapshot_config(snapshot_directory));
  ASSERT_TRUE(recovered.has_value());
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  ASSERT_TRUE(snapshot_storage.has_value());
  auto pending = request_snapshot(*runtime, expected.raft_snapshot);
  ASSERT_TRUE(pending.has_value()) << pending.error().to_string();
  const std::uint64_t request_sequence = runtime->durable_physical_sequence();

  auto completed = complete_recovered_tablet_movement_raft_snapshot(
      *recovered, table_id(), *snapshot_storage, *pending, *runtime);

  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  EXPECT_GT(completed->durable_physical_sequence, request_sequence);
  EXPECT_EQ(completed->durable_physical_sequence, runtime->durable_physical_sequence());
  EXPECT_EQ(completed->application_snapshot.raft_snapshot, expected.raft_snapshot);
  EXPECT_EQ(completed->acknowledgement.group_id, group_id());
  EXPECT_EQ(completed->acknowledgement.source, 4U);
  EXPECT_EQ(completed->acknowledgement.outbound.destination, 1U);
  const auto* response =
      std::get_if<raft::InstallSnapshotResponse>(&completed->acknowledgement.outbound.message);
  ASSERT_NE(response, nullptr);
  EXPECT_TRUE(response->success);
  EXPECT_EQ(response->term, runtime->find_group(group_id())->persistent_state().current_term);
  EXPECT_EQ(response->last_included_index, 9U);
  EXPECT_EQ(runtime->find_group(group_id())->persistent_state().snapshot, expected.raft_snapshot);
  auto installed = snapshot_storage->load(9U);
  ASSERT_TRUE(installed.has_value());
  EXPECT_EQ(installed->bytes, *bytes);
  EXPECT_TRUE(recovered->movement.mark_caught_up(9U).is_ok());

  ASSERT_TRUE(runtime->close().is_ok());
  auto reopened =
      raft::DurableMultiRaftRuntime::open_existing(4U, log_config(log_directory), {}, groups);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->find_group(group_id())->persistent_state().snapshot, expected.raft_snapshot);
  EXPECT_EQ(reopened->find_group(group_id())->commit_index(), 9U);
  EXPECT_EQ(reopened->find_group(group_id())->applied_index(), 9U);
}

TEST(TabletMovementRaftSnapshotCompletionTest, RejectsPendingAndTargetMismatchWithoutRaftInstall) {
  const auto expected = application_snapshot();
  auto bytes = encode_raft_tablet_application_snapshot_v1(expected);
  ASSERT_TRUE(bytes.has_value());
  auto recovered = recovered_movement(*bytes);
  ASSERT_TRUE(recovered.has_value());

  TemporaryDirectory source_log{"chronos-movement-raft-wrong-source-log"};
  TemporaryDirectory source_rtas{"chronos-movement-raft-wrong-source-rtas"};
  auto source_runtime = raft::DurableMultiRaftRuntime::create_new(4U, log_config(source_log),
                                                                  {{group_id(), {1U, 2U, 3U}}});
  auto source_storage = RaftTabletSnapshotStorage::create(snapshot_config(source_rtas));
  ASSERT_TRUE(source_runtime.has_value());
  ASSERT_TRUE(source_storage.has_value());
  auto wrong_source = request_snapshot(*source_runtime, expected.raft_snapshot, 2U);
  ASSERT_TRUE(wrong_source.has_value());
  auto source_rejected = complete_recovered_tablet_movement_raft_snapshot(
      *recovered, table_id(), *source_storage, *wrong_source, *source_runtime);
  ASSERT_FALSE(source_rejected.has_value());
  EXPECT_EQ(source_rejected.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(source_runtime->find_group(group_id())->persistent_state().snapshot.last_included_index,
            0U);

  TemporaryDirectory metadata_log{"chronos-movement-raft-wrong-metadata-log"};
  TemporaryDirectory metadata_rtas{"chronos-movement-raft-wrong-metadata-rtas"};
  auto metadata_runtime = raft::DurableMultiRaftRuntime::create_new(4U, log_config(metadata_log),
                                                                    {{group_id(), {1U, 2U, 3U}}});
  auto metadata_storage = RaftTabletSnapshotStorage::create(snapshot_config(metadata_rtas));
  ASSERT_TRUE(metadata_runtime.has_value());
  ASSERT_TRUE(metadata_storage.has_value());
  auto different_metadata = expected.raft_snapshot;
  different_metadata.part_set_checksum.back() = std::byte{0x5AU};
  auto wrong_metadata = request_snapshot(*metadata_runtime, different_metadata);
  ASSERT_TRUE(wrong_metadata.has_value());
  auto metadata_rejected = complete_recovered_tablet_movement_raft_snapshot(
      *recovered, table_id(), *metadata_storage, *wrong_metadata, *metadata_runtime);
  ASSERT_FALSE(metadata_rejected.has_value());
  EXPECT_EQ(metadata_rejected.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(
      metadata_runtime->find_group(group_id())->persistent_state().snapshot.last_included_index,
      0U);

  TemporaryDirectory target_log{"chronos-movement-raft-wrong-target-log"};
  TemporaryDirectory target_rtas{"chronos-movement-raft-wrong-target-rtas"};
  auto target_runtime = raft::DurableMultiRaftRuntime::create_new(5U, log_config(target_log),
                                                                  {{group_id(), {1U, 2U, 3U}}});
  auto target_storage = RaftTabletSnapshotStorage::create(snapshot_config(target_rtas));
  ASSERT_TRUE(target_runtime.has_value());
  ASSERT_TRUE(target_storage.has_value());
  auto wrong_target = request_snapshot(*target_runtime, expected.raft_snapshot);
  ASSERT_TRUE(wrong_target.has_value());
  auto target_rejected = complete_recovered_tablet_movement_raft_snapshot(
      *recovered, table_id(), *target_storage, *wrong_target, *target_runtime);
  ASSERT_FALSE(target_rejected.has_value());
  EXPECT_EQ(target_rejected.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(target_runtime->find_group(group_id())->persistent_state().snapshot.last_included_index,
            0U);
}

TEST(TabletMovementRaftSnapshotCompletionTest, RejectsAdvancedMovementAndMissingPendingInstall) {
  TemporaryDirectory log_directory{"chronos-movement-raft-order-log"};
  TemporaryDirectory snapshot_directory{"chronos-movement-raft-order-rtas"};
  const auto expected = application_snapshot();
  auto bytes = encode_raft_tablet_application_snapshot_v1(expected);
  ASSERT_TRUE(bytes.has_value());
  auto ready = recovered_movement(*bytes, raft::TabletMovementPhase::kReady);
  auto catching = recovered_movement(*bytes);
  auto runtime = raft::DurableMultiRaftRuntime::create_new(4U, log_config(log_directory),
                                                           {{group_id(), {1U, 2U, 3U}}});
  auto storage = RaftTabletSnapshotStorage::create(snapshot_config(snapshot_directory));
  ASSERT_TRUE(ready.has_value());
  ASSERT_TRUE(catching.has_value());
  ASSERT_TRUE(runtime.has_value());
  ASSERT_TRUE(storage.has_value());
  const raft::GroupSnapshotInstall fabricated{
      group_id(), raft::PendingSnapshotInstall{1U, expected.raft_snapshot}};

  auto advanced = complete_recovered_tablet_movement_raft_snapshot(*ready, table_id(), *storage,
                                                                   fabricated, *runtime);
  ASSERT_FALSE(advanced.has_value());
  EXPECT_EQ(advanced.error().code(), common::StatusCode::kInvalidArgument);
  auto absent = complete_recovered_tablet_movement_raft_snapshot(*catching, table_id(), *storage,
                                                                 fabricated, *runtime);
  ASSERT_FALSE(absent.has_value());
  EXPECT_EQ(absent.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(runtime->find_group(group_id())->persistent_state().snapshot.last_included_index, 0U);
}

} // namespace
} // namespace chronos::ingest
