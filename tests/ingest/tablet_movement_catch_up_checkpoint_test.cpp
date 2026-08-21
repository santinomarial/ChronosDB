#include "chronos/common/crc32c.hpp"
#include "chronos/ingest/tablet_movement_catch_up_checkpoint.hpp"
#include "chronos/ingest/tablet_movement_raft_snapshot_completion.hpp"

#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <variant>
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

[[nodiscard]] raft::GroupId group_id() {
  common::Uuid::Bytes bytes{};
  bytes.fill(std::byte{3U});
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

[[nodiscard]] common::Result<raft::TabletMovement>
catching_movement(const std::vector<std::byte>& bytes) {
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
  return std::move(*movement);
}

[[nodiscard]] common::Result<raft::GroupSnapshotInstall>
request_snapshot(raft::DurableMultiRaftRuntime& runtime, raft::SnapshotMetadata metadata) {
  const raft::Term request_term = metadata.last_included_term;
  auto requested = runtime.execute_batch(
      {{group_id(), raft::ReceiveOperation{
                        1U, raft::InstallSnapshotRequest{request_term, 1U, std::move(metadata)}}}});
  if (!requested.has_value())
    return common::make_unexpected(requested.error());
  if (requested->size() != 1U) {
    return common::make_unexpected(common::Status{common::StatusCode::kInternal,
                                                  "test snapshot request did not become pending"});
  }
  if (!requested->front().status.is_ok())
    return common::make_unexpected(requested->front().status);
  const auto& pending_transition = requested->front().transition;
  if (!pending_transition.has_value()) {
    return common::make_unexpected(common::Status{common::StatusCode::kInternal,
                                                  "test snapshot request did not become pending"});
  }
  const auto& transition = *pending_transition;
  if (!transition.snapshot_install.has_value()) {
    return common::make_unexpected(common::Status{common::StatusCode::kInternal,
                                                  "test snapshot request did not become pending"});
  }
  return *transition.snapshot_install;
}

[[nodiscard]] raft::TabletMovementCheckpointStorageConfig
checkpoint_config(const TemporaryDirectory& directory) {
  return {.directory_path = directory.path().string(), .tablet_id = tablet_id()};
}

[[nodiscard]] raft::TabletMovementSnapshotChunkStorageConfig
chunk_config(const TemporaryDirectory& directory,
             const raft::TabletMovementSnapshotSession& session) {
  return {.directory_path = directory.path().string(), .session = session};
}

[[nodiscard]] RaftTabletSnapshotStorageConfig snapshot_config(const TemporaryDirectory& directory) {
  return {.directory_path = directory.path().string(), .group_id = group_id()};
}

[[nodiscard]] raft::RaftPersistentLogConfig log_config(const TemporaryDirectory& directory) {
  return {.directory_path = directory.path().string()};
}

TEST(TabletMovementCatchUpCheckpointTest, ReconcilesRaftCompleteCrashIntoReadyReference) {
  TemporaryDirectory checkpoints{"chronos-catch-up-reference-checkpoints"};
  TemporaryDirectory chunks{"chronos-catch-up-reference-chunks"};
  TemporaryDirectory snapshots{"chronos-catch-up-reference-snapshots"};
  TemporaryDirectory log{"chronos-catch-up-reference-log"};
  const auto application = application_snapshot();
  auto bytes = encode_raft_tablet_application_snapshot_v1(application);
  ASSERT_TRUE(bytes.has_value());
  const raft::SnapshotTransferMetadata transfer{7U, 9U, 4U, bytes->size(), common::crc32c(*bytes)};
  const raft::TabletMovementSnapshotSession session{tablet_id(), 10U, 1U, 4U, transfer};
  const std::vector<raft::RaftGroupConfiguration> groups{{group_id(), {1U, 2U, 3U}}};

  {
    auto movement = catching_movement(*bytes);
    auto checkpoint_storage =
        raft::TabletMovementCheckpointStorage::create(checkpoint_config(checkpoints));
    auto chunk_storage =
        raft::TabletMovementSnapshotChunkStorage::create(chunk_config(chunks, session));
    auto snapshot_storage = RaftTabletSnapshotStorage::create(snapshot_config(snapshots));
    auto runtime = raft::DurableMultiRaftRuntime::create_new(4U, log_config(log), groups);
    ASSERT_TRUE(movement.has_value());
    ASSERT_TRUE(checkpoint_storage.has_value());
    ASSERT_TRUE(chunk_storage.has_value());
    ASSERT_TRUE(snapshot_storage.has_value());
    ASSERT_TRUE(runtime.has_value());
    ASSERT_TRUE(
        chunk_storage->install(raft::TabletMovementSnapshotChunk{session, 0U, *bytes}).has_value());
    const raft::TabletMovementCheckpointReferenceGeneration first{
        1U, raft::TabletMovementCheckpointReference{movement->record(), 10U}};
    ASSERT_TRUE(
        raft::install_verified_tablet_movement_reference(*checkpoint_storage, *chunk_storage, first)
            .has_value());
    auto loaded = checkpoint_storage->load_latest_any();
    ASSERT_TRUE(loaded.has_value());
    const auto& loaded_generation = *loaded;
    if (!loaded_generation.has_value()) {
      FAIL() << "expected installed tablet movement generation";
    }
    auto recovered = raft::recover_tablet_movement_generation(*loaded_generation, *chunk_storage);
    ASSERT_TRUE(recovered.has_value());
    auto pending = request_snapshot(*runtime, application.raft_snapshot);
    ASSERT_TRUE(pending.has_value());
    ASSERT_TRUE(complete_recovered_tablet_movement_raft_snapshot(
                    *recovered, table_id(), *snapshot_storage, *pending, *runtime)
                    .has_value());
    ASSERT_EQ(recovered->movement.record().phase, raft::TabletMovementPhase::kCatchingUp);
    ASSERT_TRUE(runtime->close().is_ok());
  }

  {
    auto checkpoint_storage =
        raft::TabletMovementCheckpointStorage::open_existing(checkpoint_config(checkpoints));
    auto chunk_storage =
        raft::TabletMovementSnapshotChunkStorage::open_existing(chunk_config(chunks, session));
    auto snapshot_storage = RaftTabletSnapshotStorage::open_existing(snapshot_config(snapshots));
    auto runtime = raft::DurableMultiRaftRuntime::open_existing(4U, log_config(log), {}, groups);
    ASSERT_TRUE(checkpoint_storage.has_value());
    ASSERT_TRUE(chunk_storage.has_value());
    ASSERT_TRUE(snapshot_storage.has_value());
    ASSERT_TRUE(runtime.has_value());
    auto latest = checkpoint_storage->load_latest_any();
    ASSERT_TRUE(latest.has_value());
    const auto& latest_generation = *latest;
    if (!latest_generation.has_value()) {
      FAIL() << "expected latest tablet movement generation";
    }
    auto recovered = raft::recover_tablet_movement_generation(*latest_generation, *chunk_storage);
    ASSERT_TRUE(recovered.has_value());
    auto missing_owner = checkpoint_recovered_tablet_movement_catch_up(
        *recovered, table_id(), *snapshot_storage, *runtime, *checkpoint_storage);
    ASSERT_FALSE(missing_owner.has_value());
    EXPECT_EQ(missing_owner.error().code(), common::StatusCode::kNotSupported);
    EXPECT_EQ(recovered->movement.record().phase, raft::TabletMovementPhase::kCatchingUp);

    auto installed = checkpoint_recovered_tablet_movement_catch_up(
        *recovered, table_id(), *snapshot_storage, *runtime, *checkpoint_storage, *chunk_storage);
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    EXPECT_EQ(installed->checkpoint_generation, 2U);
    EXPECT_EQ(recovered->checkpoint_generation, 2U);
    EXPECT_EQ(recovered->movement.record().phase, raft::TabletMovementPhase::kReady);
    auto ready = checkpoint_storage->load_latest_any();
    ASSERT_TRUE(ready.has_value());
    const auto& ready_generation = *ready;
    if (!ready_generation.has_value()) {
      FAIL() << "expected ready tablet movement generation";
    }
    EXPECT_TRUE(std::holds_alternative<raft::TabletMovementCheckpointReferenceGeneration>(
        ready_generation->generation));
    auto reopened_ready =
        raft::recover_tablet_movement_generation(*ready_generation, *chunk_storage);
    ASSERT_TRUE(reopened_ready.has_value());
    EXPECT_EQ(reopened_ready->movement.record().phase, raft::TabletMovementPhase::kReady);
    ASSERT_TRUE(runtime->close().is_ok());
  }
}

TEST(TabletMovementCatchUpCheckpointTest, AdvancesLegacySelfContainedGeneration) {
  TemporaryDirectory checkpoints{"chronos-catch-up-legacy-checkpoints"};
  TemporaryDirectory snapshots{"chronos-catch-up-legacy-snapshots"};
  TemporaryDirectory log{"chronos-catch-up-legacy-log"};
  const auto application = application_snapshot();
  auto bytes = encode_raft_tablet_application_snapshot_v1(application);
  ASSERT_TRUE(bytes.has_value()) << bytes.error().to_string();
  auto movement = catching_movement(*bytes);
  ASSERT_TRUE(movement.has_value());
  auto checkpoint_storage =
      raft::TabletMovementCheckpointStorage::create(checkpoint_config(checkpoints));
  auto snapshot_storage = RaftTabletSnapshotStorage::create(snapshot_config(snapshots));
  auto runtime =
      raft::DurableMultiRaftRuntime::create_new(4U, log_config(log), {{group_id(), {1U, 2U, 3U}}});
  ASSERT_TRUE(checkpoint_storage.has_value());
  ASSERT_TRUE(snapshot_storage.has_value());
  ASSERT_TRUE(runtime.has_value());
  ASSERT_TRUE(
      checkpoint_storage->install({1U, raft::TabletMovementCheckpoint{movement->record(), *bytes}})
          .has_value());
  auto latest = checkpoint_storage->load_latest_any();
  ASSERT_TRUE(latest.has_value());
  const auto& latest_generation = *latest;
  if (!latest_generation.has_value()) {
    FAIL() << "expected latest tablet movement generation";
  }
  auto recovered = raft::recover_tablet_movement_generation(*latest_generation);
  ASSERT_TRUE(recovered.has_value());
  auto pending = request_snapshot(*runtime, application.raft_snapshot);
  ASSERT_TRUE(pending.has_value());
  ASSERT_TRUE(complete_recovered_tablet_movement_raft_snapshot(
                  *recovered, table_id(), *snapshot_storage, *pending, *runtime)
                  .has_value());

  auto installed = checkpoint_recovered_tablet_movement_catch_up(
      *recovered, table_id(), *snapshot_storage, *runtime, *checkpoint_storage);
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  EXPECT_EQ(installed->checkpoint_generation, 2U);
  auto ready = checkpoint_storage->load_latest_any();
  ASSERT_TRUE(ready.has_value());
  const auto& ready_generation = *ready;
  if (!ready_generation.has_value()) {
    FAIL() << "expected ready tablet movement generation";
  }
  EXPECT_TRUE(std::holds_alternative<raft::TabletMovementCheckpointGeneration>(
      ready_generation->generation));
  auto reopened_ready = raft::recover_tablet_movement_generation(*ready_generation);
  ASSERT_TRUE(reopened_ready.has_value());
  EXPECT_EQ(reopened_ready->movement.record().phase, raft::TabletMovementPhase::kReady);
}

TEST(TabletMovementCatchUpCheckpointTest, LeavesCatchingCheckpointWhenRaftBoundaryIsMissing) {
  TemporaryDirectory checkpoints{"chronos-catch-up-missing-checkpoints"};
  TemporaryDirectory snapshots{"chronos-catch-up-missing-snapshots"};
  TemporaryDirectory log{"chronos-catch-up-missing-log"};
  const auto application = application_snapshot();
  auto bytes = encode_raft_tablet_application_snapshot_v1(application);
  ASSERT_TRUE(bytes.has_value());
  auto movement = catching_movement(*bytes);
  ASSERT_TRUE(movement.has_value());
  auto checkpoint_storage =
      raft::TabletMovementCheckpointStorage::create(checkpoint_config(checkpoints));
  auto snapshot_storage = RaftTabletSnapshotStorage::create(snapshot_config(snapshots));
  auto runtime =
      raft::DurableMultiRaftRuntime::create_new(4U, log_config(log), {{group_id(), {1U, 2U, 3U}}});
  ASSERT_TRUE(checkpoint_storage.has_value());
  ASSERT_TRUE(snapshot_storage.has_value());
  ASSERT_TRUE(runtime.has_value());
  ASSERT_TRUE(
      checkpoint_storage->install({1U, raft::TabletMovementCheckpoint{movement->record(), *bytes}})
          .has_value());
  auto latest = checkpoint_storage->load_latest_any();
  ASSERT_TRUE(latest.has_value());
  const auto& latest_generation = *latest;
  if (!latest_generation.has_value()) {
    FAIL() << "expected latest tablet movement generation";
  }
  auto recovered = raft::recover_tablet_movement_generation(*latest_generation);
  ASSERT_TRUE(recovered.has_value());

  auto rejected = checkpoint_recovered_tablet_movement_catch_up(
      *recovered, table_id(), *snapshot_storage, *runtime, *checkpoint_storage);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(recovered->checkpoint_generation, 1U);
  EXPECT_EQ(recovered->movement.record().phase, raft::TabletMovementPhase::kCatchingUp);
  auto still_first = checkpoint_storage->load_latest_any();
  ASSERT_TRUE(still_first.has_value());
  const auto& first_generation = *still_first;
  if (!first_generation.has_value()) {
    FAIL() << "expected original tablet movement generation";
  }
  EXPECT_EQ(std::visit([](const auto& value) { return value.checkpoint_generation; },
                       first_generation->generation),
            1U);
}

} // namespace
} // namespace chronos::ingest
