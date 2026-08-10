#include "chronos/common/crc32c.hpp"
#include "chronos/ingest/tablet_movement_snapshot_handoff.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
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

[[nodiscard]] schema::TabletId tablet_id(const std::byte seed = std::byte{5U}) {
  return id<schema::TabletId>(seed);
}

[[nodiscard]] RaftTabletApplicationSnapshot
application_snapshot(const schema::TabletId tablet = tablet_id(),
                     std::vector<raft::NodeId> voters = {1U, 2U, 3U}) {
  raft::SnapshotMetadata metadata{.last_included_index = 9U,
                                  .last_included_term = 4U,
                                  .manifest_generation = 7U,
                                  .part_set_checksum = {},
                                  .configuration_index = 6U,
                                  .voters = std::move(voters)};
  metadata.part_set_checksum.front() = std::byte{0xA5U};
  return {.group_id = group_id(),
          .table_id = table_id(),
          .tablet_id = tablet,
          .raft_snapshot = std::move(metadata),
          .entries = {}};
}

[[nodiscard]] common::Result<raft::RecoveredTabletMovementGeneration>
recovered_movement(const std::vector<std::byte>& bytes, const raft::TabletMovementPhase phase,
                   const std::uint64_t manifest_generation = 7U) {
  auto movement = raft::TabletMovement::begin(tablet_id(), 10U, 1U, 4U, {1U, 2U, 3U});
  if (!movement.has_value())
    return common::make_unexpected(movement.error());
  common::Status status =
      movement->begin_snapshot({manifest_generation, 9U, 4U, bytes.size(), common::crc32c(bytes)});
  if (!status.is_ok())
    return common::make_unexpected(std::move(status));
  status = movement->accept_snapshot_chunk(0U, bytes, common::crc32c(bytes));
  if (!status.is_ok())
    return common::make_unexpected(std::move(status));
  if (phase != raft::TabletMovementPhase::kTransferringSnapshot) {
    status = movement->finish_snapshot();
    if (!status.is_ok())
      return common::make_unexpected(std::move(status));
  }
  if (phase == raft::TabletMovementPhase::kReady ||
      phase == raft::TabletMovementPhase::kTargetPromoted ||
      phase == raft::TabletMovementPhase::kComplete) {
    status = movement->mark_caught_up(9U);
    if (!status.is_ok())
      return common::make_unexpected(std::move(status));
  }
  if (phase == raft::TabletMovementPhase::kTargetPromoted ||
      phase == raft::TabletMovementPhase::kComplete) {
    status = movement->promote_target(10U, 11U);
    if (!status.is_ok())
      return common::make_unexpected(std::move(status));
  }
  if (phase == raft::TabletMovementPhase::kComplete) {
    status = movement->remove_source(11U, 12U);
    if (!status.is_ok())
      return common::make_unexpected(std::move(status));
  }
  return raft::RecoveredTabletMovementGeneration{3U, true, std::move(*movement)};
}

[[nodiscard]] RaftTabletSnapshotStorageConfig
storage_config(const TemporaryDirectory& directory, const raft::GroupId group = group_id()) {
  return {.directory_path = directory.path().string(), .group_id = group};
}

TEST(TabletMovementSnapshotHandoffTest, InstallsExactRtasAndRetriesAfterMovementAdvances) {
  TemporaryDirectory directory{"chronos-movement-rtas"};
  ASSERT_FALSE(directory.path().empty());
  const auto expected = application_snapshot();
  auto bytes = encode_raft_tablet_application_snapshot_v1(expected);
  ASSERT_TRUE(bytes.has_value()) << bytes.error().to_string();
  auto recovered = recovered_movement(*bytes, raft::TabletMovementPhase::kCatchingUp);
  auto storage = RaftTabletSnapshotStorage::create(storage_config(directory));
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();

  auto installed = install_recovered_tablet_movement_snapshot(*recovered, table_id(), *storage);
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  EXPECT_EQ(installed->checkpoint_generation, 3U);
  EXPECT_EQ(installed->group_id, expected.group_id);
  EXPECT_EQ(installed->raft_snapshot, expected.raft_snapshot);
  EXPECT_FALSE(installed->installation.already_present);
  auto loaded = storage->load(9U);
  ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
  EXPECT_EQ(loaded->snapshot, expected);
  EXPECT_EQ(loaded->bytes, *bytes);

  ASSERT_TRUE(recovered->movement.mark_caught_up(9U).is_ok());
  auto ready_retry = install_recovered_tablet_movement_snapshot(*recovered, table_id(), *storage);
  ASSERT_TRUE(ready_retry.has_value()) << ready_retry.error().to_string();
  EXPECT_TRUE(ready_retry->installation.already_present);
  ASSERT_TRUE(recovered->movement.promote_target(10U, 11U).is_ok());
  auto promoted_retry =
      install_recovered_tablet_movement_snapshot(*recovered, table_id(), *storage);
  ASSERT_TRUE(promoted_retry.has_value()) << promoted_retry.error().to_string();
  EXPECT_TRUE(promoted_retry->installation.already_present);
}

TEST(TabletMovementSnapshotHandoffTest, InstallsFromReopenedReferenceAndChunkOwners) {
  TemporaryDirectory checkpoints{"chronos-movement-rtas-checkpoints"};
  TemporaryDirectory chunks{"chronos-movement-rtas-chunks"};
  TemporaryDirectory snapshots{"chronos-movement-rtas-snapshots"};
  ASSERT_FALSE(checkpoints.path().empty());
  ASSERT_FALSE(chunks.path().empty());
  ASSERT_FALSE(snapshots.path().empty());
  const auto expected = application_snapshot();
  auto bytes = encode_raft_tablet_application_snapshot_v1(expected);
  ASSERT_TRUE(bytes.has_value());

  auto movement = raft::TabletMovement::begin(tablet_id(), 10U, 1U, 4U, {1U, 2U, 3U});
  ASSERT_TRUE(movement.has_value());
  const raft::SnapshotTransferMetadata transfer{7U, 9U, 4U, bytes->size(), common::crc32c(*bytes)};
  ASSERT_TRUE(movement->begin_snapshot(transfer).is_ok());
  ASSERT_TRUE(movement->accept_snapshot_chunk(0U, *bytes, common::crc32c(*bytes)).is_ok());
  ASSERT_TRUE(movement->finish_snapshot().is_ok());
  const raft::TabletMovementCheckpointReferenceGeneration reference{
      1U, raft::TabletMovementCheckpointReference{movement->record(), 10U}};
  const raft::TabletMovementSnapshotSession session{tablet_id(), 10U, 1U, 4U, transfer};

  {
    auto checkpoint_storage = raft::TabletMovementCheckpointStorage::create(
        {.directory_path = checkpoints.path().string(), .tablet_id = tablet_id()});
    auto chunk_storage = raft::TabletMovementSnapshotChunkStorage::create(
        {.directory_path = chunks.path().string(), .session = session});
    ASSERT_TRUE(checkpoint_storage.has_value()) << checkpoint_storage.error().to_string();
    ASSERT_TRUE(chunk_storage.has_value()) << chunk_storage.error().to_string();
    ASSERT_TRUE(
        chunk_storage->install(raft::TabletMovementSnapshotChunk{session, 0U, *bytes}).has_value());
    ASSERT_TRUE(raft::install_verified_tablet_movement_reference(*checkpoint_storage,
                                                                 *chunk_storage, reference)
                    .has_value());
  }

  auto checkpoint_storage = raft::TabletMovementCheckpointStorage::open_existing(
      {.directory_path = checkpoints.path().string(), .tablet_id = tablet_id()});
  auto chunk_storage = raft::TabletMovementSnapshotChunkStorage::open_existing(
      {.directory_path = chunks.path().string(), .session = session});
  auto snapshot_storage = RaftTabletSnapshotStorage::create(storage_config(snapshots));
  ASSERT_TRUE(checkpoint_storage.has_value());
  ASSERT_TRUE(chunk_storage.has_value());
  ASSERT_TRUE(snapshot_storage.has_value());
  auto latest = checkpoint_storage->load_latest_any();
  ASSERT_TRUE(latest.has_value());
  ASSERT_TRUE(latest->has_value());
  auto recovered = raft::recover_tablet_movement_generation(**latest, *chunk_storage);
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  auto installed =
      install_recovered_tablet_movement_snapshot(*recovered, table_id(), *snapshot_storage);
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  auto loaded = snapshot_storage->load(9U);
  ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
  EXPECT_EQ(loaded->snapshot, expected);
  EXPECT_EQ(loaded->bytes, *bytes);
}

TEST(TabletMovementSnapshotHandoffTest, RejectsIncompleteAndAdvancedWithoutPriorInstall) {
  const auto expected = application_snapshot();
  auto bytes = encode_raft_tablet_application_snapshot_v1(expected);
  ASSERT_TRUE(bytes.has_value());

  TemporaryDirectory incomplete_directory{"chronos-movement-rtas-incomplete"};
  auto incomplete_storage = RaftTabletSnapshotStorage::create(storage_config(incomplete_directory));
  auto incomplete = recovered_movement(*bytes, raft::TabletMovementPhase::kTransferringSnapshot);
  ASSERT_TRUE(incomplete_storage.has_value());
  ASSERT_TRUE(incomplete.has_value());
  auto early =
      install_recovered_tablet_movement_snapshot(*incomplete, table_id(), *incomplete_storage);
  ASSERT_FALSE(early.has_value());
  EXPECT_EQ(early.error().code(), common::StatusCode::kUnavailable);

  TemporaryDirectory ready_directory{"chronos-movement-rtas-ready"};
  auto ready_storage = RaftTabletSnapshotStorage::create(storage_config(ready_directory));
  auto ready = recovered_movement(*bytes, raft::TabletMovementPhase::kReady);
  ASSERT_TRUE(ready_storage.has_value());
  ASSERT_TRUE(ready.has_value());
  auto missing_ready =
      install_recovered_tablet_movement_snapshot(*ready, table_id(), *ready_storage);
  ASSERT_FALSE(missing_ready.has_value());
  EXPECT_EQ(missing_ready.error().code(), common::StatusCode::kCorruption);

  TemporaryDirectory promoted_directory{"chronos-movement-rtas-promoted"};
  auto promoted_storage = RaftTabletSnapshotStorage::create(storage_config(promoted_directory));
  auto promoted = recovered_movement(*bytes, raft::TabletMovementPhase::kTargetPromoted);
  ASSERT_TRUE(promoted_storage.has_value());
  ASSERT_TRUE(promoted.has_value());
  auto missing =
      install_recovered_tablet_movement_snapshot(*promoted, table_id(), *promoted_storage);
  ASSERT_FALSE(missing.has_value());
  EXPECT_EQ(missing.error().code(), common::StatusCode::kCorruption);
}

TEST(TabletMovementSnapshotHandoffTest, RejectsIdentityMetadataVotersAndDestinationMismatch) {
  TemporaryDirectory directory{"chronos-movement-rtas-mismatch"};
  auto storage = RaftTabletSnapshotStorage::create(storage_config(directory));
  ASSERT_TRUE(storage.has_value());

  auto bytes = encode_raft_tablet_application_snapshot_v1(application_snapshot());
  ASSERT_TRUE(bytes.has_value());
  auto recovered = recovered_movement(*bytes, raft::TabletMovementPhase::kCatchingUp);
  ASSERT_TRUE(recovered.has_value());
  auto wrong_table = install_recovered_tablet_movement_snapshot(
      *recovered, id<schema::TableId>(std::byte{8U}), *storage);
  ASSERT_FALSE(wrong_table.has_value());
  EXPECT_EQ(wrong_table.error().code(), common::StatusCode::kCorruption);

  auto wrong_metadata = recovered_movement(*bytes, raft::TabletMovementPhase::kCatchingUp, 8U);
  ASSERT_TRUE(wrong_metadata.has_value());
  auto metadata = install_recovered_tablet_movement_snapshot(*wrong_metadata, table_id(), *storage);
  ASSERT_FALSE(metadata.has_value());
  EXPECT_EQ(metadata.error().code(), common::StatusCode::kCorruption);

  auto foreign_tablet_bytes =
      encode_raft_tablet_application_snapshot_v1(application_snapshot(tablet_id(std::byte{9U})));
  ASSERT_TRUE(foreign_tablet_bytes.has_value());
  auto foreign_tablet =
      recovered_movement(*foreign_tablet_bytes, raft::TabletMovementPhase::kCatchingUp);
  ASSERT_TRUE(foreign_tablet.has_value());
  auto tablet = install_recovered_tablet_movement_snapshot(*foreign_tablet, table_id(), *storage);
  ASSERT_FALSE(tablet.has_value());
  EXPECT_EQ(tablet.error().code(), common::StatusCode::kCorruption);

  auto foreign_voters_bytes =
      encode_raft_tablet_application_snapshot_v1(application_snapshot(tablet_id(), {1U, 2U}));
  ASSERT_TRUE(foreign_voters_bytes.has_value());
  auto foreign_voters =
      recovered_movement(*foreign_voters_bytes, raft::TabletMovementPhase::kCatchingUp);
  ASSERT_TRUE(foreign_voters.has_value());
  auto voters = install_recovered_tablet_movement_snapshot(*foreign_voters, table_id(), *storage);
  ASSERT_FALSE(voters.has_value());
  EXPECT_EQ(voters.error().code(), common::StatusCode::kCorruption);

  TemporaryDirectory other_group_directory{"chronos-movement-rtas-other-group"};
  auto other_group_storage = RaftTabletSnapshotStorage::create(
      storage_config(other_group_directory, group_id(std::byte{7U})));
  ASSERT_TRUE(other_group_storage.has_value());
  auto group =
      install_recovered_tablet_movement_snapshot(*recovered, table_id(), *other_group_storage);
  ASSERT_FALSE(group.has_value());
  EXPECT_EQ(group.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(TabletMovementSnapshotHandoffTest, RejectsNonRtasTransferredBytes) {
  TemporaryDirectory directory{"chronos-movement-non-rtas"};
  auto storage = RaftTabletSnapshotStorage::create(storage_config(directory));
  const std::vector<std::byte> bytes{std::byte{1U}, std::byte{2U}, std::byte{3U}};
  auto recovered = recovered_movement(bytes, raft::TabletMovementPhase::kCatchingUp);
  ASSERT_TRUE(storage.has_value());
  ASSERT_TRUE(recovered.has_value());
  auto rejected = install_recovered_tablet_movement_snapshot(*recovered, table_id(), *storage);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::ingest
