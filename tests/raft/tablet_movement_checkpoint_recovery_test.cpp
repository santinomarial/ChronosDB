#include "chronos/common/crc32c.hpp"
#include "chronos/raft/tablet_movement_checkpoint_recovery.hpp"

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::raft {
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

[[nodiscard]] schema::TabletId tablet_id(const std::byte seed = std::byte{1U}) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return schema::TabletId::from_bytes(bytes).value();
}

[[nodiscard]] TabletMovementCheckpointStorageConfig
checkpoint_config(const std::filesystem::path& directory) {
  return {.directory_path = directory.string(), .tablet_id = tablet_id()};
}

[[nodiscard]] TabletMovementSnapshotChunkStorageConfig
chunk_config(const std::filesystem::path& directory, const TabletMovementSnapshotSession& session) {
  return {.directory_path = directory.string(), .session = session};
}

[[nodiscard]] TabletMovementSnapshotChunk chunk(const TabletMovementSnapshotSession& session,
                                                const std::uint64_t offset,
                                                std::vector<std::byte> bytes) {
  return {session, offset, std::move(bytes)};
}

TEST(TabletMovementCheckpointRecoveryTest, RecoversCheckpointBoundaryWhenChunksAreAhead) {
  TemporaryDirectory checkpoints{"chronos-movement-recovery-checkpoints"};
  TemporaryDirectory chunks{"chronos-movement-recovery-chunks"};
  ASSERT_FALSE(checkpoints.path().empty());
  ASSERT_FALSE(chunks.path().empty());
  const std::vector<std::byte> snapshot{std::byte{1U}, std::byte{2U}, std::byte{3U}, std::byte{4U}};
  const TabletMovementSnapshotSession session{
      tablet_id(), 1U, 1U, 4U, {9U, 20U, 3U, snapshot.size(), common::crc32c(snapshot)}};
  auto movement = TabletMovement::begin(tablet_id(), 1U, 1U, 4U, {1U, 2U, 3U});
  ASSERT_TRUE(movement.has_value());
  ASSERT_TRUE(movement->begin_snapshot(session.snapshot).is_ok());
  ASSERT_TRUE(
      movement
          ->accept_snapshot_chunk(0U, {snapshot.data(), 2U}, common::crc32c({snapshot.data(), 2U}))
          .is_ok());
  const TabletMovementCheckpointReferenceGeneration reference{
      1U, TabletMovementCheckpointReference{movement->record(), 1U}};
  {
    auto checkpoint_storage =
        TabletMovementCheckpointStorage::create(checkpoint_config(checkpoints.path()));
    auto chunk_storage =
        TabletMovementSnapshotChunkStorage::create(chunk_config(chunks.path(), session));
    ASSERT_TRUE(checkpoint_storage.has_value()) << checkpoint_storage.error().to_string();
    ASSERT_TRUE(chunk_storage.has_value()) << chunk_storage.error().to_string();
    ASSERT_TRUE(chunk_storage->install(chunk(session, 0U, {snapshot.begin(), snapshot.begin() + 2}))
                    .has_value());
    auto installed =
        install_verified_tablet_movement_reference(*checkpoint_storage, *chunk_storage, reference);
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    ASSERT_TRUE(chunk_storage->install(chunk(session, 2U, {snapshot.begin() + 2, snapshot.end()}))
                    .has_value());
  }

  auto checkpoint_storage =
      TabletMovementCheckpointStorage::open_existing(checkpoint_config(checkpoints.path()));
  auto chunk_storage =
      TabletMovementSnapshotChunkStorage::open_existing(chunk_config(chunks.path(), session));
  ASSERT_TRUE(checkpoint_storage.has_value()) << checkpoint_storage.error().to_string();
  ASSERT_TRUE(chunk_storage.has_value()) << chunk_storage.error().to_string();
  EXPECT_EQ(*chunk_storage->received_bytes(), snapshot.size());
  auto interior = chunk_storage->load_prefix_through(1U);
  ASSERT_FALSE(interior.has_value());
  EXPECT_EQ(interior.error().code(), common::StatusCode::kInvalidArgument);
  auto latest = checkpoint_storage->load_latest_any();
  ASSERT_TRUE(latest.has_value()) << latest.error().to_string();
  auto& latest_generation = *latest;
  if (!latest_generation.has_value()) {
    ADD_FAILURE() << "expected an installed movement checkpoint generation";
    return;
  }
  const auto& loaded_generation = *latest_generation;
  auto without_chunks = recover_tablet_movement_generation(loaded_generation);
  ASSERT_FALSE(without_chunks.has_value());
  EXPECT_EQ(without_chunks.error().code(), common::StatusCode::kNotSupported);
  auto recovered = recover_tablet_movement_generation(loaded_generation, *chunk_storage);
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_TRUE(recovered->used_external_prefix);
  EXPECT_EQ(recovered->movement.record().received_bytes, 2U);
  EXPECT_TRUE(std::ranges::equal(recovered->movement.received_snapshot(),
                                 common::ByteView(snapshot.data(), 2U)));
}

TEST(TabletMovementCheckpointRecoveryTest, RejectsMissingPrefixAndWrongSessionBeforeInstall) {
  TemporaryDirectory checkpoints{"chronos-movement-recovery-missing-checkpoints"};
  TemporaryDirectory chunks{"chronos-movement-recovery-missing-chunks"};
  ASSERT_FALSE(checkpoints.path().empty());
  ASSERT_FALSE(chunks.path().empty());
  const std::vector<std::byte> snapshot{std::byte{1U}, std::byte{2U}};
  const TabletMovementSnapshotSession session{
      tablet_id(), 1U, 1U, 4U, {2U, 3U, 1U, snapshot.size(), common::crc32c(snapshot)}};
  auto movement = TabletMovement::begin(tablet_id(), 1U, 1U, 4U, {1U, 2U, 3U});
  ASSERT_TRUE(movement.has_value());
  ASSERT_TRUE(movement->begin_snapshot(session.snapshot).is_ok());
  ASSERT_TRUE(movement->accept_snapshot_chunk(0U, snapshot, common::crc32c(snapshot)).is_ok());
  const TabletMovementCheckpointReferenceGeneration reference{
      1U, TabletMovementCheckpointReference{movement->record(), 1U}};
  auto checkpoint_storage =
      TabletMovementCheckpointStorage::create(checkpoint_config(checkpoints.path()));
  auto chunk_storage =
      TabletMovementSnapshotChunkStorage::create(chunk_config(chunks.path(), session));
  ASSERT_TRUE(checkpoint_storage.has_value());
  ASSERT_TRUE(chunk_storage.has_value());
  ASSERT_TRUE(chunk_storage->install(chunk(session, 0U, {snapshot.front()})).has_value());
  auto missing =
      install_verified_tablet_movement_reference(*checkpoint_storage, *chunk_storage, reference);
  ASSERT_FALSE(missing.has_value());
  EXPECT_EQ(missing.error().code(), common::StatusCode::kUnavailable);
  auto latest = checkpoint_storage->load_latest_any();
  ASSERT_TRUE(latest.has_value()) << latest.error().to_string();
  EXPECT_FALSE(latest->has_value());
  ASSERT_TRUE(checkpoint_storage->install_reference(reference).has_value());
  auto durable = checkpoint_storage->load_any_generation(1U);
  ASSERT_TRUE(durable.has_value()) << durable.error().to_string();
  auto corrupt_recovery = recover_tablet_movement_generation(*durable, *chunk_storage);
  ASSERT_FALSE(corrupt_recovery.has_value());
  EXPECT_EQ(corrupt_recovery.error().code(), common::StatusCode::kCorruption);

  TemporaryDirectory foreign_chunks{"chronos-movement-recovery-foreign-chunks"};
  ASSERT_FALSE(foreign_chunks.path().empty());
  auto foreign_session = session;
  foreign_session.tablet_id = tablet_id(std::byte{2U});
  auto foreign = TabletMovementSnapshotChunkStorage::create(
      chunk_config(foreign_chunks.path(), foreign_session));
  ASSERT_TRUE(foreign.has_value());
  auto wrong = install_verified_tablet_movement_reference(*checkpoint_storage, *foreign, reference);
  ASSERT_FALSE(wrong.has_value());
  EXPECT_EQ(wrong.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(TabletMovementCheckpointRecoveryTest, RecoversSelfContainedGenerationWithoutChunkOwner) {
  auto movement = TabletMovement::begin(tablet_id(), 1U, 1U, 4U, {1U, 2U, 3U});
  ASSERT_TRUE(movement.has_value());
  TabletMovementCheckpointGeneration checkpoint{3U,
                                                TabletMovementCheckpoint{movement->record(), {}}};
  LoadedTabletMovementCheckpointGeneration loaded{
      "generation-00000000000000000003.movc",
      TabletMovementCheckpointGenerationValue{std::move(checkpoint)},
      {}};
  auto recovered = recover_tablet_movement_generation(loaded);
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_FALSE(recovered->used_external_prefix);
  EXPECT_EQ(recovered->checkpoint_generation, 3U);
  EXPECT_EQ(recovered->movement.record().phase, TabletMovementPhase::kAddingTarget);
}

TEST(TabletMovementCheckpointRecoveryTest, RejectsCompletedReferenceWithWrongWholeChecksum) {
  TemporaryDirectory checkpoints{"chronos-movement-recovery-crc-checkpoints"};
  TemporaryDirectory chunks{"chronos-movement-recovery-crc-chunks"};
  ASSERT_FALSE(checkpoints.path().empty());
  ASSERT_FALSE(chunks.path().empty());
  const std::vector<std::byte> snapshot{std::byte{7U}};
  auto movement = TabletMovement::begin(tablet_id(), 1U, 1U, 4U, {1U, 2U, 3U});
  ASSERT_TRUE(movement.has_value());
  ASSERT_TRUE(
      movement->begin_snapshot({1U, 1U, 1U, snapshot.size(), common::crc32c(snapshot)}).is_ok());
  ASSERT_TRUE(movement->accept_snapshot_chunk(0U, snapshot, common::crc32c(snapshot)).is_ok());
  ASSERT_TRUE(movement->finish_snapshot().is_ok());
  TabletMovementCheckpointReference reference{movement->record(), 1U};
  reference.record.snapshot.content_crc32c ^= 1U;
  const auto session = *tablet_movement_snapshot_session(reference);
  auto checkpoint_storage =
      TabletMovementCheckpointStorage::create(checkpoint_config(checkpoints.path()));
  auto chunk_storage =
      TabletMovementSnapshotChunkStorage::create(chunk_config(chunks.path(), session));
  ASSERT_TRUE(checkpoint_storage.has_value());
  ASSERT_TRUE(chunk_storage.has_value());
  ASSERT_TRUE(chunk_storage->install(chunk(session, 0U, snapshot)).has_value());
  auto invalid = install_verified_tablet_movement_reference(*checkpoint_storage, *chunk_storage,
                                                            {1U, reference});
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().code(), common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::raft
