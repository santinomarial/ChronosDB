#include "chronos/common/crc32c.hpp"
#include "chronos/raft/tablet_movement_checkpoint_storage.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace chronos::raft {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-movement-checkpoint-XXXXXX").string();
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

[[nodiscard]] TabletMovementCheckpoint checkpoint(const TabletMovement& movement) {
  return {movement.record(),
          {movement.received_snapshot().begin(), movement.received_snapshot().end()}};
}

[[nodiscard]] TabletMovementCheckpointStorageConfig
config(const std::filesystem::path& directory, const schema::TabletId owner_tablet = tablet_id()) {
  return {.directory_path = directory.string(), .tablet_id = owner_tablet};
}

TEST(TabletMovementCheckpointStorageTest, InstallsSelectsAndReopensExactGenerations) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  auto movement = TabletMovement::begin(tablet_id(), 1U, 1U, 4U, {1U, 2U, 3U});
  ASSERT_TRUE(movement.has_value());
  TabletMovementCheckpointGeneration first{1U, checkpoint(*movement)};
  {
    auto storage = TabletMovementCheckpointStorage::create(config(directory.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    const auto locked = TabletMovementCheckpointStorage::open_existing(config(directory.path()));
    ASSERT_FALSE(locked.has_value());
    EXPECT_EQ(locked.error().code(), common::StatusCode::kUnavailable);
    auto installed = storage->install(first);
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    EXPECT_FALSE(installed->already_present);
    auto repeated = storage->install(first);
    ASSERT_TRUE(repeated.has_value());
    EXPECT_TRUE(repeated->already_present);

    auto conflicting = first;
    conflicting.checkpoint.record.placement_epoch = 2U;
    const auto conflict = storage->install(conflicting);
    ASSERT_FALSE(conflict.has_value());
    EXPECT_EQ(conflict.error().code(), common::StatusCode::kCorruption);
    EXPECT_FALSE(storage->install({3U, checkpoint(*movement)}).has_value());

    const std::vector<std::byte> snapshot{std::byte{3U}, std::byte{4U}};
    ASSERT_TRUE(
        movement->begin_snapshot({5U, 8U, 2U, snapshot.size(), common::crc32c(snapshot)}).is_ok());
    ASSERT_TRUE(movement->accept_snapshot_chunk(0U, snapshot, common::crc32c(snapshot)).is_ok());
    ASSERT_TRUE(movement->finish_snapshot().is_ok());
    TabletMovementCheckpointGeneration second{2U, checkpoint(*movement)};
    installed = storage->install(second);
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    EXPECT_EQ(installed->file_name, "generation-00000000000000000002.movc");
    auto latest = storage->load_latest();
    ASSERT_TRUE(latest.has_value()) << latest.error().to_string();
    ASSERT_TRUE(latest->has_value());
    EXPECT_EQ((*latest)->generation, second);
  }

  auto reopened = TabletMovementCheckpointStorage::open_existing(config(directory.path()));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  auto latest = reopened->load_latest();
  ASSERT_TRUE(latest.has_value()) << latest.error().to_string();
  ASSERT_TRUE(latest->has_value());
  EXPECT_EQ((*latest)->generation.checkpoint_generation, 2U);
  auto recovered = TabletMovement::recover((*latest)->generation.checkpoint.record,
                                           (*latest)->generation.checkpoint.received_snapshot);
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->record().phase, TabletMovementPhase::kCatchingUp);
}

TEST(TabletMovementCheckpointStorageTest, RejectsCorruptionOwnerMismatchAndRenamedGeneration) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  auto movement = TabletMovement::begin(tablet_id(), 1U, 1U, 4U, {1U, 2U, 3U});
  ASSERT_TRUE(movement.has_value());
  std::string first_name;
  {
    auto storage = TabletMovementCheckpointStorage::create(config(directory.path()));
    ASSERT_TRUE(storage.has_value());
    auto installed = storage->install({1U, checkpoint(*movement)});
    ASSERT_TRUE(installed.has_value());
    first_name = installed->file_name;
  }

  {
    auto wrong_owner = TabletMovementCheckpointStorage::open_existing(
        config(directory.path(), tablet_id(std::byte{2U})));
    ASSERT_TRUE(wrong_owner.has_value());
    auto mismatched = wrong_owner->load_latest();
    ASSERT_FALSE(mismatched.has_value());
    EXPECT_EQ(mismatched.error().code(), common::StatusCode::kCorruption);
  }

  const std::string second_name = *tablet_movement_checkpoint_generation_file_name(2U);
  std::filesystem::copy_file(directory.path() / first_name, directory.path() / second_name);
  auto reopened = TabletMovementCheckpointStorage::open_existing(config(directory.path()));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  auto renamed = reopened->load_latest();
  ASSERT_FALSE(renamed.has_value());
  EXPECT_EQ(renamed.error().code(), common::StatusCode::kCorruption);
}

TEST(TabletMovementCheckpointStorageTest, CleansCanonicalTemporaryAndRejectsDamagedFinal) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  {
    auto storage = TabletMovementCheckpointStorage::create(config(directory.path()));
    ASSERT_TRUE(storage.has_value());
  }
  const std::string temporary = *tablet_movement_checkpoint_generation_file_name(1U) + ".tmp";
  {
    std::ofstream output{directory.path() / temporary, std::ios::binary};
    output.put('x');
  }
  {
    auto reopened = TabletMovementCheckpointStorage::open_existing(config(directory.path()));
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    EXPECT_FALSE(std::filesystem::exists(directory.path() / temporary));
    auto movement = TabletMovement::begin(tablet_id(), 1U, 1U, 4U, {1U, 2U, 3U});
    ASSERT_TRUE(movement.has_value());
    ASSERT_TRUE(reopened->install({1U, checkpoint(*movement)}).has_value());
  }
  const auto installed_path =
      directory.path() / *tablet_movement_checkpoint_generation_file_name(1U);
  {
    std::fstream file{installed_path, std::ios::binary | std::ios::in | std::ios::out};
    ASSERT_TRUE(file.good());
    file.seekp(100);
    file.put('x');
  }
  auto reopened = TabletMovementCheckpointStorage::open_existing(config(directory.path()));
  ASSERT_TRUE(reopened.has_value());
  auto latest = reopened->load_latest();
  ASSERT_FALSE(latest.has_value());
  EXPECT_EQ(latest.error().code(), common::StatusCode::kCorruption);
}

TEST(TabletMovementCheckpointStorageTest, UsesCanonicalNonzeroGenerationNames) {
  EXPECT_FALSE(tablet_movement_checkpoint_generation_file_name(0U).has_value());
  EXPECT_EQ(*tablet_movement_checkpoint_generation_file_name(42U),
            "generation-00000000000000000042.movc");
}

} // namespace
} // namespace chronos::raft
