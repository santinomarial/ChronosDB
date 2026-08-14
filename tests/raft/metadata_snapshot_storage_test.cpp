#include "chronos/common/status.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/metadata_snapshot_storage.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>

namespace chronos::raft {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-metadata-snapshot-XXXXXX").string();
    if (char* created = ::mkdtemp(pattern.data()); created != nullptr)
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

[[nodiscard]] GroupId group() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{8U};
  return GroupId{bytes};
}
[[nodiscard]] MetadataApplicationSnapshot snapshot(const LogIndex included = 7U) {
  SnapshotMetadata metadata{.last_included_index = included,
                            .last_included_term = included == 7U ? 2U : 3U,
                            .manifest_generation = included,
                            .part_set_checksum = {},
                            .configuration_index = 4U,
                            .voters = {1U, 2U}};
  metadata.part_set_checksum.front() = std::byte{0x5AU};
  return {.group_id = group(),
          .raft_snapshot = std::move(metadata),
          .entries = {{.index = 3U,
                       .term = 1U,
                       .type = kRaftMetadataCommandEntryType,
                       .payload = {std::byte{1U}, std::byte{2U}}},
                      {.index = 6U,
                       .term = 2U,
                       .type = kRaftMetadataCommandEntryType,
                       .payload = {std::byte{3U}}}}};
}
[[nodiscard]] MetadataSnapshotStorageConfig config(const TemporaryDirectory& directory) {
  return {.directory_path = directory.path().string(), .group_id = group()};
}

TEST(MetadataSnapshotStorageTest, InstallsRetriesSelectsAndReopensHighestSnapshot) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  {
    auto storage = MetadataSnapshotStorage::create(config(directory));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto competing = MetadataSnapshotStorage::create(config(directory));
    ASSERT_FALSE(competing.has_value());
    EXPECT_EQ(competing.error().code(), common::StatusCode::kUnavailable);

    const MetadataApplicationSnapshot first = snapshot();
    auto installed = storage->install(first);
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    EXPECT_FALSE(installed->already_present);
    EXPECT_EQ(installed->file_name, "metadata-snapshot-00000000000000000007.rmas");
    auto repeated = storage->install(first);
    ASSERT_TRUE(repeated.has_value());
    EXPECT_TRUE(repeated->already_present);

    MetadataApplicationSnapshot conflict = first;
    conflict.entries.front().payload.front() ^= std::byte{1U};
    auto rejected = storage->install(conflict);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);

    ASSERT_TRUE(storage->install(snapshot(8U)).has_value());
    auto latest = storage->load_latest();
    ASSERT_TRUE(latest.has_value()) << latest.error().to_string();
    const std::optional<LoadedMetadataSnapshot>& selected = *latest;
    if (!selected.has_value()) {
      ADD_FAILURE() << "installed metadata snapshot is missing";
      return;
    }
    EXPECT_EQ(selected.value().snapshot, snapshot(8U));
  }
  auto reopened = MetadataSnapshotStorage::open_existing(config(directory));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  auto latest = reopened->load_latest();
  ASSERT_TRUE(latest.has_value());
  const std::optional<LoadedMetadataSnapshot>& selected = *latest;
  if (!selected.has_value()) {
    ADD_FAILURE() << "reopened metadata snapshot is missing";
    return;
  }
  EXPECT_EQ(selected.value().snapshot, snapshot(8U));
}

TEST(MetadataSnapshotStorageTest, CleansTemporaryAndRejectsDamagedInstalledBytes) {
  TemporaryDirectory directory;
  std::filesystem::path final_path;
  {
    auto storage = MetadataSnapshotStorage::create(config(directory));
    ASSERT_TRUE(storage.has_value());
    auto installed = storage->install(snapshot());
    ASSERT_TRUE(installed.has_value());
    final_path = directory.path() / installed->file_name;
  }
  const std::filesystem::path first_temporary =
      directory.path() / "metadata-snapshot-00000000000000000008.rmas.tmp";
  const std::filesystem::path second_temporary =
      directory.path() / "metadata-snapshot-00000000000000000009.rmas.tmp";
  {
    std::ofstream output{first_temporary, std::ios::binary | std::ios::trunc};
    output.put('x');
  }
  {
    std::ofstream output{second_temporary, std::ios::binary | std::ios::trunc};
    output.put('x');
  }
  {
    auto reopened = MetadataSnapshotStorage::open_existing(config(directory));
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    EXPECT_FALSE(std::filesystem::exists(first_temporary));
    EXPECT_FALSE(std::filesystem::exists(second_temporary));
  }
  {
    std::fstream file{final_path, std::ios::binary | std::ios::in | std::ios::out};
    ASSERT_TRUE(file.good());
    file.seekp(160);
    file.put('x');
  }
  auto reopened = MetadataSnapshotStorage::open_existing(config(directory));
  ASSERT_TRUE(reopened.has_value());
  auto latest = reopened->load_latest();
  ASSERT_FALSE(latest.has_value());
  EXPECT_EQ(latest.error().code(), common::StatusCode::kCorruption);
}

TEST(MetadataSnapshotStorageTest, ReclaimsOlderAndCrashOrphanedFutureSnapshots) {
  TemporaryDirectory directory;
  auto storage = MetadataSnapshotStorage::create(config(directory));
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  ASSERT_TRUE(storage->install(snapshot(7U)).has_value());
  ASSERT_TRUE(storage->install(snapshot(8U)).has_value());
  ASSERT_TRUE(storage->install(snapshot(9U)).has_value());

  auto reclaimed = storage->reclaim_obsolete(8U);

  ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
  EXPECT_EQ(reclaimed->authoritative_index, 8U);
  EXPECT_EQ(reclaimed->reclaimed_files, 2U);
  EXPECT_EQ(storage->load(7U).error().code(), common::StatusCode::kNotFound);
  EXPECT_TRUE(storage->load(8U).has_value());
  EXPECT_EQ(storage->load(9U).error().code(), common::StatusCode::kNotFound);

  auto orphaned = storage->reclaim_obsolete(std::nullopt);
  ASSERT_TRUE(orphaned.has_value()) << orphaned.error().to_string();
  EXPECT_EQ(orphaned->reclaimed_files, 1U);
  auto latest = storage->load_latest();
  ASSERT_TRUE(latest.has_value()) << latest.error().to_string();
  EXPECT_FALSE(latest->has_value());
}

} // namespace
} // namespace chronos::raft
