#include "chronos/common/status.hpp"
#include "chronos/ingest/raft_tablet_snapshot_storage.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
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
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-raft-app-snapshot-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr) {
      path_ = created;
    }
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  TemporaryDirectory(const TemporaryDirectory&) = delete;
  TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] raft::GroupId group_id() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{3U};
  return raft::GroupId{bytes};
}

[[nodiscard]] RaftTabletApplicationSnapshot snapshot(const raft::LogIndex included = 9U) {
  raft::SnapshotMetadata metadata{.last_included_index = included,
                                  .last_included_term = included == 9U ? 4U : 5U,
                                  .manifest_generation = included,
                                  .part_set_checksum = {},
                                  .configuration_index = 6U,
                                  .voters = {1U, 2U}};
  metadata.part_set_checksum.front() = std::byte{0xA5U};
  std::vector<RaftTabletSnapshotEntry> entries;
  entries.push_back(
      {.index = 7U, .term = 3U, .payload = {std::byte{1U}, std::byte{2U}, std::byte{3U}}});
  entries.push_back(
      {.index = 9U, .term = 4U, .payload = {std::byte{4U}, std::byte{5U}, std::byte{6U}}});
  return RaftTabletApplicationSnapshot{.group_id = group_id(),
                                       .table_id = id<schema::TableId>(4U),
                                       .tablet_id = id<schema::TabletId>(5U),
                                       .raft_snapshot = std::move(metadata),
                                       .entries = std::move(entries)};
}

[[nodiscard]] RaftTabletSnapshotStorageConfig config(const TemporaryDirectory& directory) {
  return {.directory_path = directory.path().string(), .group_id = group_id()};
}

TEST(RaftTabletSnapshotStorageTest, InstallsIdempotentlyAndSelectsHighestAfterReopen) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  {
    auto storage = RaftTabletSnapshotStorage::create(config(directory));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    const auto locked = RaftTabletSnapshotStorage::create(config(directory));
    ASSERT_FALSE(locked.has_value());
    EXPECT_EQ(locked.error().code(), common::StatusCode::kUnavailable);

    const RaftTabletApplicationSnapshot first = snapshot();
    auto installed = storage->install(first);
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    EXPECT_FALSE(installed->already_present);
    EXPECT_EQ(installed->file_name, "snapshot-00000000000000000009.rtas");
    auto repeated = storage->install(first);
    ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
    EXPECT_TRUE(repeated->already_present);
    RaftTabletApplicationSnapshot conflicting = first;
    conflicting.entries.front().payload.front() ^= std::byte{0x01U};
    const auto rejected = storage->install(conflicting);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);

    auto loaded = storage->load(9U);
    ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
    EXPECT_EQ(loaded->snapshot, first);
    ASSERT_TRUE(storage->install(snapshot(10U)).has_value());
    auto latest = storage->load_latest();
    ASSERT_TRUE(latest.has_value()) << latest.error().to_string();
    ASSERT_TRUE(latest->has_value());
    EXPECT_EQ(latest->transform([](const LoadedRaftTabletSnapshot& loaded) {
      return loaded.snapshot.raft_snapshot.last_included_index;
    }),
              raft::LogIndex{10U});
  }

  auto reopened = RaftTabletSnapshotStorage::open_existing(config(directory));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  auto latest = reopened->load_latest();
  ASSERT_TRUE(latest.has_value()) << latest.error().to_string();
  ASSERT_TRUE(latest->has_value());
  EXPECT_EQ(
      latest->transform([](const LoadedRaftTabletSnapshot& loaded) { return loaded.snapshot; }),
      snapshot(10U));
}

TEST(RaftTabletSnapshotStorageTest, CleansInterruptedTemporaryAndRejectsCorruptInstalledBytes) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  std::filesystem::path final_path;
  {
    auto storage = RaftTabletSnapshotStorage::create(config(directory));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto installed = storage->install(snapshot());
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    final_path = directory.path() / installed->file_name;
  }

  const std::filesystem::path temporary =
      directory.path() / "snapshot-00000000000000000010.rtas.tmp";
  {
    std::ofstream output{temporary, std::ios::binary | std::ios::trunc};
    output.put('x');
  }
  {
    auto reopened = RaftTabletSnapshotStorage::open_existing(config(directory));
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    EXPECT_FALSE(std::filesystem::exists(temporary));
    const auto installed = reopened->load(9U);
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    EXPECT_EQ(installed->snapshot, snapshot());
  }

  {
    std::fstream file{final_path, std::ios::binary | std::ios::in | std::ios::out};
    ASSERT_TRUE(file.good());
    file.seekp(200);
    file.put('x');
  }
  auto reopened = RaftTabletSnapshotStorage::open_existing(config(directory));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  auto latest = reopened->load_latest();
  ASSERT_FALSE(latest.has_value());
  EXPECT_EQ(latest.error().code(), common::StatusCode::kCorruption);
}

TEST(RaftTabletSnapshotStorageTest, ReclaimsOnlyFilesOutsideDurableRaftAuthority) {
  TemporaryDirectory directory;
  auto storage = RaftTabletSnapshotStorage::create(config(directory));
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  ASSERT_TRUE(storage->install(snapshot(9U)).has_value());
  ASSERT_TRUE(storage->install(snapshot(10U)).has_value());
  ASSERT_TRUE(storage->install(snapshot(11U)).has_value());

  auto reclaimed = storage->reclaim_obsolete(10U);

  ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
  EXPECT_EQ(reclaimed->authoritative_index, 10U);
  EXPECT_EQ(reclaimed->reclaimed_files, 2U);
  EXPECT_EQ(storage->load(9U).error().code(), common::StatusCode::kNotFound);
  EXPECT_TRUE(storage->load(10U).has_value());
  EXPECT_EQ(storage->load(11U).error().code(), common::StatusCode::kNotFound);

  auto orphaned = storage->reclaim_obsolete(std::nullopt);
  ASSERT_TRUE(orphaned.has_value()) << orphaned.error().to_string();
  EXPECT_FALSE(orphaned->authoritative_index.has_value());
  EXPECT_EQ(orphaned->reclaimed_files, 1U);
  auto latest = storage->load_latest();
  ASSERT_TRUE(latest.has_value()) << latest.error().to_string();
  EXPECT_FALSE(latest->has_value());
}

} // namespace
} // namespace chronos::ingest
