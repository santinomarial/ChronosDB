#include "chronos/common/crc32c.hpp"
#include "chronos/raft/tablet_movement_snapshot_chunk_storage.hpp"

#include <cstddef>
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
        (std::filesystem::temp_directory_path() / "chronos-movement-chunks-XXXXXX").string();
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

[[nodiscard]] TabletMovementSnapshotSession session(const std::vector<std::byte>& snapshot,
                                                    const schema::TabletId owner = tablet_id()) {
  return {owner, 7U, 1U, 4U, {9U, 20U, 3U, snapshot.size(), common::crc32c(snapshot)}};
}

[[nodiscard]] TabletMovementSnapshotChunkStorageConfig
config(const std::filesystem::path& directory, const TabletMovementSnapshotSession& owner) {
  return {.directory_path = directory.string(), .session = owner};
}

[[nodiscard]] TabletMovementSnapshotChunk chunk(const TabletMovementSnapshotSession& owner,
                                                const std::uint64_t offset,
                                                std::vector<std::byte> bytes) {
  return {owner, offset, std::move(bytes)};
}

void write_bytes(const std::filesystem::path& path, const std::vector<std::byte>& bytes) {
  std::ofstream output{path, std::ios::binary};
  for (const std::byte value : bytes)
    output.put(static_cast<char>(std::to_integer<unsigned char>(value)));
}

TEST(TabletMovementSnapshotChunkStorageTest, InstallsRetriesFinalizesAndReopensExactPrefix) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const std::vector<std::byte> snapshot{std::byte{1U}, std::byte{2U}, std::byte{3U}, std::byte{4U},
                                        std::byte{5U}};
  const auto owner = session(snapshot);
  const auto first = chunk(owner, 0U, {snapshot.begin(), snapshot.begin() + 2});
  {
    auto storage = TabletMovementSnapshotChunkStorage::create(config(directory.path(), owner));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto locked =
        TabletMovementSnapshotChunkStorage::open_existing(config(directory.path(), owner));
    ASSERT_FALSE(locked.has_value());
    EXPECT_EQ(locked.error().code(), common::StatusCode::kUnavailable);

    auto installed = storage->install(first);
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    EXPECT_FALSE(installed->already_present);
    EXPECT_EQ(installed->file_name, "chunk-00000000000000000000.mchk");
    auto repeated = storage->install(first);
    ASSERT_TRUE(repeated.has_value());
    EXPECT_TRUE(repeated->already_present);
    ASSERT_TRUE(storage->received_bytes().has_value());
    EXPECT_EQ(*storage->received_bytes(), 2U);
    auto prefix = storage->load_received_prefix();
    ASSERT_TRUE(prefix.has_value());
    EXPECT_EQ(*prefix, first.bytes);
    auto incomplete = storage->finalize();
    ASSERT_FALSE(incomplete.has_value());
    EXPECT_EQ(incomplete.error().code(), common::StatusCode::kUnavailable);

    auto gap = storage->install(chunk(owner, 3U, {snapshot.begin() + 3, snapshot.end()}));
    ASSERT_FALSE(gap.has_value());
    EXPECT_EQ(gap.error().code(), common::StatusCode::kUnavailable);
    installed = storage->install(chunk(owner, 2U, {snapshot.begin() + 2, snapshot.end()}));
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    auto complete = storage->finalize();
    ASSERT_TRUE(complete.has_value()) << complete.error().to_string();
    EXPECT_EQ(*complete, snapshot);
  }

  auto reopened =
      TabletMovementSnapshotChunkStorage::open_existing(config(directory.path(), owner));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(*reopened->received_bytes(), snapshot.size());
  auto repeated = reopened->install(first);
  ASSERT_TRUE(repeated.has_value());
  EXPECT_TRUE(repeated->already_present);
  auto complete = reopened->finalize();
  ASSERT_TRUE(complete.has_value()) << complete.error().to_string();
  EXPECT_EQ(*complete, snapshot);
}

TEST(TabletMovementSnapshotChunkStorageTest, RejectsConflictingRetryAndForeignSession) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const std::vector<std::byte> snapshot{std::byte{1U}, std::byte{2U}, std::byte{3U}};
  const auto owner = session(snapshot);
  auto storage = TabletMovementSnapshotChunkStorage::create(config(directory.path(), owner));
  ASSERT_TRUE(storage.has_value());
  ASSERT_TRUE(storage->install(chunk(owner, 0U, {snapshot.front()})).has_value());

  auto conflict = storage->install(chunk(owner, 0U, {std::byte{9U}}));
  ASSERT_FALSE(conflict.has_value());
  EXPECT_EQ(conflict.error().code(), common::StatusCode::kCorruption);
  auto foreign_owner = owner;
  foreign_owner.tablet_id = tablet_id(std::byte{2U});
  auto foreign = storage->install(chunk(foreign_owner, 1U, {snapshot.begin() + 1, snapshot.end()}));
  ASSERT_FALSE(foreign.has_value());
  EXPECT_EQ(foreign.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(TabletMovementSnapshotChunkStorageTest, CleansTemporaryAndRejectsDamageAndWrongOwner) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const std::vector<std::byte> snapshot{std::byte{1U}, std::byte{2U}};
  const auto owner = session(snapshot);
  {
    auto storage = TabletMovementSnapshotChunkStorage::create(config(directory.path(), owner));
    ASSERT_TRUE(storage.has_value());
  }
  const std::string temporary = *tablet_movement_snapshot_chunk_file_name(0U) + ".tmp";
  write_bytes(directory.path() / temporary, {std::byte{9U}});
  {
    auto storage =
        TabletMovementSnapshotChunkStorage::open_existing(config(directory.path(), owner));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    EXPECT_FALSE(std::filesystem::exists(directory.path() / temporary));
    ASSERT_TRUE(storage->install(chunk(owner, 0U, snapshot)).has_value());
  }

  auto wrong_owner = owner;
  wrong_owner.placement_epoch++;
  auto mismatched =
      TabletMovementSnapshotChunkStorage::open_existing(config(directory.path(), wrong_owner));
  ASSERT_FALSE(mismatched.has_value());
  EXPECT_EQ(mismatched.error().code(), common::StatusCode::kCorruption);

  const auto path = directory.path() / *tablet_movement_snapshot_chunk_file_name(0U);
  {
    std::fstream file{path, std::ios::binary | std::ios::in | std::ios::out};
    ASSERT_TRUE(file.good());
    file.seekp(static_cast<std::streamoff>(kTabletMovementSnapshotChunkHeaderSize));
    file.put('x');
  }
  auto damaged = TabletMovementSnapshotChunkStorage::open_existing(config(directory.path(), owner));
  ASSERT_FALSE(damaged.has_value());
  EXPECT_EQ(damaged.error().code(), common::StatusCode::kCorruption);
}

TEST(TabletMovementSnapshotChunkStorageTest, RejectsGapRenamedOffsetAndWholeContentMismatch) {
  {
    TemporaryDirectory directory;
    ASSERT_FALSE(directory.path().empty());
    const std::vector<std::byte> snapshot{std::byte{1U}, std::byte{2U}, std::byte{3U}};
    const auto owner = session(snapshot);
    {
      auto storage = TabletMovementSnapshotChunkStorage::create(config(directory.path(), owner));
      ASSERT_TRUE(storage.has_value());
    }
    auto encoded = encode_tablet_movement_snapshot_chunk_v1(
        chunk(owner, 1U, {snapshot.begin() + 1, snapshot.end()}));
    ASSERT_TRUE(encoded.has_value());
    write_bytes(directory.path() / *tablet_movement_snapshot_chunk_file_name(1U), *encoded);
    auto gap = TabletMovementSnapshotChunkStorage::open_existing(config(directory.path(), owner));
    ASSERT_FALSE(gap.has_value());
    EXPECT_EQ(gap.error().code(), common::StatusCode::kCorruption);
  }
  {
    TemporaryDirectory directory;
    ASSERT_FALSE(directory.path().empty());
    const std::vector<std::byte> snapshot{std::byte{1U}, std::byte{2U}};
    const auto owner = session(snapshot);
    {
      auto storage = TabletMovementSnapshotChunkStorage::create(config(directory.path(), owner));
      ASSERT_TRUE(storage.has_value());
      ASSERT_TRUE(storage->install(chunk(owner, 0U, snapshot)).has_value());
    }
    std::filesystem::rename(directory.path() / *tablet_movement_snapshot_chunk_file_name(0U),
                            directory.path() / *tablet_movement_snapshot_chunk_file_name(1U));
    auto renamed =
        TabletMovementSnapshotChunkStorage::open_existing(config(directory.path(), owner));
    ASSERT_FALSE(renamed.has_value());
    EXPECT_EQ(renamed.error().code(), common::StatusCode::kCorruption);
  }
  {
    TemporaryDirectory directory;
    ASSERT_FALSE(directory.path().empty());
    const std::vector<std::byte> declared{std::byte{1U}, std::byte{2U}};
    auto owner = session(declared);
    owner.snapshot.content_crc32c =
        common::crc32c(std::vector<std::byte>{std::byte{8U}, std::byte{9U}});
    auto storage = TabletMovementSnapshotChunkStorage::create(config(directory.path(), owner));
    ASSERT_TRUE(storage.has_value());
    ASSERT_TRUE(storage->install(chunk(owner, 0U, declared)).has_value());
    auto final = storage->finalize();
    ASSERT_FALSE(final.has_value());
    EXPECT_EQ(final.error().code(), common::StatusCode::kCorruption);
  }
}

TEST(TabletMovementSnapshotChunkStorageTest, UsesCanonicalOffsetNamesAndBoundsChunkCount) {
  EXPECT_EQ(*tablet_movement_snapshot_chunk_file_name(42U), "chunk-00000000000000000042.mchk");
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const std::vector<std::byte> snapshot{std::byte{1U}, std::byte{2U}};
  const auto owner = session(snapshot);
  auto limited = config(directory.path(), owner);
  limited.maximum_chunks = 1U;
  auto storage = TabletMovementSnapshotChunkStorage::create(limited);
  ASSERT_TRUE(storage.has_value());
  ASSERT_TRUE(storage->install(chunk(owner, 0U, {snapshot.front()})).has_value());
  auto exhausted_result = storage->install(chunk(owner, 1U, {snapshot.back()}));
  ASSERT_FALSE(exhausted_result.has_value());
  EXPECT_EQ(exhausted_result.error().code(), common::StatusCode::kResourceExhausted);

  TemporaryDirectory recovery_directory;
  ASSERT_FALSE(recovery_directory.path().empty());
  auto two_chunks = config(recovery_directory.path(), owner);
  two_chunks.maximum_chunks = 2U;
  {
    auto complete_storage = TabletMovementSnapshotChunkStorage::create(two_chunks);
    ASSERT_TRUE(complete_storage.has_value());
    ASSERT_TRUE(complete_storage->install(chunk(owner, 0U, {snapshot.front()})).has_value());
    ASSERT_TRUE(complete_storage->install(chunk(owner, 1U, {snapshot.back()})).has_value());
  }
  auto recovery_limited = config(recovery_directory.path(), owner);
  recovery_limited.maximum_chunks = 1U;
  auto excessive = TabletMovementSnapshotChunkStorage::open_existing(recovery_limited);
  ASSERT_FALSE(excessive.has_value());
  EXPECT_EQ(excessive.error().code(), common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::raft
