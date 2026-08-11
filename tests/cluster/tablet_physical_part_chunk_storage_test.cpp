#include "chronos/cluster/tablet_physical_part_chunk_storage.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace chronos::cluster {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-physical-part-XXXXXX").string();
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

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

template <typename Identity> [[nodiscard]] Identity id(const std::uint8_t seed) {
  return Identity::from_uuid(uuid(seed)).value();
}

[[nodiscard]] TabletPhysicalPartTransferSession session(const std::vector<std::byte>& object) {
  return {.table_id = id<schema::TableId>(1U),
          .tablet_id = id<schema::TabletId>(2U),
          .group_id = uuid(3U),
          .placement_epoch = 4U,
          .source_node = 5U,
          .target_node = 6U,
          .manifest_generation = 7U,
          .part_id = id<cseg::PartId>(8U),
          .total_bytes = object.size(),
          .content_sha256 = ingest::sha256(object).value()};
}

[[nodiscard]] TabletPhysicalPartChunkStorageConfig
config(const std::filesystem::path& directory, const TabletPhysicalPartTransferSession& owner) {
  return {.directory_path = directory.string(),
          .session = owner,
          .codec_limits = {.maximum_object_bytes = 1024U,
                           .maximum_chunk_bytes = 16U,
                           .maximum_encoded_bytes = 1024U}};
}

[[nodiscard]] TabletPhysicalPartChunk chunk(const TabletPhysicalPartTransferSession& owner,
                                            const std::uint64_t offset,
                                            std::vector<std::byte> bytes) {
  return {.session = owner, .offset = offset, .bytes = std::move(bytes)};
}

void write_bytes(const std::filesystem::path& path, const std::vector<std::byte>& bytes) {
  std::ofstream output{path, std::ios::binary};
  for (const std::byte value : bytes)
    output.put(static_cast<char>(std::to_integer<unsigned char>(value)));
}

TEST(TabletPhysicalPartChunkStorageTest, InstallsRetriesStreamsCompletionAndReopens) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const std::vector<std::byte> object{std::byte{1U}, std::byte{2U}, std::byte{3U}, std::byte{4U},
                                      std::byte{5U}};
  const auto owner = session(object);
  const auto first = chunk(owner, 0U, {object.begin(), object.begin() + 2});
  {
    auto storage = TabletPhysicalPartChunkStorage::create(config(directory.path(), owner));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    EXPECT_EQ(TabletPhysicalPartChunkStorage::open_existing(config(directory.path(), owner))
                  .error()
                  .code(),
              common::StatusCode::kUnavailable);
    auto installed = storage->install(first);
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    EXPECT_FALSE(installed->already_present);
    EXPECT_EQ(installed->file_name, "part-chunk-00000000000000000000.pchk");
    EXPECT_TRUE(storage->install(first)->already_present);
    EXPECT_EQ(*storage->received_bytes(), 2U);
    EXPECT_EQ(storage->finalize().error().code(), common::StatusCode::kUnavailable);
    EXPECT_EQ(storage->install(chunk(owner, 3U, {object.back()})).error().code(),
              common::StatusCode::kUnavailable);
    ASSERT_TRUE(storage->install(chunk(owner, 2U, {object.begin() + 2, object.end()})).has_value());
    auto completed = storage->finalize();
    ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
    EXPECT_EQ(completed->received_bytes, object.size());
    EXPECT_EQ(completed->chunk_count, 2U);
    EXPECT_EQ(completed->content_sha256, owner.content_sha256);
  }
  auto wrong_owner = owner;
  wrong_owner.placement_epoch++;
  EXPECT_EQ(TabletPhysicalPartChunkStorage::open_existing(config(directory.path(), wrong_owner))
                .error()
                .code(),
            common::StatusCode::kCorruption);
  auto reopened = TabletPhysicalPartChunkStorage::open_existing(config(directory.path(), owner));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(*reopened->received_bytes(), object.size());
  EXPECT_TRUE(reopened->finalize().has_value());
  auto loaded = reopened->load_chunk(2U);
  ASSERT_TRUE(loaded.has_value());
  EXPECT_EQ(loaded->chunk.bytes, (std::vector<std::byte>{object.begin() + 2, object.end()}));
}

TEST(TabletPhysicalPartChunkStorageTest, RejectsConflictForeignSessionGapAndBadDigest) {
  TemporaryDirectory directory;
  const std::vector<std::byte> object{std::byte{1U}, std::byte{2U}, std::byte{3U}};
  const auto owner = session(object);
  auto storage = TabletPhysicalPartChunkStorage::create(config(directory.path(), owner));
  ASSERT_TRUE(storage.has_value());
  ASSERT_TRUE(storage->install(chunk(owner, 0U, {object.front()})).has_value());
  EXPECT_EQ(storage->install(chunk(owner, 0U, {std::byte{9U}})).error().code(),
            common::StatusCode::kCorruption);
  auto foreign = owner;
  foreign.placement_epoch++;
  EXPECT_EQ(storage->install(chunk(foreign, 1U, {object.begin() + 1, object.end()})).error().code(),
            common::StatusCode::kInvalidArgument);
  ASSERT_TRUE(storage->install(chunk(owner, 1U, {object.begin() + 1, object.end()})).has_value());

  TemporaryDirectory mismatch_directory;
  auto mismatch = owner;
  mismatch.content_sha256 = ingest::sha256(std::vector<std::byte>{std::byte{9U}}).value();
  auto mismatched =
      TabletPhysicalPartChunkStorage::create(config(mismatch_directory.path(), mismatch));
  ASSERT_TRUE(mismatched.has_value());
  ASSERT_TRUE(mismatched->install(chunk(mismatch, 0U, object)).has_value());
  EXPECT_EQ(mismatched->finalize().error().code(), common::StatusCode::kCorruption);
}

TEST(TabletPhysicalPartChunkStorageTest, CleansTemporaryAndRejectsDamageAndRecoveryGap) {
  TemporaryDirectory directory;
  const std::vector<std::byte> object{std::byte{1U}, std::byte{2U}};
  const auto owner = session(object);
  {
    auto storage = TabletPhysicalPartChunkStorage::create(config(directory.path(), owner));
    ASSERT_TRUE(storage.has_value());
  }
  const std::string temporary = *tablet_physical_part_chunk_file_name(0U) + ".tmp";
  write_bytes(directory.path() / temporary, {std::byte{9U}});
  {
    auto storage = TabletPhysicalPartChunkStorage::open_existing(config(directory.path(), owner));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    EXPECT_FALSE(std::filesystem::exists(directory.path() / temporary));
    ASSERT_TRUE(storage->install(chunk(owner, 0U, object)).has_value());
  }
  const auto path = directory.path() / *tablet_physical_part_chunk_file_name(0U);
  {
    std::fstream file{path, std::ios::binary | std::ios::in | std::ios::out};
    file.seekp(static_cast<std::streamoff>(kTabletPhysicalPartChunkHeaderSize));
    file.put('x');
  }
  EXPECT_EQ(
      TabletPhysicalPartChunkStorage::open_existing(config(directory.path(), owner)).error().code(),
      common::StatusCode::kCorruption);

  TemporaryDirectory gap_directory;
  auto encoded =
      encode_tablet_physical_part_chunk_v1(chunk(owner, 1U, {object.back()}),
                                           config(gap_directory.path(), owner).codec_limits)
          .value();
  write_bytes(gap_directory.path() / *tablet_physical_part_chunk_file_name(1U), encoded);
  write_bytes(gap_directory.path() / "LOCK", {});
  EXPECT_EQ(TabletPhysicalPartChunkStorage::open_existing(config(gap_directory.path(), owner))
                .error()
                .code(),
            common::StatusCode::kCorruption);
}

TEST(TabletPhysicalPartChunkStorageTest, EnforcesChunkCountAndCanonicalNames) {
  EXPECT_EQ(*tablet_physical_part_chunk_file_name(42U), "part-chunk-00000000000000000042.pchk");
  TemporaryDirectory directory;
  const std::vector<std::byte> object{std::byte{1U}, std::byte{2U}};
  const auto owner = session(object);
  auto limited = config(directory.path(), owner);
  limited.maximum_chunks = 1U;
  auto storage = TabletPhysicalPartChunkStorage::create(limited);
  ASSERT_TRUE(storage.has_value());
  ASSERT_TRUE(storage->install(chunk(owner, 0U, {object.front()})).has_value());
  EXPECT_EQ(storage->install(chunk(owner, 1U, {object.back()})).error().code(),
            common::StatusCode::kResourceExhausted);
}

TEST(TabletPhysicalPartChunkStorageTest, ReclaimsDurablyAndResumesADeletedSuffix) {
  TemporaryDirectory directory;
  const std::vector<std::byte> object{std::byte{1U}, std::byte{2U}, std::byte{3U}};
  const auto owner = session(object);
  const auto first = chunk(owner, 0U, {object.begin(), object.begin() + 2});
  const auto encoded_first =
      encode_tablet_physical_part_chunk_v1(first, config(directory.path(), owner).codec_limits)
          .value();
  {
    auto storage = TabletPhysicalPartChunkStorage::create(config(directory.path(), owner));
    ASSERT_TRUE(storage.has_value());
    ASSERT_TRUE(storage->install(first).has_value());
    EXPECT_EQ(storage->reclaim().error().code(), common::StatusCode::kUnavailable);
    ASSERT_TRUE(storage->install(chunk(owner, 2U, {object.back()})).has_value());
    auto reclaimed = storage->reclaim();
    ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
    EXPECT_FALSE(reclaimed->marker_already_present);
    EXPECT_EQ(reclaimed->removed_chunks, 2U);
    EXPECT_EQ(reclaimed->removed_payload_bytes, object.size());
    EXPECT_TRUE(storage->is_reclaimed());
    EXPECT_EQ(*storage->received_bytes(), 0U);
    EXPECT_EQ(storage->install(first).error().code(), common::StatusCode::kUnavailable);
    EXPECT_EQ(storage->load_chunk(0U).error().code(), common::StatusCode::kUnavailable);
    EXPECT_EQ(storage->finalize().error().code(), common::StatusCode::kUnavailable);
  }
  EXPECT_TRUE(std::filesystem::exists(directory.path() / "RECLAIMED"));
  EXPECT_FALSE(
      std::filesystem::exists(directory.path() / *tablet_physical_part_chunk_file_name(0U)));

  // A marker plus a contiguous prefix is the only possible interrupted-delete recovery state.
  write_bytes(directory.path() / *tablet_physical_part_chunk_file_name(0U), encoded_first);
  auto reopened = TabletPhysicalPartChunkStorage::open_existing(config(directory.path(), owner));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_TRUE(reopened->is_reclaimed());
  EXPECT_EQ(*reopened->transfer_session(), owner);
  EXPECT_EQ(*reopened->received_bytes(), 2U);
  auto resumed = reopened->reclaim();
  ASSERT_TRUE(resumed.has_value()) << resumed.error().to_string();
  EXPECT_TRUE(resumed->marker_already_present);
  EXPECT_EQ(resumed->removed_chunks, 1U);
  EXPECT_EQ(resumed->removed_payload_bytes, 2U);
  EXPECT_EQ(reopened->reclaim()->removed_chunks, 0U);
}

TEST(TabletPhysicalPartChunkStorageTest, RejectsDamagedReclamationMarker) {
  TemporaryDirectory directory;
  const std::vector<std::byte> object{std::byte{1U}};
  const auto owner = session(object);
  {
    auto storage = TabletPhysicalPartChunkStorage::create(config(directory.path(), owner));
    ASSERT_TRUE(storage.has_value());
    ASSERT_TRUE(storage->install(chunk(owner, 0U, object)).has_value());
    ASSERT_TRUE(storage->reclaim().has_value());
  }
  {
    std::fstream marker{directory.path() / "RECLAIMED",
                        std::ios::binary | std::ios::in | std::ios::out};
    marker.seekp(24);
    marker.put('x');
  }
  EXPECT_EQ(
      TabletPhysicalPartChunkStorage::open_existing(config(directory.path(), owner)).error().code(),
      common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::cluster
