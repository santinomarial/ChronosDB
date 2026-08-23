#include "chronos/common/status.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/metadata_snapshot.hpp"
#include "chronos/raft/metadata_snapshot_storage.hpp"

#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

constexpr std::size_t kMaximumEntryCount = 65'536U;

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-metadata-scale-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
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

[[nodiscard]] std::vector<std::byte> command_payload() {
  return encode_metadata_command_v1(ClusterNodeMetadata{7U, "n"}).value();
}

[[nodiscard]] MetadataApplicationSnapshot snapshot(const std::size_t entry_count) {
  common::Uuid::Bytes group_bytes{};
  group_bytes.front() = std::byte{7U};
  SnapshotMetadata metadata{.last_included_index = entry_count,
                            .last_included_term = 3U,
                            .manifest_generation = entry_count,
                            .part_set_checksum = {},
                            .configuration_index = entry_count,
                            .voters = {1U, 2U, 3U, 4U, 5U, 6U, 7U, 8U, 9U}};
  metadata.part_set_checksum.fill(std::byte{0x5aU});
  MetadataApplicationSnapshot value{
      .group_id = GroupId{group_bytes}, .raft_snapshot = std::move(metadata), .entries = {}};
  const std::vector<std::byte> payload = command_payload();
  value.entries.reserve(entry_count);
  for (std::size_t ordinal = 0U; ordinal < entry_count; ++ordinal) {
    const Term term = 1U + static_cast<Term>((ordinal * 3U) / entry_count);
    value.entries.push_back({.index = ordinal + 1U,
                             .term = term,
                             .type = kRaftMetadataCommandEntryType,
                             .payload = payload});
  }
  value.entries.back().term = value.raft_snapshot.last_included_term;
  return value;
}

[[nodiscard]] MetadataSnapshotStorageConfig storage_config(const TemporaryDirectory& directory) {
  common::Uuid::Bytes group_bytes{};
  group_bytes.front() = std::byte{7U};
  return {.directory_path = directory.path().string(), .group_id = GroupId{group_bytes}};
}

TEST(MetadataSnapshotScaleTest, ExactMaximumEntryCatalogRoundTripsAndNextEntryIsRejected) {
  MetadataApplicationSnapshot expected = snapshot(kMaximumEntryCount);
  ASSERT_EQ(expected.entries.size(), MetadataSnapshotCodecLimits{}.maximum_entries);
  ASSERT_TRUE(decode_metadata_command_v1(expected.entries.front().payload).has_value());

  const auto encoded = encode_metadata_application_snapshot_v1(expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const std::size_t aligned_entry_size =
      (kMetadataSnapshotEntryHeaderSize + expected.entries.front().payload.size() + 7U) &
      ~std::size_t{7U};
  EXPECT_EQ(encoded->size(),
            kMetadataSnapshotHeaderSize + expected.raft_snapshot.voters.size() * sizeof(NodeId) +
                expected.entries.size() * aligned_entry_size + kMetadataSnapshotTrailerSize);

  const auto decoded = decode_metadata_application_snapshot_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, expected);
  const auto reencoded = encode_metadata_application_snapshot_v1(*decoded);
  ASSERT_TRUE(reencoded.has_value()) << reencoded.error().to_string();
  EXPECT_EQ(*reencoded, *encoded);

  MetadataSnapshotCodecLimits lower_limits;
  --lower_limits.maximum_entries;
  const auto lower_encode = encode_metadata_application_snapshot_v1(expected, lower_limits);
  ASSERT_FALSE(lower_encode.has_value());
  EXPECT_EQ(lower_encode.error().code(), common::StatusCode::kResourceExhausted);
  const auto lower_decode = decode_metadata_application_snapshot_v1(*encoded, lower_limits);
  ASSERT_FALSE(lower_decode.has_value());
  EXPECT_EQ(lower_decode.error().code(), common::StatusCode::kResourceExhausted);

  ++expected.raft_snapshot.last_included_index;
  ++expected.raft_snapshot.manifest_generation;
  ++expected.raft_snapshot.configuration_index;
  expected.entries.push_back({.index = expected.raft_snapshot.last_included_index,
                              .term = expected.raft_snapshot.last_included_term,
                              .type = kRaftMetadataCommandEntryType,
                              .payload = command_payload()});
  const auto over_limit = encode_metadata_application_snapshot_v1(expected);
  ASSERT_FALSE(over_limit.has_value());
  EXPECT_EQ(over_limit.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(MetadataSnapshotScaleTest, ExactMaximumEntryCatalogInstallsAndRecoversAfterReopen) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const MetadataApplicationSnapshot expected = snapshot(kMaximumEntryCount);
  const auto expected_bytes = encode_metadata_application_snapshot_v1(expected);
  ASSERT_TRUE(expected_bytes.has_value()) << expected_bytes.error().to_string();

  {
    auto storage = MetadataSnapshotStorage::create(storage_config(directory));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    const auto installed = storage->install(expected);
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    EXPECT_FALSE(installed->already_present);

    const auto loaded = storage->load(expected.raft_snapshot.last_included_index);
    ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
    EXPECT_EQ(loaded->snapshot, expected);
    EXPECT_EQ(loaded->bytes, *expected_bytes);

    const auto repeated = storage->install(expected);
    ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
    EXPECT_TRUE(repeated->already_present);
  }

  auto reopened = MetadataSnapshotStorage::open_existing(storage_config(directory));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  const auto latest = reopened->load_latest();
  ASSERT_TRUE(latest.has_value()) << latest.error().to_string();
  const std::optional<LoadedMetadataSnapshot>& selected = *latest;
  if (!selected.has_value()) {
    ADD_FAILURE() << "maximum metadata snapshot is missing after reopen";
    return;
  }
  EXPECT_EQ(selected.value().snapshot, expected);
  EXPECT_EQ(selected.value().bytes, *expected_bytes);
}

} // namespace
} // namespace chronos::raft
