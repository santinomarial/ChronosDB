#include "chronos/common/status.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/metadata_snapshot.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

constexpr std::size_t kMaximumEntryCount = 65'536U;

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

} // namespace
} // namespace chronos::raft
