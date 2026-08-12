#include "chronos/common/status.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/metadata_snapshot.hpp"
#include "chronos/raft/schema_definition_codec.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>

namespace chronos::raft {
namespace {

[[nodiscard]] MetadataApplicationSnapshot snapshot() {
  common::Uuid::Bytes group_bytes{};
  group_bytes.front() = std::byte{7U};
  SnapshotMetadata metadata{.last_included_index = 8U,
                            .last_included_term = 3U,
                            .manifest_generation = 8U,
                            .part_set_checksum = {},
                            .configuration_index = 6U,
                            .voters = {1U, 2U, 3U}};
  metadata.part_set_checksum.front() = std::byte{0xA5U};
  return {.group_id = GroupId{group_bytes},
          .raft_snapshot = std::move(metadata),
          .entries = {{.index = 2U,
                       .term = 1U,
                       .type = kRaftMetadataCommandEntryType,
                       .payload = {std::byte{1U}, std::byte{2U}, std::byte{3U}}},
                      {.index = 7U,
                       .term = 3U,
                       .type = kRaftSchemaDefinitionEntryType,
                       .payload = {std::byte{4U}, std::byte{5U}}}}};
}

TEST(MetadataSnapshotTest, RoundTripsCanonicalApplicationEntriesAndInternalGaps) {
  const MetadataApplicationSnapshot expected = snapshot();
  auto encoded = encode_metadata_application_snapshot_v1(expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->size() % 8U, 4U);
  auto decoded = decode_metadata_application_snapshot_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, expected);
}

TEST(MetadataSnapshotTest, RejectsDamageUnknownTypesAndNoncanonicalOrder) {
  auto encoded = encode_metadata_application_snapshot_v1(snapshot()).value();
  encoded[170U] ^= std::byte{1U};
  auto damaged = decode_metadata_application_snapshot_v1(encoded);
  ASSERT_FALSE(damaged.has_value());
  EXPECT_EQ(damaged.error().code(), common::StatusCode::kCorruption);

  MetadataApplicationSnapshot unknown = snapshot();
  unknown.entries.front().type = 99U;
  auto unknown_result = encode_metadata_application_snapshot_v1(unknown);
  ASSERT_FALSE(unknown_result.has_value());
  EXPECT_EQ(unknown_result.error().code(), common::StatusCode::kInvalidArgument);

  MetadataApplicationSnapshot duplicate = snapshot();
  duplicate.entries.back().index = duplicate.entries.front().index;
  auto duplicate_result = encode_metadata_application_snapshot_v1(duplicate);
  ASSERT_FALSE(duplicate_result.has_value());
  EXPECT_EQ(duplicate_result.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(MetadataSnapshotTest, SupportsInternalOnlyPrefixAndEnforcesResourceBounds) {
  MetadataApplicationSnapshot internal_only = snapshot();
  internal_only.entries.clear();
  auto encoded = encode_metadata_application_snapshot_v1(internal_only);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  ASSERT_TRUE(decode_metadata_application_snapshot_v1(*encoded).has_value());

  const auto bounded =
      encode_metadata_application_snapshot_v1(snapshot(), {.maximum_snapshot_bytes = 1024U,
                                                           .maximum_entries = 1U,
                                                           .maximum_entry_payload_bytes = 16U,
                                                           .maximum_voters = 3U});
  ASSERT_FALSE(bounded.has_value());
  EXPECT_EQ(bounded.error().code(), common::StatusCode::kResourceExhausted);

  encoded = encode_metadata_application_snapshot_v1(snapshot());
  ASSERT_TRUE(encoded.has_value());
  const auto decoded =
      decode_metadata_application_snapshot_v1(*encoded, {.maximum_snapshot_bytes = 1024U,
                                                         .maximum_entries = 1U,
                                                         .maximum_entry_payload_bytes = 16U,
                                                         .maximum_voters = 3U});
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::raft
