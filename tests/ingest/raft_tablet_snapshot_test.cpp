#include "chronos/common/status.hpp"
#include "chronos/ingest/raft_tablet_snapshot.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::ingest {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] RaftTabletApplicationSnapshot snapshot() {
  common::Uuid::Bytes group_bytes{};
  group_bytes.front() = std::byte{3U};
  raft::SnapshotMetadata metadata{.last_included_index = 9U,
                                  .last_included_term = 4U,
                                  .manifest_generation = 7U,
                                  .part_set_checksum = {},
                                  .configuration_index = 6U,
                                  .voters = {1U, 2U}};
  metadata.part_set_checksum.front() = std::byte{0xA5U};
  std::vector<RaftTabletSnapshotEntry> entries;
  entries.push_back(
      {.index = 7U, .term = 3U, .payload = {std::byte{1U}, std::byte{2U}, std::byte{3U}}});
  entries.push_back({.index = 9U,
                     .term = 4U,
                     .payload = {std::byte{4U}, std::byte{5U}, std::byte{6U}, std::byte{7U}}});
  return RaftTabletApplicationSnapshot{.group_id = raft::GroupId{group_bytes},
                                       .table_id = id<schema::TableId>(4U),
                                       .tablet_id = id<schema::TabletId>(5U),
                                       .raft_snapshot = std::move(metadata),
                                       .entries = std::move(entries)};
}

TEST(RaftTabletSnapshotTest, RoundTripsCanonicalApplicationPrefix) {
  const RaftTabletApplicationSnapshot expected = snapshot();
  auto encoded = encode_raft_tablet_application_snapshot_v1(expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->size() % 8U, 4U);
  EXPECT_EQ((*encoded)[8U], std::byte{1U});
  EXPECT_EQ((*encoded)[9U], std::byte{0U});
  EXPECT_EQ((*encoded)[10U], std::byte{0U});
  EXPECT_EQ((*encoded)[11U], std::byte{0U});
  auto decoded = decode_raft_tablet_application_snapshot_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, expected);
}

TEST(RaftTabletSnapshotTest, RejectsCorruptionAndNoncanonicalEntries) {
  auto encoded = encode_raft_tablet_application_snapshot_v1(snapshot()).value();
  encoded[200U] ^= std::byte{0x01U};
  auto corrupt = decode_raft_tablet_application_snapshot_v1(encoded);
  ASSERT_FALSE(corrupt.has_value());
  EXPECT_EQ(corrupt.error().code(), common::StatusCode::kCorruption);

  RaftTabletApplicationSnapshot duplicate = snapshot();
  duplicate.entries[1U].index = duplicate.entries[0U].index;
  const auto rejected = encode_raft_tablet_application_snapshot_v1(duplicate);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);

  RaftTabletApplicationSnapshot wrong_term = snapshot();
  wrong_term.entries.back().term = 3U;
  const auto term_rejected = encode_raft_tablet_application_snapshot_v1(wrong_term);
  ASSERT_FALSE(term_rejected.has_value());
  EXPECT_EQ(term_rejected.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(RaftTabletSnapshotTest, EnforcesCallerResourceLimits) {
  const RaftTabletApplicationSnapshot input = snapshot();
  auto encoded =
      encode_raft_tablet_application_snapshot_v1(input, {.maximum_snapshot_bytes = 1024U,
                                                         .maximum_entries = 1U,
                                                         .maximum_entry_payload_bytes = 16U,
                                                         .maximum_voters = 3U});
  ASSERT_FALSE(encoded.has_value());
  EXPECT_EQ(encoded.error().code(), common::StatusCode::kResourceExhausted);

  encoded = encode_raft_tablet_application_snapshot_v1(input);
  ASSERT_TRUE(encoded.has_value());
  const auto decoded =
      decode_raft_tablet_application_snapshot_v1(*encoded, {.maximum_snapshot_bytes = 1024U,
                                                            .maximum_entries = 1U,
                                                            .maximum_entry_payload_bytes = 16U,
                                                            .maximum_voters = 3U});
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::ingest
