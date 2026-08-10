#include "chronos/common/crc32c.hpp"
#include "chronos/raft/tablet_movement_snapshot_chunk.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::raft {
namespace {

[[nodiscard]] schema::TabletId tablet_id() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{3U};
  return schema::TabletId::from_bytes(bytes).value();
}

[[nodiscard]] TabletMovementSnapshotChunk chunk() {
  std::vector<std::byte> complete{std::byte{1U}, std::byte{2U}, std::byte{3U}, std::byte{4U}};
  return {{tablet_id(), 7U, 1U, 4U, {9U, 20U, 3U, complete.size(), common::crc32c(complete)}},
          2U,
          {std::byte{3U}, std::byte{4U}}};
}

TEST(TabletMovementSnapshotChunkTest, RoundTripsExactSessionOffsetAndBytes) {
  const TabletMovementSnapshotChunk expected = chunk();
  auto encoded = encode_tablet_movement_snapshot_chunk_v1(expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto decoded = decode_tablet_movement_snapshot_chunk_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, expected);
}

TEST(TabletMovementSnapshotChunkTest, RejectsDamageBoundsAndInvalidSession) {
  auto encoded = encode_tablet_movement_snapshot_chunk_v1(chunk()).value();
  encoded[kTabletMovementSnapshotChunkHeaderSize] ^= std::byte{1U};
  auto damaged = decode_tablet_movement_snapshot_chunk_v1(encoded);
  ASSERT_FALSE(damaged.has_value());
  EXPECT_EQ(damaged.error().code(), common::StatusCode::kCorruption);

  TabletMovementSnapshotChunk past_end = chunk();
  past_end.offset = 3U;
  auto bounded = encode_tablet_movement_snapshot_chunk_v1(past_end);
  ASSERT_FALSE(bounded.has_value());
  EXPECT_EQ(bounded.error().code(), common::StatusCode::kInvalidArgument);

  TabletMovementSnapshotChunk invalid = chunk();
  invalid.session.source_node = invalid.session.target_node;
  auto rejected = encode_tablet_movement_snapshot_chunk_v1(invalid);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::raft
