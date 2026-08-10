#include "chronos/common/crc32c.hpp"
#include "chronos/raft/tablet_movement_checkpoint_reference.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::raft {
namespace {

[[nodiscard]] schema::TabletId tablet_id() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{5U};
  return schema::TabletId::from_bytes(bytes).value();
}

[[nodiscard]] common::Result<TabletMovement> movement() {
  return TabletMovement::begin(tablet_id(), 7U, 1U, 4U, {1U, 2U, 3U});
}

TEST(TabletMovementCheckpointReferenceTest, RoundTripsPartialProgressWithoutEmbeddingPrefix) {
  auto active = movement();
  ASSERT_TRUE(active.has_value());
  const std::vector<std::byte> snapshot{std::byte{1U}, std::byte{2U}, std::byte{3U}, std::byte{4U}};
  ASSERT_TRUE(
      active->begin_snapshot({9U, 20U, 3U, snapshot.size(), common::crc32c(snapshot)}).is_ok());
  const common::ByteView prefix{snapshot.data(), 2U};
  ASSERT_TRUE(active->accept_snapshot_chunk(0U, prefix, common::crc32c(prefix)).is_ok());
  const TabletMovementCheckpointReference expected{active->record(), 7U};

  auto encoded = encode_tablet_movement_checkpoint_reference_v1(expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_LT(encoded->size(), 256U);
  auto decoded = decode_tablet_movement_checkpoint_reference_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, expected);
  auto owner = tablet_movement_snapshot_session(*decoded);
  ASSERT_TRUE(owner.has_value()) << owner.error().to_string();
  EXPECT_EQ(owner->placement_epoch, 7U);
  EXPECT_EQ(owner->snapshot, expected.record.snapshot);

  auto recovered = TabletMovement::recover(decoded->record, {prefix.begin(), prefix.end()});
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->record().received_bytes, prefix.size());
}

TEST(TabletMovementCheckpointReferenceTest, RetainsOriginalEpochAfterPlacementAdvances) {
  auto active = movement();
  ASSERT_TRUE(active.has_value());
  const std::vector<std::byte> snapshot{std::byte{7U}};
  ASSERT_TRUE(
      active->begin_snapshot({1U, 1U, 1U, snapshot.size(), common::crc32c(snapshot)}).is_ok());
  ASSERT_TRUE(active->accept_snapshot_chunk(0U, snapshot, common::crc32c(snapshot)).is_ok());
  ASSERT_TRUE(active->finish_snapshot().is_ok());
  ASSERT_TRUE(active->mark_caught_up(1U).is_ok());

  ASSERT_TRUE(active->promote_target(7U, 8U).is_ok());
  TabletMovementCheckpointReference promoted{active->record(), 7U};
  EXPECT_TRUE(encode_tablet_movement_checkpoint_reference_v1(promoted).has_value());
  ASSERT_TRUE(active->remove_source(8U, 9U).is_ok());
  TabletMovementCheckpointReference complete{active->record(), 7U};
  auto encoded = encode_tablet_movement_checkpoint_reference_v1(complete);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto decoded = decode_tablet_movement_checkpoint_reference_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->record.placement_epoch, 9U);

  complete.snapshot_session_placement_epoch = 8U;
  auto wrong_epoch = encode_tablet_movement_checkpoint_reference_v1(complete);
  ASSERT_FALSE(wrong_epoch.has_value());
  EXPECT_EQ(wrong_epoch.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(TabletMovementCheckpointReferenceTest, RejectsNoSessionDamageAndUnboundBytes) {
  auto adding = movement();
  ASSERT_TRUE(adding.has_value());
  auto no_session = encode_tablet_movement_checkpoint_reference_v1({adding->record(), 7U});
  ASSERT_FALSE(no_session.has_value());
  EXPECT_EQ(no_session.error().code(), common::StatusCode::kInvalidArgument);

  const std::vector<std::byte> snapshot{std::byte{1U}, std::byte{2U}};
  ASSERT_TRUE(
      adding->begin_snapshot({2U, 3U, 1U, snapshot.size(), common::crc32c(snapshot)}).is_ok());
  ASSERT_TRUE(
      adding
          ->accept_snapshot_chunk(0U, {snapshot.data(), 1U}, common::crc32c({snapshot.data(), 1U}))
          .is_ok());
  TabletMovementCheckpointReference reference{adding->record(), 7U};
  EXPECT_TRUE(validate_tablet_movement_record(reference.record).is_ok());
  EXPECT_FALSE(validate_tablet_movement_state(reference.record, {}).is_ok());

  auto encoded = encode_tablet_movement_checkpoint_reference_v1(reference).value();
  encoded[kTabletMovementCheckpointReferenceHeaderSize] ^= std::byte{1U};
  auto damaged = decode_tablet_movement_checkpoint_reference_v1(encoded);
  ASSERT_FALSE(damaged.has_value());
  EXPECT_EQ(damaged.error().code(), common::StatusCode::kCorruption);
  auto invalid_limits =
      encode_tablet_movement_checkpoint_reference_v1(reference, {.maximum_checkpoint_bytes = 64U});
  ASSERT_FALSE(invalid_limits.has_value());
  EXPECT_EQ(invalid_limits.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::raft
