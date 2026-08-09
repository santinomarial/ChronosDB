#include "chronos/common/crc32c.hpp"
#include "chronos/raft/rebalancing.hpp"

#include <algorithm>
#include <cstddef>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::raft {
namespace {

[[nodiscard]] schema::TabletId tablet_id() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{1U};
  return schema::TabletId::from_bytes(bytes).value();
}

TEST(TabletMovementTest, TransfersRetryablyCatchesUpAndRemovesSourceOnlyAfterPromotion) {
  auto movement = TabletMovement::begin(tablet_id(), 7U, 1U, 4U, {1U, 2U, 3U});
  ASSERT_TRUE(movement.has_value()) << movement.error().to_string();
  const std::vector<std::byte> snapshot{std::byte{1U}, std::byte{2U}, std::byte{3U}, std::byte{4U}};
  ASSERT_TRUE(
      movement->begin_snapshot({9U, 20U, 3U, snapshot.size(), common::crc32c(snapshot)}).is_ok());
  const common::ByteView first{snapshot.data(), 2U};
  EXPECT_TRUE(movement->accept_snapshot_chunk(0U, first, common::crc32c(first)).is_ok());
  EXPECT_TRUE(movement->accept_snapshot_chunk(0U, first, common::crc32c(first)).is_ok());
  EXPECT_TRUE(movement->restart_snapshot_transfer().is_ok());
  EXPECT_EQ(movement->record().received_bytes, 0U);
  EXPECT_TRUE(movement->accept_snapshot_chunk(0U, snapshot, common::crc32c(snapshot)).is_ok());
  EXPECT_TRUE(movement->finish_snapshot().is_ok());
  EXPECT_EQ(movement->mark_caught_up(19U).code(), common::StatusCode::kUnavailable);
  EXPECT_TRUE(movement->mark_caught_up(20U).is_ok());
  EXPECT_FALSE(movement->remove_source(7U, 8U).is_ok());
  EXPECT_FALSE(movement->promote_target(6U, 7U).is_ok());
  EXPECT_TRUE(movement->promote_target(7U, 8U).is_ok());
  auto promoted = movement->record();
  EXPECT_TRUE(std::ranges::find(promoted.voting_replicas, 1U) != promoted.voting_replicas.end());
  EXPECT_TRUE(std::ranges::find(promoted.voting_replicas, 4U) != promoted.voting_replicas.end());
  EXPECT_TRUE(movement->remove_source(8U, 9U).is_ok());
  const auto complete = movement->record();
  EXPECT_EQ(complete.phase, TabletMovementPhase::kComplete);
  EXPECT_TRUE(std::ranges::find(complete.voting_replicas, 1U) == complete.voting_replicas.end());
  EXPECT_TRUE(std::ranges::find(complete.voting_replicas, 4U) != complete.voting_replicas.end());
}

TEST(TabletMovementTest, RejectsCorruptAndGappedSnapshotChunks) {
  auto movement = TabletMovement::begin(tablet_id(), 1U, 1U, 4U, {1U, 2U, 3U});
  ASSERT_TRUE(movement.has_value());
  const std::vector<std::byte> snapshot{std::byte{1U}, std::byte{2U}};
  ASSERT_TRUE(
      movement->begin_snapshot({1U, 1U, 1U, snapshot.size(), common::crc32c(snapshot)}).is_ok());
  EXPECT_FALSE(movement->accept_snapshot_chunk(1U, snapshot, common::crc32c(snapshot)).is_ok());
  EXPECT_FALSE(movement->accept_snapshot_chunk(0U, snapshot, 0U).is_ok());
}

} // namespace
} // namespace chronos::raft
