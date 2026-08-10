#include "chronos/common/crc32c.hpp"
#include "chronos/raft/tablet_movement_checkpoint.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::raft {
namespace {

[[nodiscard]] schema::TabletId tablet_id() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{1U};
  return schema::TabletId::from_bytes(bytes).value();
}

[[nodiscard]] TabletMovementCheckpoint checkpoint(const TabletMovement& movement) {
  return TabletMovementCheckpoint{movement.record(),
                                  std::vector<std::byte>{movement.received_snapshot().begin(),
                                                         movement.received_snapshot().end()}};
}

TEST(TabletMovementCheckpointTest, RoundTripsPartialTransferAndResumesExactly) {
  auto movement = TabletMovement::begin(tablet_id(), 7U, 1U, 4U, {1U, 2U, 3U});
  ASSERT_TRUE(movement.has_value());
  const std::vector<std::byte> snapshot{std::byte{1U}, std::byte{2U}, std::byte{3U}, std::byte{4U}};
  ASSERT_TRUE(
      movement->begin_snapshot({9U, 20U, 3U, snapshot.size(), common::crc32c(snapshot)}).is_ok());
  const common::ByteView prefix{snapshot.data(), 2U};
  ASSERT_TRUE(movement->accept_snapshot_chunk(0U, prefix, common::crc32c(prefix)).is_ok());

  auto encoded = encode_tablet_movement_checkpoint_v1(checkpoint(*movement));
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto decoded = decode_tablet_movement_checkpoint_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, checkpoint(*movement));
  auto recovered = TabletMovement::recover(decoded->record, decoded->received_snapshot);
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->record().phase, TabletMovementPhase::kTransferringSnapshot);
  EXPECT_EQ(recovered->record().received_bytes, 2U);

  const common::ByteView suffix{snapshot.data() + 2U, 2U};
  EXPECT_TRUE(recovered->accept_snapshot_chunk(2U, suffix, common::crc32c(suffix)).is_ok());
  EXPECT_TRUE(recovered->finish_snapshot().is_ok());
  EXPECT_TRUE(recovered->mark_caught_up(20U).is_ok());
  auto ready_encoded = encode_tablet_movement_checkpoint_v1(checkpoint(*recovered));
  ASSERT_TRUE(ready_encoded.has_value());
  auto ready = decode_tablet_movement_checkpoint_v1(*ready_encoded);
  ASSERT_TRUE(ready.has_value());
  EXPECT_EQ(ready->record.phase, TabletMovementPhase::kReady);
  EXPECT_EQ(ready->received_snapshot, snapshot);

  TabletMovementCheckpointGeneration generation{7U, checkpoint(*recovered)};
  auto generated = encode_tablet_movement_checkpoint_generation_v1(generation);
  ASSERT_TRUE(generated.has_value()) << generated.error().to_string();
  auto decoded_generation = decode_tablet_movement_checkpoint_generation_v1(*generated);
  ASSERT_TRUE(decoded_generation.has_value()) << decoded_generation.error().to_string();
  EXPECT_EQ(*decoded_generation, generation);
}

TEST(TabletMovementCheckpointTest, RoundTripsEmptyAndCompletePhases) {
  auto adding = TabletMovement::begin(tablet_id(), 1U, 1U, 4U, {1U, 2U, 3U});
  ASSERT_TRUE(adding.has_value());
  auto empty = encode_tablet_movement_checkpoint_v1(checkpoint(*adding));
  ASSERT_TRUE(empty.has_value());
  auto decoded_empty = decode_tablet_movement_checkpoint_v1(*empty);
  ASSERT_TRUE(decoded_empty.has_value()) << decoded_empty.error().to_string();
  EXPECT_EQ(decoded_empty->record.phase, TabletMovementPhase::kAddingTarget);

  const std::vector<std::byte> snapshot{std::byte{7U}};
  ASSERT_TRUE(adding->begin_snapshot({1U, 1U, 1U, 1U, common::crc32c(snapshot)}).is_ok());
  ASSERT_TRUE(adding->accept_snapshot_chunk(0U, snapshot, common::crc32c(snapshot)).is_ok());
  ASSERT_TRUE(adding->finish_snapshot().is_ok());
  ASSERT_TRUE(adding->mark_caught_up(1U).is_ok());
  ASSERT_TRUE(adding->promote_target(1U, 2U).is_ok());
  ASSERT_TRUE(adding->remove_source(2U, 3U).is_ok());
  auto complete = encode_tablet_movement_checkpoint_v1(checkpoint(*adding));
  ASSERT_TRUE(complete.has_value());
  auto decoded = decode_tablet_movement_checkpoint_v1(*complete);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->record.phase, TabletMovementPhase::kComplete);
  EXPECT_EQ(decoded->record.voting_replicas, (std::vector<NodeId>{2U, 3U, 4U}));
}

TEST(TabletMovementCheckpointTest, RejectsDamageTruncationAndInconsistentRecovery) {
  auto movement = TabletMovement::begin(tablet_id(), 1U, 1U, 4U, {1U, 2U, 3U});
  ASSERT_TRUE(movement.has_value());
  auto encoded = encode_tablet_movement_checkpoint_v1(checkpoint(*movement)).value();
  encoded[kTabletMovementCheckpointHeaderSize] ^= std::byte{1U};
  EXPECT_EQ(decode_tablet_movement_checkpoint_v1(encoded).error().code(),
            common::StatusCode::kCorruption);
  encoded.pop_back();
  EXPECT_EQ(decode_tablet_movement_checkpoint_v1(encoded).error().code(),
            common::StatusCode::kCorruption);

  TabletMovementRecord inconsistent = movement->record();
  inconsistent.learners.clear();
  EXPECT_EQ(TabletMovement::recover(std::move(inconsistent), {}).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(
      encode_tablet_movement_checkpoint_v1(checkpoint(*movement), {.maximum_checkpoint_bytes = 64U})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_EQ(
      encode_tablet_movement_checkpoint_generation_v1({0U, checkpoint(*movement)}).error().code(),
      common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::raft
