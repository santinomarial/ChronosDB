#include "chronos/common/crc32c.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/tablet_reconfiguration.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <optional>
#include <variant>
#include <vector>

namespace chronos::raft {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] GroupId group(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return GroupId{bytes};
}

[[nodiscard]] TabletMovement ready_movement(const std::uint64_t placement_epoch = 7U) {
  auto movement =
      TabletMovement::begin(id<schema::TabletId>(3U), placement_epoch, 1U, 4U, {1U, 2U, 3U});
  EXPECT_TRUE(movement.has_value());
  const std::vector<std::byte> snapshot{std::byte{1U}, std::byte{2U}};
  EXPECT_TRUE(
      movement->begin_snapshot({9U, 5U, 1U, snapshot.size(), common::crc32c(snapshot)}).is_ok());
  EXPECT_TRUE(movement->accept_snapshot_chunk(0U, snapshot, common::crc32c(snapshot)).is_ok());
  EXPECT_TRUE(movement->finish_snapshot().is_ok());
  EXPECT_TRUE(movement->mark_caught_up(5U).is_ok());
  return std::move(*movement);
}

void execute_raft_action(RaftNode& node, TabletReconfigurationAction action) {
  if (const auto* begin = std::get_if<BeginMembershipChangeOperation>(&action.request.operation)) {
    auto transition = node.begin_membership_change(begin->new_voters);
    ASSERT_TRUE(transition.has_value()) << transition.error().to_string();
    return;
  }
  ASSERT_TRUE(std::holds_alternative<FinalizeMembershipChangeOperation>(action.request.operation));
  auto transition = node.finalize_membership_change();
  ASSERT_TRUE(transition.has_value()) << transition.error().to_string();
}

void commit_with(RaftNode& node, const LogIndex index, const NodeId first, const NodeId second) {
  auto one = node.receive(first, AppendEntriesResponse{1U, true, index, std::nullopt, 0U});
  ASSERT_TRUE(one.has_value()) << one.error().to_string();
  auto two = node.receive(second, AppendEntriesResponse{1U, true, index, std::nullopt, 0U});
  ASSERT_TRUE(two.has_value()) << two.error().to_string();
  ASSERT_EQ(node.commit_index(), index);
}

void apply_placement_action(MetadataStateMachine& metadata, const LogIndex index,
                            const TabletReconfigurationAction& action) {
  ASSERT_EQ(action.kind, TabletReconfigurationActionKind::kPublishPlacement);
  const auto* proposal = std::get_if<ProposeOperation>(&action.request.operation);
  ASSERT_NE(proposal, nullptr);
  ASSERT_EQ(proposal->type, kRaftMetadataCommandEntryType);
  auto decoded = decode_metadata_command_v1(proposal->payload);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_TRUE(metadata.apply_committed(index, std::move(*decoded)).is_ok());
}

TEST(TabletReconfigurationTest, CommitsJointMembershipBeforeEachPlacementEpoch) {
  const GroupId tablet_group = group(10U);
  const GroupId metadata_group = group(11U);
  const auto table = id<schema::TableId>(2U);
  const auto tablet = id<schema::TabletId>(3U);
  auto metadata = MetadataStateMachine::create();
  ASSERT_TRUE(metadata.has_value());
  ASSERT_TRUE(
      metadata->apply_committed(1U, TabletPlacementMetadata{table, tablet, 7U, {1U, 2U, 3U}, 1U})
          .is_ok());
  auto node = RaftNode::create(1U, {1U, 2U, 3U});
  ASSERT_TRUE(node.has_value());
  ASSERT_TRUE(node->start_election().has_value());
  ASSERT_TRUE(node->receive(2U, RequestVoteResponse{1U, true}).has_value());
  auto coordinator = TabletReconfigurationCoordinator::create(tablet_group, metadata_group, table,
                                                              ready_movement(), 1U);
  ASSERT_TRUE(coordinator.has_value()) << coordinator.error().to_string();

  auto begin_promotion = coordinator->reconcile(*node, *metadata);
  ASSERT_TRUE(begin_promotion.has_value());
  ASSERT_TRUE(begin_promotion->has_value());
  EXPECT_EQ((*begin_promotion)->kind, TabletReconfigurationActionKind::kBeginJointMembership);
  EXPECT_EQ((*begin_promotion)->id,
            (TabletReconfigurationActionId{
                tablet, 7U, TabletReconfigurationActionKind::kBeginJointMembership}));
  EXPECT_EQ((*begin_promotion)->request.group_id, tablet_group);
  execute_raft_action(*node, std::move(**begin_promotion));
  auto waiting = coordinator->reconcile(*node, *metadata);
  ASSERT_TRUE(waiting.has_value());
  EXPECT_FALSE(waiting->has_value());
  commit_with(*node, 1U, 2U, 4U);

  auto finalize_promotion = coordinator->reconcile(*node, *metadata);
  ASSERT_TRUE(finalize_promotion.has_value());
  ASSERT_TRUE(finalize_promotion->has_value());
  EXPECT_EQ((*finalize_promotion)->kind, TabletReconfigurationActionKind::kFinalizeJointMembership);
  EXPECT_EQ((*finalize_promotion)->id,
            (TabletReconfigurationActionId{
                tablet, 7U, TabletReconfigurationActionKind::kFinalizeJointMembership}));
  execute_raft_action(*node, std::move(**finalize_promotion));
  commit_with(*node, 2U, 2U, 4U);

  auto publish_promotion = coordinator->reconcile(*node, *metadata);
  ASSERT_TRUE(publish_promotion.has_value());
  ASSERT_TRUE(publish_promotion->has_value());
  EXPECT_EQ((*publish_promotion)->request.group_id, metadata_group);
  EXPECT_EQ((*publish_promotion)->id,
            (TabletReconfigurationActionId{tablet, 7U,
                                           TabletReconfigurationActionKind::kPublishPlacement}));
  apply_placement_action(*metadata, 2U, **publish_promotion);
  EXPECT_EQ(metadata->find_tablet(tablet)->placement_epoch, 8U);
  EXPECT_EQ(metadata->find_tablet(tablet)->replicas, (std::vector<NodeId>{1U, 2U, 3U, 4U}));

  auto begin_removal = coordinator->reconcile(*node, *metadata);
  ASSERT_TRUE(begin_removal.has_value());
  ASSERT_TRUE(begin_removal->has_value());
  EXPECT_EQ(coordinator->record().phase, TabletMovementPhase::kTargetPromoted);
  EXPECT_EQ((*begin_removal)->id,
            (TabletReconfigurationActionId{
                tablet, 8U, TabletReconfigurationActionKind::kBeginJointMembership}));
  execute_raft_action(*node, std::move(**begin_removal));
  commit_with(*node, 3U, 2U, 3U);
  auto finalize_removal = coordinator->reconcile(*node, *metadata);
  ASSERT_TRUE(finalize_removal.has_value());
  ASSERT_TRUE(finalize_removal->has_value());
  EXPECT_EQ((*finalize_removal)->id,
            (TabletReconfigurationActionId{
                tablet, 8U, TabletReconfigurationActionKind::kFinalizeJointMembership}));
  execute_raft_action(*node, std::move(**finalize_removal));
  commit_with(*node, 4U, 2U, 3U);
  EXPECT_EQ(node->role(), Role::kFollower);

  auto publish_removal = coordinator->reconcile(*node, *metadata);
  ASSERT_TRUE(publish_removal.has_value());
  ASSERT_TRUE(publish_removal->has_value());
  EXPECT_EQ((*publish_removal)->id,
            (TabletReconfigurationActionId{tablet, 8U,
                                           TabletReconfigurationActionKind::kPublishPlacement}));
  apply_placement_action(*metadata, 3U, **publish_removal);
  EXPECT_EQ(metadata->find_tablet(tablet)->placement_epoch, 9U);
  EXPECT_EQ(metadata->find_tablet(tablet)->replicas, (std::vector<NodeId>{2U, 3U, 4U}));
  EXPECT_FALSE(metadata->find_tablet(tablet)->leader_hint.has_value());

  auto completed = coordinator->reconcile(*node, *metadata);
  ASSERT_TRUE(completed.has_value());
  EXPECT_FALSE(completed->has_value());
  EXPECT_EQ(coordinator->record().phase, TabletMovementPhase::kComplete);
  EXPECT_EQ(coordinator->record().placement_epoch, 9U);
}

TEST(TabletReconfigurationTest, ReconstructsTheSamePendingActionIdentityAfterRestart) {
  const GroupId tablet_group = group(10U);
  const GroupId metadata_group = group(11U);
  const auto table = id<schema::TableId>(2U);
  const auto tablet = id<schema::TabletId>(3U);
  auto metadata = MetadataStateMachine::create();
  ASSERT_TRUE(metadata.has_value());
  ASSERT_TRUE(
      metadata->apply_committed(1U, TabletPlacementMetadata{table, tablet, 7U, {1U, 2U, 3U}, 1U})
          .is_ok());
  auto node = RaftNode::create(1U, {1U, 2U, 3U});
  ASSERT_TRUE(node.has_value());
  ASSERT_TRUE(node->start_election().has_value());
  ASSERT_TRUE(node->receive(2U, RequestVoteResponse{1U, true}).has_value());

  TabletMovement before_restart = ready_movement();
  const TabletMovementRecord durable_record = before_restart.record();
  const std::vector<std::byte> durable_snapshot{before_restart.received_snapshot().begin(),
                                                before_restart.received_snapshot().end()};
  auto first = TabletReconfigurationCoordinator::create(tablet_group, metadata_group, table,
                                                        std::move(before_restart), 1U);
  ASSERT_TRUE(first.has_value());
  auto first_action = first->reconcile(*node, *metadata);
  ASSERT_TRUE(first_action.has_value());
  ASSERT_TRUE(first_action->has_value());

  auto recovered = TabletMovement::recover(durable_record, durable_snapshot);
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  auto restarted = TabletReconfigurationCoordinator::create(tablet_group, metadata_group, table,
                                                            std::move(*recovered), 1U);
  ASSERT_TRUE(restarted.has_value());
  auto retried = restarted->reconcile(*node, *metadata);
  ASSERT_TRUE(retried.has_value());
  ASSERT_TRUE(retried->has_value());

  EXPECT_EQ((*retried)->id, (*first_action)->id);
  EXPECT_EQ((*retried)->kind, (*first_action)->kind);
  EXPECT_EQ((*retried)->request.group_id, (*first_action)->request.group_id);
}

TEST(TabletReconfigurationTest, ResumesSourceRemovalAndCompleteStateAfterRestart) {
  const GroupId tablet_group = group(10U);
  const GroupId metadata_group = group(11U);
  const auto table = id<schema::TableId>(2U);
  const auto tablet = id<schema::TabletId>(3U);

  auto promoted = ready_movement();
  ASSERT_TRUE(promoted.promote_target(7U, 8U).is_ok());
  auto promoted_metadata = MetadataStateMachine::create();
  ASSERT_TRUE(promoted_metadata.has_value());
  ASSERT_TRUE(
      promoted_metadata
          ->apply_committed(1U, TabletPlacementMetadata{table, tablet, 8U, {1U, 2U, 3U, 4U}, 2U})
          .is_ok());
  auto promoted_node = RaftNode::create(2U, {1U, 2U, 3U, 4U});
  ASSERT_TRUE(promoted_node.has_value());
  ASSERT_TRUE(promoted_node->start_election().has_value());
  ASSERT_TRUE(promoted_node->receive(1U, RequestVoteResponse{1U, true}).has_value());
  ASSERT_TRUE(promoted_node->receive(3U, RequestVoteResponse{1U, true}).has_value());
  ASSERT_EQ(promoted_node->role(), Role::kLeader);
  auto resumed = TabletReconfigurationCoordinator::create(tablet_group, metadata_group, table,
                                                          std::move(promoted), 2U);
  ASSERT_TRUE(resumed.has_value()) << resumed.error().to_string();
  auto removal = resumed->reconcile(*promoted_node, *promoted_metadata);
  ASSERT_TRUE(removal.has_value()) << removal.error().to_string();
  ASSERT_TRUE(removal->has_value());
  EXPECT_EQ((*removal)->kind, TabletReconfigurationActionKind::kBeginJointMembership);
  const auto* begin = std::get_if<BeginMembershipChangeOperation>(&(*removal)->request.operation);
  ASSERT_NE(begin, nullptr);
  EXPECT_EQ(begin->new_voters, (std::vector<NodeId>{2U, 3U, 4U}));
  EXPECT_EQ((*removal)->id,
            (TabletReconfigurationActionId{
                tablet, 8U, TabletReconfigurationActionKind::kBeginJointMembership}));

  auto complete = ready_movement();
  ASSERT_TRUE(complete.promote_target(7U, 8U).is_ok());
  ASSERT_TRUE(complete.remove_source(8U, 9U).is_ok());
  auto complete_metadata = MetadataStateMachine::create();
  ASSERT_TRUE(complete_metadata.has_value());
  ASSERT_TRUE(
      complete_metadata
          ->apply_committed(1U, TabletPlacementMetadata{table, tablet, 9U, {2U, 3U, 4U}, 2U})
          .is_ok());
  auto complete_node = RaftNode::create(2U, {2U, 3U, 4U});
  ASSERT_TRUE(complete_node.has_value());
  auto terminal = TabletReconfigurationCoordinator::create(tablet_group, metadata_group, table,
                                                           std::move(complete), 2U);
  ASSERT_TRUE(terminal.has_value()) << terminal.error().to_string();
  auto no_action = terminal->reconcile(*complete_node, *complete_metadata);
  ASSERT_TRUE(no_action.has_value()) << no_action.error().to_string();
  EXPECT_FALSE(no_action->has_value());
  EXPECT_EQ(terminal->record().phase, TabletMovementPhase::kComplete);

  const std::uint64_t maximum_epoch = std::numeric_limits<std::uint64_t>::max();
  auto maximum_promoted = ready_movement(maximum_epoch - 2U);
  ASSERT_TRUE(maximum_promoted.promote_target(maximum_epoch - 2U, maximum_epoch - 1U).is_ok());
  auto maximum_promoted_coordinator = TabletReconfigurationCoordinator::create(
      tablet_group, metadata_group, table, std::move(maximum_promoted));
  EXPECT_TRUE(maximum_promoted_coordinator.has_value())
      << maximum_promoted_coordinator.error().to_string();
  auto maximum_complete = ready_movement(maximum_epoch - 2U);
  ASSERT_TRUE(maximum_complete.promote_target(maximum_epoch - 2U, maximum_epoch - 1U).is_ok());
  ASSERT_TRUE(maximum_complete.remove_source(maximum_epoch - 1U, maximum_epoch).is_ok());
  auto maximum_complete_coordinator = TabletReconfigurationCoordinator::create(
      tablet_group, metadata_group, table, std::move(maximum_complete));
  EXPECT_TRUE(maximum_complete_coordinator.has_value())
      << maximum_complete_coordinator.error().to_string();
}

TEST(TabletReconfigurationTest, RejectsDivergentPlacementAndJointIntent) {
  const auto table = id<schema::TableId>(2U);
  const auto tablet = id<schema::TabletId>(3U);
  auto metadata = MetadataStateMachine::create();
  ASSERT_TRUE(metadata.has_value());
  ASSERT_TRUE(
      metadata->apply_committed(1U, TabletPlacementMetadata{table, tablet, 8U, {1U, 2U, 3U}, 1U})
          .is_ok());
  auto node = RaftNode::create(1U, {1U, 2U, 3U});
  ASSERT_TRUE(node.has_value());
  auto coordinator =
      TabletReconfigurationCoordinator::create(group(10U), group(11U), table, ready_movement(), 1U);
  ASSERT_TRUE(coordinator.has_value());

  auto divergent = coordinator->reconcile(*node, *metadata);

  ASSERT_FALSE(divergent.has_value());
  EXPECT_EQ(divergent.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(coordinator->record().phase, TabletMovementPhase::kReady);
}

} // namespace
} // namespace chronos::raft
