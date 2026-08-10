#include "chronos/raft/tablet_reconfiguration.hpp"

#include "chronos/raft/metadata_codec.hpp"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return common::Status{common::StatusCode::kCorruption, message};
}

[[nodiscard]] bool same_voters(const std::span<const NodeId> observed,
                               const std::vector<NodeId>& expected) {
  return std::ranges::equal(observed, expected);
}

[[nodiscard]] std::vector<NodeId> with_target(const TabletMovementRecord& record) {
  std::vector<NodeId> result = record.voting_replicas;
  result.push_back(record.target_node);
  std::ranges::sort(result);
  return result;
}

[[nodiscard]] std::vector<NodeId> without_source(const TabletMovementRecord& record) {
  std::vector<NodeId> result = record.voting_replicas;
  const auto source = std::ranges::find(result, record.source_node);
  if (source != result.end())
    result.erase(source);
  return result;
}

} // namespace

class TabletReconfigurationCoordinator::Impl {
public:
  Impl(GroupId tablet_group, GroupId metadata_group, schema::TableId table, TabletMovement owner,
       const std::optional<NodeId> hint) noexcept
      : tablet_group_id(std::move(tablet_group)), metadata_group_id(std::move(metadata_group)),
        table_id(table), movement(std::move(owner)), leader_hint(hint) {}

  [[nodiscard]] common::Result<std::optional<TabletReconfigurationAction>>
  raft_action(const TabletMovementRecord& record, const RaftNode& node,
              const std::vector<NodeId>& old_voters, const std::vector<NodeId>& new_voters) const {
    if (node.joint_membership_active()) {
      if (!same_voters(node.joint_old_voters(), old_voters) ||
          !same_voters(node.joint_new_voters(), new_voters)) {
        return common::make_unexpected(
            corruption("tablet Raft joint configuration differs from movement intent"));
      }
      if (node.final_membership_pending() || !node.joint_membership_can_finalize())
        return std::optional<TabletReconfigurationAction>{};
      if (node.role() != Role::kLeader) {
        return common::make_unexpected(common::Status{
            common::StatusCode::kUnavailable,
            "tablet reconfiguration finalization requires the current group leader"});
      }
      return std::optional<TabletReconfigurationAction>{TabletReconfigurationAction{
          {record.tablet_id, record.placement_epoch,
           TabletReconfigurationActionKind::kFinalizeJointMembership},
          TabletReconfigurationActionKind::kFinalizeJointMembership,
          DurableRaftRequest{tablet_group_id, FinalizeMembershipChangeOperation{}}}};
    }
    if (!same_voters(node.committed_voters(), old_voters)) {
      return common::make_unexpected(
          corruption("tablet Raft stable configuration differs from movement intent"));
    }
    if (node.role() != Role::kLeader) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kUnavailable,
                         "tablet reconfiguration start requires the current group leader"});
    }
    return std::optional<TabletReconfigurationAction>{TabletReconfigurationAction{
        {record.tablet_id, record.placement_epoch,
         TabletReconfigurationActionKind::kBeginJointMembership},
        TabletReconfigurationActionKind::kBeginJointMembership,
        DurableRaftRequest{tablet_group_id, BeginMembershipChangeOperation{new_voters}}}};
  }

  [[nodiscard]] common::Result<std::optional<TabletReconfigurationAction>>
  placement_action(const TabletMovementRecord& record, const std::vector<NodeId>& replicas,
                   const std::uint64_t epoch) const {
    std::optional<NodeId> hint = leader_hint;
    if (hint.has_value() && !std::binary_search(replicas.begin(), replicas.end(), *hint))
      hint.reset();
    auto payload = encode_metadata_command_v1(
        TabletPlacementMetadata{table_id, record.tablet_id, epoch, replicas, hint});
    if (!payload.has_value())
      return common::make_unexpected(payload.error());
    return std::optional<TabletReconfigurationAction>{TabletReconfigurationAction{
        {record.tablet_id, record.placement_epoch,
         TabletReconfigurationActionKind::kPublishPlacement},
        TabletReconfigurationActionKind::kPublishPlacement,
        DurableRaftRequest{metadata_group_id,
                           ProposeOperation{kRaftMetadataCommandEntryType, std::move(*payload)}}}};
  }

  GroupId tablet_group_id;
  GroupId metadata_group_id;
  schema::TableId table_id;
  TabletMovement movement;
  std::optional<NodeId> leader_hint;
};

TabletReconfigurationCoordinator::TabletReconfigurationCoordinator(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
TabletReconfigurationCoordinator::~TabletReconfigurationCoordinator() = default;
TabletReconfigurationCoordinator::TabletReconfigurationCoordinator(
    TabletReconfigurationCoordinator&&) noexcept = default;
TabletReconfigurationCoordinator&
TabletReconfigurationCoordinator::operator=(TabletReconfigurationCoordinator&&) noexcept = default;

common::Result<TabletReconfigurationCoordinator>
TabletReconfigurationCoordinator::create(GroupId tablet_group_id, GroupId metadata_group_id,
                                         const schema::TableId table_id, TabletMovement movement,
                                         const std::optional<NodeId> leader_hint) {
  const TabletMovementRecord record = movement.record();
  if (tablet_group_id.is_nil() || metadata_group_id.is_nil() ||
      tablet_group_id == metadata_group_id || table_id.uuid().is_nil() ||
      record.phase != TabletMovementPhase::kReady ||
      record.placement_epoch > std::numeric_limits<std::uint64_t>::max() - 2U ||
      (leader_hint.has_value() &&
       !std::binary_search(record.voting_replicas.begin(), record.voting_replicas.end(),
                           *leader_hint))) {
    return common::make_unexpected(
        invalid("tablet reconfiguration identities, phase, or leader hint are invalid"));
  }
  return TabletReconfigurationCoordinator{
      std::make_unique<Impl>(std::move(tablet_group_id), std::move(metadata_group_id), table_id,
                             std::move(movement), leader_hint)};
}

common::Result<std::optional<TabletReconfigurationAction>>
TabletReconfigurationCoordinator::reconcile(const RaftNode& tablet_group,
                                            const MetadataStateMachine& metadata) {
  TabletMovementRecord record = impl_->movement.record();
  const TabletPlacementMetadata* placement = metadata.find_tablet(record.tablet_id);
  if (placement == nullptr || placement->table_id != impl_->table_id) {
    return common::make_unexpected(
        corruption("tablet movement has no matching committed metadata placement"));
  }

  if (record.phase == TabletMovementPhase::kReady) {
    const std::vector<NodeId> promoted = with_target(record);
    if (same_voters(tablet_group.committed_voters(), promoted) &&
        !tablet_group.joint_membership_active()) {
      if (placement->placement_epoch == record.placement_epoch + 1U &&
          placement->replicas == promoted) {
        const common::Status advanced =
            impl_->movement.promote_target(record.placement_epoch, record.placement_epoch + 1U);
        if (!advanced.is_ok())
          return common::make_unexpected(advanced);
        record = impl_->movement.record();
      } else if (placement->placement_epoch == record.placement_epoch &&
                 placement->replicas == record.voting_replicas) {
        return impl_->placement_action(record, promoted, record.placement_epoch + 1U);
      } else {
        return common::make_unexpected(
            corruption("target promotion metadata is stale or divergent"));
      }
    } else {
      if (placement->placement_epoch != record.placement_epoch ||
          placement->replicas != record.voting_replicas) {
        return common::make_unexpected(
            corruption("pre-promotion metadata differs from movement source configuration"));
      }
      return impl_->raft_action(record, tablet_group, record.voting_replicas, promoted);
    }
  }

  if (record.phase == TabletMovementPhase::kTargetPromoted) {
    const std::vector<NodeId> final_voters = without_source(record);
    if (same_voters(tablet_group.committed_voters(), final_voters) &&
        !tablet_group.joint_membership_active()) {
      if (placement->placement_epoch == record.placement_epoch + 1U &&
          placement->replicas == final_voters) {
        const common::Status advanced =
            impl_->movement.remove_source(record.placement_epoch, record.placement_epoch + 1U);
        if (!advanced.is_ok())
          return common::make_unexpected(advanced);
        return std::optional<TabletReconfigurationAction>{};
      }
      if (placement->placement_epoch == record.placement_epoch &&
          placement->replicas == record.voting_replicas) {
        return impl_->placement_action(record, final_voters, record.placement_epoch + 1U);
      }
      return common::make_unexpected(corruption("source removal metadata is stale or divergent"));
    }
    if (placement->placement_epoch != record.placement_epoch ||
        placement->replicas != record.voting_replicas) {
      return common::make_unexpected(
          corruption("pre-removal metadata differs from promoted configuration"));
    }
    return impl_->raft_action(record, tablet_group, record.voting_replicas, final_voters);
  }

  if (record.phase == TabletMovementPhase::kComplete)
    return std::optional<TabletReconfigurationAction>{};
  return common::make_unexpected(
      invalid("tablet reconfiguration movement is not ready for membership reconciliation"));
}

TabletMovementRecord TabletReconfigurationCoordinator::record() const {
  return impl_->movement.record();
}

} // namespace chronos::raft
