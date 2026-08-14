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

struct TabletRaftView {
  Role role;
  std::span<const NodeId> voters;
  std::span<const NodeId> committed_voters;
  std::span<const NodeId> joint_old_voters;
  std::span<const NodeId> joint_new_voters;
  bool joint_membership_active;
  bool joint_membership_can_finalize;
  bool final_membership_pending;
};

[[nodiscard]] TabletRaftView view(const RaftNode& node) noexcept {
  return TabletRaftView{node.role(),
                        node.voters(),
                        node.committed_voters(),
                        node.joint_old_voters(),
                        node.joint_new_voters(),
                        node.joint_membership_active(),
                        node.joint_membership_can_finalize(),
                        node.final_membership_pending()};
}

[[nodiscard]] TabletRaftView view(const RaftGroupObservation& observed) noexcept {
  return TabletRaftView{observed.role,
                        observed.voters,
                        observed.committed_voters,
                        observed.joint_old_voters,
                        observed.joint_new_voters,
                        observed.joint_membership_active,
                        observed.joint_membership_can_finalize,
                        observed.final_membership_pending};
}

[[nodiscard]] bool canonical_voters(const std::vector<NodeId>& voters) noexcept {
  return !voters.empty() && voters.front() != 0U && std::ranges::is_sorted(voters) &&
         std::adjacent_find(voters.begin(), voters.end()) == voters.end();
}

[[nodiscard]] bool valid_role(const Role role) noexcept {
  return role == Role::kFollower || role == Role::kCandidate || role == Role::kLeader;
}

[[nodiscard]] bool valid_observation(const RaftGroupObservation& observed,
                                     const GroupId& expected_group) noexcept {
  if (observed.group_id != expected_group || observed.node_id == 0U || !valid_role(observed.role) ||
      observed.applied_index > observed.commit_index ||
      observed.commit_index > observed.last_log_index || !canonical_voters(observed.voters) ||
      !canonical_voters(observed.committed_voters) ||
      (observed.leader_id.has_value() && *observed.leader_id == 0U) ||
      (observed.role == Role::kLeader &&
       (observed.current_term == 0U || observed.leader_id != observed.node_id))) {
    return false;
  }
  if (!observed.joint_membership_active) {
    return observed.joint_old_voters.empty() && observed.joint_new_voters.empty() &&
           observed.voters == observed.committed_voters &&
           !observed.joint_membership_can_finalize && !observed.final_membership_pending;
  }
  if (!canonical_voters(observed.joint_old_voters) ||
      !canonical_voters(observed.joint_new_voters) ||
      observed.committed_voters != observed.joint_old_voters ||
      (observed.joint_membership_can_finalize && observed.final_membership_pending)) {
    return false;
  }
  return std::ranges::includes(observed.voters, observed.joint_old_voters) &&
         std::ranges::includes(observed.voters, observed.joint_new_voters) &&
         std::ranges::all_of(observed.voters, [&](const NodeId voter) {
           return std::binary_search(observed.joint_old_voters.begin(),
                                     observed.joint_old_voters.end(), voter) ||
                  std::binary_search(observed.joint_new_voters.begin(),
                                     observed.joint_new_voters.end(), voter);
         });
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
  Impl(GroupId tablet_group, schema::TableId table, GroupId metadata_group, TabletMovement owner,
       const std::optional<NodeId> hint) noexcept
      : tablet_group_id(tablet_group), metadata_group_id(metadata_group), table_id(table),
        movement(std::move(owner)), leader_hint(hint) {}

  [[nodiscard]] common::Result<std::optional<TabletReconfigurationAction>>
  raft_action(const TabletMovementRecord& record, const TabletRaftView node,
              const std::vector<NodeId>& old_voters, const std::vector<NodeId>& new_voters) const {
    if (node.joint_membership_active) {
      if (!same_voters(node.joint_old_voters, old_voters) ||
          !same_voters(node.joint_new_voters, new_voters)) {
        return common::make_unexpected(
            corruption("tablet Raft joint configuration differs from movement intent"));
      }
      if (node.final_membership_pending || !node.joint_membership_can_finalize)
        return std::optional<TabletReconfigurationAction>{};
      if (node.role != Role::kLeader) {
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
    if (!same_voters(node.committed_voters, old_voters)) {
      return common::make_unexpected(
          corruption("tablet Raft stable configuration differs from movement intent"));
    }
    if (node.role != Role::kLeader) {
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

  [[nodiscard]] common::Result<std::optional<TabletReconfigurationAction>>
  reconcile(TabletRaftView tablet_group, const MetadataStateMachine& metadata);

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
  const bool resumable_phase = record.phase == TabletMovementPhase::kReady ||
                               record.phase == TabletMovementPhase::kTargetPromoted ||
                               record.phase == TabletMovementPhase::kComplete;
  const std::uint64_t maximum_epoch = std::numeric_limits<std::uint64_t>::max();
  const bool epoch_has_room = (record.phase == TabletMovementPhase::kReady &&
                               record.placement_epoch <= maximum_epoch - 2U) ||
                              (record.phase == TabletMovementPhase::kTargetPromoted &&
                               record.placement_epoch <= maximum_epoch - 1U) ||
                              record.phase == TabletMovementPhase::kComplete;
  if (tablet_group_id.is_nil() || metadata_group_id.is_nil() ||
      tablet_group_id == metadata_group_id || table_id.uuid().is_nil() || !resumable_phase ||
      !epoch_has_room ||
      (leader_hint.has_value() &&
       !std::binary_search(record.voting_replicas.begin(), record.voting_replicas.end(),
                           *leader_hint))) {
    return common::make_unexpected(
        invalid("tablet reconfiguration identities, phase, or leader hint are invalid"));
  }
  return TabletReconfigurationCoordinator{std::make_unique<Impl>(
      tablet_group_id, table_id, metadata_group_id, std::move(movement), leader_hint)};
}

common::Result<std::optional<TabletReconfigurationAction>>
TabletReconfigurationCoordinator::Impl::reconcile(const TabletRaftView tablet_group,
                                                  const MetadataStateMachine& metadata) {
  TabletMovementRecord record = movement.record();
  const TabletPlacementMetadata* placement = metadata.find_tablet(record.tablet_id);
  if (placement == nullptr || placement->table_id != table_id) {
    return common::make_unexpected(
        corruption("tablet movement has no matching committed metadata placement"));
  }

  if (record.phase == TabletMovementPhase::kReady) {
    const std::vector<NodeId> promoted = with_target(record);
    if (same_voters(tablet_group.committed_voters, promoted) &&
        !tablet_group.joint_membership_active) {
      if (placement->placement_epoch == record.placement_epoch + 1U &&
          placement->replicas == promoted) {
        const common::Status advanced =
            movement.promote_target(record.placement_epoch, record.placement_epoch + 1U);
        if (!advanced.is_ok())
          return common::make_unexpected(advanced);
        record = movement.record();
      } else if (placement->placement_epoch == record.placement_epoch &&
                 placement->replicas == record.voting_replicas) {
        return placement_action(record, promoted, record.placement_epoch + 1U);
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
      return raft_action(record, tablet_group, record.voting_replicas, promoted);
    }
  }

  if (record.phase == TabletMovementPhase::kTargetPromoted) {
    const std::vector<NodeId> final_voters = without_source(record);
    if (same_voters(tablet_group.committed_voters, final_voters) &&
        !tablet_group.joint_membership_active) {
      if (placement->placement_epoch == record.placement_epoch + 1U &&
          placement->replicas == final_voters) {
        const common::Status advanced =
            movement.remove_source(record.placement_epoch, record.placement_epoch + 1U);
        if (!advanced.is_ok())
          return common::make_unexpected(advanced);
        return std::optional<TabletReconfigurationAction>{};
      }
      if (placement->placement_epoch == record.placement_epoch &&
          placement->replicas == record.voting_replicas) {
        return placement_action(record, final_voters, record.placement_epoch + 1U);
      }
      return common::make_unexpected(corruption("source removal metadata is stale or divergent"));
    }
    if (placement->placement_epoch != record.placement_epoch ||
        placement->replicas != record.voting_replicas) {
      return common::make_unexpected(
          corruption("pre-removal metadata differs from promoted configuration"));
    }
    return raft_action(record, tablet_group, record.voting_replicas, final_voters);
  }

  if (record.phase == TabletMovementPhase::kComplete)
    return std::optional<TabletReconfigurationAction>{};
  return common::make_unexpected(
      invalid("tablet reconfiguration movement is not ready for membership reconciliation"));
}

common::Result<std::optional<TabletReconfigurationAction>>
TabletReconfigurationCoordinator::reconcile(const RaftNode& tablet_group,
                                            const MetadataStateMachine& metadata) {
  return impl_->reconcile(view(tablet_group), metadata);
}

common::Result<std::optional<TabletReconfigurationAction>>
TabletReconfigurationCoordinator::reconcile(const RaftGroupObservation& tablet_group,
                                            const MetadataStateMachine& metadata) {
  if (!valid_observation(tablet_group, impl_->tablet_group_id)) {
    return common::make_unexpected(
        invalid("tablet reconfiguration Raft observation is invalid or belongs to another group"));
  }
  return impl_->reconcile(view(tablet_group), metadata);
}

TabletMovementRecord TabletReconfigurationCoordinator::record() const {
  return impl_->movement.record();
}

TabletMovement TabletReconfigurationCoordinator::take_movement() && noexcept {
  return std::move(impl_->movement);
}

} // namespace chronos::raft
