#include "chronos/ingest/tablet_movement_raft_snapshot_completion.hpp"

#include <new>
#include <optional>
#include <stdexcept>
#include <utility>

namespace chronos::ingest {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

} // namespace

common::Result<TabletMovementRaftSnapshotCompletionReport>
complete_recovered_tablet_movement_raft_snapshot(
    const raft::RecoveredTabletMovementGeneration& recovered,
    const schema::TableId expected_table_id, RaftTabletSnapshotStorage& snapshot_storage,
    const raft::GroupSnapshotInstall& pending, raft::DurableMultiRaftRuntime& runtime,
    const RaftTabletSnapshotCodecLimits codec_limits) {
  try {
    const raft::TabletMovementRecord movement = recovered.movement.record();
    if (movement.phase != raft::TabletMovementPhase::kCatchingUp) {
      return common::make_unexpected(
          invalid("Raft snapshot completion requires catching-up movement state"));
    }
    auto application = install_recovered_tablet_movement_snapshot(recovered, expected_table_id,
                                                                  snapshot_storage, codec_limits);
    if (!application.has_value())
      return common::make_unexpected(application.error());
    if (pending.group_id != application->group_id ||
        pending.install.source != movement.source_node ||
        pending.install.snapshot != application->raft_snapshot) {
      return common::make_unexpected(
          corruption("pending Raft snapshot differs from the durable movement RTAS"));
    }
    const raft::RaftNode* target = runtime.find_group(application->group_id);
    if (target == nullptr)
      return common::make_unexpected(invalid("target Raft group is absent"));
    if (target->node_id() != movement.target_node) {
      return common::make_unexpected(
          corruption("movement target differs from the local Raft group owner"));
    }

    auto completed = runtime.execute_batch(
        {{application->group_id, raft::CompleteSnapshotInstallOperation{
                                     pending.install.source, pending.install.snapshot, true}}});
    if (!completed.has_value())
      return common::make_unexpected(completed.error());
    if (completed->size() != 1U)
      return common::make_unexpected(corruption("Raft snapshot completion result count changed"));
    if (!completed->front().status.is_ok())
      return common::make_unexpected(completed->front().status);
    std::optional<raft::MultiRaftTransition> transition = std::move(completed->front().transition);
    if (!transition.has_value()) {
      return common::make_unexpected(corruption("Raft snapshot completion returned no transition"));
    }
    raft::MultiRaftTransition& completed_transition = transition.value();
    if (!completed_transition.persistence.has_value() ||
        completed_transition.persistence->group_id != application->group_id ||
        completed_transition.persistence->state.snapshot != application->raft_snapshot ||
        completed_transition.advanced_commit_index !=
            application->raft_snapshot.last_included_index ||
        completed_transition.outbound.size() != 1U) {
      return common::make_unexpected(
          corruption("Raft snapshot completion transition is inconsistent"));
    }
    raft::GroupOutboundMessage acknowledgement = std::move(completed_transition.outbound.front());
    const auto* response =
        std::get_if<raft::InstallSnapshotResponse>(&acknowledgement.outbound.message);
    const std::uint64_t durable_sequence = runtime.durable_physical_sequence();
    if (acknowledgement.group_id != application->group_id ||
        acknowledgement.source != movement.target_node ||
        acknowledgement.outbound.destination != movement.source_node || response == nullptr ||
        !response->success ||
        response->term != completed_transition.persistence->state.current_term ||
        response->last_included_index != application->raft_snapshot.last_included_index ||
        durable_sequence != completed_transition.persistence->physical_sequence) {
      return common::make_unexpected(
          corruption("Raft snapshot completion acknowledgement is inconsistent"));
    }
    target = runtime.find_group(application->group_id);
    if (target == nullptr || target->persistent_state().snapshot != application->raft_snapshot ||
        target->commit_index() < application->raft_snapshot.last_included_index ||
        target->applied_index() < application->raft_snapshot.last_included_index) {
      return common::make_unexpected(
          corruption("durable Raft state does not contain the completed snapshot"));
    }
    return TabletMovementRaftSnapshotCompletionReport{
        .application_snapshot = std::move(*application),
        .acknowledgement = std::move(acknowledgement),
        .durable_physical_sequence = durable_sequence};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft movement completion allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Raft movement completion exceeded container limits"));
  }
}

} // namespace chronos::ingest
