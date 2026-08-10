#include "chronos/raft/durable_tablet_reconfiguration.hpp"

#include <limits>
#include <new>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::raft {

class detail::PreparedTabletReconfigurationDispatchFactory {
public:
  [[nodiscard]] static PreparedTabletReconfigurationDispatch
  create(TabletReconfigurationAction action,
         PreparedTabletReconfigurationAction preparation) noexcept {
    return PreparedTabletReconfigurationDispatch{std::move(action), std::move(preparation)};
  }
};

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

[[nodiscard]] common::Status unsupported(const char* message) {
  return {common::StatusCode::kNotSupported, message};
}

[[nodiscard]] bool allowed_phase_change(const TabletMovementRecord& before,
                                        const TabletMovementRecord& after) noexcept {
  if (before.phase == TabletMovementPhase::kReady &&
      after.phase == TabletMovementPhase::kTargetPromoted) {
    return before.placement_epoch != std::numeric_limits<std::uint64_t>::max() &&
           after.placement_epoch == before.placement_epoch + 1U;
  }
  if (before.phase == TabletMovementPhase::kTargetPromoted &&
      after.phase == TabletMovementPhase::kComplete) {
    return before.placement_epoch != std::numeric_limits<std::uint64_t>::max() &&
           after.placement_epoch == before.placement_epoch + 1U;
  }
  return false;
}

[[nodiscard]] std::uint64_t session_epoch(const TabletMovementRecord& record) noexcept {
  switch (record.phase) {
  case TabletMovementPhase::kTargetPromoted:
    return record.placement_epoch - 1U;
  case TabletMovementPhase::kComplete:
    return record.placement_epoch - 2U;
  default:
    return record.placement_epoch;
  }
}

[[nodiscard]] common::Status
validate_prepared_dispatch(const PreparedTabletReconfigurationDispatch& dispatch) {
  if (!dispatch.is_valid())
    return invalid("prepared reconfiguration dispatch was moved from");
  if (dispatch.action().id != dispatch.preparation().id)
    return corruption("prepared reconfiguration dispatch identity is inconsistent");
  return common::Status::ok();
}

[[nodiscard]] DurableRaftRequest
request_for_execution(const PreparedTabletReconfigurationDispatch& dispatch) {
  DurableRaftRequest request = dispatch.action().request;
  if (auto* proposal = std::get_if<ProposeOperation>(&request.operation); proposal != nullptr) {
    request.operation = ProposeExactRetainedOperation{proposal->type, std::move(proposal->payload)};
  }
  return request;
}

[[nodiscard]] common::Result<DurableTabletReconfigurationResult>
reconcile_impl(RecoveredTabletMovementGeneration& recovered, GroupId tablet_group_id,
               GroupId metadata_group_id, const schema::TableId table_id,
               const RaftNode& tablet_group, const MetadataStateMachine& metadata,
               TabletMovementCheckpointStorage& checkpoint_storage,
               const TabletMovementSnapshotChunkStorage* const chunk_storage,
               const std::optional<NodeId> leader_hint, const TabletMovementLimits limits) {
  const TabletMovementRecord before = recovered.movement.record();
  const common::ByteView received = recovered.movement.received_snapshot();
  std::vector<std::byte> candidate_bytes{received.begin(), received.end()};
  auto candidate_movement = TabletMovement::recover(before, std::move(candidate_bytes), limits);
  if (!candidate_movement.has_value())
    return common::make_unexpected(candidate_movement.error());
  auto candidate = TabletReconfigurationCoordinator::create(
      std::move(tablet_group_id), std::move(metadata_group_id), table_id,
      std::move(*candidate_movement), leader_hint);
  if (!candidate.has_value())
    return common::make_unexpected(candidate.error());
  auto action = candidate->reconcile(tablet_group, metadata);
  if (!action.has_value())
    return common::make_unexpected(action.error());
  const TabletMovementRecord after = candidate->record();
  if (after == before) {
    return DurableTabletReconfigurationResult{.action = std::move(*action),
                                              .installed_checkpoint = std::nullopt};
  }
  if (!allowed_phase_change(before, after)) {
    return common::make_unexpected(
        corruption("reconfiguration candidate changed movement unexpectedly"));
  }
  if (recovered.checkpoint_generation == std::numeric_limits<std::uint64_t>::max())
    return common::make_unexpected(exhausted("tablet movement generation is exhausted"));
  if (recovered.used_external_prefix && chunk_storage == nullptr) {
    return common::make_unexpected(
        unsupported("external reconfiguration requires its durable chunk owner"));
  }

  const std::uint64_t next_generation = recovered.checkpoint_generation + 1U;
  common::Result<InstalledTabletMovementCheckpoint> installed =
      common::make_unexpected(invalid("movement checkpoint representation is invalid"));
  if (recovered.used_external_prefix) {
    installed = install_verified_tablet_movement_reference(
        checkpoint_storage, *chunk_storage,
        TabletMovementCheckpointReferenceGeneration{
            next_generation, TabletMovementCheckpointReference{after, session_epoch(after)}},
        limits);
  } else {
    installed = checkpoint_storage.install(TabletMovementCheckpointGeneration{
        next_generation,
        TabletMovementCheckpoint{after, std::vector<std::byte>{received.begin(), received.end()}}});
  }
  if (!installed.has_value())
    return common::make_unexpected(installed.error());

  recovered.movement = std::move(*candidate).take_movement();
  recovered.checkpoint_generation = next_generation;
  return DurableTabletReconfigurationResult{.action = std::move(*action),
                                            .installed_checkpoint = std::move(*installed)};
}

[[nodiscard]] common::Result<PreparedDurableTabletReconfigurationResult>
prepare_dispatch(common::Result<DurableTabletReconfigurationResult> reconciled,
                 TabletReconfigurationActionLedger& action_ledger) {
  if (!reconciled.has_value())
    return common::make_unexpected(std::move(reconciled).error());
  if (!reconciled->action.has_value()) {
    return PreparedDurableTabletReconfigurationResult{
        .dispatch = std::nullopt,
        .installed_checkpoint = std::move(reconciled->installed_checkpoint)};
  }
  auto prepared = action_ledger.prepare(*reconciled->action);
  if (!prepared.has_value())
    return common::make_unexpected(std::move(prepared).error());
  return PreparedDurableTabletReconfigurationResult{
      .dispatch = detail::PreparedTabletReconfigurationDispatchFactory::create(
          std::move(*reconciled->action), std::move(*prepared)),
      .installed_checkpoint = std::move(reconciled->installed_checkpoint)};
}

} // namespace

PreparedTabletReconfigurationDispatch::PreparedTabletReconfigurationDispatch(
    TabletReconfigurationAction action, PreparedTabletReconfigurationAction preparation) noexcept
    : action_(std::move(action)), preparation_(std::move(preparation)) {}

PreparedTabletReconfigurationDispatch::PreparedTabletReconfigurationDispatch(
    PreparedTabletReconfigurationDispatch&& other) noexcept
    : action_(std::move(other.action_)), preparation_(std::move(other.preparation_)),
      valid_(std::exchange(other.valid_, false)) {}

PreparedTabletReconfigurationDispatch& PreparedTabletReconfigurationDispatch::operator=(
    PreparedTabletReconfigurationDispatch&& other) noexcept {
  if (this != &other) {
    action_ = std::move(other.action_);
    preparation_ = std::move(other.preparation_);
    valid_ = std::exchange(other.valid_, false);
  }
  return *this;
}

bool PreparedTabletReconfigurationDispatch::is_valid() const noexcept {
  return valid_;
}

const TabletReconfigurationAction& PreparedTabletReconfigurationDispatch::action() const noexcept {
  return action_;
}

const PreparedTabletReconfigurationAction&
PreparedTabletReconfigurationDispatch::preparation() const noexcept {
  return preparation_;
}

common::Result<DurableTabletReconfigurationResult> reconcile_durable_tablet_reconfiguration(
    RecoveredTabletMovementGeneration& recovered, GroupId tablet_group_id,
    GroupId metadata_group_id, const schema::TableId table_id, const RaftNode& tablet_group,
    const MetadataStateMachine& metadata, TabletMovementCheckpointStorage& checkpoint_storage,
    const std::optional<NodeId> leader_hint, const TabletMovementLimits limits) {
  try {
    return reconcile_impl(recovered, std::move(tablet_group_id), std::move(metadata_group_id),
                          table_id, tablet_group, metadata, checkpoint_storage, nullptr,
                          leader_hint, limits);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("durable reconfiguration allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("durable reconfiguration exceeded container limits"));
  }
}

common::Result<DurableTabletReconfigurationResult> reconcile_durable_tablet_reconfiguration(
    RecoveredTabletMovementGeneration& recovered, GroupId tablet_group_id,
    GroupId metadata_group_id, const schema::TableId table_id, const RaftNode& tablet_group,
    const MetadataStateMachine& metadata, TabletMovementCheckpointStorage& checkpoint_storage,
    const TabletMovementSnapshotChunkStorage& chunk_storage,
    const std::optional<NodeId> leader_hint, const TabletMovementLimits limits) {
  try {
    return reconcile_impl(recovered, std::move(tablet_group_id), std::move(metadata_group_id),
                          table_id, tablet_group, metadata, checkpoint_storage, &chunk_storage,
                          leader_hint, limits);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("durable reconfiguration allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("durable reconfiguration exceeded container limits"));
  }
}

common::Result<PreparedDurableTabletReconfigurationResult>
reconcile_and_prepare_durable_tablet_reconfiguration(
    RecoveredTabletMovementGeneration& recovered, GroupId tablet_group_id,
    GroupId metadata_group_id, const schema::TableId table_id, const RaftNode& tablet_group,
    const MetadataStateMachine& metadata, TabletMovementCheckpointStorage& checkpoint_storage,
    TabletReconfigurationActionLedger& action_ledger, const std::optional<NodeId> leader_hint,
    const TabletMovementLimits limits) {
  return prepare_dispatch(
      reconcile_durable_tablet_reconfiguration(recovered, std::move(tablet_group_id),
                                               std::move(metadata_group_id), table_id, tablet_group,
                                               metadata, checkpoint_storage, leader_hint, limits),
      action_ledger);
}

common::Result<PreparedDurableTabletReconfigurationResult>
reconcile_and_prepare_durable_tablet_reconfiguration(
    RecoveredTabletMovementGeneration& recovered, GroupId tablet_group_id,
    GroupId metadata_group_id, const schema::TableId table_id, const RaftNode& tablet_group,
    const MetadataStateMachine& metadata, TabletMovementCheckpointStorage& checkpoint_storage,
    const TabletMovementSnapshotChunkStorage& chunk_storage,
    TabletReconfigurationActionLedger& action_ledger, const std::optional<NodeId> leader_hint,
    const TabletMovementLimits limits) {
  return prepare_dispatch(reconcile_durable_tablet_reconfiguration(
                              recovered, std::move(tablet_group_id), std::move(metadata_group_id),
                              table_id, tablet_group, metadata, checkpoint_storage, chunk_storage,
                              leader_hint, limits),
                          action_ledger);
}

common::Result<DurableRaftResult>
execute_local_prepared_tablet_reconfiguration(const PreparedTabletReconfigurationDispatch& dispatch,
                                              DurableMultiRaftRuntime& runtime) {
  common::Status valid = validate_prepared_dispatch(dispatch);
  if (!valid.is_ok())
    return common::make_unexpected(std::move(valid));
  try {
    auto executed = runtime.execute_batch({request_for_execution(dispatch)});
    if (!executed.has_value())
      return common::make_unexpected(std::move(executed).error());
    if (executed->size() != 1U) {
      return common::make_unexpected(
          corruption("prepared reconfiguration execution returned an invalid result count"));
    }
    return std::move(executed->front());
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("prepared reconfiguration execution allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("prepared reconfiguration execution exceeded container limits"));
  }
}

common::Result<AsyncDurableRaftCompletion> try_submit_local_prepared_tablet_reconfiguration(
    const PreparedTabletReconfigurationDispatch& dispatch, AsyncDurableMultiRaftRuntime& runtime) {
  common::Status valid = validate_prepared_dispatch(dispatch);
  if (!valid.is_ok())
    return common::make_unexpected(std::move(valid));
  try {
    return runtime.try_submit({request_for_execution(dispatch)});
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("prepared reconfiguration admission allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("prepared reconfiguration admission exceeded container limits"));
  }
}

} // namespace chronos::raft
