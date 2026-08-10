#include "chronos/raft/durable_tablet_reconfiguration.hpp"

#include <limits>
#include <new>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::raft {
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

} // namespace

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

} // namespace chronos::raft
