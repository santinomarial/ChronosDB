#include "chronos/ingest/tablet_movement_catch_up_checkpoint.hpp"

#include <limits>
#include <new>
#include <stdexcept>
#include <utility>
#include <vector>

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

[[nodiscard]] common::Status unsupported(const char* message) {
  return {common::StatusCode::kNotSupported, message};
}

[[nodiscard]] common::Result<raft::InstalledTabletMovementCheckpoint> checkpoint_impl(
    raft::RecoveredTabletMovementGeneration& recovered, const schema::TableId expected_table_id,
    RaftTabletSnapshotStorage& snapshot_storage, const raft::DurableMultiRaftRuntime& runtime,
    raft::TabletMovementCheckpointStorage& checkpoint_storage,
    const raft::TabletMovementSnapshotChunkStorage* const chunk_storage,
    const RaftTabletSnapshotCodecLimits codec_limits,
    const raft::TabletMovementLimits movement_limits) {
  if (recovered.checkpoint_generation == std::numeric_limits<std::uint64_t>::max()) {
    return common::make_unexpected(exhausted("tablet movement checkpoint generation is exhausted"));
  }
  const raft::TabletMovementRecord current = recovered.movement.record();
  if (current.phase != raft::TabletMovementPhase::kCatchingUp) {
    return common::make_unexpected(
        invalid("catch-up checkpoint reconciliation requires catching-up movement"));
  }
  if (recovered.used_external_prefix && chunk_storage == nullptr) {
    return common::make_unexpected(
        unsupported("external movement catch-up requires its durable chunk owner"));
  }
  auto application = install_recovered_tablet_movement_snapshot(recovered, expected_table_id,
                                                                snapshot_storage, codec_limits);
  if (!application.has_value())
    return common::make_unexpected(application.error());
  const raft::RaftNode* target = runtime.find_group(application->group_id);
  if (target == nullptr)
    return common::make_unexpected(invalid("target Raft group is absent"));
  if (target->node_id() != current.target_node ||
      target->persistent_state().snapshot != application->raft_snapshot ||
      target->commit_index() < current.snapshot.applied_index ||
      target->applied_index() < current.snapshot.applied_index) {
    return common::make_unexpected(
        corruption("target Raft state does not contain the movement snapshot boundary"));
  }

  const common::ByteView received = recovered.movement.received_snapshot();
  std::vector<std::byte> candidate_bytes{received.begin(), received.end()};
  auto candidate =
      raft::TabletMovement::recover(current, std::move(candidate_bytes), movement_limits);
  if (!candidate.has_value())
    return common::make_unexpected(candidate.error());
  common::Status advanced = candidate->mark_caught_up(target->applied_index());
  if (!advanced.is_ok())
    return common::make_unexpected(std::move(advanced));
  const raft::TabletMovementRecord ready = candidate->record();
  const std::uint64_t next_generation = recovered.checkpoint_generation + 1U;

  common::Result<raft::InstalledTabletMovementCheckpoint> installed =
      common::make_unexpected(invalid("movement checkpoint representation is invalid"));
  if (recovered.used_external_prefix) {
    installed = raft::install_verified_tablet_movement_reference(
        checkpoint_storage, *chunk_storage,
        raft::TabletMovementCheckpointReferenceGeneration{
            next_generation,
            raft::TabletMovementCheckpointReference{ready, current.placement_epoch}},
        movement_limits);
  } else {
    const common::ByteView ready_bytes = candidate->received_snapshot();
    installed = checkpoint_storage.install(raft::TabletMovementCheckpointGeneration{
        next_generation,
        raft::TabletMovementCheckpoint{
            ready, std::vector<std::byte>{ready_bytes.begin(), ready_bytes.end()}}});
  }
  if (!installed.has_value())
    return common::make_unexpected(installed.error());

  advanced = recovered.movement.mark_caught_up(target->applied_index());
  if (!advanced.is_ok()) {
    return common::make_unexpected(
        corruption("durable ready checkpoint could not advance live movement"));
  }
  recovered.checkpoint_generation = next_generation;
  return std::move(*installed);
}

} // namespace

common::Result<raft::InstalledTabletMovementCheckpoint>
checkpoint_recovered_tablet_movement_catch_up(
    raft::RecoveredTabletMovementGeneration& recovered, const schema::TableId expected_table_id,
    RaftTabletSnapshotStorage& snapshot_storage, const raft::DurableMultiRaftRuntime& runtime,
    raft::TabletMovementCheckpointStorage& checkpoint_storage,
    const RaftTabletSnapshotCodecLimits codec_limits,
    const raft::TabletMovementLimits movement_limits) {
  try {
    return checkpoint_impl(recovered, expected_table_id, snapshot_storage, runtime,
                           checkpoint_storage, nullptr, codec_limits, movement_limits);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("movement catch-up allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("movement catch-up exceeded container limits"));
  }
}

common::Result<raft::InstalledTabletMovementCheckpoint>
checkpoint_recovered_tablet_movement_catch_up(
    raft::RecoveredTabletMovementGeneration& recovered, const schema::TableId expected_table_id,
    RaftTabletSnapshotStorage& snapshot_storage, const raft::DurableMultiRaftRuntime& runtime,
    raft::TabletMovementCheckpointStorage& checkpoint_storage,
    const raft::TabletMovementSnapshotChunkStorage& chunk_storage,
    const RaftTabletSnapshotCodecLimits codec_limits,
    const raft::TabletMovementLimits movement_limits) {
  try {
    return checkpoint_impl(recovered, expected_table_id, snapshot_storage, runtime,
                           checkpoint_storage, &chunk_storage, codec_limits, movement_limits);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("movement catch-up allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("movement catch-up exceeded container limits"));
  }
}

} // namespace chronos::ingest
