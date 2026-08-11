#include "chronos/cluster/tablet_physical_movement_readiness.hpp"

#include "chronos/manifest/raft_tablet_physical_snapshot.hpp"
#include "chronos/manifest/temporal_codec.hpp"

#include <algorithm>
#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}
[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}
[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}
[[nodiscard]] common::Status unsupported(const char* message) {
  return {common::StatusCode::kNotSupported, message};
}

[[nodiscard]] common::Result<raft::InstalledTabletMovementCheckpoint>
verify_existing_ready_checkpoint(
    const raft::RecoveredTabletMovementGeneration& recovered,
    raft::TabletMovementCheckpointStorage& checkpoint_storage,
    const raft::TabletMovementSnapshotChunkStorage* const chunk_storage,
    const raft::TabletMovementLimits movement_limits) {
  if (recovered.used_external_prefix && chunk_storage == nullptr) {
    return common::make_unexpected(
        unsupported("external physical readiness retry requires its durable chunk owner"));
  }
  auto loaded = checkpoint_storage.load_any_generation(recovered.checkpoint_generation);
  if (!loaded.has_value())
    return common::make_unexpected(loaded.error());
  common::Result<raft::RecoveredTabletMovementGeneration> durable =
      chunk_storage == nullptr
          ? raft::recover_tablet_movement_generation(*loaded, movement_limits)
          : raft::recover_tablet_movement_generation(*loaded, *chunk_storage, movement_limits);
  if (!durable.has_value())
    return common::make_unexpected(durable.error());
  if (durable->checkpoint_generation != recovered.checkpoint_generation ||
      durable->used_external_prefix != recovered.used_external_prefix ||
      durable->movement.record() != recovered.movement.record() ||
      !std::ranges::equal(durable->movement.received_snapshot(),
                          recovered.movement.received_snapshot())) {
    return common::make_unexpected(
        corruption("durable ready checkpoint differs from recovered movement"));
  }
  return raft::InstalledTabletMovementCheckpoint{.checkpoint_generation =
                                                     recovered.checkpoint_generation,
                                                 .file_name = std::move(loaded->file_name),
                                                 .already_present = true};
}

[[nodiscard]] common::Result<TabletPhysicalMovementReadinessReport>
checkpoint_impl(raft::RecoveredTabletMovementGeneration& recovered,
                const schema::TableId expected_table_id,
                ingest::RaftTabletSnapshotStorage& snapshot_storage,
                const raft::DurableMultiRaftRuntime& runtime,
                raft::TabletMovementCheckpointStorage& checkpoint_storage,
                const raft::TabletMovementSnapshotChunkStorage* const chunk_storage,
                const manifest::TemporalDatabaseStorageSnapshot& destination,
                const ingest::RaftTabletSnapshotCodecLimits codec_limits,
                const raft::TabletMovementLimits movement_limits) {
  auto application = ingest::install_recovered_tablet_movement_snapshot(
      recovered, expected_table_id, snapshot_storage, codec_limits);
  if (!application.has_value())
    return common::make_unexpected(application.error());
  auto selected = manifest::decode_manifest_v2_temporal_exact(destination.manifest_bytes());
  if (!selected.has_value())
    return common::make_unexpected(selected.error().status());
  auto physical = manifest::build_raft_tablet_physical_snapshot(
      *selected, application->group_id, application->tablet_id,
      application->raft_snapshot.last_included_index);
  if (!physical.has_value())
    return common::make_unexpected(physical.error());
  const ingest::Sha256Digest expected{application->raft_snapshot.part_set_checksum};
  if (physical->part_set_checksum() != expected)
    return common::make_unexpected(
        corruption("published destination part set differs from the installed Raft snapshot"));

  const raft::TabletMovementRecord movement = recovered.movement.record();
  common::Result<raft::InstalledTabletMovementCheckpoint> ready =
      common::make_unexpected(invalid("physical movement is not at a readiness boundary"));
  if (movement.phase == raft::TabletMovementPhase::kCatchingUp) {
    ready = chunk_storage == nullptr
                ? ingest::checkpoint_recovered_tablet_movement_catch_up(
                      recovered, expected_table_id, snapshot_storage, runtime, checkpoint_storage,
                      codec_limits, movement_limits)
                : ingest::checkpoint_recovered_tablet_movement_catch_up(
                      recovered, expected_table_id, snapshot_storage, runtime, checkpoint_storage,
                      *chunk_storage, codec_limits, movement_limits);
  } else if (movement.phase == raft::TabletMovementPhase::kReady) {
    const raft::RaftNode* target = runtime.find_group(application->group_id);
    if (target == nullptr || target->node_id() != movement.target_node ||
        target->persistent_state().snapshot != application->raft_snapshot ||
        target->commit_index() < movement.snapshot.applied_index ||
        target->applied_index() < movement.snapshot.applied_index) {
      return common::make_unexpected(
          corruption("target Raft state no longer contains the movement snapshot boundary"));
    }
    ready = verify_existing_ready_checkpoint(recovered, checkpoint_storage, chunk_storage,
                                             movement_limits);
  }
  if (!ready.has_value())
    return common::make_unexpected(ready.error());
  return TabletPhysicalMovementReadinessReport{.application_snapshot = std::move(*application),
                                               .ready_checkpoint = std::move(*ready),
                                               .movement = recovered.movement.record(),
                                               .destination_manifest_generation =
                                                   destination.generation(),
                                               .part_set_checksum = physical->part_set_checksum()};
}

} // namespace

common::Result<TabletPhysicalMovementReadinessReport> checkpoint_tablet_physical_movement_readiness(
    raft::RecoveredTabletMovementGeneration& recovered, const schema::TableId expected_table_id,
    ingest::RaftTabletSnapshotStorage& snapshot_storage,
    const raft::DurableMultiRaftRuntime& runtime,
    raft::TabletMovementCheckpointStorage& checkpoint_storage,
    const manifest::TemporalDatabaseStorageSnapshot& destination,
    const ingest::RaftTabletSnapshotCodecLimits codec_limits,
    const raft::TabletMovementLimits movement_limits) {
  try {
    return checkpoint_impl(recovered, expected_table_id, snapshot_storage, runtime,
                           checkpoint_storage, nullptr, destination, codec_limits, movement_limits);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("physical movement readiness allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("physical movement readiness exceeded limits"));
  }
}

common::Result<TabletPhysicalMovementReadinessReport> checkpoint_tablet_physical_movement_readiness(
    raft::RecoveredTabletMovementGeneration& recovered, const schema::TableId expected_table_id,
    ingest::RaftTabletSnapshotStorage& snapshot_storage,
    const raft::DurableMultiRaftRuntime& runtime,
    raft::TabletMovementCheckpointStorage& checkpoint_storage,
    const raft::TabletMovementSnapshotChunkStorage& chunk_storage,
    const manifest::TemporalDatabaseStorageSnapshot& destination,
    const ingest::RaftTabletSnapshotCodecLimits codec_limits,
    const raft::TabletMovementLimits movement_limits) {
  try {
    return checkpoint_impl(recovered, expected_table_id, snapshot_storage, runtime,
                           checkpoint_storage, &chunk_storage, destination, codec_limits,
                           movement_limits);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("physical movement readiness allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("physical movement readiness exceeded limits"));
  }
}

} // namespace chronos::cluster
