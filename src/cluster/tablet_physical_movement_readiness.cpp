#include "chronos/cluster/tablet_physical_movement_readiness.hpp"

#include "chronos/manifest/raft_tablet_physical_snapshot.hpp"
#include "chronos/manifest/temporal_codec.hpp"

#include <new>
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

  common::Result<raft::InstalledTabletMovementCheckpoint> ready =
      chunk_storage == nullptr
          ? ingest::checkpoint_recovered_tablet_movement_catch_up(
                recovered, expected_table_id, snapshot_storage, runtime, checkpoint_storage,
                codec_limits, movement_limits)
          : ingest::checkpoint_recovered_tablet_movement_catch_up(
                recovered, expected_table_id, snapshot_storage, runtime, checkpoint_storage,
                *chunk_storage, codec_limits, movement_limits);
  if (!ready.has_value())
    return common::make_unexpected(ready.error());
  return TabletPhysicalMovementReadinessReport{.application_snapshot = std::move(*application),
                                               .ready_checkpoint = std::move(*ready),
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
