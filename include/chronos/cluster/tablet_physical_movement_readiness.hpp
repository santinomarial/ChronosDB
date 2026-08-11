#ifndef CHRONOS_CLUSTER_TABLET_PHYSICAL_MOVEMENT_READINESS_HPP_
#define CHRONOS_CLUSTER_TABLET_PHYSICAL_MOVEMENT_READINESS_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/raft_tablet_snapshot_storage.hpp"
#include "chronos/ingest/sha256.hpp"
#include "chronos/ingest/tablet_movement_catch_up_checkpoint.hpp"
#include "chronos/ingest/tablet_movement_snapshot_handoff.hpp"
#include "chronos/manifest/temporal_publication.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/tablet_movement_checkpoint_recovery.hpp"

#include <cstdint>

namespace chronos::cluster {

struct TabletPhysicalMovementReadinessReport {
  ingest::TabletMovementSnapshotHandoffReport application_snapshot;
  raft::InstalledTabletMovementCheckpoint ready_checkpoint;
  std::uint64_t destination_manifest_generation{};
  ingest::Sha256Digest part_set_checksum;
};

// Proves both halves of a catching-up target before installing its ready checkpoint: the exact
// RTAS/Raft application boundary and one loaded, atomically published destination Manifest v2
// epoch containing the same canonical physical part set. The application RTAS may be installed as
// an idempotent candidate before a missing physical proof is rejected; movement remains
// catching-up.
[[nodiscard]] common::Result<TabletPhysicalMovementReadinessReport>
checkpoint_tablet_physical_movement_readiness(
    raft::RecoveredTabletMovementGeneration& recovered, schema::TableId expected_table_id,
    ingest::RaftTabletSnapshotStorage& snapshot_storage,
    const raft::DurableMultiRaftRuntime& runtime,
    raft::TabletMovementCheckpointStorage& checkpoint_storage,
    const manifest::TemporalDatabaseStorageSnapshot& destination,
    ingest::RaftTabletSnapshotCodecLimits codec_limits = {},
    raft::TabletMovementLimits movement_limits = {});

// External-prefix equivalent. The new ready generation retains and revalidates its exact durable
// RTAS chunk reference.
[[nodiscard]] common::Result<TabletPhysicalMovementReadinessReport>
checkpoint_tablet_physical_movement_readiness(
    raft::RecoveredTabletMovementGeneration& recovered, schema::TableId expected_table_id,
    ingest::RaftTabletSnapshotStorage& snapshot_storage,
    const raft::DurableMultiRaftRuntime& runtime,
    raft::TabletMovementCheckpointStorage& checkpoint_storage,
    const raft::TabletMovementSnapshotChunkStorage& chunk_storage,
    const manifest::TemporalDatabaseStorageSnapshot& destination,
    ingest::RaftTabletSnapshotCodecLimits codec_limits = {},
    raft::TabletMovementLimits movement_limits = {});

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_TABLET_PHYSICAL_MOVEMENT_READINESS_HPP_
