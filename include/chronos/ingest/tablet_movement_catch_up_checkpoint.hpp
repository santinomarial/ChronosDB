#ifndef CHRONOS_INGEST_TABLET_MOVEMENT_CATCH_UP_CHECKPOINT_HPP_
#define CHRONOS_INGEST_TABLET_MOVEMENT_CATCH_UP_CHECKPOINT_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/tablet_movement_snapshot_handoff.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/tablet_movement_checkpoint_recovery.hpp"

namespace chronos::ingest {

// Reconciles a self-contained catching-up generation with an exact installed RTAS/Raft boundary.
// The next ready checkpoint is made durable before the supplied live movement advances.
[[nodiscard]] common::Result<raft::InstalledTabletMovementCheckpoint>
checkpoint_recovered_tablet_movement_catch_up(
    raft::RecoveredTabletMovementGeneration& recovered, schema::TableId expected_table_id,
    RaftTabletSnapshotStorage& snapshot_storage, const raft::DurableMultiRaftRuntime& runtime,
    raft::TabletMovementCheckpointStorage& checkpoint_storage,
    RaftTabletSnapshotCodecLimits codec_limits = {},
    raft::TabletMovementLimits movement_limits = {});

// The external-prefix overload retains the reference representation and revalidates the exact
// durable chunk session before installing the next ready generation.
[[nodiscard]] common::Result<raft::InstalledTabletMovementCheckpoint>
checkpoint_recovered_tablet_movement_catch_up(
    raft::RecoveredTabletMovementGeneration& recovered, schema::TableId expected_table_id,
    RaftTabletSnapshotStorage& snapshot_storage, const raft::DurableMultiRaftRuntime& runtime,
    raft::TabletMovementCheckpointStorage& checkpoint_storage,
    const raft::TabletMovementSnapshotChunkStorage& chunk_storage,
    RaftTabletSnapshotCodecLimits codec_limits = {},
    raft::TabletMovementLimits movement_limits = {});

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_TABLET_MOVEMENT_CATCH_UP_CHECKPOINT_HPP_
