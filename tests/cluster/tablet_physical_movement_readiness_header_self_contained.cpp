#include "chronos/cluster/tablet_physical_movement_readiness.hpp"

[[maybe_unused]] auto* const kCheckpointTabletPhysicalMovementReadiness = static_cast<
    chronos::common::Result<chronos::cluster::TabletPhysicalMovementReadinessReport> (*)(
        chronos::raft::RecoveredTabletMovementGeneration&, chronos::schema::TableId,
        chronos::ingest::RaftTabletSnapshotStorage&, const chronos::raft::DurableMultiRaftRuntime&,
        chronos::raft::TabletMovementCheckpointStorage&,
        const chronos::manifest::TemporalDatabaseStorageSnapshot&,
        chronos::ingest::RaftTabletSnapshotCodecLimits, chronos::raft::TabletMovementLimits)>(
    &chronos::cluster::checkpoint_tablet_physical_movement_readiness);
