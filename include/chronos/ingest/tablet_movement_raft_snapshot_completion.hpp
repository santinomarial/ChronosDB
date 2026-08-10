#ifndef CHRONOS_INGEST_TABLET_MOVEMENT_RAFT_SNAPSHOT_COMPLETION_HPP_
#define CHRONOS_INGEST_TABLET_MOVEMENT_RAFT_SNAPSHOT_COMPLETION_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/tablet_movement_snapshot_handoff.hpp"
#include "chronos/raft/durable_runtime.hpp"

#include <cstdint>

namespace chronos::ingest {

struct TabletMovementRaftSnapshotCompletionReport {
  TabletMovementSnapshotHandoffReport application_snapshot;
  raft::GroupOutboundMessage acknowledgement;
  std::uint64_t durable_physical_sequence{};
};

// Installs/verifies the movement's RTAS first, exact-matches it to the pending Raft request, then
// durably installs the Raft snapshot metadata. The returned success response is released only after
// DurableMultiRaftRuntime has synchronized that persistent transition.
[[nodiscard]] common::Result<TabletMovementRaftSnapshotCompletionReport>
complete_recovered_tablet_movement_raft_snapshot(
    const raft::RecoveredTabletMovementGeneration& recovered, schema::TableId expected_table_id,
    RaftTabletSnapshotStorage& snapshot_storage, const raft::GroupSnapshotInstall& pending,
    raft::DurableMultiRaftRuntime& runtime, RaftTabletSnapshotCodecLimits codec_limits = {});

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_TABLET_MOVEMENT_RAFT_SNAPSHOT_COMPLETION_HPP_
