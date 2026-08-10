#ifndef CHRONOS_INGEST_TABLET_MOVEMENT_SNAPSHOT_HANDOFF_HPP_
#define CHRONOS_INGEST_TABLET_MOVEMENT_SNAPSHOT_HANDOFF_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/raft_tablet_snapshot_storage.hpp"
#include "chronos/raft/tablet_movement_checkpoint_recovery.hpp"
#include "chronos/schema/identity.hpp"

#include <cstdint>

namespace chronos::ingest {

struct TabletMovementSnapshotHandoffReport {
  std::uint64_t checkpoint_generation{};
  raft::GroupId group_id;
  schema::TableId table_id;
  schema::TabletId tablet_id;
  raft::SnapshotMetadata raft_snapshot;
  InstalledRaftTabletSnapshot installation;
};

// Adopts the exact completed RTAS bytes from an authoritative recovered movement generation. The
// transfer metadata, table, tablet, stable voters, and destination group owner are verified before
// first installation. After promotion, only an exact already-installed retry is accepted.
[[nodiscard]] common::Result<TabletMovementSnapshotHandoffReport>
install_recovered_tablet_movement_snapshot(const raft::RecoveredTabletMovementGeneration& recovered,
                                           schema::TableId expected_table_id,
                                           RaftTabletSnapshotStorage& snapshot_storage,
                                           RaftTabletSnapshotCodecLimits codec_limits = {});

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_TABLET_MOVEMENT_SNAPSHOT_HANDOFF_HPP_
