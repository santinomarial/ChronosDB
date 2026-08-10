#ifndef CHRONOS_RAFT_TABLET_MOVEMENT_CHECKPOINT_RECOVERY_HPP_
#define CHRONOS_RAFT_TABLET_MOVEMENT_CHECKPOINT_RECOVERY_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/tablet_movement_checkpoint_storage.hpp"
#include "chronos/raft/tablet_movement_snapshot_chunk_storage.hpp"

#include <cstdint>

namespace chronos::raft {

struct RecoveredTabletMovementGeneration {
  std::uint64_t checkpoint_generation{};
  bool used_external_prefix{};
  TabletMovement movement;
};

// Verifies that the reference's exact chunk-boundary prefix is already durable and fully valid
// before installing its checkpoint generation. A durable suffix beyond the reference is ignored.
[[nodiscard]] common::Result<InstalledTabletMovementCheckpoint>
install_verified_tablet_movement_reference(
    TabletMovementCheckpointStorage& checkpoint_storage,
    const TabletMovementSnapshotChunkStorage& chunk_storage,
    const TabletMovementCheckpointReferenceGeneration& generation,
    TabletMovementLimits limits = {});

// Recovers a self-contained generation. A reference generation fails closed without its chunk
// owner rather than falling back to an older checkpoint.
[[nodiscard]] common::Result<RecoveredTabletMovementGeneration>
recover_tablet_movement_generation(const LoadedTabletMovementCheckpointGeneration& generation,
                                   TabletMovementLimits limits = {});

// Recovers either alternative. Reference recovery exact-matches the chunk session and reconstructs
// only the checkpointed boundary even when later chunks reached durability first.
[[nodiscard]] common::Result<RecoveredTabletMovementGeneration>
recover_tablet_movement_generation(const LoadedTabletMovementCheckpointGeneration& generation,
                                   const TabletMovementSnapshotChunkStorage& chunk_storage,
                                   TabletMovementLimits limits = {});

} // namespace chronos::raft

#endif // CHRONOS_RAFT_TABLET_MOVEMENT_CHECKPOINT_RECOVERY_HPP_
