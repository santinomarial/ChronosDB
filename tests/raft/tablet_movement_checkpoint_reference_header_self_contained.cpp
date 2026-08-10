#include "chronos/raft/tablet_movement_checkpoint_reference.hpp"

[[maybe_unused]] constexpr auto kMovementReferenceHeader =
    chronos::raft::kTabletMovementCheckpointReferenceHeaderSize;
[[maybe_unused]] constexpr auto kMovementReferenceGenerationHeader =
    chronos::raft::kTabletMovementCheckpointReferenceGenerationHeaderSize;
