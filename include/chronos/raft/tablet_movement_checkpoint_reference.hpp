#ifndef CHRONOS_RAFT_TABLET_MOVEMENT_CHECKPOINT_REFERENCE_HPP_
#define CHRONOS_RAFT_TABLET_MOVEMENT_CHECKPOINT_REFERENCE_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/raft/rebalancing.hpp"
#include "chronos/raft/tablet_movement_snapshot_chunk.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::raft {

inline constexpr std::size_t kTabletMovementCheckpointReferenceHeaderSize = 64U;
inline constexpr std::size_t kTabletMovementCheckpointReferenceTrailerSize = 4U;
inline constexpr std::size_t kTabletMovementCheckpointReferenceGenerationHeaderSize = 64U;
inline constexpr std::size_t kTabletMovementCheckpointReferenceGenerationTrailerSize = 4U;
inline constexpr std::size_t kMaximumTabletMovementCheckpointReferenceSize = std::size_t{1U} << 20U;

struct TabletMovementCheckpointReference {
  TabletMovementRecord record;
  // The chunk session begins at this placement epoch. The live record advances by one after target
  // promotion and by two after source removal, so current placement_epoch cannot replace it.
  std::uint64_t snapshot_session_placement_epoch{};

  friend bool operator==(const TabletMovementCheckpointReference&,
                         const TabletMovementCheckpointReference&) = default;
};

struct TabletMovementCheckpointReferenceCodecLimits {
  std::size_t maximum_checkpoint_bytes{kMaximumTabletMovementCheckpointReferenceSize};
  TabletMovementLimits movement{};
};

struct TabletMovementCheckpointReferenceGeneration {
  std::uint64_t checkpoint_generation{};
  TabletMovementCheckpointReference reference;

  friend bool operator==(const TabletMovementCheckpointReferenceGeneration&,
                         const TabletMovementCheckpointReferenceGeneration&) = default;
};

[[nodiscard]] common::Result<TabletMovementSnapshotSession>
tablet_movement_snapshot_session(const TabletMovementCheckpointReference& reference,
                                 TabletMovementLimits limits = {});

[[nodiscard]] common::Result<std::vector<std::byte>> encode_tablet_movement_checkpoint_reference_v1(
    const TabletMovementCheckpointReference& reference,
    TabletMovementCheckpointReferenceCodecLimits limits = {});

[[nodiscard]] common::Result<TabletMovementCheckpointReference>
decode_tablet_movement_checkpoint_reference_v1(
    common::ByteView bytes, TabletMovementCheckpointReferenceCodecLimits limits = {});

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_tablet_movement_checkpoint_reference_generation_v1(
    const TabletMovementCheckpointReferenceGeneration& generation,
    TabletMovementCheckpointReferenceCodecLimits limits = {});

[[nodiscard]] common::Result<TabletMovementCheckpointReferenceGeneration>
decode_tablet_movement_checkpoint_reference_generation_v1(
    common::ByteView bytes, TabletMovementCheckpointReferenceCodecLimits limits = {});

} // namespace chronos::raft

#endif // CHRONOS_RAFT_TABLET_MOVEMENT_CHECKPOINT_REFERENCE_HPP_
