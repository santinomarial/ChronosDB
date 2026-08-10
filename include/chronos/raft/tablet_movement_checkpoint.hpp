#ifndef CHRONOS_RAFT_TABLET_MOVEMENT_CHECKPOINT_HPP_
#define CHRONOS_RAFT_TABLET_MOVEMENT_CHECKPOINT_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/raft/rebalancing.hpp"

#include <cstddef>
#include <vector>

namespace chronos::raft {

inline constexpr std::size_t kTabletMovementCheckpointHeaderSize = 64U;
inline constexpr std::size_t kTabletMovementCheckpointTrailerSize = 4U;
inline constexpr std::size_t kMaximumTabletMovementCheckpointSize = (1U << 30U) + (1U << 20U);

struct TabletMovementCheckpoint {
  TabletMovementRecord record;
  std::vector<std::byte> received_snapshot;

  friend bool operator==(const TabletMovementCheckpoint&,
                         const TabletMovementCheckpoint&) = default;
};

struct TabletMovementCheckpointCodecLimits {
  std::size_t maximum_checkpoint_bytes{kMaximumTabletMovementCheckpointSize};
  TabletMovementLimits movement;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_tablet_movement_checkpoint_v1(const TabletMovementCheckpoint& checkpoint,
                                     TabletMovementCheckpointCodecLimits limits = {});

[[nodiscard]] common::Result<TabletMovementCheckpoint>
decode_tablet_movement_checkpoint_v1(common::ByteView bytes,
                                     TabletMovementCheckpointCodecLimits limits = {});

} // namespace chronos::raft

#endif // CHRONOS_RAFT_TABLET_MOVEMENT_CHECKPOINT_HPP_
