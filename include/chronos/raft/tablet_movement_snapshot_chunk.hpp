#ifndef CHRONOS_RAFT_TABLET_MOVEMENT_SNAPSHOT_CHUNK_HPP_
#define CHRONOS_RAFT_TABLET_MOVEMENT_SNAPSHOT_CHUNK_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/raft/rebalancing.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::raft {

inline constexpr std::size_t kTabletMovementSnapshotChunkHeaderSize = 128U;
inline constexpr std::size_t kTabletMovementSnapshotChunkTrailerSize = 4U;
inline constexpr std::size_t kMaximumTabletMovementSnapshotChunkSize = 16U * 1024U * 1024U;

struct TabletMovementSnapshotSession {
  schema::TabletId tablet_id;
  std::uint64_t placement_epoch{};
  NodeId source_node{};
  NodeId target_node{};
  SnapshotTransferMetadata snapshot;

  friend bool operator==(const TabletMovementSnapshotSession&,
                         const TabletMovementSnapshotSession&) = default;
};

struct TabletMovementSnapshotChunk {
  TabletMovementSnapshotSession session;
  std::uint64_t offset{};
  std::vector<std::byte> bytes;

  friend bool operator==(const TabletMovementSnapshotChunk&,
                         const TabletMovementSnapshotChunk&) = default;
};

struct TabletMovementSnapshotChunkCodecLimits {
  std::size_t maximum_snapshot_bytes{1U << 30U};
  std::size_t maximum_chunk_bytes{4U * 1024U * 1024U};
  std::size_t maximum_encoded_bytes{kMaximumTabletMovementSnapshotChunkSize};
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_tablet_movement_snapshot_chunk_v1(const TabletMovementSnapshotChunk& chunk,
                                         TabletMovementSnapshotChunkCodecLimits limits = {});

[[nodiscard]] common::Result<TabletMovementSnapshotChunk>
decode_tablet_movement_snapshot_chunk_v1(common::ByteView bytes,
                                         TabletMovementSnapshotChunkCodecLimits limits = {});

} // namespace chronos::raft

#endif // CHRONOS_RAFT_TABLET_MOVEMENT_SNAPSHOT_CHUNK_HPP_
