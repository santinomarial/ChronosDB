#ifndef CHRONOS_CLUSTER_TABLET_PHYSICAL_PART_CHUNK_HPP_
#define CHRONOS_CLUSTER_TABLET_PHYSICAL_PART_CHUNK_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/types.hpp"
#include "chronos/ingest/sha256.hpp"
#include "chronos/raft/multi_raft.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kTabletPhysicalPartChunkHeaderSize = 192U;
inline constexpr std::size_t kTabletPhysicalPartChunkTrailerSize = 4U;
inline constexpr std::size_t kMaximumTabletPhysicalPartChunkSize = std::size_t{16U} * 1024U * 1024U;

struct TabletPhysicalPartTransferSession {
  schema::TableId table_id;
  schema::TabletId tablet_id;
  raft::GroupId group_id;
  std::uint64_t placement_epoch{};
  raft::NodeId source_node{};
  raft::NodeId target_node{};
  std::uint64_t manifest_generation{};
  cseg::PartId part_id;
  std::uint64_t total_bytes{};
  ingest::Sha256Digest content_sha256;

  friend bool operator==(const TabletPhysicalPartTransferSession&,
                         const TabletPhysicalPartTransferSession&) = default;
};

struct TabletPhysicalPartChunk {
  TabletPhysicalPartTransferSession session;
  std::uint64_t offset{};
  std::vector<std::byte> bytes;

  friend bool operator==(const TabletPhysicalPartChunk&, const TabletPhysicalPartChunk&) = default;
};

struct TabletPhysicalPartChunkCodecLimits {
  std::uint64_t maximum_object_bytes{cseg::format::kMaximumFileLength};
  std::size_t maximum_chunk_bytes{std::size_t{4U} * 1024U * 1024U};
  std::size_t maximum_encoded_bytes{kMaximumTabletPhysicalPartChunkSize};
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_tablet_physical_part_chunk_v1(const TabletPhysicalPartChunk& chunk,
                                     TabletPhysicalPartChunkCodecLimits limits = {});

[[nodiscard]] common::Result<TabletPhysicalPartChunk>
decode_tablet_physical_part_chunk_v1(common::ByteView bytes,
                                     TabletPhysicalPartChunkCodecLimits limits = {});

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_TABLET_PHYSICAL_PART_CHUNK_HPP_
