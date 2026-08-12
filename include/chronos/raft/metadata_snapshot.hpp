#ifndef CHRONOS_RAFT_METADATA_SNAPSHOT_HPP_
#define CHRONOS_RAFT_METADATA_SNAPSHOT_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/raft/multi_raft.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::raft {

inline constexpr std::size_t kMetadataSnapshotHeaderSize = 128U;
inline constexpr std::size_t kMetadataSnapshotEntryHeaderSize = 32U;
inline constexpr std::size_t kMetadataSnapshotTrailerSize = 4U;
inline constexpr std::size_t kMaximumMetadataSnapshotSize = 1024U * 1024U * 1024U;

struct MetadataSnapshotCodecLimits {
  std::size_t maximum_snapshot_bytes{256U * 1024U * 1024U};
  std::size_t maximum_entries{65'536U};
  std::size_t maximum_entry_payload_bytes{16U * 1024U * 1024U};
  std::size_t maximum_voters{9U};
};

struct MetadataSnapshotEntry {
  LogIndex index{};
  Term term{};
  std::uint8_t type{};
  std::vector<std::byte> payload;

  friend bool operator==(const MetadataSnapshotEntry&, const MetadataSnapshotEntry&) = default;
};

// Exact application-bearing entries from one compacted metadata-group prefix. Internal Raft
// entries are represented by index gaps because SnapshotMetadata retains their membership result.
struct MetadataApplicationSnapshot {
  GroupId group_id;
  SnapshotMetadata raft_snapshot;
  std::vector<MetadataSnapshotEntry> entries;

  friend bool operator==(const MetadataApplicationSnapshot&,
                         const MetadataApplicationSnapshot&) = default;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_metadata_application_snapshot_v1(const MetadataApplicationSnapshot& snapshot,
                                        MetadataSnapshotCodecLimits limits = {});

[[nodiscard]] common::Result<MetadataApplicationSnapshot>
decode_metadata_application_snapshot_v1(common::ByteView bytes,
                                        MetadataSnapshotCodecLimits limits = {});

} // namespace chronos::raft

#endif // CHRONOS_RAFT_METADATA_SNAPSHOT_HPP_
