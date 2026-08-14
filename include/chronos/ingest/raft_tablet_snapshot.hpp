#ifndef CHRONOS_INGEST_RAFT_TABLET_SNAPSHOT_HPP_
#define CHRONOS_INGEST_RAFT_TABLET_SNAPSHOT_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/raft/multi_raft.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::ingest {

inline constexpr std::size_t kRaftTabletSnapshotHeaderSize = 160U;
inline constexpr std::size_t kRaftTabletSnapshotEntryHeaderSize = 24U;
inline constexpr std::size_t kRaftTabletSnapshotTrailerSize = 4U;
inline constexpr std::size_t kMaximumRaftTabletSnapshotSize = std::size_t{1024U} * 1024U * 1024U;

struct RaftTabletSnapshotCodecLimits {
  std::size_t maximum_snapshot_bytes{std::size_t{256U} * 1024U * 1024U};
  std::size_t maximum_entries{65'536U};
  std::size_t maximum_entry_payload_bytes{16'777'168U};
  std::size_t maximum_voters{9U};
};

struct RaftTabletSnapshotEntry {
  raft::LogIndex index{};
  raft::Term term{};
  std::vector<std::byte> payload;

  friend bool operator==(const RaftTabletSnapshotEntry&, const RaftTabletSnapshotEntry&) = default;
};

// Owned application state for the Raft prefix named by raft_snapshot. Entries contain exact
// COLUMNAR_APPEND v1 payloads in original logical-index order; membership-only index gaps are
// represented by raft_snapshot rather than copied into the tablet application stream.
struct RaftTabletApplicationSnapshot {
  raft::GroupId group_id;
  schema::TableId table_id;
  schema::TabletId tablet_id;
  raft::SnapshotMetadata raft_snapshot;
  std::vector<RaftTabletSnapshotEntry> entries;

  friend bool operator==(const RaftTabletApplicationSnapshot&,
                         const RaftTabletApplicationSnapshot&) = default;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_raft_tablet_application_snapshot_v1(const RaftTabletApplicationSnapshot& snapshot,
                                           RaftTabletSnapshotCodecLimits limits = {});

[[nodiscard]] common::Result<RaftTabletApplicationSnapshot>
decode_raft_tablet_application_snapshot_v1(common::ByteView bytes,
                                           RaftTabletSnapshotCodecLimits limits = {});

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_RAFT_TABLET_SNAPSHOT_HPP_
