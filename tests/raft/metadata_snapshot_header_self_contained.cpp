#include "chronos/raft/metadata_snapshot.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<chronos::raft::MetadataApplicationSnapshot>);
static_assert(chronos::raft::kMaximumMetadataSnapshotSize == std::size_t{1024U} * 1024U * 1024U);
static_assert(chronos::raft::MetadataSnapshotCodecLimits{}.maximum_snapshot_bytes ==
              std::size_t{256U} * 1024U * 1024U);
static_assert(chronos::raft::MetadataSnapshotCodecLimits{}.maximum_entry_payload_bytes ==
              std::size_t{16U} * 1024U * 1024U);
