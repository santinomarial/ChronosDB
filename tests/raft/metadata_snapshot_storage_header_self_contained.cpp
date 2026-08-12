#include "chronos/raft/metadata_snapshot_storage.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::raft::MetadataSnapshotStorage>);
static_assert(std::is_move_constructible_v<chronos::raft::MetadataSnapshotStorage>);
