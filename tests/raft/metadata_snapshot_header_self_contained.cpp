#include "chronos/raft/metadata_snapshot.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<chronos::raft::MetadataApplicationSnapshot>);
