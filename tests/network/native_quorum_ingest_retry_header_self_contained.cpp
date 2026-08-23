#include "chronos/network/native_quorum_ingest_retry.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::network::NativeQuorumIngestRetry>);
static_assert(!std::is_copy_constructible_v<chronos::network::NativeQuorumIngestRetry>);
static_assert(std::is_move_constructible_v<chronos::network::NativeQuorumIngestRetry>);
