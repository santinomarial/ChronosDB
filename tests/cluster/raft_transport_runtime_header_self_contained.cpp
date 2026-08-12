#include "chronos/cluster/raft_transport_runtime.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::cluster::RaftTransportRuntime>);
static_assert(std::is_move_constructible_v<chronos::cluster::RaftTransportRuntime>);
