#include "chronos/cluster/raft_transport_peer_pool.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::cluster::RaftTransportPeerPool>);
static_assert(std::is_move_constructible_v<chronos::cluster::RaftTransportPeerPool>);
