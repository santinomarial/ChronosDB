#include "chronos/cluster/raft_transport_peer_reconnect.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::cluster::RaftTransportPeerReconnect>);
static_assert(std::is_move_constructible_v<chronos::cluster::RaftTransportPeerReconnect>);
