#include "chronos/cluster/raft_transport_tcp_server.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::cluster::RaftTransportTcpServer>);
static_assert(std::is_move_constructible_v<chronos::cluster::RaftTransportTcpServer>);
