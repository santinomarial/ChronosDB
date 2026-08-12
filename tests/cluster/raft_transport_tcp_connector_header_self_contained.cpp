#include "chronos/cluster/raft_transport_tcp_connector.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::cluster::RaftTransportTcpConnector>);
static_assert(std::is_move_constructible_v<chronos::cluster::RaftTransportTcpConnector>);
