#include "chronos/cluster/raft_transport_tls_server.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::cluster::RaftTransportTlsServer>);
static_assert(std::is_move_constructible_v<chronos::cluster::RaftTransportTlsServer>);
