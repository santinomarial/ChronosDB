#include "chronos/cluster/raft_read_authority_tcp_client.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::cluster::RaftReadAuthorityTcpClient>);
