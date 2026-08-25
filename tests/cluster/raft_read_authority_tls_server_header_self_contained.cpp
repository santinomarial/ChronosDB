#include "chronos/cluster/raft_read_authority_tls_server.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::cluster::RaftReadAuthorityTlsServer>);
