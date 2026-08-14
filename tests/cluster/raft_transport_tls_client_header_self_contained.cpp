#include "chronos/cluster/raft_transport_tls_client.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::cluster::RaftTransportTlsClient>);
static_assert(std::is_move_constructible_v<chronos::cluster::RaftTransportTlsClient>);
static_assert(chronos::cluster::RaftTransportTlsClientLimits{}.maximum_queued_bytes ==
              std::size_t{64U} * 1024U * 1024U);
