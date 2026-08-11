#include "chronos/cluster/distributed_query_tls_client.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::cluster::DistributedQueryTlsClient>);
static_assert(std::is_nothrow_move_constructible_v<chronos::cluster::DistributedQueryTlsClient>);
