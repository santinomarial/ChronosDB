#include "chronos/cluster/distributed_query_tcp_client.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::cluster::DistributedQueryTcpClient>);
static_assert(std::is_nothrow_move_constructible_v<chronos::cluster::DistributedQueryTcpClient>);
