#include "chronos/cluster/distributed_grouped_query_tcp_server.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::cluster::DistributedGroupedQueryTcpServer>);
static_assert(
    std::is_nothrow_move_constructible_v<chronos::cluster::DistributedGroupedQueryTcpServer>);
