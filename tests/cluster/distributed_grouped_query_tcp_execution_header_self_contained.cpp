#include "chronos/cluster/distributed_grouped_query_tcp_execution.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<chronos::cluster::DistributedGroupedQueryTcpExecution>);
