#include "chronos/cluster/distributed_vector_aggregate_query_tcp_server_v2.hpp"

#include <type_traits>

static_assert(
    !std::is_copy_constructible_v<chronos::cluster::DistributedVectorAggregateQueryTcpServerV2>);
static_assert(
    std::is_move_constructible_v<chronos::cluster::DistributedVectorAggregateQueryTcpServerV2>);
