#include "chronos/service/replicated_distributed_mutable_vector_grouped_aggregate_query_tcp_server.hpp"

#include <type_traits>

static_assert(
    std::is_aggregate_v<
        chronos::service::ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServerConfig>);
static_assert(std::is_move_constructible_v<
              chronos::service::ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer>);
static_assert(!std::is_copy_constructible_v<
              chronos::service::ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer>);
