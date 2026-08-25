#include "chronos/cluster/distributed_vector_grouped_aggregate_query_tcp_client_v2.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateQueryTcpClientV2>);
static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateQueryTcpClientV2>);
