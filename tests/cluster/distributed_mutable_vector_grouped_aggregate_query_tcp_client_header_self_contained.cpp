#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tcp_client.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedMutableVectorGroupedAggregateQueryTcpClient>);
