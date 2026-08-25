#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tls.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedMutableVectorGroupedAggregateQueryTlsClient>);
static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedMutableVectorGroupedAggregateQueryTlsServer>);
