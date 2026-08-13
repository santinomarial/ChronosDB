#include "chronos/cluster/distributed_vector_aggregate_query_transport_v2.hpp"

#include <type_traits>

static_assert(
    std::is_move_constructible_v<chronos::cluster::DistributedVectorAggregateQueryResponseV2>);
static_assert(!std::is_copy_constructible_v<
              chronos::cluster::DistributedVectorAggregateQueryResponseV2Reader>);
