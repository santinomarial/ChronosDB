#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_query_execution.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleQueryExecution>);
static_assert(!std::is_copy_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleQueryExecution>);
