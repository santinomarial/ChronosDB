#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_retry.hpp"

#include <type_traits>

static_assert(
    std::is_move_constructible_v<chronos::cluster::DistributedVectorGroupedAggregateShuffleRetry>);
static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleAttempt>);
