#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_retry.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleResultRetry>);
