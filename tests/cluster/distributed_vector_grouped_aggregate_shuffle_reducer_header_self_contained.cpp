#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_reducer.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleReducer>);
