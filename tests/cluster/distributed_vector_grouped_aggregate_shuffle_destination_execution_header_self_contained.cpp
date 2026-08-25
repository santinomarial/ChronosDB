#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_destination_execution.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleDestinationExecution>);
static_assert(!std::is_copy_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleDestinationExecution>);
