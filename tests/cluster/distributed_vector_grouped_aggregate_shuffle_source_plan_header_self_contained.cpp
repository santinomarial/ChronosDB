#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_source_plan.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleSourcePlan>);
static_assert(!std::is_copy_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleSourcePlan>);
