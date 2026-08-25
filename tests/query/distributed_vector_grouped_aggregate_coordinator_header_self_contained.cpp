#include "chronos/query/distributed_vector_grouped_aggregate_coordinator.hpp"

#include <type_traits>

static_assert(
    std::is_move_constructible_v<chronos::query::DistributedVectorGroupedAggregateCoordinator>);
static_assert(
    !std::is_copy_constructible_v<chronos::query::DistributedVectorGroupedAggregateCoordinator>);
static_assert(
    std::is_aggregate_v<chronos::query::DistributedVectorGroupedAggregateCoordinatorLimits>);
