#include "chronos/query/aggregate.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::VectorAggregateInput>);
static_assert(std::is_aggregate_v<chronos::query::VectorAggregateDefinition>);
static_assert(std::is_aggregate_v<chronos::query::VectorAggregateOutputShape>);
static_assert(std::is_aggregate_v<chronos::query::UngroupedAggregateLimits>);
