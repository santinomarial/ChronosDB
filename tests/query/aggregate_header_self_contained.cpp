#include "chronos/query/aggregate.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::VectorAggregateInput>);
static_assert(std::is_aggregate_v<chronos::query::VectorAggregateDefinition>);
static_assert(std::is_aggregate_v<chronos::query::VectorAggregateOutputShape>);
static_assert(std::is_aggregate_v<chronos::query::UngroupedAggregateLimits>);
static_assert(std::is_aggregate_v<chronos::query::VectorGroupKeyDefinition>);
static_assert(std::is_aggregate_v<chronos::query::GroupedAggregateLimits>);
static_assert(std::is_move_constructible_v<chronos::query::MergeableVectorAggregateState>);
static_assert(!std::is_copy_constructible_v<chronos::query::MergeableVectorAggregateState>);
static_assert(chronos::query::kDefaultAggregateExtremumByteLimit > 0U);
