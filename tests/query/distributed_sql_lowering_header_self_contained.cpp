#include "chronos/query/distributed_sql_lowering.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::DistributedVectorRowsSqlPlan>);
static_assert(std::is_move_constructible_v<chronos::query::DistributedVectorRowsSqlPlan>);
static_assert(std::is_aggregate_v<chronos::query::DistributedVectorRowsSqlLoweringLimits>);
static_assert(std::is_aggregate_v<chronos::query::DistributedVectorAggregateSqlPlan>);
static_assert(std::is_move_constructible_v<chronos::query::DistributedVectorAggregateSqlPlan>);
static_assert(std::is_aggregate_v<chronos::query::DistributedVectorAggregateSqlLoweringLimits>);
