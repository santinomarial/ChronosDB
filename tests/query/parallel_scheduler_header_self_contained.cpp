#include "chronos/query/parallel_scheduler.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::ParallelSchedulerLimits>);
static_assert(std::is_aggregate_v<chronos::query::ParallelSchedulerMetrics>);
static_assert(!std::is_copy_constructible_v<chronos::query::ParallelMergeOperator>);
