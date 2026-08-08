#include "chronos/query/physical_optimizer.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::query::OptimizedPhysicalPipelinePlan>);
static_assert(!std::is_copy_constructible_v<chronos::query::OptimizedPhysicalPipelinePlan>);
static_assert(std::is_nothrow_move_constructible_v<chronos::query::OptimizedPhysicalPipelinePlan>);
static_assert(chronos::query::kDefaultPhysicalOptimizerSortLimit == 256U);
