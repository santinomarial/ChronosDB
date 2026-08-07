#include "chronos/query/physical_plan.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::query::PhysicalPipelinePlan>);
static_assert(!std::is_copy_constructible_v<chronos::query::PhysicalPipelinePlan>);
static_assert(std::is_nothrow_move_constructible_v<chronos::query::PhysicalPipelinePlan>);
static_assert(std::variant_size_v<chronos::query::PhysicalPipelineStage> == 6U);
static_assert(chronos::query::kDefaultPhysicalPipelineStageLimit == 256U);
