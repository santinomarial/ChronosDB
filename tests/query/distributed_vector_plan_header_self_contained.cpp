#include "chronos/query/distributed_vector_plan.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::DistributedVectorPlanIntent>);
static_assert(!std::is_copy_constructible_v<chronos::query::EncodedDistributedVectorPlanIntent>);
