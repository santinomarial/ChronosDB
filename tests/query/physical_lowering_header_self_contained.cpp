#include "chronos/query/physical_lowering.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<chronos::query::PhysicalPipelinePlan>);
