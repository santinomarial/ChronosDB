#include "chronos/cseg/pruning.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<chronos::cseg::CsegEventTimePruningPlan>);
