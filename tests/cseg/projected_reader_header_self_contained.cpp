#include "chronos/cseg/projected_reader.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::cseg::CsegProjectedGranuleReadPlan>);
static_assert(std::is_copy_constructible_v<chronos::cseg::CsegProjectedGranuleReadPlan>);
