#include "chronos/common/time_source.hpp"

#include <type_traits>

static_assert(std::is_base_of_v<chronos::common::TimeSource, chronos::common::SystemTimeSource>);
