#include "chronos/common/log.hpp"

#include <type_traits>

static_assert(std::is_trivially_copyable_v<chronos::common::LogField>);
