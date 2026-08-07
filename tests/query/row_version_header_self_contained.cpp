#include "chronos/query/row_version.hpp"

#include <type_traits>

static_assert(chronos::query::kVectorRowVersionColumnCount == 4U);
static_assert(!std::is_default_constructible_v<chronos::query::VectorRowVersionLayout>);
static_assert(std::is_trivially_copyable_v<chronos::query::VectorRowVersionLayout>);
