#include "chronos/query/column_output.hpp"

#include <type_traits>

static_assert(std::is_base_of_v<chronos::query::PhysicalOperator,
                                chronos::query::SourceColumnOutputOperator>);
static_assert(chronos::query::kMaximumSourceColumnOutputWidth > 0U);
