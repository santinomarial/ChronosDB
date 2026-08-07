#include "chronos/query/column_output.hpp"

#include <type_traits>

static_assert(std::is_base_of_v<chronos::query::PhysicalOperator,
                                chronos::query::SourceColumnOutputOperator>);
static_assert(
    std::is_base_of_v<chronos::query::PhysicalOperator, chronos::query::ColumnOutputOperator>);
static_assert(chronos::query::kMaximumSourceColumnOutputWidth > 0U);
static_assert(chronos::query::kMaximumColumnOutputWidth > 0U);
static_assert(std::variant_size_v<chronos::query::ColumnOutputPosition> == 2U);
