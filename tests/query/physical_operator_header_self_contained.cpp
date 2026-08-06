#include "chronos/query/physical_operator.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::query::AccountedVectorChunk>);
static_assert(!std::is_copy_constructible_v<chronos::query::AccountedVectorChunk>);
static_assert(std::is_nothrow_move_constructible_v<chronos::query::AccountedVectorChunk>);
static_assert(!std::is_default_constructible_v<chronos::query::PhysicalOperatorStep>);
static_assert(!std::is_copy_constructible_v<chronos::query::PhysicalOperatorStep>);
static_assert(std::is_nothrow_move_constructible_v<chronos::query::PhysicalOperatorStep>);
static_assert(std::has_virtual_destructor_v<chronos::query::PhysicalOperator>);
static_assert(
    std::is_base_of_v<chronos::query::PhysicalOperator, chronos::query::ColumnSubsetOperator>);
static_assert(chronos::query::kMaximumColumnSubsetWidth == 4'096U);
