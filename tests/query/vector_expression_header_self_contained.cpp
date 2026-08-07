#include "chronos/query/vector_expression.hpp"

#include <type_traits>
#include <variant>

static_assert(std::variant_size_v<chronos::query::VectorExpressionInstruction> == 4U);
static_assert(std::is_copy_constructible_v<chronos::query::VectorExpression>);
static_assert(std::is_move_constructible_v<chronos::query::VectorExpression>);
