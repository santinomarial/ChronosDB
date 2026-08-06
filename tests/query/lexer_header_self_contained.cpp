#include "chronos/query/lexer.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::query::SqlTokenStream>);
static_assert(std::is_nothrow_move_constructible_v<chronos::query::SqlTokenStream>);
