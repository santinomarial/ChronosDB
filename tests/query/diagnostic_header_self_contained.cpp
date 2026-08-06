#include "chronos/query/diagnostic.hpp"

#include <type_traits>

static_assert(std::is_copy_constructible_v<chronos::query::SqlDiagnostic>);
