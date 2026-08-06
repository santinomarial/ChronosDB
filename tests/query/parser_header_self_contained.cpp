#include "chronos/query/parser.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::SqlParserLimits>);
