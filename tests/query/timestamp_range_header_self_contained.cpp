#include "chronos/query/timestamp_range.hpp"

#include <limits>
#include <type_traits>

static_assert(std::is_trivially_copyable_v<chronos::query::TimestampRangePredicate>);
static_assert(chronos::query::TimestampRangePredicate{
    .lower = chronos::query::TimestampRangeBound{.value = std::numeric_limits<std::int64_t>::min(),
                                                 .inclusive = true},
    .upper = chronos::query::TimestampRangeBound{.value = std::numeric_limits<std::int64_t>::max(),
                                                 .inclusive = true}}
                  .matches(0));
