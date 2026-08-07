#include "chronos/query/head_scan.hpp"

#include <type_traits>

static_assert(
    std::is_base_of_v<chronos::query::PhysicalOperator, chronos::query::HeadScanOperator>);
static_assert(std::is_aggregate_v<chronos::query::HeadScanLimits>);
