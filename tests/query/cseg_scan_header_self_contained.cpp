#include "chronos/query/cseg_scan.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::query::CsegPartPin>);
static_assert(std::is_copy_constructible_v<chronos::query::CsegPartPin>);
static_assert(
    std::is_base_of_v<chronos::query::PhysicalOperator, chronos::query::CsegScanOperator>);
