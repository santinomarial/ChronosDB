#include "chronos/query/temporal_cseg_snapshot.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::TemporalCsegResolutionLimits>);
