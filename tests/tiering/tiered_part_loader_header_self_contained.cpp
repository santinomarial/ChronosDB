#include "chronos/tiering/tiered_part_loader.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::tiering::TieredTemporalPartLoadLimits>);
