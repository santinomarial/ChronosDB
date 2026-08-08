#include "chronos/query/snapshot_pipeline.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::SnapshotTabletPipelineLimits>);
static_assert(std::is_aggregate_v<chronos::query::SnapshotTabletSourceBinding>);
