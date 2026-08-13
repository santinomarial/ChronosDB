#include "chronos/cluster/distributed_vector_row_finalization_v2.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::cluster::DistributedVectorRowFinalizationLimitsV2>);
static_assert(std::is_aggregate_v<chronos::cluster::DistributedVectorRowsFinalizedResultV2>);

namespace {
[[maybe_unused]] const auto kFinalize = &chronos::cluster::finalize_distributed_vector_rows_v2;
} // namespace
