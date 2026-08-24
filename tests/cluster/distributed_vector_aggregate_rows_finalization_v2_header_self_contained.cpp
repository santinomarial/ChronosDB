#include "chronos/cluster/distributed_vector_aggregate_rows_finalization_v2.hpp"

#include <type_traits>

static_assert(
    std::is_aggregate_v<chronos::cluster::DistributedVectorAggregateRowsFinalizationLimitsV2>);

namespace {
using Finalize =
    chronos::common::Result<chronos::cluster::DistributedVectorAggregateFinalizedResultV2> (*)(
        chronos::cluster::DistributedVectorQueryExecutionResultV2&&,
        const chronos::query::DistributedVectorPlanIntent&,
        chronos::query::DistributedVectorResultSchema&&,
        chronos::cluster::DistributedVectorAggregateRowsFinalizationLimitsV2);
[[maybe_unused]] const Finalize kFinalize =
    &chronos::cluster::finalize_distributed_vector_aggregate_rows_v2;
} // namespace
