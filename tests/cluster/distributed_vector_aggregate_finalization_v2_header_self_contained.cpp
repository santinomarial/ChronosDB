#include "chronos/cluster/distributed_vector_aggregate_finalization_v2.hpp"

#include <type_traits>

static_assert(
    std::is_aggregate_v<chronos::cluster::DistributedVectorAggregateFinalizationLimitsV2>);
static_assert(std::is_aggregate_v<chronos::cluster::DistributedVectorAggregateFinalizedResultV2>);

namespace {
using Finalize =
    chronos::common::Result<chronos::cluster::DistributedVectorAggregateFinalizedResultV2> (*)(
        const chronos::query::DistributedVectorPlanIntent&,
        chronos::query::DistributedVectorAggregateQueryResultV2&&,
        chronos::cluster::DistributedVectorAggregateFinalizationLimitsV2);
[[maybe_unused]] const Finalize kFinalize =
    &chronos::cluster::finalize_distributed_vector_aggregate_v2;
using FinalizeProjection =
    chronos::common::Result<chronos::cluster::DistributedVectorAggregateFinalizedResultV2> (*)(
        const chronos::query::DistributedVectorPlanIntent&,
        chronos::query::DistributedVectorAggregateQueryResultV2&&,
        const chronos::query::DistributedVectorAggregateCoordinatorProjection&,
        chronos::cluster::DistributedVectorAggregateFinalizationLimitsV2);
[[maybe_unused]] const FinalizeProjection kFinalizeProjection =
    &chronos::cluster::finalize_distributed_vector_aggregate_with_projection_v2;
} // namespace
