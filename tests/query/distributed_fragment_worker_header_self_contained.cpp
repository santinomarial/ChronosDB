#include "chronos/query/distributed_fragment_worker.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::DistributedAggregateWorkerLimits>);
static_assert(std::is_aggregate_v<chronos::query::DistributedAggregateWorkerRequest>);
static_assert(std::is_aggregate_v<chronos::query::DistributedGroupedFloat64WorkerRequest>);
static_assert(std::is_aggregate_v<chronos::query::DistributedVectorRowsWorkerLimitsV2>);
static_assert(std::is_aggregate_v<chronos::query::DistributedVectorRowsWorkerRequestV2>);
static_assert(std::has_virtual_destructor_v<chronos::query::DistributedVectorRowsChunkConsumerV2>);
static_assert(std::is_aggregate_v<chronos::query::DistributedVectorAggregateWorkerLimitsV2>);
static_assert(std::is_aggregate_v<chronos::query::DistributedVectorAggregateWorkerRequestV2>);
static_assert(std::is_aggregate_v<chronos::query::DistributedVectorGroupedAggregateWorkerLimitsV2>);
static_assert(
    std::is_aggregate_v<chronos::query::DistributedVectorGroupedAggregateWorkerRequestV2>);

namespace {
using Execute = chronos::common::Result<chronos::query::ExchangeMessage> (*)(
    const chronos::query::DistributedAggregateWorkerRequest&);
[[maybe_unused]] const Execute kExecute = &chronos::query::execute_distributed_aggregate_fragment;
using ExecuteGrouped =
    chronos::common::Result<chronos::query::DistributedGroupedFloat64WorkerResult> (*)(
        const chronos::query::DistributedGroupedFloat64WorkerRequest&);
[[maybe_unused]] const ExecuteGrouped kExecuteGrouped =
    &chronos::query::execute_distributed_grouped_float64_fragment;
using ExecuteVectorRows =
    chronos::common::Result<chronos::query::DistributedVectorRowsWorkerResultV2> (*)(
        const chronos::query::DistributedVectorRowsWorkerRequestV2&,
        chronos::query::DistributedVectorRowsChunkConsumerV2&);
[[maybe_unused]] const ExecuteVectorRows kExecuteVectorRows =
    &chronos::query::execute_distributed_vector_rows_fragment_v2;
using ExecuteVectorAggregate =
    chronos::common::Result<chronos::query::DistributedVectorAggregateWorkerResultV2> (*)(
        const chronos::query::DistributedVectorAggregateWorkerRequestV2&);
[[maybe_unused]] const ExecuteVectorAggregate kExecuteVectorAggregate =
    &chronos::query::execute_distributed_vector_aggregate_fragment_v2;
using ExecuteVectorGroupedAggregate =
    chronos::common::Result<chronos::query::DistributedVectorGroupedAggregateWorkerResultV2> (*)(
        const chronos::query::DistributedVectorGroupedAggregateWorkerRequestV2&);
[[maybe_unused]] const ExecuteVectorGroupedAggregate kExecuteVectorGroupedAggregate =
    &chronos::query::execute_distributed_vector_grouped_aggregate_fragment_v2;
} // namespace
