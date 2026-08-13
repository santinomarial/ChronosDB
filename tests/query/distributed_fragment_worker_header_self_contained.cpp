#include "chronos/query/distributed_fragment_worker.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::DistributedAggregateWorkerLimits>);
static_assert(std::is_aggregate_v<chronos::query::DistributedAggregateWorkerRequest>);
static_assert(std::is_aggregate_v<chronos::query::DistributedGroupedFloat64WorkerRequest>);

namespace {
using Execute = chronos::common::Result<chronos::query::ExchangeMessage> (*)(
    const chronos::query::DistributedAggregateWorkerRequest&);
[[maybe_unused]] const Execute kExecute = &chronos::query::execute_distributed_aggregate_fragment;
using ExecuteGrouped =
    chronos::common::Result<chronos::query::DistributedGroupedFloat64WorkerResult> (*)(
        const chronos::query::DistributedGroupedFloat64WorkerRequest&);
[[maybe_unused]] const ExecuteGrouped kExecuteGrouped =
    &chronos::query::execute_distributed_grouped_float64_fragment;
} // namespace
