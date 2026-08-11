#include "chronos/query/distributed_fragment_worker.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::DistributedAggregateWorkerLimits>);
static_assert(std::is_aggregate_v<chronos::query::DistributedAggregateWorkerRequest>);

namespace {
[[maybe_unused]] const auto kExecute = &chronos::query::execute_distributed_aggregate_fragment;
}
