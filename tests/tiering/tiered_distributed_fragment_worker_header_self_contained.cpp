#include "chronos/tiering/tiered_distributed_fragment_worker.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::tiering::TieredDistributedAggregateWorkerRequest>);

namespace {
[[maybe_unused]] const auto kExecute =
    &chronos::tiering::execute_tiered_distributed_aggregate_fragment;
}
