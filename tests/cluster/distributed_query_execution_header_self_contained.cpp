#include "chronos/cluster/distributed_query_execution.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::cluster::DistributedQueryExecution>);
static_assert(std::is_move_constructible_v<chronos::cluster::DistributedQueryExecution>);

namespace {
[[maybe_unused]] const auto kCreateExecution = &chronos::cluster::DistributedQueryExecution::create;
[[maybe_unused]] const auto kCreateExecutionFromBoundSnapshot =
    &chronos::cluster::DistributedQueryExecution::create_from_bound_snapshot;
} // namespace
