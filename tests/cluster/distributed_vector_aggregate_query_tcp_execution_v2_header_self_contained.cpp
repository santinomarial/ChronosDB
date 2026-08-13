#include "chronos/cluster/distributed_vector_aggregate_query_tcp_execution_v2.hpp"

#include <type_traits>

static_assert(
    !std::is_copy_constructible_v<chronos::cluster::DistributedVectorAggregateQueryTcpExecutionV2>);
static_assert(
    std::is_move_constructible_v<chronos::cluster::DistributedVectorAggregateQueryTcpExecutionV2>);

namespace {
[[maybe_unused]] const auto kCreate =
    &chronos::cluster::DistributedVectorAggregateQueryTcpExecutionV2::create;
} // namespace
