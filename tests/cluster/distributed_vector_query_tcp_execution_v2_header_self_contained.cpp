#include "chronos/cluster/distributed_vector_query_tcp_execution_v2.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::cluster::DistributedVectorQueryTcpExecutionConfigV2>);
static_assert(std::is_aggregate_v<chronos::cluster::DistributedVectorQueryTcpExecutionMetricsV2>);
static_assert(
    !std::is_copy_constructible_v<chronos::cluster::DistributedVectorQueryTcpExecutionV2>);
static_assert(
    std::is_nothrow_move_constructible_v<chronos::cluster::DistributedVectorQueryTcpExecutionV2>);

namespace {
[[maybe_unused]] const auto kCreate =
    &chronos::cluster::DistributedVectorQueryTcpExecutionV2::create;
} // namespace
