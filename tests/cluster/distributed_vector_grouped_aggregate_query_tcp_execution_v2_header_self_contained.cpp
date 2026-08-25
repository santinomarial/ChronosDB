#include "chronos/cluster/distributed_vector_grouped_aggregate_query_tcp_execution_v2.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateQueryTcpExecutionV2>);
static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateQueryTcpExecutionV2>);
static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateQueryTcpExecutionConfigV2>);

namespace {
[[maybe_unused]] const auto kCreate =
    &chronos::cluster::DistributedVectorGroupedAggregateQueryTcpExecutionV2::create;
} // namespace
