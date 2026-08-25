#include "chronos/service/replicated_distributed_vector_grouped_aggregate_query_tcp_server_v2.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<
              chronos::service::ReplicatedDistributedVectorGroupedAggregateQueryTcpServerConfigV2>);
static_assert(!std::is_copy_constructible_v<
              chronos::service::ReplicatedDistributedVectorGroupedAggregateQueryTcpServerV2>);
static_assert(std::is_nothrow_move_constructible_v<
              chronos::service::ReplicatedDistributedVectorGroupedAggregateQueryTcpServerV2>);

namespace {
[[maybe_unused]] const auto kStart =
    &chronos::service::ReplicatedDistributedVectorGroupedAggregateQueryTcpServerV2::start;
} // namespace
