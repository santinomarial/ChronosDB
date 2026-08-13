#include "chronos/service/replicated_distributed_vector_aggregate_query_tcp_server_v2.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<
              chronos::service::ReplicatedDistributedVectorAggregateQueryTcpServerConfigV2>);
static_assert(!std::is_copy_constructible_v<
              chronos::service::ReplicatedDistributedVectorAggregateQueryTcpServerV2>);
static_assert(std::is_nothrow_move_constructible_v<
              chronos::service::ReplicatedDistributedVectorAggregateQueryTcpServerV2>);

namespace {
[[maybe_unused]] const auto kStart =
    &chronos::service::ReplicatedDistributedVectorAggregateQueryTcpServerV2::start;
} // namespace
