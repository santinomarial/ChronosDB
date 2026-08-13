#include "chronos/service/replicated_distributed_vector_query_tcp_server_v2.hpp"

#include <type_traits>

static_assert(
    std::is_aggregate_v<chronos::service::ReplicatedDistributedVectorQueryTcpServerConfigV2>);
static_assert(
    !std::is_copy_constructible_v<chronos::service::ReplicatedDistributedVectorQueryTcpServerV2>);
static_assert(std::is_nothrow_move_constructible_v<
              chronos::service::ReplicatedDistributedVectorQueryTcpServerV2>);

namespace {
[[maybe_unused]] const auto kStart =
    &chronos::service::ReplicatedDistributedVectorQueryTcpServerV2::start;
} // namespace
