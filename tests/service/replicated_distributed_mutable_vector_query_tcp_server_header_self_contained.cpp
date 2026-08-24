#include "chronos/service/replicated_distributed_mutable_vector_query_tcp_server.hpp"

#include <type_traits>

static_assert(
    std::is_aggregate_v<chronos::service::ReplicatedDistributedMutableVectorQueryTcpServerConfig>);
static_assert(!std::is_copy_constructible_v<
              chronos::service::ReplicatedDistributedMutableVectorQueryTcpServer>);
static_assert(std::is_nothrow_move_constructible_v<
              chronos::service::ReplicatedDistributedMutableVectorQueryTcpServer>);

namespace {
[[maybe_unused]] const auto kStart =
    &chronos::service::ReplicatedDistributedMutableVectorQueryTcpServer::start;
} // namespace
