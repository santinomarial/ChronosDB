#include "chronos/service/replicated_distributed_mutable_query_control_tcp_server.hpp"

#include <type_traits>

static_assert(
    std::is_aggregate_v<chronos::service::ReplicatedDistributedMutableQueryControlTcpServerConfig>);
static_assert(!std::is_copy_constructible_v<
              chronos::service::ReplicatedDistributedMutableQueryControlTcpServer>);
static_assert(std::is_nothrow_move_constructible_v<
              chronos::service::ReplicatedDistributedMutableQueryControlTcpServer>);

namespace {
[[maybe_unused]] const auto kStart =
    &chronos::service::ReplicatedDistributedMutableQueryControlTcpServer::start;
} // namespace
