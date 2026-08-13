#include "chronos/service/replicated_distributed_query_tcp_server.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::service::ReplicatedDistributedQueryTcpServerConfig>);
static_assert(!std::is_copy_constructible_v<chronos::service::ReplicatedDistributedQueryTcpServer>);
static_assert(std::is_move_constructible_v<chronos::service::ReplicatedDistributedQueryTcpServer>);
