#include "chronos/service/replicated_distributed_grouped_query_tcp_server.hpp"

#include <type_traits>

static_assert(
    !std::is_copy_constructible_v<chronos::service::ReplicatedDistributedGroupedQueryTcpServer>);
static_assert(std::is_nothrow_move_constructible_v<
              chronos::service::ReplicatedDistributedGroupedQueryTcpServer>);
