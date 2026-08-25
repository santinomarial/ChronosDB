#include "chronos/cluster/distributed_mutable_query_control_tcp.hpp"

#include <type_traits>

static_assert(
    !std::is_copy_constructible_v<chronos::cluster::DistributedMutableQueryControlTcpServer>);
static_assert(std::is_nothrow_move_constructible_v<
              chronos::cluster::DistributedMutableQueryControlTcpServer>);
