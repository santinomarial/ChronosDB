#include "chronos/cluster/distributed_mutable_vector_query_tcp.hpp"

#include <type_traits>

static_assert(
    std::is_move_constructible_v<chronos::cluster::DistributedMutableVectorQueryTcpClient>);
static_assert(
    std::is_move_constructible_v<chronos::cluster::DistributedMutableVectorQueryTcpServer>);
