#include "chronos/cluster/distributed_mutable_vector_query_tls.hpp"

#include <type_traits>

static_assert(
    std::is_move_constructible_v<chronos::cluster::DistributedMutableVectorQueryTlsClient>);
static_assert(
    std::is_move_constructible_v<chronos::cluster::DistributedMutableVectorQueryTlsServer>);
