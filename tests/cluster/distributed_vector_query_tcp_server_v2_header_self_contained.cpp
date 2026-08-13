#include "chronos/cluster/distributed_vector_query_tcp_server_v2.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::cluster::DistributedVectorQueryTcpServerV2>);
static_assert(
    std::is_nothrow_move_constructible_v<chronos::cluster::DistributedVectorQueryTcpServerV2>);

namespace {
[[maybe_unused]] const auto kStart = &chronos::cluster::DistributedVectorQueryTcpServerV2::start;
} // namespace
