#include "chronos/cluster/distributed_vector_query_tcp_client_v2.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<chronos::cluster::DistributedVectorQueryTcpClientV2>);
static_assert(std::is_aggregate_v<chronos::cluster::DistributedVectorQueryTcpClientConfigV2>);

namespace {
[[maybe_unused]] const auto kBegin = &chronos::cluster::DistributedVectorQueryTcpClientV2::begin;
} // namespace
