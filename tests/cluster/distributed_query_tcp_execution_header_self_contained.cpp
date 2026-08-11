#include "chronos/cluster/distributed_query_tcp_execution.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::cluster::DistributedQueryTcpExecution>);
static_assert(std::is_nothrow_move_constructible_v<chronos::cluster::DistributedQueryTcpExecution>);

namespace {
[[maybe_unused]] const auto kCreateTcpExecution =
    &chronos::cluster::DistributedQueryTcpExecution::create;
} // namespace
