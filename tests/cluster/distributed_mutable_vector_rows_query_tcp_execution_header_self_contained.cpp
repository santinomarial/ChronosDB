#include "chronos/cluster/distributed_mutable_vector_rows_query_tcp_execution.hpp"

#include <type_traits>

static_assert(
    !std::is_copy_constructible_v<chronos::cluster::DistributedMutableVectorRowsQueryTcpExecution>);
static_assert(
    std::is_move_constructible_v<chronos::cluster::DistributedMutableVectorRowsQueryTcpExecution>);

namespace {
[[maybe_unused]] const auto kCreate =
    &chronos::cluster::DistributedMutableVectorRowsQueryTcpExecution::create;
}
