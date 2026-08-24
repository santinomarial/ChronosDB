#include "chronos/cluster/distributed_mutable_vector_query_execution.hpp"

#include <type_traits>

static_assert(
    !std::is_copy_constructible_v<chronos::cluster::DistributedMutableVectorQueryExecution>);
static_assert(
    std::is_move_constructible_v<chronos::cluster::DistributedMutableVectorQueryExecution>);

namespace {
[[maybe_unused]] const auto kCreate =
    &chronos::cluster::DistributedMutableVectorQueryExecution::create;
}
