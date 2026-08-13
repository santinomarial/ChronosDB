#include "chronos/cluster/distributed_vector_aggregate_query_execution_v2.hpp"

#include <type_traits>

static_assert(
    !std::is_copy_constructible_v<chronos::cluster::DistributedVectorAggregateQueryExecutionV2>);
static_assert(
    std::is_move_constructible_v<chronos::cluster::DistributedVectorAggregateQueryExecutionV2>);

namespace {
[[maybe_unused]] const auto kCreate =
    &chronos::cluster::DistributedVectorAggregateQueryExecutionV2::create;
} // namespace
