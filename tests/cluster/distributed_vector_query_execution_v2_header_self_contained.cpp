#include "chronos/cluster/distributed_vector_query_execution_v2.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::cluster::DistributedVectorQueryExecutionLimitsV2>);
static_assert(std::is_aggregate_v<chronos::cluster::DistributedVectorQueryExecutionResultV2>);
static_assert(!std::is_copy_constructible_v<chronos::cluster::DistributedVectorQueryExecutionV2>);
static_assert(std::is_move_constructible_v<chronos::cluster::DistributedVectorQueryExecutionV2>);

namespace {
[[maybe_unused]] const auto kCreate = &chronos::cluster::DistributedVectorQueryExecutionV2::create;
} // namespace
