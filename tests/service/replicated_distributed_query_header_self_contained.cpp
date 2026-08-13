#include "chronos/service/replicated_distributed_query.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::service::ReplicatedDistributedAggregateQueryConfig>);
static_assert(
    std::is_aggregate_v<chronos::service::ReplicatedDistributedVectorAggregateQueryConfigV2>);
static_assert(
    !std::is_copy_constructible_v<chronos::service::ReplicatedFollowerDistributedAggregateQuery>);
static_assert(
    std::is_move_constructible_v<chronos::service::ReplicatedFollowerDistributedAggregateQuery>);

namespace {
[[maybe_unused]] const auto kCreateReplicatedDistributedAggregateQuery =
    &chronos::service::create_replicated_distributed_aggregate_query;
[[maybe_unused]] const auto kCreateReplicatedFollowerDistributedAggregateQuery =
    &chronos::service::create_replicated_follower_distributed_aggregate_query;
[[maybe_unused]] const auto kCreateReplicatedDistributedVectorAggregateQueryV2 =
    &chronos::service::create_replicated_distributed_vector_aggregate_query_v2;
[[maybe_unused]] const auto kCreateReplicatedFollowerDistributedVectorAggregateQueryV2 =
    &chronos::service::create_replicated_follower_distributed_vector_aggregate_query_v2;
} // namespace
