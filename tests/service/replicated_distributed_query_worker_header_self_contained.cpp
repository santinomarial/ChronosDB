#include "chronos/service/replicated_distributed_grouped_query_receiver.hpp"
#include "chronos/service/replicated_distributed_query_worker.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::service::ReplicatedDistributedQueryWorkerContext>);
static_assert(std::is_aggregate_v<chronos::service::ReplicatedDistributedQueryWorkerConfig>);
static_assert(!std::is_copy_constructible_v<chronos::service::ReplicatedDistributedQueryWorker>);
static_assert(std::is_move_constructible_v<chronos::service::ReplicatedDistributedQueryWorker>);
static_assert(std::is_aggregate_v<chronos::service::ReplicatedDistributedGroupedQueryWorkerConfig>);
static_assert(
    !std::is_copy_constructible_v<chronos::service::ReplicatedDistributedGroupedQueryWorker>);
static_assert(
    std::is_move_constructible_v<chronos::service::ReplicatedDistributedGroupedQueryWorker>);
static_assert(
    std::is_aggregate_v<chronos::service::ReplicatedDistributedVectorQueryWorkerConfigV2>);
static_assert(
    !std::is_copy_constructible_v<chronos::service::ReplicatedDistributedVectorQueryWorkerV2>);
static_assert(
    std::is_move_constructible_v<chronos::service::ReplicatedDistributedVectorQueryWorkerV2>);
static_assert(
    std::is_aggregate_v<chronos::service::ReplicatedDistributedGroupedQueryReceiverConfig>);
static_assert(
    !std::is_copy_constructible_v<chronos::service::ReplicatedDistributedGroupedQueryReceiver>);
static_assert(
    std::is_move_constructible_v<chronos::service::ReplicatedDistributedGroupedQueryReceiver>);
