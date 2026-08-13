#include "chronos/service/replicated_distributed_query_worker.hpp"

#include <functional>
#include <utility>

namespace chronos::service {

ReplicatedDistributedQueryWorker::ReplicatedDistributedQueryWorker(
    ReplicatedDistributedQueryWorkerConfig config) noexcept
    : config_(config) {}

common::Result<ReplicatedDistributedQueryWorker>
ReplicatedDistributedQueryWorker::create(ReplicatedDistributedQueryWorkerConfig config) {
  if (config.local_node_id == 0U || config.storage == nullptr ||
      config.context_provider == nullptr) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument,
                       "replicated distributed query worker configuration is invalid"});
  }
  return ReplicatedDistributedQueryWorker{config};
}

common::Result<query::ExchangeMessage> ReplicatedDistributedQueryWorker::execute(
    const query::DistributedAggregateFragmentDispatch& dispatch) {
  auto context = config_.context_provider->acquire(dispatch);
  if (!context.has_value())
    return common::make_unexpected(context.error());
  if (!context->lineage) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument,
                       "replicated distributed query worker context has no schema lineage"});
  }
  return query::execute_distributed_aggregate_fragment(
      {.dispatch = std::cref(dispatch),
       .storage = std::cref(*config_.storage),
       .snapshot = std::cref(context->snapshot),
       .lineage = std::cref(*context->lineage),
       .placement = std::cref(context->placement),
       .raft_group_id = context->raft_group_id,
       .local_node = config_.local_node_id,
       .local_linearizable_barrier = context->local_linearizable_barrier,
       .limits = config_.limits});
}

} // namespace chronos::service
