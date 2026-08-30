#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_shuffle_source_worker.hpp"

#include <new>

namespace chronos::cluster {

DistributedMutableVectorGroupedAggregateShuffleSourceWorker::
    DistributedMutableVectorGroupedAggregateShuffleSourceWorker(
        DistributedMutableVectorGroupedAggregateQueryWorkerService& worker,
        DistributedVectorGroupedAggregateShuffleJobService& jobs) noexcept
    : worker_(&worker), jobs_(&jobs) {}

common::Result<DistributedMutableVectorGroupedAggregateShuffleSourceWorker>
DistributedMutableVectorGroupedAggregateShuffleSourceWorker::create(
    DistributedMutableVectorGroupedAggregateQueryWorkerService& worker,
    DistributedVectorGroupedAggregateShuffleJobService& jobs) noexcept {
  return DistributedMutableVectorGroupedAggregateShuffleSourceWorker{worker, jobs};
}

common::Result<query::DistributedVectorGroupedAggregateAuthority>
DistributedMutableVectorGroupedAggregateShuffleSourceWorker::bind_authority(
    const query::DistributedMutableVectorFragment& fragment) {
  return worker_->bind_authority(fragment);
}

common::Result<query::DistributedVectorGroupedAggregateWorkerResultV2>
DistributedMutableVectorGroupedAggregateShuffleSourceWorker::execute(
    const query::DistributedMutableVectorFragment& fragment) {
  auto result = worker_->execute(fragment);
  if (!result.has_value())
    return common::make_unexpected(result.error());
  auto published =
      jobs_->publish_local_source(fragment.query_id, fragment.tablet_id, result->messages);
  if (!published.has_value())
    return common::make_unexpected(published.error());
  return result;
}

} // namespace chronos::cluster
