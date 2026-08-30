#ifndef CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_SHUFFLE_SOURCE_WORKER_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_SHUFFLE_SOURCE_WORKER_HPP_

#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_transport.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_service.hpp"

namespace chronos::cluster {

// Borrowed decorator for the packaged mutable grouped worker. Direct grouped queries pass through
// unchanged. When the query ID names a prepared reducer/source job with installed routes, the
// complete canonical tablet result is atomically handed to that local job before a worker response
// becomes visible. The reducer-job service serializes this query-thread publication with its
// query-control poll owner.
class DistributedMutableVectorGroupedAggregateShuffleSourceWorker final
    : public DistributedMutableVectorGroupedAggregateQueryWorkerService {
public:
  DistributedMutableVectorGroupedAggregateShuffleSourceWorker() = delete;

  [[nodiscard]] static common::Result<DistributedMutableVectorGroupedAggregateShuffleSourceWorker>
  create(DistributedMutableVectorGroupedAggregateQueryWorkerService& worker,
         DistributedVectorGroupedAggregateShuffleJobService& jobs) noexcept;

  [[nodiscard]] common::Result<query::DistributedVectorGroupedAggregateAuthority>
  bind_authority(const query::DistributedMutableVectorFragment& fragment) override;
  [[nodiscard]] common::Result<query::DistributedVectorGroupedAggregateWorkerResultV2>
  execute(const query::DistributedMutableVectorFragment& fragment) override;

private:
  DistributedMutableVectorGroupedAggregateShuffleSourceWorker(
      DistributedMutableVectorGroupedAggregateQueryWorkerService& worker,
      DistributedVectorGroupedAggregateShuffleJobService& jobs) noexcept;

  DistributedMutableVectorGroupedAggregateQueryWorkerService* worker_{};
  DistributedVectorGroupedAggregateShuffleJobService* jobs_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_SHUFFLE_SOURCE_WORKER_HPP_
