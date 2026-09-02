#ifndef CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_EXECUTION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_EXECUTION_HPP_

#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_coordinator_execution.hpp"
#include "chronos/common/result.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::cluster {

struct DistributedMutableVectorGroupedAggregateShuffleJobExecutionConfig {
  DistributedMutableVectorGroupedAggregateQueryExecutionLimits worker_execution{};
  DistributedMutableVectorGroupedAggregateQueryTcpExecutionConfig worker_transport;
  DistributedVectorGroupedAggregateShuffleAuthorityLimits authority{};
  DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionConfig reducers;
};

struct DistributedMutableVectorGroupedAggregateShuffleJobExecutionMetrics {
  DistributedMutableVectorGroupedAggregateQueryTcpExecutionMetrics workers;
  DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionMetrics reducers;
};

enum class DistributedMutableVectorGroupedAggregateShuffleJobExecutionState : std::uint8_t {
  kPreparingReducers = 1,
  kCollectingSources = 2,
  kSealingReducers = 3,
  kCollectingResults = 4,
  kComplete = 5,
  kResultTaken = 6,
  kFailed = 7,
  kCancelled = 8,
  kCancelling = 9,
};

// Owns one independent-process mutable grouped query from reducer PREPARE through route
// installation, authenticated lease activation/renewal, worker source publication, receipt-proven
// source fan-out, reducer SEAL, returned partition collection, and atomic Native finalization.
// Workers may start only after every reducer has installed the complete route set and accepted its
// lease. The local and remote worker services must use the packaged source-publishing decorator.
// Failure or explicit cancellation remains nonterminal while reducer CANCEL acknowledgements are
// pending. One caller thread serializes all progress, renewal, and cancellation.
class DistributedMutableVectorGroupedAggregateShuffleJobExecution {
public:
  DistributedMutableVectorGroupedAggregateShuffleJobExecution() noexcept;
  ~DistributedMutableVectorGroupedAggregateShuffleJobExecution();
  DistributedMutableVectorGroupedAggregateShuffleJobExecution(
      const DistributedMutableVectorGroupedAggregateShuffleJobExecution&) = delete;
  DistributedMutableVectorGroupedAggregateShuffleJobExecution&
  operator=(const DistributedMutableVectorGroupedAggregateShuffleJobExecution&) = delete;
  DistributedMutableVectorGroupedAggregateShuffleJobExecution(
      DistributedMutableVectorGroupedAggregateShuffleJobExecution&&) noexcept;
  DistributedMutableVectorGroupedAggregateShuffleJobExecution&
  operator=(DistributedMutableVectorGroupedAggregateShuffleJobExecution&&) noexcept;

  [[nodiscard]] static common::Result<DistributedMutableVectorGroupedAggregateShuffleJobExecution>
  create(raft::NodeId source_node_id,
         std::vector<query::DistributedMutableVectorFragment> fragments,
         std::vector<query::VectorGroupKeyDefinition> keys,
         std::vector<query::VectorAggregateDefinition> aggregates,
         DistributedMutableVectorGroupedAggregateShuffleJobExecutionConfig config);

  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();
  [[nodiscard]] common::Result<DistributedVectorRowsFinalizedResultV2> take_result();
  [[nodiscard]] DistributedMutableVectorGroupedAggregateShuffleJobExecutionState
  state() const noexcept;
  [[nodiscard]] DistributedMutableVectorGroupedAggregateShuffleJobExecutionMetrics
  metrics() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedMutableVectorGroupedAggregateShuffleJobExecution(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_EXECUTION_HPP_
