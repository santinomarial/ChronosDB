#ifndef CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_SHUFFLE_EXECUTION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_SHUFFLE_EXECUTION_HPP_

#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_query_execution.hpp"
#include "chronos/common/result.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::cluster {

struct DistributedMutableVectorGroupedAggregateShuffleExecutionConfig {
  DistributedMutableVectorGroupedAggregateQueryExecutionLimits worker_execution;
  DistributedMutableVectorGroupedAggregateQueryTcpExecutionConfig worker_transport;
  DistributedVectorGroupedAggregateShuffleQueryExecutionConfig shuffle;
  DistributedVectorGroupedAggregateShuffleAuthorityLimits authority;
};

struct DistributedMutableVectorGroupedAggregateShuffleExecutionMetrics {
  DistributedMutableVectorGroupedAggregateQueryTcpExecutionMetrics workers;
  DistributedVectorGroupedAggregateShuffleQueryExecutionMetrics shuffle;
};

enum class DistributedMutableVectorGroupedAggregateShuffleExecutionState : std::uint8_t {
  kCollectingSources = 1,
  kShuffling = 2,
  kComplete = 3,
  kFailed = 4,
  kCancelled = 5,
};

// Owns mutable worker scheduling followed by partitioned grouped shuffle and atomic Native
// finalization. Every method is serialized by one caller thread. Worker/security dependencies,
// route TLS contexts, and shuffle destination security dependencies are borrowed and outlive it.
class DistributedMutableVectorGroupedAggregateShuffleExecution {
public:
  DistributedMutableVectorGroupedAggregateShuffleExecution() noexcept;
  ~DistributedMutableVectorGroupedAggregateShuffleExecution();
  DistributedMutableVectorGroupedAggregateShuffleExecution(
      const DistributedMutableVectorGroupedAggregateShuffleExecution&) = delete;
  DistributedMutableVectorGroupedAggregateShuffleExecution&
  operator=(const DistributedMutableVectorGroupedAggregateShuffleExecution&) = delete;
  DistributedMutableVectorGroupedAggregateShuffleExecution(
      DistributedMutableVectorGroupedAggregateShuffleExecution&&) noexcept;
  DistributedMutableVectorGroupedAggregateShuffleExecution&
  operator=(DistributedMutableVectorGroupedAggregateShuffleExecution&&) noexcept;

  [[nodiscard]] static common::Result<DistributedMutableVectorGroupedAggregateShuffleExecution>
  create(raft::NodeId source_node_id,
         std::vector<query::DistributedMutableVectorFragment> fragments,
         std::vector<query::VectorGroupKeyDefinition> keys,
         std::vector<query::VectorAggregateDefinition> aggregates,
         DistributedMutableVectorGroupedAggregateShuffleExecutionConfig config);

  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();
  [[nodiscard]] DistributedMutableVectorGroupedAggregateShuffleExecutionState
  state() const noexcept;
  [[nodiscard]] DistributedMutableVectorGroupedAggregateShuffleExecutionMetrics
  metrics() const noexcept;
  [[nodiscard]] const std::optional<DistributedVectorRowsFinalizedResultV2>&
  result() const noexcept;
  [[nodiscard]] common::Result<DistributedVectorRowsFinalizedResultV2> take_result();
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedMutableVectorGroupedAggregateShuffleExecution(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_SHUFFLE_EXECUTION_HPP_
