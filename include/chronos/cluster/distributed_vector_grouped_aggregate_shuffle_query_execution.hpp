#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_QUERY_EXECUTION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_QUERY_EXECUTION_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_finalization_v2.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_source_plan.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tcp_execution.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_sql_lowering.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::cluster {

struct DistributedVectorGroupedAggregateShuffleSourceInput {
  schema::TabletId tablet_id;
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> messages;
};

struct DistributedVectorGroupedAggregateShuffleQueryExecutionConfig {
  std::vector<DistributedVectorGroupedAggregateShuffleDestinationExecutionConfig> destinations;
  std::optional<DistributedVectorGroupedAggregateShuffleTcpExecutionConfig> transport;
  DistributedVectorGroupedAggregateShuffleSourcePlanLimits source_plan;
  DistributedVectorGroupedAggregateFinalizationLimitsV2 finalization;
  std::optional<query::DistributedVectorGroupedAggregateCoordinatorProjection>
      coordinator_projection;
  std::size_t maximum_planning_memory_bytes{
      query::kDefaultDistributedVectorGroupedPartitionOutputBytes};
  std::size_t maximum_result_working_memory_bytes{
      kDefaultDistributedVectorGroupedAggregateShuffleResultWorkingMemoryBytes};
};

struct DistributedVectorGroupedAggregateShuffleQueryExecutionMetrics {
  std::size_t source_tablets{};
  std::size_t destination_nodes{};
  std::size_t local_edges{};
  std::size_t remote_edges{};
  std::size_t ready_destinations{};
  DistributedVectorGroupedAggregateShuffleTcpExecutionMetrics transport;
  DistributedVectorGroupedAggregateShuffleResultExecutionMetrics result;
};

enum class DistributedVectorGroupedAggregateShuffleQueryExecutionState : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
  kCancelled = 4,
};

// Owns one complete post-worker grouped shuffle: immutable authority, source fan-out, all
// destination reducers/listeners, remote receipt scheduling, canonical result gathering, and
// atomic Native finalization. All methods are serialized by one caller thread. TLS/security
// dependencies and route TLS contexts are borrowed and outlive the owner.
class DistributedVectorGroupedAggregateShuffleQueryExecution {
public:
  DistributedVectorGroupedAggregateShuffleQueryExecution() noexcept;
  ~DistributedVectorGroupedAggregateShuffleQueryExecution();
  DistributedVectorGroupedAggregateShuffleQueryExecution(
      const DistributedVectorGroupedAggregateShuffleQueryExecution&) = delete;
  DistributedVectorGroupedAggregateShuffleQueryExecution&
  operator=(const DistributedVectorGroupedAggregateShuffleQueryExecution&) = delete;
  DistributedVectorGroupedAggregateShuffleQueryExecution(
      DistributedVectorGroupedAggregateShuffleQueryExecution&&) noexcept;
  DistributedVectorGroupedAggregateShuffleQueryExecution&
  operator=(DistributedVectorGroupedAggregateShuffleQueryExecution&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleQueryExecution>
  create(DistributedVectorGroupedAggregateShuffleAuthority authority,
         std::vector<query::DistributedMutableVectorFragment> fragments,
         std::vector<DistributedVectorGroupedAggregateShuffleSourceInput> sources,
         DistributedVectorGroupedAggregateShuffleQueryExecutionConfig config);

  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();

  [[nodiscard]] DistributedVectorGroupedAggregateShuffleQueryExecutionState state() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleQueryExecutionMetrics
  metrics() const noexcept;
  [[nodiscard]] const std::optional<DistributedVectorRowsFinalizedResultV2>&
  result() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleQueryExecution(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_QUERY_EXECUTION_HPP_
