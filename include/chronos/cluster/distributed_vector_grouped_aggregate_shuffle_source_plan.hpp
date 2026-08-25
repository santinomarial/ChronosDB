#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_SOURCE_PLAN_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_SOURCE_PLAN_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_retry.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_vector_grouped_aggregate_partitioner.hpp"

#include <cstddef>
#include <span>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kDefaultDistributedVectorGroupedAggregateShuffleSourcePlanOuterBytes =
    std::size_t{512U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorGroupedAggregateShuffleSourcePlanOuterBytes =
    std::size_t{1024U} * 1024U * 1024U;

struct DistributedVectorGroupedAggregateShuffleSourcePlanLimits {
  query::DistributedVectorGroupedAggregatePartitionerLimits partitioner;
  DistributedVectorGroupedAggregateShuffleRetryLimits retry;
  std::size_t maximum_total_outer_encoded_bytes{
      kDefaultDistributedVectorGroupedAggregateShuffleSourcePlanOuterBytes};
};

struct DistributedVectorGroupedAggregateShuffleSourcePlanMetrics {
  std::size_t local_edges{};
  std::size_t remote_edges{};
  std::size_t nested_encoded_bytes{};
  std::size_t outer_encoded_bytes{};
};

// Atomically partitions one complete canonical source-tablet stream across the immutable shuffle
// authority. Self-routes become in-process complete streams; remote routes become finite retry
// owners. The authority is borrowed by remote retries and must outlive this single-thread-affine
// owner. The caller-owned input is not retained.
class DistributedVectorGroupedAggregateShuffleSourcePlan {
public:
  DistributedVectorGroupedAggregateShuffleSourcePlan() = delete;
  DistributedVectorGroupedAggregateShuffleSourcePlan(
      const DistributedVectorGroupedAggregateShuffleSourcePlan&) = delete;
  DistributedVectorGroupedAggregateShuffleSourcePlan&
  operator=(const DistributedVectorGroupedAggregateShuffleSourcePlan&) = delete;
  DistributedVectorGroupedAggregateShuffleSourcePlan(
      DistributedVectorGroupedAggregateShuffleSourcePlan&&) noexcept = default;
  DistributedVectorGroupedAggregateShuffleSourcePlan&
  operator=(DistributedVectorGroupedAggregateShuffleSourcePlan&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleSourcePlan>
  create(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
         const schema::TabletId& tablet_id,
         std::span<const query::EncodedDistributedVectorGroupedAggregateExchangeMessage> input,
         const query::QueryResourceContext& resources,
         DistributedVectorGroupedAggregateShuffleSourcePlanLimits limits = {});

  [[nodiscard]] const schema::TabletId& tablet_id() const noexcept;
  [[nodiscard]] raft::NodeId source_node_id() const noexcept;
  [[nodiscard]] std::span<const DistributedVectorGroupedAggregateShuffleCompleteStream>
  local_streams() const noexcept;
  [[nodiscard]] std::span<const DistributedVectorGroupedAggregateShuffleRetry>
  remote_retries() const noexcept;
  [[nodiscard]] std::vector<DistributedVectorGroupedAggregateShuffleCompleteStream>
  take_local_streams() noexcept;
  [[nodiscard]] std::vector<DistributedVectorGroupedAggregateShuffleRetry>
  take_remote_retries() noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleSourcePlanMetrics metrics() const noexcept;

private:
  DistributedVectorGroupedAggregateShuffleSourcePlan(
      schema::TabletId tablet_id, raft::NodeId source_node_id,
      std::vector<DistributedVectorGroupedAggregateShuffleCompleteStream> local_streams,
      std::vector<DistributedVectorGroupedAggregateShuffleRetry> remote_retries,
      DistributedVectorGroupedAggregateShuffleSourcePlanMetrics metrics) noexcept;

  schema::TabletId tablet_id_;
  raft::NodeId source_node_id_{};
  std::vector<DistributedVectorGroupedAggregateShuffleCompleteStream> local_streams_;
  std::vector<DistributedVectorGroupedAggregateShuffleRetry> remote_retries_;
  DistributedVectorGroupedAggregateShuffleSourcePlanMetrics metrics_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_SOURCE_PLAN_HPP_
