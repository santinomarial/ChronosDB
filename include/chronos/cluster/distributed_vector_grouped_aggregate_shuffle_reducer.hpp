#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_REDUCER_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_REDUCER_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_stream.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_vector_grouped_aggregate_coordinator.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kDefaultDistributedVectorGroupedAggregateShuffleReducerBytes =
    std::size_t{256U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorGroupedAggregateShuffleReducerBytes =
    std::size_t{1024U} * 1024U * 1024U;

struct DistributedVectorGroupedAggregateShuffleReducerLimits {
  std::size_t maximum_source_stream_bytes{
      kDefaultDistributedVectorGroupedAggregateShuffleStreamBytes};
  std::size_t maximum_total_stream_bytes{
      kDefaultDistributedVectorGroupedAggregateShuffleReducerBytes};
  query::DistributedVectorGroupedAggregateCoordinatorLimits coordinator;
};

struct DistributedVectorGroupedAggregateShuffleReducerMetrics {
  std::size_t accepted_sources{};
  std::size_t duplicate_streams{};
  std::size_t retained_stream_bytes{};
};

// Admits exactly one complete stream from every ordered authority source for one local partition.
// Exact whole-stream retries are idempotent, conflicts fail closed, and finish delegates merging in
// authority source order to the shared grouped coordinator. The authority is borrowed and outlives
// this single-thread-affine owner.
class DistributedVectorGroupedAggregateShuffleReducer {
public:
  DistributedVectorGroupedAggregateShuffleReducer() = delete;
  DistributedVectorGroupedAggregateShuffleReducer(
      const DistributedVectorGroupedAggregateShuffleReducer&) = delete;
  DistributedVectorGroupedAggregateShuffleReducer&
  operator=(const DistributedVectorGroupedAggregateShuffleReducer&) = delete;
  DistributedVectorGroupedAggregateShuffleReducer(
      DistributedVectorGroupedAggregateShuffleReducer&&) noexcept = default;
  DistributedVectorGroupedAggregateShuffleReducer&
  operator=(DistributedVectorGroupedAggregateShuffleReducer&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleReducer>
  create(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
         std::uint32_t partition_id, raft::NodeId local_node_id,
         DistributedVectorGroupedAggregateShuffleReducerLimits limits = {});

  [[nodiscard]] common::Status
  accept_stream(const DistributedVectorGroupedAggregateShuffleCompleteStream& stream);
  [[nodiscard]] common::Status finish();
  [[nodiscard]] common::Result<query::PhysicalOperatorStep> next();

  [[nodiscard]] std::uint32_t partition_id() const noexcept;
  [[nodiscard]] raft::NodeId local_node_id() const noexcept;
  [[nodiscard]] bool ready() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleReducerMetrics metrics() const noexcept;

private:
  struct SourceProgress {
    std::optional<std::size_t> encoded_bytes;
    std::optional<std::size_t> message_count;
    std::size_t accepted_prefix{};
    bool complete{};
  };

  DistributedVectorGroupedAggregateShuffleReducer(
      const DistributedVectorGroupedAggregateShuffleAuthority& authority,
      std::uint32_t partition_id, DistributedVectorGroupedAggregateShuffleReducerLimits limits,
      raft::NodeId local_node_id, std::map<schema::TabletId, std::size_t> source_indices,
      std::vector<SourceProgress> sources,
      query::DistributedVectorGroupedAggregateCoordinator coordinator) noexcept;

  std::reference_wrapper<const DistributedVectorGroupedAggregateShuffleAuthority> authority_;
  std::uint32_t partition_id_{};
  raft::NodeId local_node_id_{};
  DistributedVectorGroupedAggregateShuffleReducerLimits limits_;
  std::map<schema::TabletId, std::size_t> source_indices_;
  std::vector<SourceProgress> sources_;
  query::DistributedVectorGroupedAggregateCoordinator coordinator_;
  DistributedVectorGroupedAggregateShuffleReducerMetrics metrics_;
  bool ready_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_REDUCER_HPP_
