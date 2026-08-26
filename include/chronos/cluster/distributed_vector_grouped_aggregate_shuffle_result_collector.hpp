#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_COLLECTOR_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_COLLECTOR_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_stream.hpp"
#include "chronos/common/result.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kDefaultDistributedVectorGroupedAggregateShuffleResultCollectorBytes =
    std::size_t{256U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorGroupedAggregateShuffleResultCollectorBytes =
    std::size_t{1024U} * 1024U * 1024U;

struct DistributedVectorGroupedAggregateShuffleResultCollectorLimits {
  DistributedVectorGroupedAggregateShuffleResultStreamLimits stream;
  std::size_t maximum_total_encoded_bytes{
      kDefaultDistributedVectorGroupedAggregateShuffleResultCollectorBytes};
};

struct DistributedVectorGroupedAggregateShuffleResultCollectorMetrics {
  std::size_t total_partitions{};
  std::size_t accepted_partitions{};
  std::size_t duplicate_streams{};
  std::size_t retained_encoded_bytes{};
};

enum class DistributedVectorGroupedAggregateShuffleResultCollectorState : std::uint8_t {
  kCollecting = 1,
  kComplete = 2,
  kTaken = 3,
};

// Retains one canonical result stream per authority partition for a separate coordinator node.
// Exact whole-stream retries are idempotent and conflicts fail closed. Authority and raw schema
// are borrowed and outlive this single-thread-affine owner.
class DistributedVectorGroupedAggregateShuffleResultCollector {
public:
  DistributedVectorGroupedAggregateShuffleResultCollector() = delete;
  DistributedVectorGroupedAggregateShuffleResultCollector(
      const DistributedVectorGroupedAggregateShuffleResultCollector&) = delete;
  DistributedVectorGroupedAggregateShuffleResultCollector&
  operator=(const DistributedVectorGroupedAggregateShuffleResultCollector&) = delete;
  DistributedVectorGroupedAggregateShuffleResultCollector(
      DistributedVectorGroupedAggregateShuffleResultCollector&&) noexcept = default;
  DistributedVectorGroupedAggregateShuffleResultCollector&
  operator=(DistributedVectorGroupedAggregateShuffleResultCollector&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleResultCollector>
  create(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
         const query::DistributedVectorResultSchema& result_schema,
         raft::NodeId coordinator_node_id,
         DistributedVectorGroupedAggregateShuffleResultCollectorLimits limits = {});

  [[nodiscard]] common::Status
  accept_stream(DistributedVectorGroupedAggregateShuffleCompleteResultStream stream);
  // Validates by reference and moves only on first-partition success. Every failure and exact
  // duplicate leaves the caller's stream intact, permitting retry after an acknowledged server
  // handoff encounters local resource exhaustion.
  [[nodiscard]] common::Status
  accept_stream_preserving(DistributedVectorGroupedAggregateShuffleCompleteResultStream& stream);
  [[nodiscard]] common::Result<
      std::vector<DistributedVectorGroupedAggregateShuffleCompleteResultStream>>
  take_complete_streams();

  [[nodiscard]] DistributedVectorGroupedAggregateShuffleResultCollectorState state() const noexcept;
  [[nodiscard]] bool ready() const noexcept;
  [[nodiscard]] bool contains_partition(std::uint32_t partition_id) const noexcept;
  [[nodiscard]] raft::NodeId coordinator_node_id() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleResultCollectorMetrics
  metrics() const noexcept;

private:
  DistributedVectorGroupedAggregateShuffleResultCollector(
      const DistributedVectorGroupedAggregateShuffleAuthority& authority,
      const query::DistributedVectorResultSchema& result_schema, raft::NodeId coordinator_node_id,
      DistributedVectorGroupedAggregateShuffleResultCollectorLimits limits,
      std::vector<std::optional<DistributedVectorGroupedAggregateShuffleCompleteResultStream>>
          streams) noexcept;

  std::reference_wrapper<const DistributedVectorGroupedAggregateShuffleAuthority> authority_;
  std::reference_wrapper<const query::DistributedVectorResultSchema> result_schema_;
  raft::NodeId coordinator_node_id_{};
  DistributedVectorGroupedAggregateShuffleResultCollectorLimits limits_;
  std::vector<std::optional<DistributedVectorGroupedAggregateShuffleCompleteResultStream>> streams_;
  DistributedVectorGroupedAggregateShuffleResultCollectorMetrics metrics_;
  DistributedVectorGroupedAggregateShuffleResultCollectorState state_{
      DistributedVectorGroupedAggregateShuffleResultCollectorState::kCollecting};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_COLLECTOR_HPP_
