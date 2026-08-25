#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_EXECUTION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_EXECUTION_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_destination_execution.hpp"
#include "chronos/common/result.hpp"

#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t
    kDefaultDistributedVectorGroupedAggregateShuffleResultWorkingMemoryBytes =
        std::size_t{256U} * 1024U * 1024U;
inline constexpr std::size_t
    kMaximumDistributedVectorGroupedAggregateShuffleResultWorkingMemoryBytes =
        std::size_t{1024U} * 1024U * 1024U;

struct DistributedVectorGroupedAggregateShuffleResultExecutionMetrics {
  std::size_t total_partitions{};
  std::size_t completed_partitions{};
  std::size_t emitted_chunks{};
};

// Exclusively owns one sealed destination per authority node and concatenates disjoint reducer
// outputs in canonical partition-ID order. The authority is borrowed and outlives this
// single-thread-affine owner. Its resource context is reserved for global downstream operators.
class DistributedVectorGroupedAggregateShuffleResultExecution {
public:
  DistributedVectorGroupedAggregateShuffleResultExecution() noexcept;
  ~DistributedVectorGroupedAggregateShuffleResultExecution();
  DistributedVectorGroupedAggregateShuffleResultExecution(
      const DistributedVectorGroupedAggregateShuffleResultExecution&) = delete;
  DistributedVectorGroupedAggregateShuffleResultExecution&
  operator=(const DistributedVectorGroupedAggregateShuffleResultExecution&) = delete;
  DistributedVectorGroupedAggregateShuffleResultExecution(
      DistributedVectorGroupedAggregateShuffleResultExecution&&) noexcept;
  DistributedVectorGroupedAggregateShuffleResultExecution&
  operator=(DistributedVectorGroupedAggregateShuffleResultExecution&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleResultExecution>
  create(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
         std::vector<DistributedVectorGroupedAggregateShuffleDestinationExecution> destinations,
         std::size_t maximum_working_memory_bytes =
             kDefaultDistributedVectorGroupedAggregateShuffleResultWorkingMemoryBytes);

  [[nodiscard]] common::Result<query::PhysicalOperatorStep> next();
  [[nodiscard]] std::span<const query::VectorGroupKeyDefinition> key_definitions() const noexcept;
  [[nodiscard]] std::span<const query::VectorAggregateDefinition>
  aggregate_definitions() const noexcept;
  [[nodiscard]] std::optional<query::QueryResourceContext> output_resources() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleResultExecutionMetrics
  metrics() const noexcept;
  [[nodiscard]] const DistributedVectorGroupedAggregateShuffleAuthority* authority() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleResultExecution(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_EXECUTION_HPP_
