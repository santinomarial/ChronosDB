#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_COLLECTED_RESULT_EXECUTION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_COLLECTED_RESULT_EXECUTION_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_stream.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/resource_context.hpp"

#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t
    kDefaultDistributedVectorGroupedAggregateShuffleCollectedResultWorkingBytes =
        std::size_t{256U} * 1024U * 1024U;
inline constexpr std::size_t
    kMaximumDistributedVectorGroupedAggregateShuffleCollectedResultWorkingBytes =
        std::size_t{1024U} * 1024U * 1024U;

struct DistributedVectorGroupedAggregateShuffleCollectedResultExecutionLimits {
  DistributedVectorGroupedAggregateShuffleResultStreamLimits stream;
  std::size_t maximum_batch_working_bytes{query::kDefaultVectorChunkMemoryLimit};
  std::size_t maximum_working_memory_bytes{
      kDefaultDistributedVectorGroupedAggregateShuffleCollectedResultWorkingBytes};
};

struct DistributedVectorGroupedAggregateShuffleCollectedResultExecutionMetrics {
  std::size_t total_partitions{};
  std::size_t completed_partitions{};
  std::size_t decoded_batches{};
  std::size_t decoded_rows{};
  std::size_t decoded_batch_bytes{};
};

// Owns a complete canonical partition-ordered result collection and materializes each Native batch
// into one accounted vector chunk. Authority and raw schema are borrowed and outlive this
// single-thread-affine execution; returned chunks may retain its shared query-memory authority.
class DistributedVectorGroupedAggregateShuffleCollectedResultExecution {
public:
  DistributedVectorGroupedAggregateShuffleCollectedResultExecution() noexcept;
  ~DistributedVectorGroupedAggregateShuffleCollectedResultExecution();
  DistributedVectorGroupedAggregateShuffleCollectedResultExecution(
      const DistributedVectorGroupedAggregateShuffleCollectedResultExecution&) = delete;
  DistributedVectorGroupedAggregateShuffleCollectedResultExecution&
  operator=(const DistributedVectorGroupedAggregateShuffleCollectedResultExecution&) = delete;
  DistributedVectorGroupedAggregateShuffleCollectedResultExecution(
      DistributedVectorGroupedAggregateShuffleCollectedResultExecution&&) noexcept;
  DistributedVectorGroupedAggregateShuffleCollectedResultExecution&
  operator=(DistributedVectorGroupedAggregateShuffleCollectedResultExecution&&) noexcept;

  [[nodiscard]] static common::Result<
      DistributedVectorGroupedAggregateShuffleCollectedResultExecution>
  create(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
         const query::DistributedVectorResultSchema& result_schema,
         std::vector<DistributedVectorGroupedAggregateShuffleCompleteResultStream> streams,
         DistributedVectorGroupedAggregateShuffleCollectedResultExecutionLimits limits = {});

  [[nodiscard]] common::Result<query::PhysicalOperatorStep> next();
  [[nodiscard]] std::span<const query::VectorGroupKeyDefinition> key_definitions() const noexcept;
  [[nodiscard]] std::span<const query::VectorAggregateDefinition>
  aggregate_definitions() const noexcept;
  [[nodiscard]] std::optional<query::QueryResourceContext> output_resources() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleCollectedResultExecutionMetrics
  metrics() const noexcept;
  [[nodiscard]] const DistributedVectorGroupedAggregateShuffleAuthority* authority() const noexcept;
  [[nodiscard]] const query::DistributedVectorResultSchema* result_schema() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleCollectedResultExecution(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_COLLECTED_RESULT_EXECUTION_HPP_
