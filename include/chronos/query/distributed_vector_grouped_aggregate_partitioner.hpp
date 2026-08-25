#ifndef CHRONOS_QUERY_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_PARTITIONER_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_PARTITIONER_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/distributed_vector_grouped_aggregate_exchange.hpp"
#include "chronos/query/resource_context.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace chronos::query {

inline constexpr std::uint32_t kMaximumDistributedVectorGroupedAggregatePartitions = 4096U;
inline constexpr std::size_t kDefaultDistributedVectorGroupedPartitionInputBytes =
    std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kDefaultDistributedVectorGroupedPartitionOutputBytes =
    std::size_t{256U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorGroupedPartitionBytes =
    std::size_t{1024U} * 1024U * 1024U;

struct DistributedVectorGroupedAggregatePartitionerLimits {
  std::uint32_t maximum_partitions{kMaximumDistributedVectorGroupedAggregatePartitions};
  std::uint32_t maximum_input_groups{
      distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups};
  std::uint32_t maximum_groups_per_partition{
      distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups};
  std::size_t maximum_input_encoded_bytes{kDefaultDistributedVectorGroupedPartitionInputBytes};
  std::size_t maximum_partition_encoded_bytes{kDefaultDistributedVectorGroupedPartitionInputBytes};
  std::size_t maximum_total_output_encoded_bytes{
      kDefaultDistributedVectorGroupedPartitionOutputBytes};
  DistributedVectorGroupedAggregateExchangeDecodeLimits decode;
};

struct DistributedVectorGroupedAggregatePartitionStream {
  std::uint32_t partition_id{};
  std::vector<EncodedDistributedVectorGroupedAggregateExchangeMessage> messages;
  std::size_t encoded_bytes{};
};

// Deterministically splits one complete tablet-local grouped stream into one complete stream per
// partition. Hash-v1 modulo the fixed partition count selects the destination. Every partition is
// represented, including a canonical empty terminal, so a downstream all-tablet reducer can prove
// closure without timeout inference. The owner is thread-affine because decoded key/state storage
// borrows its query resource context while partition() is active.
class DistributedVectorGroupedAggregatePartitioner {
public:
  DistributedVectorGroupedAggregatePartitioner() = delete;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregatePartitioner>
  create(std::vector<VectorGroupKeyDefinition> keys,
         std::vector<VectorAggregateDefinition> aggregates, QueryResourceContext resources,
         std::uint32_t partition_count,
         DistributedVectorGroupedAggregatePartitionerLimits limits = {});

  [[nodiscard]] common::Result<std::vector<DistributedVectorGroupedAggregatePartitionStream>>
  partition(std::span<const EncodedDistributedVectorGroupedAggregateExchangeMessage> input) const;

  [[nodiscard]] std::uint32_t partition_count() const noexcept;
  [[nodiscard]] std::span<const VectorGroupKeyDefinition> key_definitions() const noexcept;
  [[nodiscard]] std::span<const VectorAggregateDefinition> aggregate_definitions() const noexcept;

private:
  DistributedVectorGroupedAggregatePartitioner(
      std::vector<VectorGroupKeyDefinition> keys, std::vector<VectorAggregateDefinition> aggregates,
      QueryResourceContext resources, std::uint32_t partition_count,
      DistributedVectorGroupedAggregatePartitionerLimits limits) noexcept;

  std::vector<VectorGroupKeyDefinition> keys_;
  std::vector<VectorAggregateDefinition> aggregates_;
  QueryResourceContext resources_;
  std::uint32_t partition_count_{};
  DistributedVectorGroupedAggregatePartitionerLimits limits_;
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_PARTITIONER_HPP_
