#ifndef CHRONOS_QUERY_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_COORDINATOR_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_COORDINATOR_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/query/aggregate.hpp"
#include "chronos/query/distributed.hpp"
#include "chronos/query/distributed_vector_grouped_aggregate_exchange.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::query {

inline constexpr std::size_t kDefaultDistributedVectorGroupedCoordinatorEncodedBytes =
    std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorGroupedCoordinatorEncodedBytes =
    std::size_t{1024U} * 1024U * 1024U;
inline constexpr std::size_t kDefaultDistributedVectorGroupedCoordinatorMemoryBytes =
    std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorGroupedCoordinatorMemoryBytes =
    std::size_t{1024U} * 1024U * 1024U;

struct DistributedVectorGroupedAggregateCoordinatorLimits {
  DistributedCoordinatorLimits messages{
      .maximum_messages_per_fragment = kMaximumGroupedAggregateGroups,
      .maximum_total_messages = kMaximumDistributedCoordinatorMessages};
  std::size_t maximum_total_encoded_bytes{kDefaultDistributedVectorGroupedCoordinatorEncodedBytes};
  std::size_t maximum_query_memory_bytes{kDefaultDistributedVectorGroupedCoordinatorMemoryBytes};
  std::size_t maximum_retained_configuration_bytes{kDefaultGroupedAggregateConfigurationByteLimit};
  DistributedVectorGroupedAggregateExchangeDecodeLimits decode;
  GroupedAggregateLimits table;
};

// Single-threaded owner for one grouped sufficient-state query. Accept retains canonical bytes as
// exact retry identity. finish() requires terminal closure for every planned tablet, then decodes
// and merges in caller-supplied tablet order and tablet-local group ordinal order. next() exposes
// final query-accounted rows only after that all-tablet gate. Exact retained messages remain
// idempotent after sealing so transport receipt loss cannot turn a retry into a false conflict.
class DistributedVectorGroupedAggregateCoordinator {
public:
  DistributedVectorGroupedAggregateCoordinator() = delete;
  ~DistributedVectorGroupedAggregateCoordinator();
  DistributedVectorGroupedAggregateCoordinator(
      const DistributedVectorGroupedAggregateCoordinator&) = delete;
  DistributedVectorGroupedAggregateCoordinator&
  operator=(const DistributedVectorGroupedAggregateCoordinator&) = delete;
  DistributedVectorGroupedAggregateCoordinator(
      DistributedVectorGroupedAggregateCoordinator&&) noexcept;
  DistributedVectorGroupedAggregateCoordinator&
  operator=(DistributedVectorGroupedAggregateCoordinator&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateCoordinator>
  create(common::Uuid query_id, std::vector<schema::TabletId> tablets,
         std::vector<VectorGroupKeyDefinition> keys,
         std::vector<VectorAggregateDefinition> definitions,
         DistributedVectorGroupedAggregateCoordinatorLimits limits = {});

  [[nodiscard]] common::Status
  accept(const DistributedVectorGroupedAggregateExchangeMessage& message);
  [[nodiscard]] common::Status worker_failed(const schema::TabletId& tablet_id,
                                             common::Status failure);
  // Resource/allocation failure before publication leaves retained canonical frames intact and may
  // be retried. Success seals input and enables next().
  [[nodiscard]] common::Status finish();
  [[nodiscard]] common::Result<PhysicalOperatorStep> next();

  [[nodiscard]] std::size_t retained_message_count() const noexcept;
  [[nodiscard]] std::size_t retained_encoded_bytes() const noexcept;
  [[nodiscard]] std::size_t group_count() const noexcept;
  [[nodiscard]] std::optional<QueryResourceContext> output_resources() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateCoordinator(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_COORDINATOR_HPP_
