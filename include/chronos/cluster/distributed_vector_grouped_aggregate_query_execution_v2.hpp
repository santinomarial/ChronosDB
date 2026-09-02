#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_EXECUTION_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_EXECUTION_V2_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/distributed_fragment_binding.hpp"
#include "chronos/query/distributed_vector_grouped_aggregate_coordinator.hpp"
#include "chronos/query/distributed_vector_grouped_aggregate_exchange.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <map>
#include <optional>
#include <span>

namespace chronos::cluster {

inline constexpr std::size_t
    kDefaultDistributedVectorGroupedAggregateQueryExecutionDecodeMemoryBytesV2 =
        std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t
    kMaximumDistributedVectorGroupedAggregateQueryExecutionDecodeMemoryBytesV2 =
        std::size_t{1024U} * 1024U * 1024U;

struct DistributedVectorGroupedAggregateQueryExecutionLimitsV2 {
  query::DistributedVectorGroupedAggregateCoordinatorLimits coordinator{};
  query::DistributedVectorGroupedAggregateExchangeDecodeLimits decode{};
  std::size_t maximum_decode_memory_bytes{
      kDefaultDistributedVectorGroupedAggregateQueryExecutionDecodeMemoryBytesV2};
};

// Portable single-threaded owner for one proof-bound grouped sufficient-state query. The caller
// owns worker scheduling and transport and supplies one complete canonical frame batch per tablet.
// Any failure after a batch begins is published as that worker's terminal failure. Output remains
// unavailable until every planned tablet has supplied an exact terminal stream and finish()
// succeeds. Borrowed snapshot and authority views remain valid until this move-only owner moves or
// is destroyed; callers serialize every method.
class DistributedVectorGroupedAggregateQueryExecutionV2 {
public:
  DistributedVectorGroupedAggregateQueryExecutionV2() = delete;
  DistributedVectorGroupedAggregateQueryExecutionV2(
      const DistributedVectorGroupedAggregateQueryExecutionV2&) = delete;
  DistributedVectorGroupedAggregateQueryExecutionV2&
  operator=(const DistributedVectorGroupedAggregateQueryExecutionV2&) = delete;
  DistributedVectorGroupedAggregateQueryExecutionV2(
      DistributedVectorGroupedAggregateQueryExecutionV2&&) noexcept = default;
  DistributedVectorGroupedAggregateQueryExecutionV2&
  operator=(DistributedVectorGroupedAggregateQueryExecutionV2&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateQueryExecutionV2>
  create(query::CompatibleDistributedVectorSnapshotV2 snapshot,
         DistributedVectorGroupedAggregateQueryExecutionLimitsV2 limits = {});

  [[nodiscard]] common::Status accept_worker_frames(
      const schema::TabletId& tablet_id,
      std::span<const query::EncodedDistributedVectorGroupedAggregateExchangeMessage> frames);
  [[nodiscard]] common::Status worker_failed(const schema::TabletId& tablet_id,
                                             common::Status failure);
  [[nodiscard]] common::Status finish();
  [[nodiscard]] common::Result<query::PhysicalOperatorStep> next();

  [[nodiscard]] const query::CompatibleDistributedVectorSnapshotV2& snapshot() const noexcept;
  [[nodiscard]] std::span<const query::VectorGroupKeyDefinition> key_definitions() const noexcept;
  [[nodiscard]] std::span<const query::VectorAggregateDefinition>
  aggregate_definitions() const noexcept;
  [[nodiscard]] const query::QueryResourceContext& decode_resources() const noexcept;
  [[nodiscard]] std::optional<query::QueryResourceContext> output_resources() const noexcept;

private:
  DistributedVectorGroupedAggregateQueryExecutionV2(
      query::CompatibleDistributedVectorSnapshotV2 snapshot,
      query::QueryResourceContext decode_resources,
      query::DistributedVectorGroupedAggregateCoordinator coordinator,
      std::map<schema::TabletId, bool> terminal_tablets,
      query::DistributedVectorGroupedAggregateExchangeDecodeLimits decode_limits) noexcept;

  [[nodiscard]] common::Status fail_batch(const schema::TabletId& tablet_id,
                                          common::Status failure);

  query::CompatibleDistributedVectorSnapshotV2 snapshot_;
  query::QueryResourceContext decode_resources_;
  query::DistributedVectorGroupedAggregateCoordinator coordinator_;
  std::map<schema::TabletId, bool> terminal_tablets_;
  query::DistributedVectorGroupedAggregateExchangeDecodeLimits decode_limits_;
  std::optional<common::Status> failure_;
  bool ready_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_EXECUTION_V2_HPP_
