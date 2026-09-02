#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_EXECUTION_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_EXECUTION_V2_HPP_

#include "chronos/cluster/distributed_vector_aggregate_query_transport_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_fragment_binding.hpp"
#include "chronos/query/distributed_vector_aggregate_coordinator.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <map>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kDefaultDistributedVectorAggregateQueryExecutionMemoryBytesV2 =
    std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorAggregateQueryExecutionMemoryBytesV2 =
    std::size_t{1024U} * 1024U * 1024U;

struct DistributedVectorAggregateQueryExecutionLimitsV2 {
  DistributedVectorAggregateQuerySenderLimitsV2 sender{};
  query::DistributedVectorAggregateCoordinatorLimitsV2 coordinator{};
  std::size_t maximum_query_memory_bytes{
      kDefaultDistributedVectorAggregateQueryExecutionMemoryBytesV2};
};

struct DistributedVectorAggregateQueryExecutionResultV2 {
  query::DistributedVectorPlanIntent plan;
  query::DistributedVectorAggregateQueryResultV2 result;
};

// Portable single-owner orchestration for one proof-bound ungrouped aggregate snapshot. It retains
// the Manifest pin, shared query resource authority, one immutable finite sender per plan-ordered
// tablet, and the bounded all-type aggregate coordinator. Callers own transports and clocks and
// serialize every method.
class DistributedVectorAggregateQueryExecutionV2 {
public:
  using TimePoint = DistributedVectorAggregateQuerySenderV2::TimePoint;

  DistributedVectorAggregateQueryExecutionV2() = delete;
  DistributedVectorAggregateQueryExecutionV2(const DistributedVectorAggregateQueryExecutionV2&) =
      delete;
  DistributedVectorAggregateQueryExecutionV2&
  operator=(const DistributedVectorAggregateQueryExecutionV2&) = delete;
  DistributedVectorAggregateQueryExecutionV2(
      DistributedVectorAggregateQueryExecutionV2&&) noexcept = default;
  DistributedVectorAggregateQueryExecutionV2&
  operator=(DistributedVectorAggregateQueryExecutionV2&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedVectorAggregateQueryExecutionV2>
  create(raft::NodeId source_node_id, query::CompatibleDistributedVectorSnapshotV2 snapshot,
         DistributedVectorAggregateQueryExecutionLimitsV2 limits = {});

  [[nodiscard]] common::Result<DistributedVectorAggregateQueryAttemptV2>
  begin_attempt(const schema::TabletId& tablet_id, TimePoint now);
  [[nodiscard]] common::Status
  accept_responses(const schema::TabletId& tablet_id,
                   std::span<const DistributedVectorAggregateQueryResponseV2> responses,
                   TimePoint now);
  [[nodiscard]] common::Status record_transport_failure(const schema::TabletId& tablet_id,
                                                        common::StatusCode code, TimePoint now);

  [[nodiscard]] common::Result<DistributedQuerySenderState>
  sender_state(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Result<std::optional<TimePoint>>
  next_attempt_not_before(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Result<std::optional<DistributedQueryLeaderHint>>
  suggested_leader(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Result<DistributedVectorAggregateQueryExecutionResultV2> finish();

  [[nodiscard]] const query::CompatibleDistributedVectorSnapshotV2& snapshot() const noexcept;
  [[nodiscard]] std::span<const query::VectorAggregateDefinition> definitions() const noexcept;
  [[nodiscard]] const query::QueryResourceContext& resources() const noexcept;

private:
  struct SenderSlot {
    schema::TabletId tablet_id;
    DistributedVectorAggregateQuerySenderV2 sender;
    bool coordinator_result_delivered{};
    bool coordinator_failure_delivered{};
  };

  DistributedVectorAggregateQueryExecutionV2(
      query::CompatibleDistributedVectorSnapshotV2 snapshot, query::QueryResourceContext resources,
      query::DistributedVectorAggregateCoordinatorV2 coordinator, std::vector<SenderSlot> senders,
      std::map<schema::TabletId, std::size_t> sender_indexes) noexcept;

  [[nodiscard]] common::Result<std::size_t> sender_index(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Status publish_terminal_state(SenderSlot& slot);

  query::CompatibleDistributedVectorSnapshotV2 snapshot_;
  query::QueryResourceContext resources_;
  query::DistributedVectorAggregateCoordinatorV2 coordinator_;
  std::vector<SenderSlot> senders_;
  std::map<schema::TabletId, std::size_t> sender_indexes_;
  bool finished_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_EXECUTION_V2_HPP_
