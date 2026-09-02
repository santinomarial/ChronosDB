#ifndef CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_EXECUTION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_EXECUTION_HPP_

#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_transport.hpp"
#include "chronos/cluster/distributed_mutable_vector_query_execution.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_vector_grouped_aggregate_coordinator.hpp"

#include <cstddef>
#include <map>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t
    kDefaultDistributedMutableVectorGroupedAggregateQueryExecutionDecodeMemoryBytes =
        std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t
    kMaximumDistributedMutableVectorGroupedAggregateQueryExecutionDecodeMemoryBytes =
        std::size_t{1024U} * 1024U * 1024U;

struct DistributedMutableVectorGroupedAggregateQueryExecutionLimits {
  DistributedMutableVectorGroupedAggregateQuerySenderLimits sender{};
  query::DistributedVectorGroupedAggregateCoordinatorLimits coordinator{};
  query::DistributedVectorGroupedAggregateExchangeDecodeLimits decode{};
  std::size_t maximum_decode_memory_bytes{
      kDefaultDistributedMutableVectorGroupedAggregateQueryExecutionDecodeMemoryBytes};
};

struct DistributedMutableVectorGroupedAggregateQueryTarget {
  schema::TabletId tablet_id;
  raft::NodeId serving_node{};

  friend bool operator==(const DistributedMutableVectorGroupedAggregateQueryTarget&,
                         const DistributedMutableVectorGroupedAggregateQueryTarget&) = default;
};

struct DistributedMutableVectorGroupedAggregateCompletedSource {
  schema::TabletId tablet_id;
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> messages;
};

// Portable single-threaded owner for one exact mutable fragment per tablet. It owns finite senders,
// shared query decode authority, complete grouped key/state authority, and the all-tablet grouped
// coordinator. Transport is caller-owned. No group becomes visible until every sender publishes a
// complete terminal stream and finish() seals the coordinator.
class DistributedMutableVectorGroupedAggregateQueryExecution {
public:
  using TimePoint = DistributedMutableVectorGroupedAggregateQuerySender::TimePoint;

  DistributedMutableVectorGroupedAggregateQueryExecution() = delete;
  DistributedMutableVectorGroupedAggregateQueryExecution(
      const DistributedMutableVectorGroupedAggregateQueryExecution&) = delete;
  DistributedMutableVectorGroupedAggregateQueryExecution&
  operator=(const DistributedMutableVectorGroupedAggregateQueryExecution&) = delete;
  DistributedMutableVectorGroupedAggregateQueryExecution(
      DistributedMutableVectorGroupedAggregateQueryExecution&&) noexcept = default;
  DistributedMutableVectorGroupedAggregateQueryExecution&
  operator=(DistributedMutableVectorGroupedAggregateQueryExecution&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedMutableVectorGroupedAggregateQueryExecution>
  create(raft::NodeId source_node_id,
         std::vector<query::DistributedMutableVectorFragment> fragments,
         std::vector<query::VectorGroupKeyDefinition> keys,
         std::vector<query::VectorAggregateDefinition> aggregates,
         DistributedMutableVectorGroupedAggregateQueryExecutionLimits limits = {});

  [[nodiscard]] common::Result<DistributedMutableVectorGroupedAggregateQueryAttempt>
  begin_attempt(const schema::TabletId& tablet_id, TimePoint now);
  [[nodiscard]] common::Status
  execute_local(const schema::TabletId& tablet_id,
                DistributedMutableVectorGroupedAggregateQueryWorkerService& worker, TimePoint now);
  [[nodiscard]] common::Status
  accept_responses(const schema::TabletId& tablet_id,
                   std::span<const DistributedVectorGroupedAggregateQueryResponseV2> responses,
                   TimePoint now);
  [[nodiscard]] common::Status record_transport_failure(const schema::TabletId& tablet_id,
                                                        common::StatusCode code, TimePoint now);
  [[nodiscard]] common::Result<DistributedQuerySenderState>
  sender_state(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Result<std::optional<TimePoint>>
  next_attempt_not_before(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Result<std::optional<DistributedQueryLeaderHint>>
  suggested_leader(const schema::TabletId& tablet_id) const;

  [[nodiscard]] common::Status finish();
  // Transfers every complete canonical tablet stream in fragment order. This is mutually
  // exclusive with finish()/next() and is intended for a partitioned shuffle coordinator.
  [[nodiscard]] common::Result<std::vector<DistributedMutableVectorGroupedAggregateCompletedSource>>
  take_completed_sources();
  [[nodiscard]] common::Result<query::PhysicalOperatorStep> next();
  [[nodiscard]] std::span<const DistributedMutableVectorGroupedAggregateQueryTarget>
  targets() const noexcept;
  [[nodiscard]] std::span<const query::VectorGroupKeyDefinition> key_definitions() const noexcept;
  [[nodiscard]] std::span<const query::VectorAggregateDefinition>
  aggregate_definitions() const noexcept;
  [[nodiscard]] const query::DistributedVectorPlanIntent& plan() const noexcept;
  [[nodiscard]] const query::DistributedVectorResultSchema& result_schema() const noexcept;
  [[nodiscard]] const DistributedMutableVectorQueryLogicalIdentity&
  logical_identity() const noexcept;
  [[nodiscard]] const query::QueryResourceContext& decode_resources() const noexcept;
  [[nodiscard]] std::optional<query::QueryResourceContext> output_resources() const noexcept;

private:
  struct SenderSlot {
    schema::TabletId tablet_id;
    DistributedMutableVectorGroupedAggregateQuerySender sender;
    bool coordinator_result_delivered{};
    bool coordinator_failure_delivered{};
  };

  DistributedMutableVectorGroupedAggregateQueryExecution(
      query::DistributedVectorPlanIntent plan, query::DistributedVectorResultSchema result_schema,
      std::vector<query::VectorGroupKeyDefinition> keys,
      std::vector<query::VectorAggregateDefinition> aggregates,
      query::QueryResourceContext decode_resources,
      query::DistributedVectorGroupedAggregateCoordinator coordinator,
      std::vector<SenderSlot> senders,
      std::vector<DistributedMutableVectorGroupedAggregateQueryTarget> targets,
      DistributedMutableVectorQueryLogicalIdentity logical_identity,
      std::map<schema::TabletId, std::size_t> sender_indexes,
      query::DistributedVectorGroupedAggregateExchangeDecodeLimits decode_limits) noexcept;

  [[nodiscard]] common::Result<std::size_t> sender_index(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Status publish_terminal_state(SenderSlot& slot);

  query::DistributedVectorPlanIntent plan_;
  query::DistributedVectorResultSchema result_schema_;
  std::vector<query::VectorGroupKeyDefinition> keys_;
  std::vector<query::VectorAggregateDefinition> aggregates_;
  query::QueryResourceContext decode_resources_;
  query::DistributedVectorGroupedAggregateCoordinator coordinator_;
  std::vector<SenderSlot> senders_;
  std::vector<DistributedMutableVectorGroupedAggregateQueryTarget> targets_;
  DistributedMutableVectorQueryLogicalIdentity logical_identity_;
  std::map<schema::TabletId, std::size_t> sender_indexes_;
  query::DistributedVectorGroupedAggregateExchangeDecodeLimits decode_limits_;
  std::optional<common::Status> failure_;
  bool ready_{};
  bool sources_taken_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_EXECUTION_HPP_
