#ifndef CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_QUERY_EXECUTION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_QUERY_EXECUTION_HPP_

#include "chronos/cluster/distributed_mutable_vector_query_transport.hpp"
#include "chronos/cluster/distributed_vector_query_execution_v2.hpp"
#include "chronos/cluster/distributed_vector_result_exchange.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_mutable_vector_fragment.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <map>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cluster {

struct DistributedMutableVectorQueryExecutionLimits {
  DistributedMutableVectorQuerySenderLimits sender;
  DistributedVectorResultCoordinatorLimitsV2 coordinator;
};

struct DistributedMutableVectorQueryTarget {
  schema::TabletId tablet_id;
  raft::NodeId serving_node{};

  friend bool operator==(const DistributedMutableVectorQueryTarget&,
                         const DistributedMutableVectorQueryTarget&) = default;
};

struct DistributedMutableVectorQueryLogicalTablet {
  schema::TabletId tablet_id;
  raft::GroupId raft_group_id;

  friend bool operator==(const DistributedMutableVectorQueryLogicalTablet&,
                         const DistributedMutableVectorQueryLogicalTablet&) = default;
};

// Logical query fields that fresh authority may not change. Serving node, exact positions,
// placement epoch, and barrier are deliberately absent because rebinding must replace them.
struct DistributedMutableVectorQueryLogicalIdentity {
  common::Uuid query_id;
  manifest::DatabaseId database_id;
  schema::TableId table_id;
  schema::SchemaId destination_schema_id;
  query::DistributedReadPolicy read_policy;
  std::vector<std::uint32_t> destination_column_ordinals;
  std::optional<cseg::EventTimePredicate> event_time_predicate;
  query::DistributedVectorPlanIntent plan;
  query::DistributedVectorResultSchema result_schema;
  std::vector<DistributedMutableVectorQueryLogicalTablet> tablets;

  friend bool operator==(const DistributedMutableVectorQueryLogicalIdentity&,
                         const DistributedMutableVectorQueryLogicalIdentity&) = default;
};

// Validates one complete fragment set and returns only the logical fields that fresh authority may
// not change. Unlike execution creation, this accepts self-led fragments because it opens no
// carrier and constructs no sender.
[[nodiscard]] common::Result<DistributedMutableVectorQueryLogicalIdentity>
distributed_mutable_vector_query_logical_identity(
    std::span<const query::DistributedMutableVectorFragment> fragments);

// Portable single-owner orchestration for one proof-bound mutable publication per tablet. The
// fragments are immutable, value-owned authority; callers own transports and clocks and serialize
// every method. Completion reuses the schema-bound v2 result value consumed by row finalization.
class DistributedMutableVectorQueryExecution {
public:
  using TimePoint = DistributedMutableVectorQuerySender::TimePoint;

  DistributedMutableVectorQueryExecution() = delete;
  DistributedMutableVectorQueryExecution(const DistributedMutableVectorQueryExecution&) = delete;
  DistributedMutableVectorQueryExecution&
  operator=(const DistributedMutableVectorQueryExecution&) = delete;
  DistributedMutableVectorQueryExecution(DistributedMutableVectorQueryExecution&&) noexcept =
      default;
  DistributedMutableVectorQueryExecution&
  operator=(DistributedMutableVectorQueryExecution&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedMutableVectorQueryExecution>
  create(raft::NodeId source_node_id,
         std::vector<query::DistributedMutableVectorFragment> fragments,
         DistributedMutableVectorQueryExecutionLimits limits = {});

  [[nodiscard]] common::Result<DistributedMutableVectorQueryAttempt>
  begin_attempt(const schema::TabletId& tablet_id, TimePoint now);
  [[nodiscard]] common::Status
  accept_responses(const schema::TabletId& tablet_id,
                   std::span<const DistributedVectorQueryResponseV2> responses, TimePoint now);
  [[nodiscard]] common::Status record_transport_failure(const schema::TabletId& tablet_id,
                                                        common::StatusCode code, TimePoint now);

  [[nodiscard]] common::Result<DistributedQuerySenderState>
  sender_state(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Result<std::optional<TimePoint>>
  next_attempt_not_before(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Result<std::optional<DistributedQueryLeaderHint>>
  suggested_leader(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Result<DistributedVectorQueryExecutionResultV2> finish();
  [[nodiscard]] std::span<const DistributedMutableVectorQueryTarget> targets() const noexcept;
  [[nodiscard]] const DistributedMutableVectorQueryLogicalIdentity&
  logical_identity() const noexcept;

private:
  struct SenderSlot {
    schema::TabletId tablet_id;
    DistributedMutableVectorQuerySender sender;
    bool coordinator_result_delivered{};
    bool coordinator_failure_delivered{};
  };

  DistributedMutableVectorQueryExecution(
      query::DistributedVectorPlanIntent plan, DistributedVectorResultCoordinatorV2 coordinator,
      std::vector<SenderSlot> senders, std::vector<DistributedMutableVectorQueryTarget> targets,
      DistributedMutableVectorQueryLogicalIdentity logical_identity,
      std::map<schema::TabletId, std::size_t> sender_indexes) noexcept;

  [[nodiscard]] common::Result<std::size_t> sender_index(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Status publish_terminal_state(SenderSlot& slot);

  query::DistributedVectorPlanIntent plan_;
  DistributedVectorResultCoordinatorV2 coordinator_;
  std::vector<SenderSlot> senders_;
  std::vector<DistributedMutableVectorQueryTarget> targets_;
  DistributedMutableVectorQueryLogicalIdentity logical_identity_;
  std::map<schema::TabletId, std::size_t> sender_indexes_;
  bool finished_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_QUERY_EXECUTION_HPP_
