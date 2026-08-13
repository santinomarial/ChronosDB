#ifndef CHRONOS_CLUSTER_DISTRIBUTED_GROUPED_QUERY_EXECUTION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_GROUPED_QUERY_EXECUTION_HPP_

#include "chronos/cluster/distributed_grouped_query_transport.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_fragment_binding.hpp"
#include "chronos/query/distributed_grouped_exchange.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <map>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cluster {

struct DistributedGroupedQueryExecutionLimits {
  query::DistributedCoordinatorLimits coordinator;
  DistributedQueryRetryLimits retry;
  query::DistributedGroupedFloat64ResultOptions result;
};

// Single-owner orchestration for one proof-bound grouped snapshot. It retains the pinned Manifest
// epoch, one finite sender per plan-ordered tablet, and the grouped coordinator. Callers own
// transports and clocks and serialize all methods.
class DistributedGroupedQueryExecution {
public:
  using TimePoint = DistributedGroupedQuerySender::TimePoint;

  DistributedGroupedQueryExecution() = delete;
  DistributedGroupedQueryExecution(const DistributedGroupedQueryExecution&) = delete;
  DistributedGroupedQueryExecution& operator=(const DistributedGroupedQueryExecution&) = delete;
  DistributedGroupedQueryExecution(DistributedGroupedQueryExecution&&) noexcept = default;
  DistributedGroupedQueryExecution&
  operator=(DistributedGroupedQueryExecution&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedGroupedQueryExecution>
  create(raft::NodeId source_node_id, query::CompatibleDistributedGroupedFloat64Snapshot snapshot,
         DistributedGroupedQueryExecutionLimits limits = {});

  [[nodiscard]] common::Result<DistributedGroupedQueryAttempt>
  begin_attempt(const schema::TabletId& tablet_id, TimePoint now);
  [[nodiscard]] common::Status
  accept_responses(const schema::TabletId& tablet_id,
                   std::span<const DistributedGroupedQueryResponse> responses, TimePoint now);
  [[nodiscard]] common::Status record_transport_failure(const schema::TabletId& tablet_id,
                                                        common::StatusCode code, TimePoint now);

  [[nodiscard]] common::Result<DistributedQuerySenderState>
  sender_state(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Result<std::optional<TimePoint>>
  next_attempt_not_before(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Result<std::optional<DistributedQueryLeaderHint>>
  suggested_leader(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Result<std::vector<query::GroupedFloat64AggregateResult>> finish() const;
  [[nodiscard]] const query::CompatibleDistributedGroupedFloat64Snapshot& snapshot() const noexcept;

private:
  struct SenderSlot {
    schema::TabletId tablet_id;
    DistributedGroupedQuerySender sender;
    bool coordinator_result_delivered{};
    bool coordinator_failure_delivered{};
  };

  DistributedGroupedQueryExecution(query::CompatibleDistributedGroupedFloat64Snapshot snapshot,
                                   query::DistributedGroupedFloat64Coordinator coordinator,
                                   std::vector<SenderSlot> senders,
                                   std::map<schema::TabletId, std::size_t> sender_indexes) noexcept;

  [[nodiscard]] common::Result<std::size_t> sender_index(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Status publish_terminal_state(SenderSlot& slot);

  query::CompatibleDistributedGroupedFloat64Snapshot snapshot_;
  query::DistributedGroupedFloat64Coordinator coordinator_;
  std::vector<SenderSlot> senders_;
  std::map<schema::TabletId, std::size_t> sender_indexes_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_GROUPED_QUERY_EXECUTION_HPP_
