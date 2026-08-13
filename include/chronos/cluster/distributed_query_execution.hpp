#ifndef CHRONOS_CLUSTER_DISTRIBUTED_QUERY_EXECUTION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_QUERY_EXECUTION_HPP_

#include "chronos/cluster/distributed_query_transport.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed.hpp"
#include "chronos/query/distributed_fragment_binding.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <vector>

namespace chronos::cluster {

struct DistributedQueryExecutionLimits {
  query::DistributedCoordinatorLimits coordinator;
  DistributedQueryRetryLimits retry;
};

// Single-owner orchestration for one compatible aggregate snapshot. It retains the pinned Manifest
// epoch, one immutable sender per plan-ordered tablet, and the coordinator. Callers own sockets and
// clocks and serialize all methods.
class DistributedQueryExecution {
public:
  using TimePoint = DistributedQuerySender::TimePoint;

  DistributedQueryExecution() = delete;
  DistributedQueryExecution(const DistributedQueryExecution&) = delete;
  DistributedQueryExecution& operator=(const DistributedQueryExecution&) = delete;
  DistributedQueryExecution(DistributedQueryExecution&&) noexcept = default;
  DistributedQueryExecution& operator=(DistributedQueryExecution&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedQueryExecution>
  create(raft::NodeId source_node_id, query::DistributedAggregatePlan plan,
         std::vector<query::DistributedReadAdmission> admissions,
         query::CompatibleDistributedAggregateSnapshot snapshot,
         DistributedQueryExecutionLimits limits = {});
  // Reconstructs the exact plan-ordered admissions already owned by the compatible dispatches.
  // This is the preferred boundary after metadata-backed snapshot binding because it cannot pair
  // the bound snapshot with a separately assembled admission vector.
  [[nodiscard]] static common::Result<DistributedQueryExecution>
  create_from_bound_snapshot(raft::NodeId source_node_id, query::DistributedAggregatePlan plan,
                             query::CompatibleDistributedAggregateSnapshot snapshot,
                             DistributedQueryExecutionLimits limits = {});

  [[nodiscard]] common::Result<DistributedQueryAttempt>
  begin_attempt(const schema::TabletId& tablet_id, TimePoint now);
  [[nodiscard]] common::Status accept_response(const schema::TabletId& tablet_id,
                                               common::ByteView response_bytes, TimePoint now);
  [[nodiscard]] common::Status record_transport_failure(const schema::TabletId& tablet_id,
                                                        common::StatusCode code, TimePoint now);

  [[nodiscard]] common::Result<DistributedQuerySenderState>
  sender_state(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Result<std::optional<TimePoint>>
  next_attempt_not_before(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Result<std::optional<DistributedQueryLeaderHint>>
  suggested_leader(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Result<query::MergeableAggregateState> finish() const;
  [[nodiscard]] const query::CompatibleDistributedAggregateSnapshot& snapshot() const noexcept;

private:
  struct SenderSlot {
    schema::TabletId tablet_id;
    DistributedQuerySender sender;
    bool coordinator_result_delivered{};
    bool coordinator_failure_delivered{};
  };

  DistributedQueryExecution(query::CompatibleDistributedAggregateSnapshot snapshot,
                            query::DistributedAggregateCoordinator coordinator,
                            std::vector<SenderSlot> senders,
                            std::map<schema::TabletId, std::size_t> sender_indexes) noexcept;

  [[nodiscard]] common::Result<std::size_t> sender_index(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Status publish_terminal_state(SenderSlot& slot);

  query::CompatibleDistributedAggregateSnapshot snapshot_;
  query::DistributedAggregateCoordinator coordinator_;
  std::vector<SenderSlot> senders_;
  std::map<schema::TabletId, std::size_t> sender_indexes_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_QUERY_EXECUTION_HPP_
