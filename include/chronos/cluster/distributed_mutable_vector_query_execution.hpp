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

private:
  struct SenderSlot {
    schema::TabletId tablet_id;
    DistributedMutableVectorQuerySender sender;
    bool coordinator_result_delivered{};
    bool coordinator_failure_delivered{};
  };

  DistributedMutableVectorQueryExecution(
      query::DistributedVectorPlanIntent plan, DistributedVectorResultCoordinatorV2 coordinator,
      std::vector<SenderSlot> senders,
      std::map<schema::TabletId, std::size_t> sender_indexes) noexcept;

  [[nodiscard]] common::Result<std::size_t> sender_index(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Status publish_terminal_state(SenderSlot& slot);

  query::DistributedVectorPlanIntent plan_;
  DistributedVectorResultCoordinatorV2 coordinator_;
  std::vector<SenderSlot> senders_;
  std::map<schema::TabletId, std::size_t> sender_indexes_;
  bool finished_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_QUERY_EXECUTION_HPP_
