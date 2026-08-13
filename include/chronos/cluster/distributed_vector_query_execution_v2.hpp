#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_EXECUTION_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_EXECUTION_V2_HPP_

#include "chronos/cluster/distributed_vector_query_transport_v2.hpp"
#include "chronos/cluster/distributed_vector_result_exchange.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_fragment_binding.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <map>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cluster {

struct DistributedVectorQueryExecutionLimitsV2 {
  DistributedVectorQuerySenderLimitsV2 sender;
  DistributedVectorResultCoordinatorLimitsV2 coordinator;
};

struct DistributedVectorQueryExecutionResultV2 {
  query::DistributedVectorPlanIntent plan;
  DistributedVectorQueryResultV2 result;
};

// Portable, single-owner orchestration for one proof-bound schema-bearing vector snapshot. It
// retains the Manifest pin, one immutable finite sender per plan-ordered tablet, and the bounded
// result coordinator. Callers own transports and clocks and serialize every method.
class DistributedVectorQueryExecutionV2 {
public:
  using TimePoint = DistributedVectorQuerySenderV2::TimePoint;

  DistributedVectorQueryExecutionV2() = delete;
  DistributedVectorQueryExecutionV2(const DistributedVectorQueryExecutionV2&) = delete;
  DistributedVectorQueryExecutionV2& operator=(const DistributedVectorQueryExecutionV2&) = delete;
  DistributedVectorQueryExecutionV2(DistributedVectorQueryExecutionV2&&) noexcept = default;
  DistributedVectorQueryExecutionV2&
  operator=(DistributedVectorQueryExecutionV2&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedVectorQueryExecutionV2>
  create(raft::NodeId source_node_id, query::CompatibleDistributedVectorSnapshotV2 snapshot,
         DistributedVectorQueryExecutionLimitsV2 limits = {});

  [[nodiscard]] common::Result<DistributedVectorQueryAttemptV2>
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
  [[nodiscard]] const query::CompatibleDistributedVectorSnapshotV2& snapshot() const noexcept;

private:
  struct SenderSlot {
    schema::TabletId tablet_id;
    DistributedVectorQuerySenderV2 sender;
    bool coordinator_result_delivered{};
    bool coordinator_failure_delivered{};
  };

  DistributedVectorQueryExecutionV2(
      query::CompatibleDistributedVectorSnapshotV2 snapshot,
      DistributedVectorResultCoordinatorV2 coordinator, std::vector<SenderSlot> senders,
      std::map<schema::TabletId, std::size_t> sender_indexes) noexcept;

  [[nodiscard]] common::Result<std::size_t> sender_index(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Status publish_terminal_state(SenderSlot& slot);

  query::CompatibleDistributedVectorSnapshotV2 snapshot_;
  DistributedVectorResultCoordinatorV2 coordinator_;
  std::vector<SenderSlot> senders_;
  std::map<schema::TabletId, std::size_t> sender_indexes_;
  bool finished_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_EXECUTION_V2_HPP_
