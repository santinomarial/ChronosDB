#ifndef CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_ROWS_QUERY_TCP_EXECUTION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_ROWS_QUERY_TCP_EXECUTION_HPP_

#include "chronos/cluster/distributed_mutable_vector_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_vector_row_finalization_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_mutable_vector_fragment.hpp"

#include <chrono>
#include <cstdint>
#include <optional>
#include <vector>

namespace chronos::cluster {

struct DistributedMutableVectorRowsQueryTcpExecutionConfig {
  raft::NodeId source_node_id{};
  DistributedMutableVectorQueryExecutionLimits execution;
  DistributedMutableVectorQueryTcpExecutionConfig tcp;
  DistributedVectorRowFinalizationLimitsV2 finalization;
};

enum class DistributedMutableVectorRowsQueryTcpExecutionState : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
  kCancelled = 4,
};

// Single-threaded Native row request owner. It consumes proof-bound fragments and routes, drives
// the finite TCP scheduler, consumes its all-tablet result exactly once, and retains only bounded
// schema-bearing QUERY_RESULT payloads. Borrowed authentication and TLS policy must outlive it.
class DistributedMutableVectorRowsQueryTcpExecution {
public:
  DistributedMutableVectorRowsQueryTcpExecution() = delete;
  DistributedMutableVectorRowsQueryTcpExecution(
      const DistributedMutableVectorRowsQueryTcpExecution&) = delete;
  DistributedMutableVectorRowsQueryTcpExecution&
  operator=(const DistributedMutableVectorRowsQueryTcpExecution&) = delete;
  DistributedMutableVectorRowsQueryTcpExecution(
      DistributedMutableVectorRowsQueryTcpExecution&&) noexcept = default;
  DistributedMutableVectorRowsQueryTcpExecution&
  operator=(DistributedMutableVectorRowsQueryTcpExecution&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedMutableVectorRowsQueryTcpExecution>
  create(std::vector<query::DistributedMutableVectorFragment> fragments,
         DistributedMutableVectorRowsQueryTcpExecutionConfig config);

  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();
  [[nodiscard]] common::Status
  rebind(std::vector<query::DistributedMutableVectorFragment> fragments,
         DistributedMutableVectorQueryTcpExecutionConfig tcp);

  [[nodiscard]] DistributedMutableVectorRowsQueryTcpExecutionState state() const noexcept;
  [[nodiscard]] DistributedMutableVectorQueryTcpExecutionMetrics metrics() const noexcept;
  [[nodiscard]] const std::optional<DistributedVectorRowsFinalizedResultV2>&
  result() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;
  [[nodiscard]] common::Result<std::optional<DistributedQueryLeaderHint>>
  suggested_leader(const schema::TabletId& tablet_id) const;

private:
  DistributedMutableVectorRowsQueryTcpExecution(
      raft::NodeId source_node_id, DistributedMutableVectorQueryExecutionLimits execution_limits,
      DistributedMutableVectorQueryTcpExecution scheduler,
      DistributedVectorRowFinalizationLimitsV2 finalization_limits);

  raft::NodeId source_node_id_{};
  DistributedMutableVectorQueryExecutionLimits execution_limits_;
  DistributedMutableVectorQueryTcpExecution scheduler_;
  DistributedVectorRowFinalizationLimitsV2 finalization_limits_;
  DistributedMutableVectorRowsQueryTcpExecutionState state_{
      DistributedMutableVectorRowsQueryTcpExecutionState::kRunning};
  std::optional<DistributedVectorRowsFinalizedResultV2> result_;
  common::Status failure_{common::StatusCode::kInternal,
                          "mutable vector rows query TCP execution has not failed"};
  bool scheduler_failed_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_ROWS_QUERY_TCP_EXECUTION_HPP_
