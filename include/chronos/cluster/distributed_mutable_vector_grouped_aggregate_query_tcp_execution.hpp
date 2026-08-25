#ifndef CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_EXECUTION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_EXECUTION_HPP_

#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_execution.hpp"
#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tcp_client.hpp"
#include "chronos/cluster/distributed_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_finalization_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/query/distributed_sql_lowering.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cluster {

struct DistributedMutableVectorGroupedAggregateQueryTcpExecutionConfig {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  raft::NodeId local_node_id{};
  DistributedMutableVectorGroupedAggregateQueryWorkerService* local_worker{};
  std::vector<DistributedQueryNodeRoute> routes;
  DistributedMutableVectorGroupedAggregateQueryTlsLimits carrier_limits;
  DistributedVectorGroupedAggregateFinalizationLimitsV2 finalization_limits;
  std::optional<query::DistributedVectorGroupedAggregateCoordinatorProjection>
      coordinator_projection;
  std::chrono::milliseconds connect_timeout{5000};
  std::optional<std::chrono::steady_clock::time_point> execution_deadline;
  std::size_t maximum_rebindings{3U};
};

struct DistributedMutableVectorGroupedAggregateQueryTcpExecutionMetrics {
  std::uint64_t attempts_started{};
  std::uint64_t retries_started{};
  std::uint64_t transport_completed_attempts{};
  std::uint64_t transport_failed_attempts{};
  std::uint64_t local_completed_attempts{};
  std::uint64_t local_failed_attempts{};
  std::uint64_t rebindings_started{};
  std::size_t active_attempts{};
};

enum class DistributedMutableVectorGroupedAggregateQueryTcpExecutionState : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
  kCancelled = 4,
};

// Single-threaded poll owner for one immutable all-tablet mutable grouped execution. A target whose
// serving node equals local_node_id executes through the borrowed in-process worker; every other
// target owns at most one grouped authority-bound TCP/mTLS client. Complete grouped output remains
// unavailable until every tablet stream closes and the coordinator seals globally. Borrowed local
// worker, route TLS contexts, and authentication policy outlive it. Native output remains
// unavailable until all tablets close and grouped merge, final projection/order/limit, and
// encoding all succeed.
class DistributedMutableVectorGroupedAggregateQueryTcpExecution {
public:
  DistributedMutableVectorGroupedAggregateQueryTcpExecution() noexcept;
  ~DistributedMutableVectorGroupedAggregateQueryTcpExecution();
  DistributedMutableVectorGroupedAggregateQueryTcpExecution(
      const DistributedMutableVectorGroupedAggregateQueryTcpExecution&) = delete;
  DistributedMutableVectorGroupedAggregateQueryTcpExecution&
  operator=(const DistributedMutableVectorGroupedAggregateQueryTcpExecution&) = delete;
  DistributedMutableVectorGroupedAggregateQueryTcpExecution(
      DistributedMutableVectorGroupedAggregateQueryTcpExecution&&) noexcept;
  DistributedMutableVectorGroupedAggregateQueryTcpExecution&
  operator=(DistributedMutableVectorGroupedAggregateQueryTcpExecution&&) noexcept;

  [[nodiscard]] static common::Result<DistributedMutableVectorGroupedAggregateQueryTcpExecution>
  create(DistributedMutableVectorGroupedAggregateQueryExecution execution,
         DistributedMutableVectorGroupedAggregateQueryTcpExecutionConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();
  [[nodiscard]] common::Status
  rebind(DistributedMutableVectorGroupedAggregateQueryExecution execution,
         DistributedMutableVectorGroupedAggregateQueryTcpExecutionConfig config);

  [[nodiscard]] DistributedMutableVectorGroupedAggregateQueryTcpExecutionState
  state() const noexcept;
  [[nodiscard]] DistributedMutableVectorGroupedAggregateQueryTcpExecutionMetrics
  metrics() const noexcept;
  [[nodiscard]] const std::optional<DistributedVectorRowsFinalizedResultV2>&
  result() const noexcept;
  [[nodiscard]] common::Result<DistributedVectorRowsFinalizedResultV2> take_result();
  [[nodiscard]] std::span<const query::VectorGroupKeyDefinition> key_definitions() const noexcept;
  [[nodiscard]] std::span<const query::VectorAggregateDefinition>
  aggregate_definitions() const noexcept;
  [[nodiscard]] const query::DistributedVectorPlanIntent& plan() const;
  [[nodiscard]] const query::DistributedVectorResultSchema& result_schema() const;
  [[nodiscard]] std::optional<query::QueryResourceContext> output_resources() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;
  [[nodiscard]] common::Result<std::optional<DistributedQueryLeaderHint>>
  suggested_leader(const schema::TabletId& tablet_id) const;

private:
  class Impl;
  explicit DistributedMutableVectorGroupedAggregateQueryTcpExecution(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_EXECUTION_HPP_
