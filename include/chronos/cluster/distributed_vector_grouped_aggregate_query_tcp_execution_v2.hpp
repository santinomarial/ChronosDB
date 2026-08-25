#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_EXECUTION_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_EXECUTION_V2_HPP_

#include "chronos/cluster/distributed_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_finalization_v2.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_query_execution_v2.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_query_tcp_client_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::cluster {

struct DistributedVectorGroupedAggregateQueryTcpExecutionConfigV2 {
  raft::NodeId source_node_id{};
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::vector<DistributedQueryNodeRoute> routes;
  DistributedVectorGroupedAggregateQuerySenderLimitsV2 sender_limits;
  DistributedVectorGroupedAggregateQueryTlsLimitsV2 carrier_limits;
  DistributedVectorGroupedAggregateFinalizationLimitsV2 finalization_limits;
  std::chrono::milliseconds connect_timeout{5000};
  std::optional<std::chrono::steady_clock::time_point> execution_deadline;
};

struct DistributedVectorGroupedAggregateQueryTcpExecutionMetricsV2 {
  std::uint64_t attempts_started{};
  std::uint64_t retries_started{};
  std::uint64_t transport_completed_attempts{};
  std::uint64_t transport_failed_attempts{};
  std::size_t active_attempts{};
};

enum class DistributedVectorGroupedAggregateQueryTcpExecutionStateV2 : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
  kCancelled = 4,
};

// Single-threaded poll owner for one pinned grouped sufficient-state v2 execution. It owns one
// finite sender and at most one grouped authority-bound TCP client per tablet while retaining the
// portable execution, Manifest pin, and shared query resource authority. Authentication policy and
// route TLS contexts are borrowed and outlive it. Native output remains unavailable until all
// tablets close and grouped merge, global ordering, limit, and encoding all finish.
class DistributedVectorGroupedAggregateQueryTcpExecutionV2 {
public:
  DistributedVectorGroupedAggregateQueryTcpExecutionV2() noexcept;
  ~DistributedVectorGroupedAggregateQueryTcpExecutionV2();
  DistributedVectorGroupedAggregateQueryTcpExecutionV2(
      const DistributedVectorGroupedAggregateQueryTcpExecutionV2&) = delete;
  DistributedVectorGroupedAggregateQueryTcpExecutionV2&
  operator=(const DistributedVectorGroupedAggregateQueryTcpExecutionV2&) = delete;
  DistributedVectorGroupedAggregateQueryTcpExecutionV2(
      DistributedVectorGroupedAggregateQueryTcpExecutionV2&&) noexcept;
  DistributedVectorGroupedAggregateQueryTcpExecutionV2&
  operator=(DistributedVectorGroupedAggregateQueryTcpExecutionV2&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateQueryTcpExecutionV2>
  create(DistributedVectorGroupedAggregateQueryExecutionV2 execution,
         DistributedVectorGroupedAggregateQueryTcpExecutionConfigV2 config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();

  [[nodiscard]] DistributedVectorGroupedAggregateQueryTcpExecutionStateV2 state() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateQueryTcpExecutionMetricsV2
  metrics() const noexcept;
  [[nodiscard]] const std::optional<DistributedVectorRowsFinalizedResultV2>&
  result() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;
  [[nodiscard]] const query::CompatibleDistributedVectorSnapshotV2& snapshot() const;
  [[nodiscard]] common::Result<std::optional<DistributedQueryLeaderHint>>
  suggested_leader(const schema::TabletId& tablet_id) const;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateQueryTcpExecutionV2(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_EXECUTION_V2_HPP_
