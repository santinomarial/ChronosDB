#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_TCP_EXECUTION_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_TCP_EXECUTION_V2_HPP_

#include "chronos/cluster/distributed_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_vector_aggregate_finalization_v2.hpp"
#include "chronos/cluster/distributed_vector_aggregate_query_execution_v2.hpp"
#include "chronos/cluster/distributed_vector_aggregate_query_tcp_client_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::cluster {

struct DistributedVectorAggregateQueryTcpExecutionConfigV2 {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::vector<DistributedQueryNodeRoute> routes;
  DistributedVectorAggregateQueryTlsLimitsV2 carrier_limits;
  DistributedVectorAggregateFinalizationLimitsV2 finalization_limits;
  std::chrono::milliseconds connect_timeout{5000};
  std::optional<std::chrono::steady_clock::time_point> execution_deadline;
};

struct DistributedVectorAggregateQueryTcpExecutionMetricsV2 {
  std::uint64_t attempts_started{};
  std::uint64_t retries_started{};
  std::uint64_t transport_completed_attempts{};
  std::uint64_t transport_failed_attempts{};
  std::size_t active_attempts{};
};

enum class DistributedVectorAggregateQueryTcpExecutionStateV2 : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
  kCancelled = 4,
};

// Single-threaded poll owner for one pinned aggregate-v2 execution. It owns the portable
// execution, its Manifest pin and shared query resource authority, and at most one definition-bound
// TCP client per tablet. Authentication policy and route TLS contexts are borrowed and outlive it.
// Only a complete all-tablet merge is finalized into one retained Native Protocol result.
class DistributedVectorAggregateQueryTcpExecutionV2 {
public:
  DistributedVectorAggregateQueryTcpExecutionV2() noexcept;
  ~DistributedVectorAggregateQueryTcpExecutionV2();
  DistributedVectorAggregateQueryTcpExecutionV2(
      const DistributedVectorAggregateQueryTcpExecutionV2&) = delete;
  DistributedVectorAggregateQueryTcpExecutionV2&
  operator=(const DistributedVectorAggregateQueryTcpExecutionV2&) = delete;
  DistributedVectorAggregateQueryTcpExecutionV2(
      DistributedVectorAggregateQueryTcpExecutionV2&&) noexcept;
  DistributedVectorAggregateQueryTcpExecutionV2&
  operator=(DistributedVectorAggregateQueryTcpExecutionV2&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorAggregateQueryTcpExecutionV2>
  create(DistributedVectorAggregateQueryExecutionV2 execution,
         DistributedVectorAggregateQueryTcpExecutionConfigV2 config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();

  [[nodiscard]] DistributedVectorAggregateQueryTcpExecutionStateV2 state() const noexcept;
  [[nodiscard]] DistributedVectorAggregateQueryTcpExecutionMetricsV2 metrics() const noexcept;
  [[nodiscard]] const std::optional<DistributedVectorAggregateFinalizedResultV2>&
  result() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;
  [[nodiscard]] const query::CompatibleDistributedVectorSnapshotV2& snapshot() const;
  [[nodiscard]] common::Result<std::optional<DistributedQueryLeaderHint>>
  suggested_leader(const schema::TabletId& tablet_id) const;

private:
  class Impl;
  explicit DistributedVectorAggregateQueryTcpExecutionV2(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_TCP_EXECUTION_V2_HPP_
