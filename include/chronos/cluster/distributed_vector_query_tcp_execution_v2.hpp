#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TCP_EXECUTION_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TCP_EXECUTION_V2_HPP_

#include "chronos/cluster/distributed_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_vector_query_execution_v2.hpp"
#include "chronos/cluster/distributed_vector_query_tcp_client_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::cluster {

struct DistributedVectorQueryTcpExecutionConfigV2 {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::vector<DistributedQueryNodeRoute> routes;
  DistributedVectorQueryTlsLimitsV2 carrier_limits{};
  std::chrono::milliseconds connect_timeout{5000};
  std::optional<std::chrono::steady_clock::time_point> execution_deadline{std::nullopt};
};

struct DistributedVectorQueryTcpExecutionMetricsV2 {
  std::uint64_t attempts_started{};
  std::uint64_t retries_started{};
  std::uint64_t transport_completed_attempts{};
  std::uint64_t transport_failed_attempts{};
  std::size_t active_attempts{};
};

enum class DistributedVectorQueryTcpExecutionStateV2 : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
  kCancelled = 4,
};

// Single-threaded poll owner for one compatible vector-v2 execution. It owns the execution and its
// Manifest pin plus at most one TCP client per tablet. Authentication policy and route TLS contexts
// are borrowed and must outlive it. Retries rotate only the prevalidated addresses of the immutable
// target node; fresh authority requires a new execution. The completed result reference remains
// valid only until this owner is moved or destroyed.
class DistributedVectorQueryTcpExecutionV2 {
public:
  DistributedVectorQueryTcpExecutionV2() noexcept;
  ~DistributedVectorQueryTcpExecutionV2();
  DistributedVectorQueryTcpExecutionV2(const DistributedVectorQueryTcpExecutionV2&) = delete;
  DistributedVectorQueryTcpExecutionV2&
  operator=(const DistributedVectorQueryTcpExecutionV2&) = delete;
  DistributedVectorQueryTcpExecutionV2(DistributedVectorQueryTcpExecutionV2&&) noexcept;
  DistributedVectorQueryTcpExecutionV2& operator=(DistributedVectorQueryTcpExecutionV2&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorQueryTcpExecutionV2>
  create(DistributedVectorQueryExecutionV2 execution,
         DistributedVectorQueryTcpExecutionConfigV2 config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();

  [[nodiscard]] DistributedVectorQueryTcpExecutionStateV2 state() const noexcept;
  [[nodiscard]] DistributedVectorQueryTcpExecutionMetricsV2 metrics() const noexcept;
  [[nodiscard]] const std::optional<DistributedVectorQueryExecutionResultV2>&
  result() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;
  [[nodiscard]] const query::CompatibleDistributedVectorSnapshotV2& snapshot() const;
  [[nodiscard]] common::Result<std::optional<DistributedQueryLeaderHint>>
  suggested_leader(const schema::TabletId& tablet_id) const;

private:
  class Impl;
  explicit DistributedVectorQueryTcpExecutionV2(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TCP_EXECUTION_V2_HPP_
