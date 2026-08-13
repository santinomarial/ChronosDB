#ifndef CHRONOS_CLUSTER_DISTRIBUTED_GROUPED_QUERY_TCP_EXECUTION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_GROUPED_QUERY_TCP_EXECUTION_HPP_

#include "chronos/cluster/distributed_grouped_query_execution.hpp"
#include "chronos/cluster/distributed_grouped_query_tcp_client.hpp"
#include "chronos/cluster/distributed_query_tcp_execution.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/query/distributed_grouped_exchange.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::cluster {

struct DistributedGroupedQueryTcpExecutionConfig {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::vector<DistributedQueryNodeRoute> routes;
  DistributedGroupedQueryTlsLimits carrier_limits;
  std::chrono::milliseconds connect_timeout{5000};
  std::optional<std::chrono::steady_clock::time_point> execution_deadline;
  std::size_t maximum_rebindings{3U};
};

struct DistributedGroupedQueryTcpExecutionMetrics {
  std::uint64_t attempts_started{};
  std::uint64_t retries_started{};
  std::uint64_t transport_completed_attempts{};
  std::uint64_t transport_failed_attempts{};
  std::uint64_t rebindings_started{};
  std::size_t active_attempts{};
};

enum class DistributedGroupedQueryTcpExecutionState : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
  kCancelled = 4,
};

// Single-threaded poll owner for one compatible grouped execution. It owns the execution and its
// Manifest pin. Authentication policy and route TLS contexts are borrowed and must outlive it.
// Routes never change under retries; a new authority requires a new execution.
class DistributedGroupedQueryTcpExecution {
public:
  DistributedGroupedQueryTcpExecution() noexcept;
  ~DistributedGroupedQueryTcpExecution();
  DistributedGroupedQueryTcpExecution(const DistributedGroupedQueryTcpExecution&) = delete;
  DistributedGroupedQueryTcpExecution&
  operator=(const DistributedGroupedQueryTcpExecution&) = delete;
  DistributedGroupedQueryTcpExecution(DistributedGroupedQueryTcpExecution&&) noexcept;
  DistributedGroupedQueryTcpExecution& operator=(DistributedGroupedQueryTcpExecution&&) noexcept;

  [[nodiscard]] static common::Result<DistributedGroupedQueryTcpExecution>
  create(DistributedGroupedQueryExecution execution,
         DistributedGroupedQueryTcpExecutionConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();
  [[nodiscard]] common::Status rebind(DistributedGroupedQueryExecution execution,
                                      DistributedGroupedQueryTcpExecutionConfig config);

  [[nodiscard]] DistributedGroupedQueryTcpExecutionState state() const noexcept;
  [[nodiscard]] DistributedGroupedQueryTcpExecutionMetrics metrics() const noexcept;
  [[nodiscard]] common::Result<std::vector<query::GroupedFloat64AggregateResult>> result() const;
  [[nodiscard]] const common::Status& failure() const noexcept;
  [[nodiscard]] const query::CompatibleDistributedGroupedFloat64Snapshot& snapshot() const;
  [[nodiscard]] common::Result<std::optional<DistributedQueryLeaderHint>>
  suggested_leader(const schema::TabletId& tablet_id) const;

private:
  class Impl;
  explicit DistributedGroupedQueryTcpExecution(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_GROUPED_QUERY_TCP_EXECUTION_HPP_
