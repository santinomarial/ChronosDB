#ifndef CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_QUERY_TCP_EXECUTION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_QUERY_TCP_EXECUTION_HPP_

#include "chronos/cluster/distributed_mutable_vector_query_execution.hpp"
#include "chronos/cluster/distributed_mutable_vector_query_tcp.hpp"
#include "chronos/cluster/distributed_query_tcp_execution.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::cluster {

struct DistributedMutableVectorQueryTcpExecutionConfig {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::vector<DistributedQueryNodeRoute> routes;
  DistributedMutableVectorQueryTlsLimits carrier_limits{};
  std::chrono::milliseconds connect_timeout{5000};
  std::optional<std::chrono::steady_clock::time_point> execution_deadline{std::nullopt};
  std::size_t maximum_rebindings{3U};
};

struct DistributedMutableVectorQueryTcpExecutionMetrics {
  std::uint64_t attempts_started{};
  std::uint64_t retries_started{};
  std::uint64_t transport_completed_attempts{};
  std::uint64_t transport_failed_attempts{};
  std::uint64_t rebindings_started{};
  std::size_t active_attempts{};
};

enum class DistributedMutableVectorQueryTcpExecutionState : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
  kCancelled = 4,
};

// Single-threaded poll owner for one immutable multi-tablet mutable execution. It owns at most one
// TCP/mTLS client per tablet. Routes and authority remain fixed for this execution; advisory leader
// hints require fresh fragment binding by a later owner. Borrowed TLS/authentication policy must
// outlive it. Completion is published only after every tablet stream closes successfully.
class DistributedMutableVectorQueryTcpExecution {
public:
  DistributedMutableVectorQueryTcpExecution() noexcept;
  ~DistributedMutableVectorQueryTcpExecution();
  DistributedMutableVectorQueryTcpExecution(const DistributedMutableVectorQueryTcpExecution&) =
      delete;
  DistributedMutableVectorQueryTcpExecution&
  operator=(const DistributedMutableVectorQueryTcpExecution&) = delete;
  DistributedMutableVectorQueryTcpExecution(DistributedMutableVectorQueryTcpExecution&&) noexcept;
  DistributedMutableVectorQueryTcpExecution&
  operator=(DistributedMutableVectorQueryTcpExecution&&) noexcept;

  [[nodiscard]] static common::Result<DistributedMutableVectorQueryTcpExecution>
  create(DistributedMutableVectorQueryExecution execution,
         DistributedMutableVectorQueryTcpExecutionConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();
  [[nodiscard]] common::Status rebind(DistributedMutableVectorQueryExecution execution,
                                      DistributedMutableVectorQueryTcpExecutionConfig config);

  [[nodiscard]] DistributedMutableVectorQueryTcpExecutionState state() const noexcept;
  [[nodiscard]] DistributedMutableVectorQueryTcpExecutionMetrics metrics() const noexcept;
  [[nodiscard]] const std::optional<DistributedVectorQueryExecutionResultV2>&
  result() const noexcept;
  // Transfers the complete all-tablet result exactly once. The execution remains terminal.
  [[nodiscard]] common::Result<DistributedVectorQueryExecutionResultV2> take_result();
  [[nodiscard]] const common::Status& failure() const noexcept;
  [[nodiscard]] common::Result<std::optional<DistributedQueryLeaderHint>>
  suggested_leader(const schema::TabletId& tablet_id) const;

private:
  class Impl;
  explicit DistributedMutableVectorQueryTcpExecution(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_QUERY_TCP_EXECUTION_HPP_
