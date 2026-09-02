#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_TCP_EXECUTION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_TCP_EXECUTION_HPP_

#include "chronos/cluster/distributed_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tcp_client.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kMaximumDistributedVectorGroupedAggregateShuffleRemoteEdges = 4096U;

struct DistributedVectorGroupedAggregateShuffleTcpExecutionConfig {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::vector<DistributedQueryNodeRoute> routes;
  DistributedVectorGroupedAggregateShuffleTlsLimits carrier_limits{};
  std::chrono::milliseconds connect_timeout{5000};
  std::optional<std::chrono::steady_clock::time_point> execution_deadline{std::nullopt};
};

struct DistributedVectorGroupedAggregateShuffleTcpExecutionMetrics {
  std::uint64_t attempts_started{};
  std::uint64_t retries_started{};
  std::uint64_t transport_completed_attempts{};
  std::uint64_t transport_failed_attempts{};
  std::size_t active_attempts{};
  std::size_t succeeded_edges{};
  std::size_t total_edges{};
};

enum class DistributedVectorGroupedAggregateShuffleTcpExecutionState : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
  kCancelled = 4,
};

// Drives one finite set of already prepared remote shuffle retries over bounded address rotation
// and TCP/mTLS clients. One thread serializes all calls. The authority, route TLS contexts,
// authenticator, and authorizer are borrowed and outlive this owner. Success means every exact
// stream receipt was authenticated; destination reducer ownership remains with the remote servers.
class DistributedVectorGroupedAggregateShuffleTcpExecution {
public:
  DistributedVectorGroupedAggregateShuffleTcpExecution() noexcept;
  ~DistributedVectorGroupedAggregateShuffleTcpExecution();
  DistributedVectorGroupedAggregateShuffleTcpExecution(
      const DistributedVectorGroupedAggregateShuffleTcpExecution&) = delete;
  DistributedVectorGroupedAggregateShuffleTcpExecution&
  operator=(const DistributedVectorGroupedAggregateShuffleTcpExecution&) = delete;
  DistributedVectorGroupedAggregateShuffleTcpExecution(
      DistributedVectorGroupedAggregateShuffleTcpExecution&&) noexcept;
  DistributedVectorGroupedAggregateShuffleTcpExecution&
  operator=(DistributedVectorGroupedAggregateShuffleTcpExecution&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleTcpExecution>
  create(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
         std::vector<DistributedVectorGroupedAggregateShuffleRetry> retries,
         DistributedVectorGroupedAggregateShuffleTcpExecutionConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();

  [[nodiscard]] DistributedVectorGroupedAggregateShuffleTcpExecutionState state() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleTcpExecutionMetrics
  metrics() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleTcpExecution(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_TCP_EXECUTION_HPP_
