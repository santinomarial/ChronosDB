#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_TCP_EXECUTION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_TCP_EXECUTION_HPP_

#include "chronos/cluster/distributed_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tcp_client.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t
    kMaximumDistributedVectorGroupedAggregateShuffleResultRemotePartitions = 4096U;

struct DistributedVectorGroupedAggregateShuffleResultTcpExecutionConfig {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::vector<DistributedQueryNodeRoute> routes;
  DistributedVectorGroupedAggregateShuffleResultTlsLimits carrier_limits{};
  std::chrono::milliseconds connect_timeout{5000};
  std::optional<std::chrono::steady_clock::time_point> execution_deadline{std::nullopt};
};

struct DistributedVectorGroupedAggregateShuffleResultTcpExecutionMetrics {
  std::uint64_t attempts_started{};
  std::uint64_t retries_started{};
  std::uint64_t transport_completed_attempts{};
  std::uint64_t transport_failed_attempts{};
  std::size_t active_attempts{};
  std::size_t succeeded_partitions{};
  std::size_t total_partitions{};
};

enum class DistributedVectorGroupedAggregateShuffleResultTcpExecutionState : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
  kCancelled = 4,
};

// Drives immutable reduced-partition results through finite retry, bounded address rotation, TCP,
// mutual TLS, and receipt validation. One event-loop thread serializes calls. Authority, raw
// schema, route TLS contexts, authenticator, and authorizer are borrowed and outlive the owner.
class DistributedVectorGroupedAggregateShuffleResultTcpExecution {
public:
  DistributedVectorGroupedAggregateShuffleResultTcpExecution() noexcept;
  ~DistributedVectorGroupedAggregateShuffleResultTcpExecution();
  DistributedVectorGroupedAggregateShuffleResultTcpExecution(
      const DistributedVectorGroupedAggregateShuffleResultTcpExecution&) = delete;
  DistributedVectorGroupedAggregateShuffleResultTcpExecution&
  operator=(const DistributedVectorGroupedAggregateShuffleResultTcpExecution&) = delete;
  DistributedVectorGroupedAggregateShuffleResultTcpExecution(
      DistributedVectorGroupedAggregateShuffleResultTcpExecution&&) noexcept;
  DistributedVectorGroupedAggregateShuffleResultTcpExecution&
  operator=(DistributedVectorGroupedAggregateShuffleResultTcpExecution&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleResultTcpExecution>
  create(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
         const query::DistributedVectorResultSchema& result_schema,
         std::vector<DistributedVectorGroupedAggregateShuffleResultRetry> retries,
         DistributedVectorGroupedAggregateShuffleResultTcpExecutionConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();

  [[nodiscard]] DistributedVectorGroupedAggregateShuffleResultTcpExecutionState
  state() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleResultTcpExecutionMetrics
  metrics() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleResultTcpExecution(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_TCP_EXECUTION_HPP_
