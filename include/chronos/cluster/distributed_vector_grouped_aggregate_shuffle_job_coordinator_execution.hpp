#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_COORDINATOR_EXECUTION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_COORDINATOR_EXECUTION_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_tcp_acquisition.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_coordinator_execution.hpp"
#include "chronos/common/result.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace chronos::cluster {

struct DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionConfig {
  raft::NodeId coordinator_node_id{};
  std::vector<DistributedQueryNodeRoute> reducer_control_routes;
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  DistributedVectorGroupedAggregateShuffleJobControlTlsLimits carrier_limits;
  std::chrono::milliseconds connect_timeout{5000};
  DistributedVectorGroupedAggregateShuffleJobControlTcpRetryLimits prepare_retry;
  DistributedVectorGroupedAggregateShuffleJobControlTcpRetryLimits route_install_retry;
  DistributedVectorGroupedAggregateShuffleJobControlTcpRetryLimits seal_retry;
  std::chrono::milliseconds reducer_execution_timeout{30000};
  std::chrono::steady_clock::time_point execution_deadline;
  std::size_t maximum_reducer_nodes{4096U};
  DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionConfig result;
};

struct DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionMetrics {
  std::size_t reducer_nodes{};
  std::size_t prepared_reducers{};
  std::size_t route_installed_reducers{};
  std::size_t sealed_reducers{};
  std::uint64_t control_attempts_started{};
  std::uint64_t control_retries_started{};
  std::uint64_t control_failed_attempts{};
  DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionMetrics result;
};

enum class DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState : std::uint8_t {
  kPreparing = 1,
  kInstallingRoutes = 2,
  kPrepared = 3,
  kSealing = 4,
  kCollectingResults = 5,
  kComplete = 6,
  kResultTaken = 7,
  kFailed = 8,
  kCancelled = 9,
};

// Owns one coordinator-side reducer set. It starts every PREPARE before blocking, publishes no
// remote shuffle route until every authority destination node has acknowledged, omits listeners
// for all-local destinations, accepts one explicit seal transition after source delivery closes,
// and publishes Native output only after every reducer's receipt-proven partition result arrives.
// Authority, finalization proof, TLS contexts, and security dependencies are borrowed and must
// outlive this single-thread-affine owner.
class DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution {
public:
  DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution() noexcept;
  ~DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution();
  DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution(
      const DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution&) = delete;
  DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution&
  operator=(const DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution&) = delete;
  DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution(
      DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution&&) noexcept;
  DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution&
  operator=(DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution&&) noexcept;

  [[nodiscard]] static common::Result<
      DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution>
  create(
      const DistributedVectorGroupedAggregateShuffleAuthority& authority,
      const DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2& finalization_authority,
      DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status seal();
  [[nodiscard]] common::Status cancel();
  [[nodiscard]] std::span<const DistributedQueryNodeRoute> prepared_routes() const noexcept;
  [[nodiscard]] common::Result<DistributedVectorRowsFinalizedResultV2> take_result();
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState
  state() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionMetrics
  metrics() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_COORDINATOR_EXECUTION_HPP_
