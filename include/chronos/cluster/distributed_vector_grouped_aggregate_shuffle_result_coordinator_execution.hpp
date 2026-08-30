#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_COORDINATOR_EXECUTION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_COORDINATOR_EXECUTION_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_finalization_v2.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_collected_result_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_collector.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tcp_server.hpp"
#include "chronos/common/result.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::cluster {

struct DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionConfig {
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  raft::NodeId coordinator_node_id{};
  DistributedVectorGroupedAggregateShuffleResultTlsLimits carrier_limits;
  std::size_t maximum_retained_server_streams{1024U};
  std::size_t maximum_accepts_per_poll{32U};
  std::size_t maximum_collected_encoded_bytes{
      kDefaultDistributedVectorGroupedAggregateShuffleResultCollectorBytes};
  std::size_t maximum_batch_working_bytes{query::kDefaultVectorChunkMemoryLimit};
  std::size_t maximum_working_memory_bytes{
      kDefaultDistributedVectorGroupedAggregateShuffleCollectedResultWorkingBytes};
  DistributedVectorGroupedAggregateFinalizationLimitsV2 finalization_limits;
  const query::DistributedVectorGroupedAggregateCoordinatorProjection* projection{};
  std::optional<std::chrono::steady_clock::time_point> execution_deadline;
};

struct DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionMetrics {
  DistributedVectorGroupedAggregateShuffleResultTcpServerMetrics server;
  DistributedVectorGroupedAggregateShuffleResultCollectorMetrics collector;
  std::uint64_t polls{};
  std::uint64_t finalization_attempts{};
  std::uint64_t finalized_rows{};
  std::size_t finalized_encoded_bytes{};
};

enum class DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kResultTaken = 3,
  kFailed = 4,
  kCancelled = 5,
};

// Owns coordinator-side result listener progress, lossless acknowledged-result handoff,
// idempotent all-partition collection, accounted Native materialization, and atomic global SQL
// finalization. One thread serializes calls. Authority, finalization proof, optional projection,
// authenticator, and authorizer are borrowed and outlive this owner.
class DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution {
public:
  DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution() noexcept;
  ~DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution();
  DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution(
      const DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution&) = delete;
  DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution&
  operator=(const DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution&) = delete;
  DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution(
      DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution&&) noexcept;
  DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution&
  operator=(DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution&&) noexcept;

  [[nodiscard]] static common::Result<
      DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution>
  create(
      const DistributedVectorGroupedAggregateShuffleAuthority& authority,
      const DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2& finalization_authority,
      DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  // Transfers same-process reducer results into the coordinator without constructing the result
  // protocol's invalid self-route. Ownership is retained across retryable admission exhaustion.
  [[nodiscard]] common::Status accept_local_streams(
      std::vector<DistributedVectorGroupedAggregateShuffleCompleteResultStream> streams);
  [[nodiscard]] common::Status cancel();
  [[nodiscard]] common::Result<DistributedVectorRowsFinalizedResultV2> take_result();

  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState
  state() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionMetrics
  metrics() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_COORDINATOR_EXECUTION_HPP_
