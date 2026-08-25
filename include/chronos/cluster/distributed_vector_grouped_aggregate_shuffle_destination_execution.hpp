#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_DESTINATION_EXECUTION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_DESTINATION_EXECUTION_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_reducer.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tcp_server.hpp"
#include "chronos/common/result.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

namespace chronos::cluster {

struct DistributedVectorGroupedAggregateShuffleDestinationExecutionConfig {
  raft::NodeId local_node_id{};
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::optional<query::QueryResourceContext> resources;
  DistributedVectorGroupedAggregateShuffleTlsLimits carrier_limits;
  DistributedVectorGroupedAggregateShuffleReducerLimits reducer_limits;
  std::size_t maximum_retained_streams{1024U};
  std::size_t maximum_accepts_per_poll{32U};
  std::size_t maximum_reducer_admissions_per_poll{1024U};
};

struct DistributedVectorGroupedAggregateShuffleDestinationExecutionMetrics {
  std::uint64_t local_stream_deliveries{};
  std::uint64_t remote_stream_deliveries{};
  std::size_t local_partitions{};
  std::size_t ready_partitions{};
  std::size_t pending_remote_streams{};
};

enum class DistributedVectorGroupedAggregateShuffleDestinationExecutionState : std::uint8_t {
  kReceiving = 1,
  kReady = 2,
  kComplete = 3,
  kFailed = 4,
  kCancelled = 5,
};

// Owns every reducer assigned to one destination node and, when remote sources exist, its bounded
// TCP listener. Acknowledged streams are retained across retryable reducer admission failure. One
// thread serializes every call. The authority and security dependencies are borrowed and outlive
// this owner.
class DistributedVectorGroupedAggregateShuffleDestinationExecution {
public:
  DistributedVectorGroupedAggregateShuffleDestinationExecution() noexcept;
  ~DistributedVectorGroupedAggregateShuffleDestinationExecution();
  DistributedVectorGroupedAggregateShuffleDestinationExecution(
      const DistributedVectorGroupedAggregateShuffleDestinationExecution&) = delete;
  DistributedVectorGroupedAggregateShuffleDestinationExecution&
  operator=(const DistributedVectorGroupedAggregateShuffleDestinationExecution&) = delete;
  DistributedVectorGroupedAggregateShuffleDestinationExecution(
      DistributedVectorGroupedAggregateShuffleDestinationExecution&&) noexcept;
  DistributedVectorGroupedAggregateShuffleDestinationExecution&
  operator=(DistributedVectorGroupedAggregateShuffleDestinationExecution&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleDestinationExecution>
  start(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
        DistributedVectorGroupedAggregateShuffleDestinationExecutionConfig config);

  [[nodiscard]] common::Status
  accept_local_stream(const DistributedVectorGroupedAggregateShuffleCompleteStream& stream);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  // Seals remote admission only after every local reducer is ready and the coordinator has proved
  // all source-side receipts. Local-only executions become complete as soon as reducers are ready.
  [[nodiscard]] common::Status seal_transport();
  [[nodiscard]] common::Status cancel();
  [[nodiscard]] common::Result<query::PhysicalOperatorStep> next(std::uint32_t partition_id);

  [[nodiscard]] DistributedVectorGroupedAggregateShuffleDestinationExecutionState
  state() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleDestinationExecutionMetrics
  metrics() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleTcpServerMetrics
  transport_metrics() const noexcept;
  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleReducerMetrics>
  reducer_metrics(std::uint32_t partition_id) const;
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleDestinationExecution(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_DESTINATION_EXECUTION_HPP_
