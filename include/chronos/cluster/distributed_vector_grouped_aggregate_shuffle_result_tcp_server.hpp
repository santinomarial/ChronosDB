#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_TCP_SERVER_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_TCP_SERVER_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tls.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct DistributedVectorGroupedAggregateShuffleResultTcpServerConfig {
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  const DistributedVectorGroupedAggregateShuffleAuthority* authority{};
  const query::DistributedVectorResultSchema* result_schema{};
  raft::NodeId coordinator_node_id{};
  DistributedVectorGroupedAggregateShuffleResultTlsLimits carrier_limits;
  std::size_t maximum_retained_streams{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

struct DistributedVectorGroupedAggregateShuffleResultTcpServerMetrics {
  std::uint64_t accepted_connections{};
  std::uint64_t rejected_connections{};
  std::uint64_t accept_errors{};
  std::uint64_t completed_connections{};
  std::uint64_t failed_connections{};
  std::size_t active_connections{};
  std::size_t retained_streams{};
};

// Owns a bounded listener and authenticated result-return sessions. Each admitted connection
// reserves one preallocated completion slot before TLS begins, so an acknowledged result cannot be
// lost to queue allocation failure. One thread owns all calls; borrowed security, authority, and
// result-schema dependencies outlive the server.
class DistributedVectorGroupedAggregateShuffleResultTcpServer {
public:
  DistributedVectorGroupedAggregateShuffleResultTcpServer() noexcept;
  ~DistributedVectorGroupedAggregateShuffleResultTcpServer();
  DistributedVectorGroupedAggregateShuffleResultTcpServer(
      const DistributedVectorGroupedAggregateShuffleResultTcpServer&) = delete;
  DistributedVectorGroupedAggregateShuffleResultTcpServer&
  operator=(const DistributedVectorGroupedAggregateShuffleResultTcpServer&) = delete;
  DistributedVectorGroupedAggregateShuffleResultTcpServer(
      DistributedVectorGroupedAggregateShuffleResultTcpServer&&) noexcept;
  DistributedVectorGroupedAggregateShuffleResultTcpServer&
  operator=(DistributedVectorGroupedAggregateShuffleResultTcpServer&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleResultTcpServer>
  start(DistributedVectorGroupedAggregateShuffleResultTcpServerConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleCompleteResultStream>
  take_next_complete_stream();
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleResultTcpServerMetrics
  metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleResultTcpServer(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_TCP_SERVER_HPP_
