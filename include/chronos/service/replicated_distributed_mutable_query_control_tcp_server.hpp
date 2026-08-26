#ifndef CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_MUTABLE_QUERY_CONTROL_TCP_SERVER_HPP_
#define CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_MUTABLE_QUERY_CONTROL_TCP_SERVER_HPP_

#include "chronos/cluster/distributed_mutable_query_control_tcp.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"
#include "chronos/service/replicated_distributed_query_worker.hpp"
#include "chronos/service/replicated_read_barrier.hpp"

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>

namespace chronos::service {

struct ReplicatedDistributedMutableQueryControlTcpServerConfig {
  ReplicatedDistributedMutableVectorQueryWorkerConfig worker;
  query::DistributedVectorGroupedAggregateWorkerLimitsV2 grouped_worker_limits;
  ReplicatedReadBarrier* read_barrier{};
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  const cluster::ClusterNodePrincipalAuthorizer* node_authorizer{};
  const cluster::DistributedQueryLeaderHintProvider* leader_hint_provider{};
  std::optional<cluster::DistributedVectorGroupedAggregateShuffleJobServiceConfig>
      grouped_shuffle_jobs;
  cluster::DistributedMutableQueryControlTlsServerLimits carrier_limits;
  std::size_t maximum_connections{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

// Owns the production inbound mutable query-control stack in dependency order: request-local row
// and grouped sufficient-state TabletState workers, per-group replicated read-authority service,
// authenticated receivers, an optional grouped-shuffle reducer-job service, then the shared
// bounded TCP/mTLS server. One non-Raft-poll thread owns polling and shutdown. Borrowed database,
// read-barrier, authentication, authorization, TLS-route, and optional hint providers must outlive
// this owner.
class ReplicatedDistributedMutableQueryControlTcpServer {
public:
  ReplicatedDistributedMutableQueryControlTcpServer() = delete;
  ~ReplicatedDistributedMutableQueryControlTcpServer();
  ReplicatedDistributedMutableQueryControlTcpServer(
      const ReplicatedDistributedMutableQueryControlTcpServer&) = delete;
  ReplicatedDistributedMutableQueryControlTcpServer&
  operator=(const ReplicatedDistributedMutableQueryControlTcpServer&) = delete;
  ReplicatedDistributedMutableQueryControlTcpServer(
      ReplicatedDistributedMutableQueryControlTcpServer&&) noexcept;
  ReplicatedDistributedMutableQueryControlTcpServer&
  operator=(ReplicatedDistributedMutableQueryControlTcpServer&&) noexcept;

  [[nodiscard]] static common::Result<ReplicatedDistributedMutableQueryControlTcpServer>
  start(ReplicatedDistributedMutableQueryControlTcpServerConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] cluster::DistributedMutableQueryControlTcpServerMetrics metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit ReplicatedDistributedMutableQueryControlTcpServer(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_MUTABLE_QUERY_CONTROL_TCP_SERVER_HPP_
