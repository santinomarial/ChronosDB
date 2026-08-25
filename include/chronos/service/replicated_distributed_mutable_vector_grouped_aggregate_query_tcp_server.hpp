#ifndef CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_SERVER_HPP_
#define CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_SERVER_HPP_

#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tcp_server.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"
#include "chronos/service/replicated_distributed_query_worker.hpp"

#include <chrono>
#include <cstddef>
#include <memory>

namespace chronos::service {

struct ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServerConfig {
  ReplicatedDistributedMutableVectorGroupedAggregateQueryWorkerConfig worker;
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  const cluster::ClusterNodePrincipalAuthorizer* node_authorizer{};
  const cluster::DistributedQueryLeaderHintProvider* leader_hint_provider{};
  cluster::DistributedMutableVectorGroupedAggregateQueryTlsLimits carrier_limits;
  std::size_t maximum_connections{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

// Owns the production inbound mutable grouped sufficient-state stack in dependency order:
// request-local TabletState worker, authenticated receiver, then bounded TCP/mTLS server. One
// thread owns polling/shutdown. Borrowed authority, authentication, authorization, and optional
// hint providers outlive it.
class ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer {
public:
  ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer() = delete;
  ~ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer();
  ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer(
      const ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer&) = delete;
  ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer&
  operator=(const ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer&) = delete;
  ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer(
      ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer&&) noexcept;
  ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer&
  operator=(ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer&&) noexcept;

  [[nodiscard]] static common::Result<
      ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer>
  start(ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServerConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] cluster::DistributedMutableVectorGroupedAggregateQueryTcpServerMetrics
  metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_SERVER_HPP_
