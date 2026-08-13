#ifndef CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_QUERY_TCP_SERVER_HPP_
#define CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_QUERY_TCP_SERVER_HPP_

#include "chronos/cluster/distributed_query_tcp_server.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"
#include "chronos/service/replicated_distributed_query_worker.hpp"

#include <chrono>
#include <cstddef>
#include <memory>

namespace chronos::service {

struct ReplicatedDistributedQueryTcpServerConfig {
  ReplicatedDistributedQueryWorkerConfig worker;
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  const cluster::ClusterNodePrincipalAuthorizer* node_authorizer{};
  const cluster::DistributedQueryLeaderHintProvider* leader_hint_provider{};
  cluster::DistributedQueryTlsServerLimits carrier_limits;
  std::size_t maximum_connections{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

// Owns the production inbound query stack in dependency order: real-CSEG worker, authenticated
// receiver, then bounded mTLS TCP server. One thread owns polling and shutdown. Borrowed storage,
// authority provider, authenticator, authorizer, and optional hint provider must outlive it.
class ReplicatedDistributedQueryTcpServer {
public:
  ReplicatedDistributedQueryTcpServer() = delete;
  ~ReplicatedDistributedQueryTcpServer();
  ReplicatedDistributedQueryTcpServer(const ReplicatedDistributedQueryTcpServer&) = delete;
  ReplicatedDistributedQueryTcpServer&
  operator=(const ReplicatedDistributedQueryTcpServer&) = delete;
  ReplicatedDistributedQueryTcpServer(ReplicatedDistributedQueryTcpServer&&) noexcept;
  ReplicatedDistributedQueryTcpServer& operator=(ReplicatedDistributedQueryTcpServer&&) noexcept;

  [[nodiscard]] static common::Result<ReplicatedDistributedQueryTcpServer>
  start(ReplicatedDistributedQueryTcpServerConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] cluster::DistributedQueryTcpServerMetrics metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit ReplicatedDistributedQueryTcpServer(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_QUERY_TCP_SERVER_HPP_
