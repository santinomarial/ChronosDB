#ifndef CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_GROUPED_QUERY_TCP_SERVER_HPP_
#define CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_GROUPED_QUERY_TCP_SERVER_HPP_

#include "chronos/cluster/distributed_grouped_query_tcp_server.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"
#include "chronos/service/replicated_distributed_query_worker.hpp"

#include <chrono>
#include <cstddef>
#include <memory>

namespace chronos::service {

struct ReplicatedDistributedGroupedQueryTcpServerConfig {
  ReplicatedDistributedGroupedQueryWorkerConfig worker;
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  const cluster::ClusterNodePrincipalAuthorizer* node_authorizer{};
  const cluster::DistributedQueryLeaderHintProvider* leader_hint_provider{};
  cluster::DistributedGroupedQueryTlsLimits carrier_limits;
  std::size_t maximum_connections{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

// Owns the production inbound grouped stack in dependency order: real-CSEG worker, authenticated
// receiver, then bounded TCP/mTLS server. One thread owns polling and shutdown. Borrowed storage,
// authority provider, authenticator, authorizer, and optional hint provider must outlive it.
class ReplicatedDistributedGroupedQueryTcpServer {
public:
  ReplicatedDistributedGroupedQueryTcpServer() = delete;
  ~ReplicatedDistributedGroupedQueryTcpServer();
  ReplicatedDistributedGroupedQueryTcpServer(const ReplicatedDistributedGroupedQueryTcpServer&) =
      delete;
  ReplicatedDistributedGroupedQueryTcpServer&
  operator=(const ReplicatedDistributedGroupedQueryTcpServer&) = delete;
  ReplicatedDistributedGroupedQueryTcpServer(ReplicatedDistributedGroupedQueryTcpServer&&) noexcept;
  ReplicatedDistributedGroupedQueryTcpServer&
  operator=(ReplicatedDistributedGroupedQueryTcpServer&&) noexcept;

  [[nodiscard]] static common::Result<ReplicatedDistributedGroupedQueryTcpServer>
  start(ReplicatedDistributedGroupedQueryTcpServerConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] cluster::DistributedGroupedQueryTcpServerMetrics metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit ReplicatedDistributedGroupedQueryTcpServer(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_GROUPED_QUERY_TCP_SERVER_HPP_
