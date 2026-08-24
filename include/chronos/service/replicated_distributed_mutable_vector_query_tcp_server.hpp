#ifndef CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_MUTABLE_VECTOR_QUERY_TCP_SERVER_HPP_
#define CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_MUTABLE_VECTOR_QUERY_TCP_SERVER_HPP_

#include "chronos/cluster/distributed_mutable_vector_query_tcp.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"
#include "chronos/service/replicated_distributed_query_worker.hpp"

#include <chrono>
#include <cstddef>
#include <memory>

namespace chronos::service {

struct ReplicatedDistributedMutableVectorQueryTcpServerConfig {
  ReplicatedDistributedMutableVectorQueryWorkerConfig worker;
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  const cluster::ClusterNodePrincipalAuthorizer* node_authorizer{};
  const cluster::DistributedQueryLeaderHintProvider* leader_hint_provider{};
  cluster::DistributedMutableVectorQueryTlsLimits carrier_limits;
  std::size_t maximum_connections{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

// Owns the production inbound mutable-query stack in dependency order: request-local TabletState
// worker, authenticated receiver, then bounded TCP/mTLS server. One thread owns polling/shutdown.
// Borrowed authority, authentication, authorization, and optional hint providers must outlive it.
class ReplicatedDistributedMutableVectorQueryTcpServer {
public:
  ReplicatedDistributedMutableVectorQueryTcpServer() = delete;
  ~ReplicatedDistributedMutableVectorQueryTcpServer();
  ReplicatedDistributedMutableVectorQueryTcpServer(
      const ReplicatedDistributedMutableVectorQueryTcpServer&) = delete;
  ReplicatedDistributedMutableVectorQueryTcpServer&
  operator=(const ReplicatedDistributedMutableVectorQueryTcpServer&) = delete;
  ReplicatedDistributedMutableVectorQueryTcpServer(
      ReplicatedDistributedMutableVectorQueryTcpServer&&) noexcept;
  ReplicatedDistributedMutableVectorQueryTcpServer&
  operator=(ReplicatedDistributedMutableVectorQueryTcpServer&&) noexcept;

  [[nodiscard]] static common::Result<ReplicatedDistributedMutableVectorQueryTcpServer>
  start(ReplicatedDistributedMutableVectorQueryTcpServerConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] cluster::DistributedMutableVectorQueryTcpServerMetrics metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit ReplicatedDistributedMutableVectorQueryTcpServer(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_MUTABLE_VECTOR_QUERY_TCP_SERVER_HPP_
