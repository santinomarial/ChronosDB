#ifndef CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_VECTOR_QUERY_TCP_SERVER_V2_HPP_
#define CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_VECTOR_QUERY_TCP_SERVER_V2_HPP_

#include "chronos/cluster/distributed_vector_query_tcp_server_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"
#include "chronos/service/replicated_distributed_query_worker.hpp"

#include <chrono>
#include <cstddef>
#include <memory>

namespace chronos::service {

struct ReplicatedDistributedVectorQueryTcpServerConfigV2 {
  ReplicatedDistributedVectorQueryWorkerConfigV2 worker;
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  const cluster::ClusterNodePrincipalAuthorizer* node_authorizer{};
  const cluster::DistributedQueryLeaderHintProvider* leader_hint_provider{};
  cluster::DistributedVectorQueryTlsLimitsV2 carrier_limits;
  std::size_t maximum_connections{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

// Owns the production inbound vector-v2 stack in dependency order: proof-revalidating real-CSEG
// worker, authenticated schema-bound receiver, then bounded TCP/mTLS server. One thread owns
// polling and shutdown. Borrowed storage, authority provider, authenticator, authorizer, and
// optional hint provider must outlive it.
class ReplicatedDistributedVectorQueryTcpServerV2 {
public:
  ReplicatedDistributedVectorQueryTcpServerV2() = delete;
  ~ReplicatedDistributedVectorQueryTcpServerV2();
  ReplicatedDistributedVectorQueryTcpServerV2(const ReplicatedDistributedVectorQueryTcpServerV2&) =
      delete;
  ReplicatedDistributedVectorQueryTcpServerV2&
  operator=(const ReplicatedDistributedVectorQueryTcpServerV2&) = delete;
  ReplicatedDistributedVectorQueryTcpServerV2(
      ReplicatedDistributedVectorQueryTcpServerV2&&) noexcept;
  ReplicatedDistributedVectorQueryTcpServerV2&
  operator=(ReplicatedDistributedVectorQueryTcpServerV2&&) noexcept;

  [[nodiscard]] static common::Result<ReplicatedDistributedVectorQueryTcpServerV2>
  start(ReplicatedDistributedVectorQueryTcpServerConfigV2 config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] cluster::DistributedVectorQueryTcpServerMetricsV2 metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit ReplicatedDistributedVectorQueryTcpServerV2(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_VECTOR_QUERY_TCP_SERVER_V2_HPP_
