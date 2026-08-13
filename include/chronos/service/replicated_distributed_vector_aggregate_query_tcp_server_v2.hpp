#ifndef CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_TCP_SERVER_V2_HPP_
#define CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_TCP_SERVER_V2_HPP_

#include "chronos/cluster/distributed_vector_aggregate_query_tcp_server_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"
#include "chronos/service/replicated_distributed_query_worker.hpp"

#include <chrono>
#include <cstddef>
#include <memory>

namespace chronos::service {

struct ReplicatedDistributedVectorAggregateQueryTcpServerConfigV2 {
  ReplicatedDistributedVectorAggregateQueryWorkerConfigV2 worker;
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  const cluster::ClusterNodePrincipalAuthorizer* node_authorizer{};
  const cluster::DistributedQueryLeaderHintProvider* leader_hint_provider{};
  cluster::DistributedVectorAggregateQueryTlsLimitsV2 carrier_limits;
  std::size_t maximum_connections{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

// Owns the production inbound aggregate-v2 stack in dependency order: proof-revalidating real-CSEG
// worker, authenticated definition-bound receiver, then bounded TCP/mTLS server. One thread owns
// polling and shutdown. Borrowed storage, authority provider, authenticator, authorizer, and
// optional hint provider outlive it.
class ReplicatedDistributedVectorAggregateQueryTcpServerV2 {
public:
  ReplicatedDistributedVectorAggregateQueryTcpServerV2() = delete;
  ~ReplicatedDistributedVectorAggregateQueryTcpServerV2();
  ReplicatedDistributedVectorAggregateQueryTcpServerV2(
      const ReplicatedDistributedVectorAggregateQueryTcpServerV2&) = delete;
  ReplicatedDistributedVectorAggregateQueryTcpServerV2&
  operator=(const ReplicatedDistributedVectorAggregateQueryTcpServerV2&) = delete;
  ReplicatedDistributedVectorAggregateQueryTcpServerV2(
      ReplicatedDistributedVectorAggregateQueryTcpServerV2&&) noexcept;
  ReplicatedDistributedVectorAggregateQueryTcpServerV2&
  operator=(ReplicatedDistributedVectorAggregateQueryTcpServerV2&&) noexcept;

  [[nodiscard]] static common::Result<ReplicatedDistributedVectorAggregateQueryTcpServerV2>
  start(ReplicatedDistributedVectorAggregateQueryTcpServerConfigV2 config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] cluster::DistributedVectorAggregateQueryTcpServerMetricsV2 metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit ReplicatedDistributedVectorAggregateQueryTcpServerV2(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_TCP_SERVER_V2_HPP_
