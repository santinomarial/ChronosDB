#ifndef CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_SERVER_V2_HPP_
#define CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_SERVER_V2_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_query_tcp_server_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"
#include "chronos/service/replicated_distributed_query_worker.hpp"

#include <chrono>
#include <cstddef>
#include <memory>

namespace chronos::service {

struct ReplicatedDistributedVectorGroupedAggregateQueryTcpServerConfigV2 {
  ReplicatedDistributedVectorGroupedAggregateQueryWorkerConfigV2 worker;
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  const cluster::ClusterNodePrincipalAuthorizer* node_authorizer{};
  const cluster::DistributedQueryLeaderHintProvider* leader_hint_provider{};
  cluster::DistributedVectorGroupedAggregateQueryTlsLimitsV2 carrier_limits;
  std::size_t maximum_connections{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

// Owns the production inbound grouped sufficient-state v2 stack in dependency order: proof-
// revalidating real-CSEG worker, authenticated receiver, then bounded TCP/mTLS server. One thread
// owns polling and shutdown. Borrowed storage, authority provider, authenticator, authorizer, and
// optional hint provider outlive it.
class ReplicatedDistributedVectorGroupedAggregateQueryTcpServerV2 {
public:
  ReplicatedDistributedVectorGroupedAggregateQueryTcpServerV2() = delete;
  ~ReplicatedDistributedVectorGroupedAggregateQueryTcpServerV2();
  ReplicatedDistributedVectorGroupedAggregateQueryTcpServerV2(
      const ReplicatedDistributedVectorGroupedAggregateQueryTcpServerV2&) = delete;
  ReplicatedDistributedVectorGroupedAggregateQueryTcpServerV2&
  operator=(const ReplicatedDistributedVectorGroupedAggregateQueryTcpServerV2&) = delete;
  ReplicatedDistributedVectorGroupedAggregateQueryTcpServerV2(
      ReplicatedDistributedVectorGroupedAggregateQueryTcpServerV2&&) noexcept;
  ReplicatedDistributedVectorGroupedAggregateQueryTcpServerV2&
  operator=(ReplicatedDistributedVectorGroupedAggregateQueryTcpServerV2&&) noexcept;

  [[nodiscard]] static common::Result<ReplicatedDistributedVectorGroupedAggregateQueryTcpServerV2>
  start(ReplicatedDistributedVectorGroupedAggregateQueryTcpServerConfigV2 config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] cluster::DistributedVectorGroupedAggregateQueryTcpServerMetricsV2
  metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit ReplicatedDistributedVectorGroupedAggregateQueryTcpServerV2(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_SERVER_V2_HPP_
