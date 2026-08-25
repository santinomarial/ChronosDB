#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_TCP_SERVER_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_TCP_SERVER_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tls.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct DistributedVectorGroupedAggregateShuffleTcpServerConfig {
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  const DistributedVectorGroupedAggregateShuffleAuthority* authority{};
  raft::NodeId local_node_id{};
  query::QueryResourceContext resources;
  DistributedVectorGroupedAggregateShuffleTlsLimits carrier_limits;
  std::size_t maximum_retained_streams{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

struct DistributedVectorGroupedAggregateShuffleTcpServerMetrics {
  std::uint64_t accepted_connections{};
  std::uint64_t rejected_connections{};
  std::uint64_t accept_errors{};
  std::uint64_t completed_connections{};
  std::uint64_t failed_connections{};
  std::size_t active_connections{};
  std::size_t retained_streams{};
};

// Owns a bounded POSIX listener and authenticated grouped-shuffle sessions. Each admitted
// connection reserves one preallocated completion slot before TLS begins, so a fully acknowledged
// stream cannot be lost to queue allocation failure. One thread owns all calls; borrowed security
// and authority dependencies outlive the server.
class DistributedVectorGroupedAggregateShuffleTcpServer {
public:
  DistributedVectorGroupedAggregateShuffleTcpServer() noexcept;
  ~DistributedVectorGroupedAggregateShuffleTcpServer();
  DistributedVectorGroupedAggregateShuffleTcpServer(
      const DistributedVectorGroupedAggregateShuffleTcpServer&) = delete;
  DistributedVectorGroupedAggregateShuffleTcpServer&
  operator=(const DistributedVectorGroupedAggregateShuffleTcpServer&) = delete;
  DistributedVectorGroupedAggregateShuffleTcpServer(
      DistributedVectorGroupedAggregateShuffleTcpServer&&) noexcept;
  DistributedVectorGroupedAggregateShuffleTcpServer&
  operator=(DistributedVectorGroupedAggregateShuffleTcpServer&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleTcpServer>
  start(DistributedVectorGroupedAggregateShuffleTcpServerConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleCompleteStream>
  take_next_complete_stream();
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleTcpServerMetrics metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleTcpServer(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_TCP_SERVER_HPP_
