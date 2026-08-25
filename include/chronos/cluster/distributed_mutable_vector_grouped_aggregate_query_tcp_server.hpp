#ifndef CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_SERVER_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_SERVER_HPP_

#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tls.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct DistributedMutableVectorGroupedAggregateQueryTcpServerConfig {
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  DistributedMutableVectorGroupedAggregateQueryReceiver* receiver{};
  DistributedMutableVectorGroupedAggregateQueryTlsLimits carrier_limits;
  std::size_t maximum_connections{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

struct DistributedMutableVectorGroupedAggregateQueryTcpServerMetrics {
  std::uint64_t accepted_connections{};
  std::uint64_t rejected_connections{};
  std::uint64_t accept_errors{};
  std::uint64_t completed_connections{};
  std::uint64_t failed_connections{};
  std::size_t active_connections{};
};

// Portable POSIX poll owner for one mutable grouped sufficient-state request and one complete
// empty-or-contiguous group stream per connection. One thread owns all calls. The authenticator
// and receiver are borrowed and outlive it.
class DistributedMutableVectorGroupedAggregateQueryTcpServer {
public:
  DistributedMutableVectorGroupedAggregateQueryTcpServer() noexcept;
  ~DistributedMutableVectorGroupedAggregateQueryTcpServer();
  DistributedMutableVectorGroupedAggregateQueryTcpServer(
      const DistributedMutableVectorGroupedAggregateQueryTcpServer&) = delete;
  DistributedMutableVectorGroupedAggregateQueryTcpServer&
  operator=(const DistributedMutableVectorGroupedAggregateQueryTcpServer&) = delete;
  DistributedMutableVectorGroupedAggregateQueryTcpServer(
      DistributedMutableVectorGroupedAggregateQueryTcpServer&&) noexcept;
  DistributedMutableVectorGroupedAggregateQueryTcpServer&
  operator=(DistributedMutableVectorGroupedAggregateQueryTcpServer&&) noexcept;

  [[nodiscard]] static common::Result<DistributedMutableVectorGroupedAggregateQueryTcpServer>
  start(DistributedMutableVectorGroupedAggregateQueryTcpServerConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] DistributedMutableVectorGroupedAggregateQueryTcpServerMetrics
  metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit DistributedMutableVectorGroupedAggregateQueryTcpServer(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_SERVER_HPP_
