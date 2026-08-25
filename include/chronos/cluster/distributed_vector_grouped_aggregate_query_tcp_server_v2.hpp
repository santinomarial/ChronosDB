#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_SERVER_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_SERVER_V2_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_query_tls_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct DistributedVectorGroupedAggregateQueryTcpServerConfigV2 {
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  DistributedVectorGroupedAggregateQueryReceiverV2* receiver{};
  DistributedVectorGroupedAggregateQueryTlsLimitsV2 carrier_limits;
  std::size_t maximum_connections{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

struct DistributedVectorGroupedAggregateQueryTcpServerMetricsV2 {
  std::uint64_t accepted_connections{};
  std::uint64_t rejected_connections{};
  std::uint64_t accept_errors{};
  std::uint64_t completed_connections{};
  std::uint64_t failed_connections{};
  std::size_t active_connections{};
};

// Portable POSIX poll owner for one grouped sufficient-state request and one complete empty-or-
// contiguous group stream per connection. One thread owns all calls. The authenticator and receiver
// are borrowed and outlive it.
class DistributedVectorGroupedAggregateQueryTcpServerV2 {
public:
  DistributedVectorGroupedAggregateQueryTcpServerV2() noexcept;
  ~DistributedVectorGroupedAggregateQueryTcpServerV2();
  DistributedVectorGroupedAggregateQueryTcpServerV2(
      const DistributedVectorGroupedAggregateQueryTcpServerV2&) = delete;
  DistributedVectorGroupedAggregateQueryTcpServerV2&
  operator=(const DistributedVectorGroupedAggregateQueryTcpServerV2&) = delete;
  DistributedVectorGroupedAggregateQueryTcpServerV2(
      DistributedVectorGroupedAggregateQueryTcpServerV2&&) noexcept;
  DistributedVectorGroupedAggregateQueryTcpServerV2&
  operator=(DistributedVectorGroupedAggregateQueryTcpServerV2&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateQueryTcpServerV2>
  start(DistributedVectorGroupedAggregateQueryTcpServerConfigV2 config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateQueryTcpServerMetricsV2 metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateQueryTcpServerV2(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_SERVER_V2_HPP_
