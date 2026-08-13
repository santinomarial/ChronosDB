#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_TCP_SERVER_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_TCP_SERVER_V2_HPP_

#include "chronos/cluster/distributed_vector_aggregate_query_tls_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct DistributedVectorAggregateQueryTcpServerConfigV2 {
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  DistributedVectorAggregateQueryReceiverV2* receiver{};
  DistributedVectorAggregateQueryTlsLimitsV2 carrier_limits;
  std::size_t maximum_connections{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

struct DistributedVectorAggregateQueryTcpServerMetricsV2 {
  std::uint64_t accepted_connections{};
  std::uint64_t rejected_connections{};
  std::uint64_t accept_errors{};
  std::uint64_t completed_connections{};
  std::uint64_t failed_connections{};
  std::size_t active_connections{};
};

// Portable POSIX poll owner for one definition-bound aggregate request and complete state vector
// per connection. One thread owns all calls. The authenticator and receiver are borrowed and
// outlive it.
class DistributedVectorAggregateQueryTcpServerV2 {
public:
  DistributedVectorAggregateQueryTcpServerV2() noexcept;
  ~DistributedVectorAggregateQueryTcpServerV2();
  DistributedVectorAggregateQueryTcpServerV2(const DistributedVectorAggregateQueryTcpServerV2&) =
      delete;
  DistributedVectorAggregateQueryTcpServerV2&
  operator=(const DistributedVectorAggregateQueryTcpServerV2&) = delete;
  DistributedVectorAggregateQueryTcpServerV2(DistributedVectorAggregateQueryTcpServerV2&&) noexcept;
  DistributedVectorAggregateQueryTcpServerV2&
  operator=(DistributedVectorAggregateQueryTcpServerV2&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorAggregateQueryTcpServerV2>
  start(DistributedVectorAggregateQueryTcpServerConfigV2 config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] DistributedVectorAggregateQueryTcpServerMetricsV2 metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit DistributedVectorAggregateQueryTcpServerV2(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_TCP_SERVER_V2_HPP_
