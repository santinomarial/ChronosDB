#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TCP_SERVER_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TCP_SERVER_V2_HPP_

#include "chronos/cluster/distributed_vector_query_tls_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct DistributedVectorQueryTcpServerConfigV2 {
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  DistributedVectorQueryReceiverV2* receiver{};
  DistributedVectorQueryTlsLimitsV2 carrier_limits;
  std::size_t maximum_connections{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

struct DistributedVectorQueryTcpServerMetricsV2 {
  std::uint64_t accepted_connections{};
  std::uint64_t rejected_connections{};
  std::uint64_t accept_errors{};
  std::uint64_t completed_connections{};
  std::uint64_t failed_connections{};
  std::size_t active_connections{};
};

// Portable POSIX poll owner for one schema-bound vector request and its bounded response stream per
// connection. One thread owns all calls. The authenticator and receiver are borrowed and must
// outlive it.
class DistributedVectorQueryTcpServerV2 {
public:
  DistributedVectorQueryTcpServerV2() noexcept;
  ~DistributedVectorQueryTcpServerV2();
  DistributedVectorQueryTcpServerV2(const DistributedVectorQueryTcpServerV2&) = delete;
  DistributedVectorQueryTcpServerV2& operator=(const DistributedVectorQueryTcpServerV2&) = delete;
  DistributedVectorQueryTcpServerV2(DistributedVectorQueryTcpServerV2&&) noexcept;
  DistributedVectorQueryTcpServerV2& operator=(DistributedVectorQueryTcpServerV2&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorQueryTcpServerV2>
  start(DistributedVectorQueryTcpServerConfigV2 config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] DistributedVectorQueryTcpServerMetricsV2 metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit DistributedVectorQueryTcpServerV2(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TCP_SERVER_V2_HPP_
