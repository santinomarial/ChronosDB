#ifndef CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_QUERY_CONTROL_TCP_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_QUERY_CONTROL_TCP_HPP_

#include "chronos/cluster/distributed_mutable_query_control_tls.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct DistributedMutableQueryControlTcpServerConfig {
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  DistributedMutableVectorQueryReceiver* mutable_receiver{};
  RaftReadAuthorityReceiver* read_authority_receiver{};
  DistributedMutableQueryControlTlsServerLimits carrier_limits;
  std::size_t maximum_connections{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

struct DistributedMutableQueryControlTcpServerMetrics {
  std::uint64_t accepted_connections{};
  std::uint64_t rejected_connections{};
  std::uint64_t accept_errors{};
  std::uint64_t completed_mutable_queries{};
  std::uint64_t completed_read_authorities{};
  std::uint64_t failed_connections{};
  std::size_t active_connections{};
};

// Bounded single-threaded listener for both authenticated private query-control protocols. TLS
// sessions are destroyed before their owning descriptors, and admission is finite.
class DistributedMutableQueryControlTcpServer {
public:
  DistributedMutableQueryControlTcpServer() noexcept;
  ~DistributedMutableQueryControlTcpServer();
  DistributedMutableQueryControlTcpServer(const DistributedMutableQueryControlTcpServer&) = delete;
  DistributedMutableQueryControlTcpServer&
  operator=(const DistributedMutableQueryControlTcpServer&) = delete;
  DistributedMutableQueryControlTcpServer(DistributedMutableQueryControlTcpServer&&) noexcept;
  DistributedMutableQueryControlTcpServer&
  operator=(DistributedMutableQueryControlTcpServer&&) noexcept;

  [[nodiscard]] static common::Result<DistributedMutableQueryControlTcpServer>
  start(DistributedMutableQueryControlTcpServerConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] DistributedMutableQueryControlTcpServerMetrics metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit DistributedMutableQueryControlTcpServer(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_QUERY_CONTROL_TCP_HPP_
