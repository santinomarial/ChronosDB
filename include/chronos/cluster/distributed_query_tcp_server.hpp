#ifndef CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TCP_SERVER_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TCP_SERVER_HPP_

#include "chronos/cluster/distributed_query_tls_server.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct DistributedQueryTcpServerConfig {
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  DistributedQueryReceiver* receiver{};
  DistributedQueryTlsServerLimits carrier_limits;
  std::size_t maximum_connections{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

struct DistributedQueryTcpServerMetrics {
  std::uint64_t accepted_connections{};
  std::uint64_t rejected_connections{};
  std::uint64_t accept_errors{};
  std::uint64_t completed_connections{};
  std::uint64_t failed_connections{};
  std::size_t active_connections{};
};

// Portable POSIX poll owner for the dedicated one-request-per-connection distributed-query trust
// domain. One thread owns all calls. The configured authenticator and receiver are borrowed and
// must outlive the server.
class DistributedQueryTcpServer {
public:
  DistributedQueryTcpServer() noexcept;
  ~DistributedQueryTcpServer();
  DistributedQueryTcpServer(const DistributedQueryTcpServer&) = delete;
  DistributedQueryTcpServer& operator=(const DistributedQueryTcpServer&) = delete;
  DistributedQueryTcpServer(DistributedQueryTcpServer&&) noexcept;
  DistributedQueryTcpServer& operator=(DistributedQueryTcpServer&&) noexcept;

  [[nodiscard]] static common::Result<DistributedQueryTcpServer>
  start(DistributedQueryTcpServerConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] DistributedQueryTcpServerMetrics metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit DistributedQueryTcpServer(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TCP_SERVER_HPP_
