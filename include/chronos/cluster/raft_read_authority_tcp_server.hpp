#ifndef CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TCP_SERVER_HPP_
#define CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TCP_SERVER_HPP_

#include "chronos/cluster/raft_read_authority_tls_server.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct RaftReadAuthorityTcpServerConfig {
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  RaftReadAuthorityReceiver* receiver{};
  RaftReadAuthorityTlsServerLimits session_limits;
  std::size_t maximum_connections{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

struct RaftReadAuthorityTcpServerMetrics {
  std::uint64_t accepted_connections{};
  std::uint64_t rejected_connections{};
  std::uint64_t accept_errors{};
  std::uint64_t completed_connections{};
  std::uint64_t failed_connections{};
  std::size_t active_connections{};
};

// Single-threaded POSIX poll owner for the one-request-per-connection authority trust domain. The
// configured authenticator and receiver are borrowed and must outlive the server.
class RaftReadAuthorityTcpServer {
public:
  RaftReadAuthorityTcpServer() noexcept;
  ~RaftReadAuthorityTcpServer();
  RaftReadAuthorityTcpServer(const RaftReadAuthorityTcpServer&) = delete;
  RaftReadAuthorityTcpServer& operator=(const RaftReadAuthorityTcpServer&) = delete;
  RaftReadAuthorityTcpServer(RaftReadAuthorityTcpServer&&) noexcept;
  RaftReadAuthorityTcpServer& operator=(RaftReadAuthorityTcpServer&&) noexcept;

  [[nodiscard]] static common::Result<RaftReadAuthorityTcpServer>
  start(RaftReadAuthorityTcpServerConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] RaftReadAuthorityTcpServerMetrics metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit RaftReadAuthorityTcpServer(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TCP_SERVER_HPP_
