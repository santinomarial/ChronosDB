#ifndef CHRONOS_CLUSTER_RAFT_OBSERVATION_TCP_SERVER_HPP_
#define CHRONOS_CLUSTER_RAFT_OBSERVATION_TCP_SERVER_HPP_

#include "chronos/cluster/raft_observation_tls_server.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct RaftObservationTcpServerConfig {
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  RaftObservationReceiver* receiver{};
  RaftObservationTlsServerLimits session_limits;
  std::size_t maximum_connections{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

struct RaftObservationTcpServerMetrics {
  std::uint64_t accepted_connections{};
  std::uint64_t rejected_connections{};
  std::uint64_t accept_errors{};
  std::uint64_t completed_connections{};
  std::uint64_t failed_connections{};
  std::size_t active_connections{};
};

// Single-threaded POSIX poll owner for the one-request-per-connection observation trust domain.
// The configured authenticator and receiver are borrowed and must outlive the server.
class RaftObservationTcpServer {
public:
  RaftObservationTcpServer() noexcept;
  ~RaftObservationTcpServer();
  RaftObservationTcpServer(const RaftObservationTcpServer&) = delete;
  RaftObservationTcpServer& operator=(const RaftObservationTcpServer&) = delete;
  RaftObservationTcpServer(RaftObservationTcpServer&&) noexcept;
  RaftObservationTcpServer& operator=(RaftObservationTcpServer&&) noexcept;

  [[nodiscard]] static common::Result<RaftObservationTcpServer>
  start(RaftObservationTcpServerConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] RaftObservationTcpServerMetrics metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit RaftObservationTcpServer(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_OBSERVATION_TCP_SERVER_HPP_
