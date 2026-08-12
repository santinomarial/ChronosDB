#ifndef CHRONOS_CLUSTER_RAFT_TRANSPORT_TCP_SERVER_HPP_
#define CHRONOS_CLUSTER_RAFT_TRANSPORT_TCP_SERVER_HPP_

#include "chronos/cluster/raft_transport_tls_server.hpp"
#include "chronos/network/tcp_socket.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

namespace chronos::cluster {

struct RaftTransportTcpServerConfig {
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  RaftTransportReceiver* receiver{};
  RaftTransportTlsServerLimits carrier_limits;
  raft::RaftTransportCodecLimits codec_limits;
  std::size_t maximum_connections{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};
struct RaftTransportTcpServerMetrics {
  std::uint64_t accepted_connections{};
  std::uint64_t rejected_connections{};
  std::uint64_t accept_errors{};
  std::uint64_t failed_connections{};
  std::uint64_t completed_results{};
  std::size_t active_connections{};
};

// Portable bounded poll owner for persistent inbound Raft TLS sessions. Result-ready sessions stay
// admitted and backpressured until the embedding takes their complete post-sync transition.
class RaftTransportTcpServer {
public:
  RaftTransportTcpServer() noexcept;
  ~RaftTransportTcpServer();
  RaftTransportTcpServer(const RaftTransportTcpServer&) = delete;
  RaftTransportTcpServer& operator=(const RaftTransportTcpServer&) = delete;
  RaftTransportTcpServer(RaftTransportTcpServer&&) noexcept;
  RaftTransportTcpServer& operator=(RaftTransportTcpServer&&) noexcept;
  [[nodiscard]] static common::Result<RaftTransportTcpServer>
  start(RaftTransportTcpServerConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Result<std::optional<RaftTransportCompletedReceive>> take_completed();
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] RaftTransportTcpServerMetrics metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit RaftTransportTcpServer(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster
#endif // CHRONOS_CLUSTER_RAFT_TRANSPORT_TCP_SERVER_HPP_
