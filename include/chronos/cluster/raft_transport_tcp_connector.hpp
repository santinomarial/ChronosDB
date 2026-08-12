#ifndef CHRONOS_CLUSTER_RAFT_TRANSPORT_TCP_CONNECTOR_HPP_
#define CHRONOS_CLUSTER_RAFT_TRANSPORT_TCP_CONNECTOR_HPP_

#include "chronos/cluster/raft_transport_peer_pool.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::cluster {

struct RaftTransportTcpConnectorConfig {
  network::Ipv4Endpoint remote_endpoint;
  const network::TlsClientContext* tls_context{};
  RaftTransportTlsClientConfig carrier;
  std::chrono::milliseconds connect_timeout{5000};
};

enum class RaftTransportTcpConnectorState : std::uint8_t {
  kConnecting = 1,
  kCarrierReady = 2,
  kFailed = 3,
  kTaken = 4,
};

// One exact-peer nonblocking TCP attempt. Complete retry frames remain owned during connect and
// move atomically into the new TLS carrier before its descriptor/carrier pair can be taken.
class RaftTransportTcpConnector {
public:
  using TimePoint = RaftTransportTlsClient::TimePoint;
  RaftTransportTcpConnector() = delete;
  ~RaftTransportTcpConnector();
  RaftTransportTcpConnector(const RaftTransportTcpConnector&) = delete;
  RaftTransportTcpConnector& operator=(const RaftTransportTcpConnector&) = delete;
  RaftTransportTcpConnector(RaftTransportTcpConnector&&) noexcept;
  RaftTransportTcpConnector& operator=(RaftTransportTcpConnector&&) noexcept;

  [[nodiscard]] static common::Result<RaftTransportTcpConnector>
  begin(std::vector<std::vector<std::byte>>&& retry_frames, RaftTransportTcpConnectorConfig config,
        TimePoint now);
  [[nodiscard]] static common::Status validate_config(RaftTransportTcpConnectorConfig config);
  [[nodiscard]] common::Status on_ready(bool writable, TimePoint now);
  [[nodiscard]] common::Result<RaftTransportConnectedPeer> take_connected_peer();
  [[nodiscard]] common::Result<std::vector<std::vector<std::byte>>> take_retry_frames();
  [[nodiscard]] RaftTransportTcpConnectorState state() const noexcept;
  [[nodiscard]] bool wants_write() const noexcept;
  [[nodiscard]] int descriptor() const noexcept;
  [[nodiscard]] std::size_t retry_frame_count() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit RaftTransportTcpConnector(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_TRANSPORT_TCP_CONNECTOR_HPP_
