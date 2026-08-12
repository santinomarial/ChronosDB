#ifndef CHRONOS_CLUSTER_RAFT_TRANSPORT_PEER_RECONNECT_HPP_
#define CHRONOS_CLUSTER_RAFT_TRANSPORT_PEER_RECONNECT_HPP_

#include "chronos/cluster/raft_transport_tcp_connector.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

namespace chronos::cluster {

struct RaftTransportPeerReconnectLimits {
  std::chrono::milliseconds initial_backoff{50};
  std::chrono::milliseconds maximum_backoff{5000};
};
struct RaftTransportPeerReconnectConfig {
  RaftTransportTcpConnectorConfig connector;
  RaftTransportPeerReconnectLimits limits;
};
enum class RaftTransportPeerReconnectState : std::uint8_t {
  kReady = 1,
  kConnecting = 2,
  kCarrierReady = 3,
  kConnected = 4,
  kBackoff = 5,
};

// Single-event-loop reconnect policy for one immutable node/address route. It retries forever with
// a capped delay and owns only complete duplicate-safe frames between connection attempts.
class RaftTransportPeerReconnect {
public:
  using TimePoint = RaftTransportTcpConnector::TimePoint;
  RaftTransportPeerReconnect() = delete;
  ~RaftTransportPeerReconnect();
  RaftTransportPeerReconnect(const RaftTransportPeerReconnect&) = delete;
  RaftTransportPeerReconnect& operator=(const RaftTransportPeerReconnect&) = delete;
  RaftTransportPeerReconnect(RaftTransportPeerReconnect&&) noexcept;
  RaftTransportPeerReconnect& operator=(RaftTransportPeerReconnect&&) noexcept;
  [[nodiscard]] static common::Result<RaftTransportPeerReconnect>
  create(RaftTransportPeerReconnectConfig config);
  [[nodiscard]] common::Status drive(TimePoint now);
  [[nodiscard]] common::Status on_ready(bool writable, TimePoint now);
  [[nodiscard]] common::Result<RaftTransportConnectedPeer> take_connected_peer();
  [[nodiscard]] common::Status accept_failed_peer(RaftTransportFailedPeer&& failed, TimePoint now);
  [[nodiscard]] RaftTransportPeerReconnectState state() const noexcept;
  [[nodiscard]] std::optional<TimePoint> next_attempt_not_before() const noexcept;
  [[nodiscard]] std::size_t attempts_started() const noexcept;
  [[nodiscard]] std::size_t retry_frame_count() const noexcept;
  [[nodiscard]] int descriptor() const noexcept;
  [[nodiscard]] bool wants_write() const noexcept;
  [[nodiscard]] const common::Status& last_failure() const noexcept;

private:
  class Impl;
  explicit RaftTransportPeerReconnect(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster
#endif // CHRONOS_CLUSTER_RAFT_TRANSPORT_PEER_RECONNECT_HPP_
