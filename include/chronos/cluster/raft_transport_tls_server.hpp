#ifndef CHRONOS_CLUSTER_RAFT_TRANSPORT_TLS_SERVER_HPP_
#define CHRONOS_CLUSTER_RAFT_TRANSPORT_TLS_SERVER_HPP_

#include "chronos/cluster/raft_transport_receiver.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tls_socket.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>

namespace chronos::cluster {

struct RaftTransportTlsServerLimits {
  std::chrono::milliseconds handshake_timeout{5000};
  std::chrono::milliseconds frame_read_timeout{30000};
};

struct RaftTransportTlsServerConfig {
  network::ConnectionAuthenticator* authenticator{};
  RaftTransportReceiver* receiver{};
  std::array<std::uint8_t, 4> peer_ipv4_address{};
  RaftTransportTlsServerLimits limits;
  raft::RaftTransportCodecLimits codec_limits;
};

enum class RaftTransportTlsServerState : std::uint8_t {
  kHandshaking = 1,
  kReadingFrame = 2,
  kAwaitingDurableResult = 3,
  kResultReady = 4,
  kFailed = 5,
};

struct RaftTransportTlsServerInterest {
  bool want_read{};
  bool want_write{};
};

struct RaftTransportCompletedReceive {
  std::uint64_t submission_sequence{};
  raft::GroupId group_id;
  raft::NodeId source_node_id{};
  raft::DurableRaftResult result;
  std::optional<raft::RaftGroupObservation> observation;
};

// Owns one persistent inbound Raft byte stream over a borrowed nonblocking mutual-TLS socket. One
// event-loop thread serializes calls. At most one frame is being read, durably executed, or waiting
// for embedding pickup. Durable-result wait has no deadline because accepted work cannot be
// cancelled safely; connection/read backpressure remains bounded to one operation.
class RaftTransportTlsServer {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  RaftTransportTlsServer() = delete;
  ~RaftTransportTlsServer();
  RaftTransportTlsServer(const RaftTransportTlsServer&) = delete;
  RaftTransportTlsServer& operator=(const RaftTransportTlsServer&) = delete;
  RaftTransportTlsServer(RaftTransportTlsServer&&) noexcept;
  RaftTransportTlsServer& operator=(RaftTransportTlsServer&&) noexcept;

  [[nodiscard]] static common::Result<RaftTransportTlsServer>
  create(network::TlsSocket socket, RaftTransportTlsServerConfig config, TimePoint now);

  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] common::Result<RaftTransportCompletedReceive> take_completed(TimePoint now);
  [[nodiscard]] RaftTransportTlsServerState state() const noexcept;
  [[nodiscard]] RaftTransportTlsServerInterest interest() const noexcept;
  [[nodiscard]] std::optional<TimePoint> next_deadline() const noexcept;
  [[nodiscard]] std::optional<std::uint64_t> outstanding_submission_sequence() const noexcept;
  [[nodiscard]] std::optional<std::uint64_t> completed_submission_sequence() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit RaftTransportTlsServer(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_TRANSPORT_TLS_SERVER_HPP_
