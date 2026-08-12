#ifndef CHRONOS_CLUSTER_RAFT_TRANSPORT_TLS_CLIENT_HPP_
#define CHRONOS_CLUSTER_RAFT_TRANSPORT_TLS_CLIENT_HPP_

#include "chronos/cluster/remote_tablet_reconfiguration.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tls_socket.hpp"
#include "chronos/raft/transport_codec.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::cluster {

struct RaftTransportTlsClientLimits {
  std::size_t maximum_queued_frames{1024U};
  std::size_t maximum_queued_bytes{64U * 1024U * 1024U};
  std::chrono::milliseconds handshake_timeout{5000};
  std::chrono::milliseconds frame_write_timeout{30000};
  raft::RaftTransportCodecLimits codec;
};

struct RaftTransportTlsClientConfig {
  raft::NodeId local_node_id{};
  raft::NodeId peer_node_id{};
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::array<std::uint8_t, 4> peer_ipv4_address{};
  RaftTransportTlsClientLimits limits;
};

enum class RaftTransportTlsClientState : std::uint8_t {
  kHandshaking = 1,
  kReady = 2,
  kFailed = 3,
};

struct RaftTransportTlsClientInterest {
  bool want_read{};
  bool want_write{};
};

// Persistent single-thread-affine outbound connection to one exact authenticated Raft peer. Queue
// storage is allocated at create(), and try_enqueue() moves bytes only after validation/admission.
// drain_retry_frames() returns complete original frames, including a partially written front frame,
// so reconnect retry is duplicate-safe rather than suffix-only.
class RaftTransportTlsClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  RaftTransportTlsClient() = delete;
  ~RaftTransportTlsClient();
  RaftTransportTlsClient(const RaftTransportTlsClient&) = delete;
  RaftTransportTlsClient& operator=(const RaftTransportTlsClient&) = delete;
  RaftTransportTlsClient(RaftTransportTlsClient&&) noexcept;
  RaftTransportTlsClient& operator=(RaftTransportTlsClient&&) noexcept;

  [[nodiscard]] static common::Result<RaftTransportTlsClient>
  create(network::TlsSocket socket, RaftTransportTlsClientConfig config, TimePoint now);

  [[nodiscard]] common::Status try_enqueue(std::vector<std::byte>& encoded_frame, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] common::Result<std::vector<std::vector<std::byte>>> drain_retry_frames();
  [[nodiscard]] RaftTransportTlsClientState state() const noexcept;
  [[nodiscard]] RaftTransportTlsClientInterest interest() const noexcept;
  [[nodiscard]] std::size_t queued_frames() const noexcept;
  [[nodiscard]] std::size_t queued_bytes() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit RaftTransportTlsClient(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_TRANSPORT_TLS_CLIENT_HPP_
