#ifndef CHRONOS_CLUSTER_RAFT_OBSERVATION_TLS_CLIENT_HPP_
#define CHRONOS_CLUSTER_RAFT_OBSERVATION_TLS_CLIENT_HPP_

#include "chronos/cluster/raft_observation_transport.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tls_socket.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct RaftObservationTlsClientLimits {
  std::chrono::milliseconds handshake_timeout{5000};
  std::chrono::milliseconds exchange_timeout{30000};
  RaftObservationTransportLimits transport{};
};

struct RaftObservationTlsClientConfig {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::array<std::uint8_t, 4U> peer_ipv4_address{};
  RaftObservationRequest request;
  RaftObservationTlsClientLimits limits;
};

enum class RaftObservationTlsClientState : std::uint8_t {
  kHandshaking = 1,
  kWritingRequest = 2,
  kReadingResponse = 3,
  kComplete = 4,
  kFailed = 5,
};

struct RaftObservationTlsInterest {
  bool want_read{};
  bool want_write{};
};

// Single-thread-affine owner for one exact observation exchange over an already-connected mTLS
// socket. Borrowed authentication dependencies and the TLS context must outlive this object; the
// caller owns the descriptor and closes it after destroying the client.
class RaftObservationTlsClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  RaftObservationTlsClient() = delete;
  ~RaftObservationTlsClient();
  RaftObservationTlsClient(const RaftObservationTlsClient&) = delete;
  RaftObservationTlsClient& operator=(const RaftObservationTlsClient&) = delete;
  RaftObservationTlsClient(RaftObservationTlsClient&&) noexcept;
  RaftObservationTlsClient& operator=(RaftObservationTlsClient&&) noexcept;

  [[nodiscard]] static common::Result<RaftObservationTlsClient>
  create(network::TlsSocket socket, RaftObservationTlsClientConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] RaftObservationTlsClientState state() const noexcept;
  [[nodiscard]] RaftObservationTlsInterest interest() const noexcept;
  [[nodiscard]] TimePoint deadline() const noexcept;
  [[nodiscard]] common::Result<raft::RaftGroupObservation> result() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit RaftObservationTlsClient(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_OBSERVATION_TLS_CLIENT_HPP_
