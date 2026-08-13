#ifndef CHRONOS_CLUSTER_RAFT_OBSERVATION_TLS_SERVER_HPP_
#define CHRONOS_CLUSTER_RAFT_OBSERVATION_TLS_SERVER_HPP_

#include "chronos/cluster/raft_observation_transport.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tls_socket.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct RaftObservationTlsServerLimits {
  std::chrono::milliseconds handshake_timeout{5000};
  std::chrono::milliseconds exchange_timeout{30000};
  RaftObservationTransportLimits transport;
};

struct RaftObservationTlsServerConfig {
  network::ConnectionAuthenticator* authenticator{};
  RaftObservationReceiver* receiver{};
  std::array<std::uint8_t, 4U> peer_ipv4_address{};
  RaftObservationTlsServerLimits limits;
};

enum class RaftObservationTlsServerState : std::uint8_t {
  kHandshaking = 1,
  kReadingRequest = 2,
  kWritingResponse = 3,
  kComplete = 4,
  kFailed = 5,
};

struct RaftObservationTlsServerInterest {
  bool want_read{};
  bool want_write{};
};

// Owns one authenticated observation exchange over an accepted nonblocking TLS socket. The TLS
// context, authenticator, receiver, and borrowed descriptor must outlive the session; one
// event-loop thread serializes every call.
class RaftObservationTlsServer {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  RaftObservationTlsServer() = delete;
  ~RaftObservationTlsServer();
  RaftObservationTlsServer(const RaftObservationTlsServer&) = delete;
  RaftObservationTlsServer& operator=(const RaftObservationTlsServer&) = delete;
  RaftObservationTlsServer(RaftObservationTlsServer&&) noexcept;
  RaftObservationTlsServer& operator=(RaftObservationTlsServer&&) noexcept;

  [[nodiscard]] static common::Result<RaftObservationTlsServer>
  create(network::TlsSocket socket, RaftObservationTlsServerConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);

  [[nodiscard]] RaftObservationTlsServerState state() const noexcept;
  [[nodiscard]] RaftObservationTlsServerInterest interest() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit RaftObservationTlsServer(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_OBSERVATION_TLS_SERVER_HPP_
