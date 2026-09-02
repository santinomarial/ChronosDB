#ifndef CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TLS_SERVER_HPP_
#define CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TLS_SERVER_HPP_

#include "chronos/cluster/raft_read_authority_transport.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tls_socket.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct RaftReadAuthorityTlsServerLimits {
  std::chrono::milliseconds handshake_timeout{5000};
  std::chrono::milliseconds exchange_timeout{30000};
  RaftReadAuthorityTransportLimits transport{};
};

struct RaftReadAuthorityTlsServerConfig {
  network::ConnectionAuthenticator* authenticator{};
  RaftReadAuthorityReceiver* receiver{};
  std::array<std::uint8_t, 4U> peer_ipv4_address{};
  RaftReadAuthorityTlsServerLimits limits;
};

enum class RaftReadAuthorityTlsServerState : std::uint8_t {
  kHandshaking = 1,
  kReadingRequest = 2,
  kWritingResponse = 3,
  kComplete = 4,
  kFailed = 5,
};

struct RaftReadAuthorityTlsServerInterest {
  bool want_read{};
  bool want_write{};
};

// Single-thread-affine owner for one authenticated authority exchange over an accepted TLS socket.
// The TLS context, descriptor, authenticator, and receiver are borrowed and must outlive it.
class RaftReadAuthorityTlsServer {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  RaftReadAuthorityTlsServer() = delete;
  ~RaftReadAuthorityTlsServer();
  RaftReadAuthorityTlsServer(const RaftReadAuthorityTlsServer&) = delete;
  RaftReadAuthorityTlsServer& operator=(const RaftReadAuthorityTlsServer&) = delete;
  RaftReadAuthorityTlsServer(RaftReadAuthorityTlsServer&&) noexcept;
  RaftReadAuthorityTlsServer& operator=(RaftReadAuthorityTlsServer&&) noexcept;

  [[nodiscard]] static common::Result<RaftReadAuthorityTlsServer>
  create(network::TlsSocket socket, RaftReadAuthorityTlsServerConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] RaftReadAuthorityTlsServerState state() const noexcept;
  [[nodiscard]] RaftReadAuthorityTlsServerInterest interest() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit RaftReadAuthorityTlsServer(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TLS_SERVER_HPP_
