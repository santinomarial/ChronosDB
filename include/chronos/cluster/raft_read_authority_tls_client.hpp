#ifndef CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TLS_CLIENT_HPP_
#define CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TLS_CLIENT_HPP_

#include "chronos/cluster/raft_read_authority_transport.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tls_socket.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct RaftReadAuthorityTlsClientLimits {
  std::chrono::milliseconds handshake_timeout{5000};
  std::chrono::milliseconds exchange_timeout{30000};
  RaftReadAuthorityTransportLimits transport{};
};

struct RaftReadAuthorityTlsClientConfig {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::array<std::uint8_t, 4U> peer_ipv4_address{};
  RaftReadAuthorityRequest request;
  RaftReadAuthorityTlsClientLimits limits;
};

enum class RaftReadAuthorityTlsClientState : std::uint8_t {
  kHandshaking = 1,
  kWritingRequest = 2,
  kReadingResponse = 3,
  kComplete = 4,
  kFailed = 5,
};

struct RaftReadAuthorityTlsInterest {
  bool want_read{};
  bool want_write{};
};

// Single-thread-affine owner for one exact authority exchange over an already-connected mTLS
// socket. The TLS context, descriptor, authenticator, and authorizer are borrowed and must outlive
// it; the caller closes the descriptor after destroying the client.
class RaftReadAuthorityTlsClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  RaftReadAuthorityTlsClient() = delete;
  ~RaftReadAuthorityTlsClient();
  RaftReadAuthorityTlsClient(const RaftReadAuthorityTlsClient&) = delete;
  RaftReadAuthorityTlsClient& operator=(const RaftReadAuthorityTlsClient&) = delete;
  RaftReadAuthorityTlsClient(RaftReadAuthorityTlsClient&&) noexcept;
  RaftReadAuthorityTlsClient& operator=(RaftReadAuthorityTlsClient&&) noexcept;

  [[nodiscard]] static common::Result<RaftReadAuthorityTlsClient>
  create(network::TlsSocket socket, RaftReadAuthorityTlsClientConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] RaftReadAuthorityTlsClientState state() const noexcept;
  [[nodiscard]] RaftReadAuthorityTlsInterest interest() const noexcept;
  [[nodiscard]] TimePoint deadline() const noexcept;
  [[nodiscard]] common::Result<RaftReadAuthority> result() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit RaftReadAuthorityTlsClient(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TLS_CLIENT_HPP_
