#ifndef CHRONOS_NETWORK_NATIVE_QUORUM_INGEST_TCP_CLIENT_HPP_
#define CHRONOS_NETWORK_NATIVE_QUORUM_INGEST_TCP_CLIENT_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/native_node_principal_authorizer.hpp"
#include "chronos/network/native_quorum_ingest_retry.hpp"
#include "chronos/network/security.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::network {

struct NativeQuorumIngestTcpClientLimits {
  std::chrono::milliseconds connect_timeout{5000};
  std::chrono::milliseconds handshake_timeout{5000};
  std::chrono::milliseconds exchange_timeout{30000};
  std::size_t maximum_io_chunk_bytes{std::size_t{64U} * 1024U};
};

struct NativeQuorumIngestTcpClientConfig {
  NativeQuorumIngestRetryConfig retry;
  ConnectionAuthenticator* authenticator{};
  const NativeNodePrincipalAuthorizer* node_authorizer{};
  NativeQuorumIngestTcpClientLimits limits;
};

enum class NativeQuorumIngestTcpClientState : std::uint8_t {
  kConnecting = 1,
  kHandshaking = 2,
  kExchanging = 3,
  kComplete = 4,
  kFailed = 5,
};

struct NativeQuorumIngestTcpInterest {
  bool want_read{};
  bool want_write{};
};

// Owns the nonblocking TCP/TLS carrier for one exact redirected QUORUM_SYNC operation. One
// event-loop thread serializes calls. Route TLS contexts, the principal authenticator, and the node
// authorizer are borrowed and must outlive the client. Only an authenticated protocol redirect can
// start another connection; ambiguous transport failures are terminal.
class NativeQuorumIngestTcpClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  NativeQuorumIngestTcpClient() = delete;
  ~NativeQuorumIngestTcpClient();
  NativeQuorumIngestTcpClient(const NativeQuorumIngestTcpClient&) = delete;
  NativeQuorumIngestTcpClient& operator=(const NativeQuorumIngestTcpClient&) = delete;
  NativeQuorumIngestTcpClient(NativeQuorumIngestTcpClient&&) noexcept;
  NativeQuorumIngestTcpClient& operator=(NativeQuorumIngestTcpClient&&) noexcept;

  [[nodiscard]] static common::Result<NativeQuorumIngestTcpClient>
  begin(NativeQuorumIngestTcpClientConfig config, std::vector<std::byte> encoded_columnar_append,
        TimePoint now);

  // Advances at most one connect, TLS, read, or write operation. Calling with neither readiness
  // is permitted and applies the active phase deadline. Failures are sticky.
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);

  [[nodiscard]] NativeQuorumIngestTcpClientState state() const noexcept;
  [[nodiscard]] NativeQuorumIngestTcpInterest interest() const noexcept;
  [[nodiscard]] int descriptor() const noexcept;
  [[nodiscard]] std::optional<TimePoint> deadline() const noexcept;
  [[nodiscard]] NativeLeaderRoute current_route() const noexcept;
  [[nodiscard]] std::size_t attempts_started() const noexcept;
  [[nodiscard]] common::Result<QuorumSyncIngestAcknowledgement> result() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit NativeQuorumIngestTcpClient(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::network

#endif // CHRONOS_NETWORK_NATIVE_QUORUM_INGEST_TCP_CLIENT_HPP_
