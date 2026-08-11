#ifndef CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TLS_CLIENT_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TLS_CLIENT_HPP_

#include "chronos/cluster/distributed_query_transport.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tls_socket.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct DistributedQueryTlsClientLimits {
  std::chrono::milliseconds handshake_timeout{5000};
  std::chrono::milliseconds exchange_timeout{30000};
};

struct DistributedQueryTlsClientConfig {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::array<std::uint8_t, 4> peer_ipv4_address{};
  DistributedQueryTlsClientLimits limits;
};

enum class DistributedQueryTlsClientState : std::uint8_t {
  kHandshaking = 1,
  kWritingRequest = 2,
  kReadingResponse = 3,
  kComplete = 4,
  kFailed = 5,
};

struct DistributedQueryTlsInterest {
  bool want_read{};
  bool want_write{};
};

// Owns one outbound distributed-query attempt over one already-connected nonblocking TLS socket.
// A single event-loop thread must serialize calls. The authenticator and node authorizer are
// borrowed and must outlive the carrier. The caller continues to own and close the descriptor
// borrowed by TlsSocket after destroying this object, and the TlsClientContext must outlive its
// session.
class DistributedQueryTlsClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedQueryTlsClient() = delete;
  ~DistributedQueryTlsClient();
  DistributedQueryTlsClient(const DistributedQueryTlsClient&) = delete;
  DistributedQueryTlsClient& operator=(const DistributedQueryTlsClient&) = delete;
  DistributedQueryTlsClient(DistributedQueryTlsClient&&) noexcept;
  DistributedQueryTlsClient& operator=(DistributedQueryTlsClient&&) noexcept;

  [[nodiscard]] static common::Result<DistributedQueryTlsClient>
  create(network::TlsSocket socket, DistributedQueryAttempt attempt,
         DistributedQueryTlsClientConfig config, TimePoint now);

  // Advances at most one TLS operation for the supplied readiness. Calling with neither readiness
  // is permitted and applies deadline expiry. Failures are sticky.
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);

  [[nodiscard]] DistributedQueryTlsClientState state() const noexcept;
  [[nodiscard]] DistributedQueryTlsInterest interest() const noexcept;
  [[nodiscard]] common::Result<common::ByteView> response_bytes() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedQueryTlsClient(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TLS_CLIENT_HPP_
