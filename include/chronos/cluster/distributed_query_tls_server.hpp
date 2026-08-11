#ifndef CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TLS_SERVER_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TLS_SERVER_HPP_

#include "chronos/cluster/distributed_query_transport.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tls_socket.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct DistributedQueryTlsServerLimits {
  std::chrono::milliseconds handshake_timeout{5000};
  std::chrono::milliseconds exchange_timeout{30000};
};

struct DistributedQueryTlsServerConfig {
  network::ConnectionAuthenticator* authenticator{};
  DistributedQueryReceiver* receiver{};
  std::array<std::uint8_t, 4> peer_ipv4_address{};
  DistributedQueryTlsServerLimits limits;
};

enum class DistributedQueryTlsServerState : std::uint8_t {
  kHandshaking = 1,
  kReadingRequest = 2,
  kWritingResponse = 3,
  kComplete = 4,
  kFailed = 5,
};

struct DistributedQueryTlsServerInterest {
  bool want_read{};
  bool want_write{};
};

// Owns one inbound distributed-query exchange over one accepted nonblocking TLS socket. A single
// event-loop thread must serialize calls. The TLS context, authenticator, receiver, and borrowed
// descriptor must outlive the session; the descriptor is closed by the caller after destruction.
class DistributedQueryTlsServer {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedQueryTlsServer() = delete;
  ~DistributedQueryTlsServer();
  DistributedQueryTlsServer(const DistributedQueryTlsServer&) = delete;
  DistributedQueryTlsServer& operator=(const DistributedQueryTlsServer&) = delete;
  DistributedQueryTlsServer(DistributedQueryTlsServer&&) noexcept;
  DistributedQueryTlsServer& operator=(DistributedQueryTlsServer&&) noexcept;

  [[nodiscard]] static common::Result<DistributedQueryTlsServer>
  create(network::TlsSocket socket, DistributedQueryTlsServerConfig config, TimePoint now);

  // Advances at most one TLS operation. Calling with neither readiness applies deadline expiry.
  // Authentication, protocol, worker, and transport failures are sticky.
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);

  [[nodiscard]] DistributedQueryTlsServerState state() const noexcept;
  [[nodiscard]] DistributedQueryTlsServerInterest interest() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedQueryTlsServer(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TLS_SERVER_HPP_
