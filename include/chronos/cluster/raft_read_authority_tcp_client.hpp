#ifndef CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TCP_CLIENT_HPP_
#define CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TCP_CLIENT_HPP_

#include "chronos/cluster/raft_read_authority_tls_client.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>

namespace chronos::cluster {

struct RaftReadAuthorityTcpClientConfig {
  network::Ipv4Endpoint remote_endpoint;
  const network::TlsClientContext* tls_context{};
  RaftReadAuthorityTlsClientConfig carrier;
  std::chrono::milliseconds connect_timeout{5000};
};

enum class RaftReadAuthorityTcpClientState : std::uint8_t {
  kConnecting = 1,
  kExchanging = 2,
  kComplete = 3,
  kFailed = 4,
};

// Owns one nonblocking TCP connection and one exact authority exchange. One event-loop thread
// serializes calls. The TLS context, authenticator, and authorizer are borrowed and must outlive
// it.
class RaftReadAuthorityTcpClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  RaftReadAuthorityTcpClient() = delete;
  ~RaftReadAuthorityTcpClient();
  RaftReadAuthorityTcpClient(const RaftReadAuthorityTcpClient&) = delete;
  RaftReadAuthorityTcpClient& operator=(const RaftReadAuthorityTcpClient&) = delete;
  RaftReadAuthorityTcpClient(RaftReadAuthorityTcpClient&&) noexcept;
  RaftReadAuthorityTcpClient& operator=(RaftReadAuthorityTcpClient&&) noexcept;

  [[nodiscard]] static common::Result<RaftReadAuthorityTcpClient>
  begin(RaftReadAuthorityTcpClientConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] RaftReadAuthorityTcpClientState state() const noexcept;
  [[nodiscard]] RaftReadAuthorityTlsInterest interest() const noexcept;
  [[nodiscard]] int descriptor() const noexcept;
  [[nodiscard]] std::optional<TimePoint> deadline() const noexcept;
  [[nodiscard]] common::Result<RaftReadAuthority> result() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit RaftReadAuthorityTcpClient(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TCP_CLIENT_HPP_
