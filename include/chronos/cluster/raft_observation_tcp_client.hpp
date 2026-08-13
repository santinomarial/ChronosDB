#ifndef CHRONOS_CLUSTER_RAFT_OBSERVATION_TCP_CLIENT_HPP_
#define CHRONOS_CLUSTER_RAFT_OBSERVATION_TCP_CLIENT_HPP_

#include "chronos/cluster/raft_observation_tls_client.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>

namespace chronos::cluster {

struct RaftObservationTcpClientConfig {
  network::Ipv4Endpoint remote_endpoint;
  const network::TlsClientContext* tls_context{};
  RaftObservationTlsClientConfig carrier;
  std::chrono::milliseconds connect_timeout{5000};
};

enum class RaftObservationTcpClientState : std::uint8_t {
  kConnecting = 1,
  kExchanging = 2,
  kComplete = 3,
  kFailed = 4,
};

// Owns one nonblocking TCP connection and one exact outbound observation exchange. One event-loop
// thread serializes calls. The TLS context, authenticator, and node authorizer are borrowed and
// must outlive the client.
class RaftObservationTcpClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  RaftObservationTcpClient() = delete;
  ~RaftObservationTcpClient();
  RaftObservationTcpClient(const RaftObservationTcpClient&) = delete;
  RaftObservationTcpClient& operator=(const RaftObservationTcpClient&) = delete;
  RaftObservationTcpClient(RaftObservationTcpClient&&) noexcept;
  RaftObservationTcpClient& operator=(RaftObservationTcpClient&&) noexcept;

  [[nodiscard]] static common::Result<RaftObservationTcpClient>
  begin(RaftObservationTcpClientConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);

  [[nodiscard]] RaftObservationTcpClientState state() const noexcept;
  [[nodiscard]] RaftObservationTlsInterest interest() const noexcept;
  [[nodiscard]] int descriptor() const noexcept;
  [[nodiscard]] std::optional<TimePoint> deadline() const noexcept;
  [[nodiscard]] common::Result<raft::RaftGroupObservation> result() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit RaftObservationTcpClient(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_OBSERVATION_TCP_CLIENT_HPP_
