#ifndef CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TCP_CLIENT_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TCP_CLIENT_HPP_

#include "chronos/cluster/distributed_query_tls_client.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct DistributedQueryTcpClientConfig {
  network::Ipv4Endpoint remote_endpoint;
  const network::TlsClientContext* tls_context{};
  DistributedQueryTlsClientConfig carrier;
  std::chrono::milliseconds connect_timeout{5000};
};

enum class DistributedQueryTcpClientState : std::uint8_t {
  kConnecting = 1,
  kExchanging = 2,
  kComplete = 3,
  kFailed = 4,
};

// Owns one nonblocking TCP connection and one outbound TLS query attempt. One event-loop thread
// serializes calls. The TLS context, authenticator, and node authorizer are borrowed and must
// outlive the client.
class DistributedQueryTcpClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedQueryTcpClient() = delete;
  ~DistributedQueryTcpClient();
  DistributedQueryTcpClient(const DistributedQueryTcpClient&) = delete;
  DistributedQueryTcpClient& operator=(const DistributedQueryTcpClient&) = delete;
  DistributedQueryTcpClient(DistributedQueryTcpClient&&) noexcept;
  DistributedQueryTcpClient& operator=(DistributedQueryTcpClient&&) noexcept;

  [[nodiscard]] static common::Result<DistributedQueryTcpClient>
  begin(DistributedQueryAttempt attempt, DistributedQueryTcpClientConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);

  [[nodiscard]] DistributedQueryTcpClientState state() const noexcept;
  [[nodiscard]] DistributedQueryTlsInterest interest() const noexcept;
  [[nodiscard]] int descriptor() const noexcept;
  [[nodiscard]] common::Result<common::ByteView> response_bytes() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedQueryTcpClient(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TCP_CLIENT_HPP_
