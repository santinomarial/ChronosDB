#ifndef CHRONOS_CLUSTER_DISTRIBUTED_GROUPED_QUERY_TCP_CLIENT_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_GROUPED_QUERY_TCP_CLIENT_HPP_

#include "chronos/cluster/distributed_grouped_query_tls.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <span>

namespace chronos::cluster {

struct DistributedGroupedQueryTcpClientConfig {
  network::Ipv4Endpoint remote_endpoint;
  const network::TlsClientContext* tls_context{};
  DistributedGroupedQueryTlsClientConfig carrier;
  std::chrono::milliseconds connect_timeout{5000};
};

enum class DistributedGroupedQueryTcpClientState : std::uint8_t {
  kConnecting = 1,
  kExchanging = 2,
  kComplete = 3,
  kFailed = 4,
};

// Owns one nonblocking TCP connection and one grouped query TLS exchange. One event-loop thread
// serializes calls. The TLS context, authenticator, and node authorizer are borrowed and must
// outlive the client.
class DistributedGroupedQueryTcpClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedGroupedQueryTcpClient() = delete;
  ~DistributedGroupedQueryTcpClient();
  DistributedGroupedQueryTcpClient(const DistributedGroupedQueryTcpClient&) = delete;
  DistributedGroupedQueryTcpClient& operator=(const DistributedGroupedQueryTcpClient&) = delete;
  DistributedGroupedQueryTcpClient(DistributedGroupedQueryTcpClient&&) noexcept;
  DistributedGroupedQueryTcpClient& operator=(DistributedGroupedQueryTcpClient&&) noexcept;

  [[nodiscard]] static common::Result<DistributedGroupedQueryTcpClient>
  begin(DistributedGroupedQueryAttempt attempt, DistributedGroupedQueryTcpClientConfig config,
        TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);

  [[nodiscard]] DistributedGroupedQueryTcpClientState state() const noexcept;
  [[nodiscard]] DistributedGroupedQueryTlsInterest interest() const noexcept;
  [[nodiscard]] int descriptor() const noexcept;
  [[nodiscard]] common::Result<std::span<const DistributedGroupedQueryResponse>> responses() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedGroupedQueryTcpClient(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_GROUPED_QUERY_TCP_CLIENT_HPP_
