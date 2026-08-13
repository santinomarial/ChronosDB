#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TCP_CLIENT_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TCP_CLIENT_V2_HPP_

#include "chronos/cluster/distributed_vector_query_tls_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <span>

namespace chronos::cluster {

struct DistributedVectorQueryTcpClientConfigV2 {
  network::Ipv4Endpoint remote_endpoint;
  const network::TlsClientContext* tls_context{};
  DistributedVectorQueryTlsClientConfigV2 carrier;
  std::chrono::milliseconds connect_timeout{5000};
};

enum class DistributedVectorQueryTcpClientStateV2 : std::uint8_t {
  kConnecting = 1,
  kExchanging = 2,
  kComplete = 3,
  kFailed = 4,
};

// Owns one nonblocking TCP connection and one schema-bound vector query v2 TLS exchange. One
// event-loop thread serializes calls. The TLS context, authenticator, and node authorizer are
// borrowed and must outlive the client.
class DistributedVectorQueryTcpClientV2 {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorQueryTcpClientV2() = delete;
  ~DistributedVectorQueryTcpClientV2();
  DistributedVectorQueryTcpClientV2(const DistributedVectorQueryTcpClientV2&) = delete;
  DistributedVectorQueryTcpClientV2& operator=(const DistributedVectorQueryTcpClientV2&) = delete;
  DistributedVectorQueryTcpClientV2(DistributedVectorQueryTcpClientV2&&) noexcept;
  DistributedVectorQueryTcpClientV2& operator=(DistributedVectorQueryTcpClientV2&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorQueryTcpClientV2>
  begin(DistributedVectorQueryAttemptV2 attempt, DistributedVectorQueryTcpClientConfigV2 config,
        TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);

  [[nodiscard]] DistributedVectorQueryTcpClientStateV2 state() const noexcept;
  [[nodiscard]] DistributedVectorQueryTlsInterestV2 interest() const noexcept;
  [[nodiscard]] int descriptor() const noexcept;
  [[nodiscard]] common::Result<std::span<const DistributedVectorQueryResponseV2>> responses() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorQueryTcpClientV2(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TCP_CLIENT_V2_HPP_
