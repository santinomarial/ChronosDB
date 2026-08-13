#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TLS_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TLS_V2_HPP_

#include "chronos/cluster/distributed_vector_query_transport_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tls_socket.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

namespace chronos::cluster {

struct DistributedVectorQueryTlsLimitsV2 {
  std::chrono::milliseconds handshake_timeout{5000};
  std::chrono::milliseconds exchange_timeout{30000};
  std::size_t maximum_response_frames{1024U};
  std::size_t maximum_response_bytes{kDefaultDistributedVectorQueryV2ResponseBytes};
};

struct DistributedVectorQueryTlsClientConfigV2 {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::array<std::uint8_t, 4> peer_ipv4_address{};
  DistributedVectorQueryTlsLimitsV2 limits;
};

struct DistributedVectorQueryTlsServerConfigV2 {
  network::ConnectionAuthenticator* authenticator{};
  DistributedVectorQueryReceiverV2* receiver{};
  std::array<std::uint8_t, 4> peer_ipv4_address{};
  DistributedVectorQueryTlsLimitsV2 limits;
};

enum class DistributedVectorQueryTlsStateV2 : std::uint8_t {
  kHandshaking = 1,
  kWritingRequest = 2,
  kReadingRequest = 3,
  kReadingResponses = 4,
  kWritingResponses = 5,
  kComplete = 6,
  kFailed = 7,
};

struct DistributedVectorQueryTlsInterestV2 {
  bool want_read{};
  bool want_write{};
};

// Owns one authenticated Fragment-v2 request and retains its complete correlated response stream.
// The connected descriptor and TLS context outlive this move-only owner; one event-loop thread
// serializes readiness calls.
class DistributedVectorQueryTlsClientV2 {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorQueryTlsClientV2() = delete;
  ~DistributedVectorQueryTlsClientV2();
  DistributedVectorQueryTlsClientV2(const DistributedVectorQueryTlsClientV2&) = delete;
  DistributedVectorQueryTlsClientV2& operator=(const DistributedVectorQueryTlsClientV2&) = delete;
  DistributedVectorQueryTlsClientV2(DistributedVectorQueryTlsClientV2&&) noexcept;
  DistributedVectorQueryTlsClientV2& operator=(DistributedVectorQueryTlsClientV2&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorQueryTlsClientV2>
  create(network::TlsSocket socket, DistributedVectorQueryAttemptV2 attempt,
         DistributedVectorQueryTlsClientConfigV2 config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedVectorQueryTlsStateV2 state() const noexcept;
  [[nodiscard]] DistributedVectorQueryTlsInterestV2 interest() const noexcept;
  [[nodiscard]] common::Result<std::span<const DistributedVectorQueryResponseV2>> responses() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorQueryTlsClientV2(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

// Authenticates one inbound peer before reading, invokes the v2 receiver once, and writes its
// complete response vector in order. One event-loop thread serializes readiness calls.
class DistributedVectorQueryTlsServerV2 {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorQueryTlsServerV2() = delete;
  ~DistributedVectorQueryTlsServerV2();
  DistributedVectorQueryTlsServerV2(const DistributedVectorQueryTlsServerV2&) = delete;
  DistributedVectorQueryTlsServerV2& operator=(const DistributedVectorQueryTlsServerV2&) = delete;
  DistributedVectorQueryTlsServerV2(DistributedVectorQueryTlsServerV2&&) noexcept;
  DistributedVectorQueryTlsServerV2& operator=(DistributedVectorQueryTlsServerV2&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorQueryTlsServerV2>
  create(network::TlsSocket socket, DistributedVectorQueryTlsServerConfigV2 config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedVectorQueryTlsStateV2 state() const noexcept;
  [[nodiscard]] DistributedVectorQueryTlsInterestV2 interest() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorQueryTlsServerV2(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TLS_V2_HPP_
