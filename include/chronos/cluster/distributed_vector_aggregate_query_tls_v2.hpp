#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_TLS_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_TLS_V2_HPP_

#include "chronos/cluster/distributed_vector_aggregate_query_transport_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tls_socket.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace chronos::cluster {

struct DistributedVectorAggregateQueryTlsLimitsV2 {
  std::chrono::milliseconds handshake_timeout{5000};
  std::chrono::milliseconds exchange_timeout{30000};
  std::size_t maximum_response_frames{query::kMaximumUngroupedAggregateWidth};
  std::size_t maximum_response_bytes{kDefaultDistributedVectorAggregateQueryV2ResponseBytes};
  query::DistributedVectorAggregateExchangeDecodeLimits payload;
};

struct DistributedVectorAggregateQueryTlsClientConfigV2 {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::array<std::uint8_t, 4> peer_ipv4_address{};
  DistributedVectorAggregateQueryTlsLimitsV2 limits;
};

struct DistributedVectorAggregateQueryTlsServerConfigV2 {
  network::ConnectionAuthenticator* authenticator{};
  DistributedVectorAggregateQueryReceiverV2* receiver{};
  std::array<std::uint8_t, 4> peer_ipv4_address{};
  DistributedVectorAggregateQueryTlsLimitsV2 limits;
};

enum class DistributedVectorAggregateQueryTlsStateV2 : std::uint8_t {
  kHandshaking = 1,
  kWritingRequest = 2,
  kReadingRequest = 3,
  kReadingResponses = 4,
  kWritingResponses = 5,
  kComplete = 6,
  kFailed = 7,
};

struct DistributedVectorAggregateQueryTlsInterestV2 {
  bool want_read{};
  bool want_write{};
};

// Owns one connected nonblocking TLS attempt, its exact definitions and query resource authority,
// and a complete fixed-width response vector. One event-loop thread serializes readiness calls.
class DistributedVectorAggregateQueryTlsClientV2 {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorAggregateQueryTlsClientV2() = delete;
  ~DistributedVectorAggregateQueryTlsClientV2();
  DistributedVectorAggregateQueryTlsClientV2(const DistributedVectorAggregateQueryTlsClientV2&) =
      delete;
  DistributedVectorAggregateQueryTlsClientV2&
  operator=(const DistributedVectorAggregateQueryTlsClientV2&) = delete;
  DistributedVectorAggregateQueryTlsClientV2(DistributedVectorAggregateQueryTlsClientV2&&) noexcept;
  DistributedVectorAggregateQueryTlsClientV2&
  operator=(DistributedVectorAggregateQueryTlsClientV2&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorAggregateQueryTlsClientV2>
  create(network::TlsSocket socket, DistributedVectorAggregateQueryAttemptV2 attempt,
         std::vector<query::VectorAggregateDefinition>&& definitions,
         query::QueryResourceContext resources,
         DistributedVectorAggregateQueryTlsClientConfigV2 config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedVectorAggregateQueryTlsStateV2 state() const noexcept;
  [[nodiscard]] DistributedVectorAggregateQueryTlsInterestV2 interest() const noexcept;
  [[nodiscard]] common::Result<std::span<const DistributedVectorAggregateQueryResponseV2>>
  responses() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorAggregateQueryTlsClientV2(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

// Authenticates before request read, invokes the definition-binding receiver once, and writes its
// complete response vector. The descriptor, TLS context, authenticator, and receiver outlive it.
class DistributedVectorAggregateQueryTlsServerV2 {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorAggregateQueryTlsServerV2() = delete;
  ~DistributedVectorAggregateQueryTlsServerV2();
  DistributedVectorAggregateQueryTlsServerV2(const DistributedVectorAggregateQueryTlsServerV2&) =
      delete;
  DistributedVectorAggregateQueryTlsServerV2&
  operator=(const DistributedVectorAggregateQueryTlsServerV2&) = delete;
  DistributedVectorAggregateQueryTlsServerV2(DistributedVectorAggregateQueryTlsServerV2&&) noexcept;
  DistributedVectorAggregateQueryTlsServerV2&
  operator=(DistributedVectorAggregateQueryTlsServerV2&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorAggregateQueryTlsServerV2>
  create(network::TlsSocket socket, DistributedVectorAggregateQueryTlsServerConfigV2 config,
         TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedVectorAggregateQueryTlsStateV2 state() const noexcept;
  [[nodiscard]] DistributedVectorAggregateQueryTlsInterestV2 interest() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorAggregateQueryTlsServerV2(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_TLS_V2_HPP_
