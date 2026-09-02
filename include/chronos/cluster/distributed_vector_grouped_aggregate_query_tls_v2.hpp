#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TLS_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TLS_V2_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_query_transport_v2.hpp"
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

struct DistributedVectorGroupedAggregateQueryTlsLimitsV2 {
  std::chrono::milliseconds handshake_timeout{5000};
  std::chrono::milliseconds exchange_timeout{30000};
  std::size_t maximum_response_frames{
      query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups};
  std::size_t maximum_response_bytes{kDefaultDistributedVectorGroupedAggregateQueryV2ResponseBytes};
  query::DistributedVectorGroupedAggregateExchangeDecodeLimits payload{};
};

struct DistributedVectorGroupedAggregateQueryTlsClientConfigV2 {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::array<std::uint8_t, 4> peer_ipv4_address{};
  DistributedVectorGroupedAggregateQueryTlsLimitsV2 limits;
};

struct DistributedVectorGroupedAggregateQueryTlsServerConfigV2 {
  network::ConnectionAuthenticator* authenticator{};
  DistributedVectorGroupedAggregateQueryReceiverV2* receiver{};
  std::array<std::uint8_t, 4> peer_ipv4_address{};
  DistributedVectorGroupedAggregateQueryTlsLimitsV2 limits;
};

enum class DistributedVectorGroupedAggregateQueryTlsStateV2 : std::uint8_t {
  kHandshaking = 1,
  kWritingRequest = 2,
  kReadingRequest = 3,
  kReadingResponses = 4,
  kWritingResponses = 5,
  kComplete = 6,
  kFailed = 7,
};

struct DistributedVectorGroupedAggregateQueryTlsInterestV2 {
  bool want_read{};
  bool want_write{};
};

// Owns one connected nonblocking TLS attempt, complete grouped authority, query resource context,
// and one complete empty-or-contiguous response vector. One event-loop thread serializes calls.
class DistributedVectorGroupedAggregateQueryTlsClientV2 {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorGroupedAggregateQueryTlsClientV2() = delete;
  ~DistributedVectorGroupedAggregateQueryTlsClientV2();
  DistributedVectorGroupedAggregateQueryTlsClientV2(
      const DistributedVectorGroupedAggregateQueryTlsClientV2&) = delete;
  DistributedVectorGroupedAggregateQueryTlsClientV2&
  operator=(const DistributedVectorGroupedAggregateQueryTlsClientV2&) = delete;
  DistributedVectorGroupedAggregateQueryTlsClientV2(
      DistributedVectorGroupedAggregateQueryTlsClientV2&&) noexcept;
  DistributedVectorGroupedAggregateQueryTlsClientV2&
  operator=(DistributedVectorGroupedAggregateQueryTlsClientV2&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateQueryTlsClientV2>
  create(network::TlsSocket socket, DistributedVectorGroupedAggregateQueryAttemptV2 attempt,
         std::vector<query::VectorGroupKeyDefinition>&& keys,
         std::vector<query::VectorAggregateDefinition>&& aggregates,
         query::QueryResourceContext resources,
         DistributedVectorGroupedAggregateQueryTlsClientConfigV2 config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedVectorGroupedAggregateQueryTlsStateV2 state() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateQueryTlsInterestV2 interest() const noexcept;
  [[nodiscard]] common::Result<std::span<const DistributedVectorGroupedAggregateQueryResponseV2>>
  responses() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateQueryTlsClientV2(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

// Authenticates before request read, invokes the authority-binding receiver once, and writes its
// complete terminal response vector. The TLS context, authenticator, and receiver outlive it.
class DistributedVectorGroupedAggregateQueryTlsServerV2 {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorGroupedAggregateQueryTlsServerV2() = delete;
  ~DistributedVectorGroupedAggregateQueryTlsServerV2();
  DistributedVectorGroupedAggregateQueryTlsServerV2(
      const DistributedVectorGroupedAggregateQueryTlsServerV2&) = delete;
  DistributedVectorGroupedAggregateQueryTlsServerV2&
  operator=(const DistributedVectorGroupedAggregateQueryTlsServerV2&) = delete;
  DistributedVectorGroupedAggregateQueryTlsServerV2(
      DistributedVectorGroupedAggregateQueryTlsServerV2&&) noexcept;
  DistributedVectorGroupedAggregateQueryTlsServerV2&
  operator=(DistributedVectorGroupedAggregateQueryTlsServerV2&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateQueryTlsServerV2>
  create(network::TlsSocket socket, DistributedVectorGroupedAggregateQueryTlsServerConfigV2 config,
         TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedVectorGroupedAggregateQueryTlsStateV2 state() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateQueryTlsInterestV2 interest() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateQueryTlsServerV2(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TLS_V2_HPP_
