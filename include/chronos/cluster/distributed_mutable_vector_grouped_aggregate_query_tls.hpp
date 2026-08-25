#ifndef CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_TLS_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_TLS_HPP_

#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_transport.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_query_tls_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tls_socket.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace chronos::cluster {

using DistributedMutableVectorGroupedAggregateQueryTlsLimits =
    DistributedVectorGroupedAggregateQueryTlsLimitsV2;
using DistributedMutableVectorGroupedAggregateQueryTlsState =
    DistributedVectorGroupedAggregateQueryTlsStateV2;
using DistributedMutableVectorGroupedAggregateQueryTlsInterest =
    DistributedVectorGroupedAggregateQueryTlsInterestV2;
using DistributedMutableVectorGroupedAggregateQueryTlsClientConfig =
    DistributedVectorGroupedAggregateQueryTlsClientConfigV2;

struct DistributedMutableVectorGroupedAggregateQueryTlsServerConfig {
  network::ConnectionAuthenticator* authenticator{};
  DistributedMutableVectorGroupedAggregateQueryReceiver* receiver{};
  std::array<std::uint8_t, 4> peer_ipv4_address{};
  DistributedMutableVectorGroupedAggregateQueryTlsLimits limits;
};

// Owns one connected nonblocking TLS attempt, exact mutable authority, grouped authority, query
// resources, and one complete empty-or-contiguous response vector. One event-loop thread owns it.
class DistributedMutableVectorGroupedAggregateQueryTlsClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedMutableVectorGroupedAggregateQueryTlsClient() = delete;
  ~DistributedMutableVectorGroupedAggregateQueryTlsClient();
  DistributedMutableVectorGroupedAggregateQueryTlsClient(
      const DistributedMutableVectorGroupedAggregateQueryTlsClient&) = delete;
  DistributedMutableVectorGroupedAggregateQueryTlsClient&
  operator=(const DistributedMutableVectorGroupedAggregateQueryTlsClient&) = delete;
  DistributedMutableVectorGroupedAggregateQueryTlsClient(
      DistributedMutableVectorGroupedAggregateQueryTlsClient&&) noexcept;
  DistributedMutableVectorGroupedAggregateQueryTlsClient&
  operator=(DistributedMutableVectorGroupedAggregateQueryTlsClient&&) noexcept;

  [[nodiscard]] static common::Result<DistributedMutableVectorGroupedAggregateQueryTlsClient>
  create(network::TlsSocket socket, DistributedMutableVectorGroupedAggregateQueryAttempt attempt,
         std::vector<query::VectorGroupKeyDefinition>&& keys,
         std::vector<query::VectorAggregateDefinition>&& aggregates,
         query::QueryResourceContext resources,
         DistributedMutableVectorGroupedAggregateQueryTlsClientConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedMutableVectorGroupedAggregateQueryTlsState state() const noexcept;
  [[nodiscard]] DistributedMutableVectorGroupedAggregateQueryTlsInterest interest() const noexcept;
  [[nodiscard]] common::Result<std::span<const DistributedVectorGroupedAggregateQueryResponseV2>>
  responses() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedMutableVectorGroupedAggregateQueryTlsClient(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

// Authenticates before reading CHDMREQ1, invokes the mutable authority-binding receiver once, and
// writes only its complete terminal response vector. Borrowed dependencies outlive the owner.
class DistributedMutableVectorGroupedAggregateQueryTlsServer {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedMutableVectorGroupedAggregateQueryTlsServer() = delete;
  ~DistributedMutableVectorGroupedAggregateQueryTlsServer();
  DistributedMutableVectorGroupedAggregateQueryTlsServer(
      const DistributedMutableVectorGroupedAggregateQueryTlsServer&) = delete;
  DistributedMutableVectorGroupedAggregateQueryTlsServer&
  operator=(const DistributedMutableVectorGroupedAggregateQueryTlsServer&) = delete;
  DistributedMutableVectorGroupedAggregateQueryTlsServer(
      DistributedMutableVectorGroupedAggregateQueryTlsServer&&) noexcept;
  DistributedMutableVectorGroupedAggregateQueryTlsServer&
  operator=(DistributedMutableVectorGroupedAggregateQueryTlsServer&&) noexcept;

  [[nodiscard]] static common::Result<DistributedMutableVectorGroupedAggregateQueryTlsServer>
  create(network::TlsSocket socket,
         DistributedMutableVectorGroupedAggregateQueryTlsServerConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedMutableVectorGroupedAggregateQueryTlsState state() const noexcept;
  [[nodiscard]] DistributedMutableVectorGroupedAggregateQueryTlsInterest interest() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedMutableVectorGroupedAggregateQueryTlsServer(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_TLS_HPP_
