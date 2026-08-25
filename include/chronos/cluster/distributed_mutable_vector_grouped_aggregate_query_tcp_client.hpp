#ifndef CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_CLIENT_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_CLIENT_HPP_

#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tls.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace chronos::cluster {

struct DistributedMutableVectorGroupedAggregateQueryTcpClientConfig {
  network::Ipv4Endpoint remote_endpoint;
  const network::TlsClientContext* tls_context{};
  DistributedMutableVectorGroupedAggregateQueryTlsClientConfig carrier;
  std::chrono::milliseconds connect_timeout{5000};
};

enum class DistributedMutableVectorGroupedAggregateQueryTcpClientState : std::uint8_t {
  kConnecting = 1,
  kExchanging = 2,
  kComplete = 3,
  kFailed = 4,
};

// Owns one nonblocking TCP connection, complete grouped authority and query resources, and one
// grouped sufficient-state v2 TLS exchange. One event-loop thread serializes calls.
// The TLS context, authenticator, and node authorizer are borrowed and outlive the client.
class DistributedMutableVectorGroupedAggregateQueryTcpClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedMutableVectorGroupedAggregateQueryTcpClient() = delete;
  ~DistributedMutableVectorGroupedAggregateQueryTcpClient();
  DistributedMutableVectorGroupedAggregateQueryTcpClient(
      const DistributedMutableVectorGroupedAggregateQueryTcpClient&) = delete;
  DistributedMutableVectorGroupedAggregateQueryTcpClient&
  operator=(const DistributedMutableVectorGroupedAggregateQueryTcpClient&) = delete;
  DistributedMutableVectorGroupedAggregateQueryTcpClient(
      DistributedMutableVectorGroupedAggregateQueryTcpClient&&) noexcept;
  DistributedMutableVectorGroupedAggregateQueryTcpClient&
  operator=(DistributedMutableVectorGroupedAggregateQueryTcpClient&&) noexcept;

  [[nodiscard]] static common::Result<DistributedMutableVectorGroupedAggregateQueryTcpClient>
  begin(DistributedMutableVectorGroupedAggregateQueryAttempt attempt,
        std::vector<query::VectorGroupKeyDefinition>&& keys,
        std::vector<query::VectorAggregateDefinition>&& aggregates,
        query::QueryResourceContext resources,
        DistributedMutableVectorGroupedAggregateQueryTcpClientConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);

  [[nodiscard]] DistributedMutableVectorGroupedAggregateQueryTcpClientState state() const noexcept;
  [[nodiscard]] DistributedMutableVectorGroupedAggregateQueryTlsInterest interest() const noexcept;
  [[nodiscard]] int descriptor() const noexcept;
  [[nodiscard]] common::Result<std::span<const DistributedVectorGroupedAggregateQueryResponseV2>>
  responses() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedMutableVectorGroupedAggregateQueryTcpClient(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_CLIENT_HPP_
