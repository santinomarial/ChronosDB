#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_CLIENT_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_CLIENT_V2_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_query_tls_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace chronos::cluster {

struct DistributedVectorGroupedAggregateQueryTcpClientConfigV2 {
  network::Ipv4Endpoint remote_endpoint;
  const network::TlsClientContext* tls_context{};
  DistributedVectorGroupedAggregateQueryTlsClientConfigV2 carrier;
  std::chrono::milliseconds connect_timeout{5000};
};

enum class DistributedVectorGroupedAggregateQueryTcpClientStateV2 : std::uint8_t {
  kConnecting = 1,
  kExchanging = 2,
  kComplete = 3,
  kFailed = 4,
};

// Owns one nonblocking TCP connection, complete grouped authority and query resources, and one
// grouped sufficient-state v2 TLS exchange. One event-loop thread serializes calls.
// The TLS context, authenticator, and node authorizer are borrowed and outlive the client.
class DistributedVectorGroupedAggregateQueryTcpClientV2 {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorGroupedAggregateQueryTcpClientV2() = delete;
  ~DistributedVectorGroupedAggregateQueryTcpClientV2();
  DistributedVectorGroupedAggregateQueryTcpClientV2(
      const DistributedVectorGroupedAggregateQueryTcpClientV2&) = delete;
  DistributedVectorGroupedAggregateQueryTcpClientV2&
  operator=(const DistributedVectorGroupedAggregateQueryTcpClientV2&) = delete;
  DistributedVectorGroupedAggregateQueryTcpClientV2(
      DistributedVectorGroupedAggregateQueryTcpClientV2&&) noexcept;
  DistributedVectorGroupedAggregateQueryTcpClientV2&
  operator=(DistributedVectorGroupedAggregateQueryTcpClientV2&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateQueryTcpClientV2>
  begin(DistributedVectorGroupedAggregateQueryAttemptV2 attempt,
        std::vector<query::VectorGroupKeyDefinition>&& keys,
        std::vector<query::VectorAggregateDefinition>&& aggregates,
        query::QueryResourceContext resources,
        DistributedVectorGroupedAggregateQueryTcpClientConfigV2 config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);

  [[nodiscard]] DistributedVectorGroupedAggregateQueryTcpClientStateV2 state() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateQueryTlsInterestV2 interest() const noexcept;
  [[nodiscard]] int descriptor() const noexcept;
  [[nodiscard]] common::Result<std::span<const DistributedVectorGroupedAggregateQueryResponseV2>>
  responses() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateQueryTcpClientV2(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TCP_CLIENT_V2_HPP_
