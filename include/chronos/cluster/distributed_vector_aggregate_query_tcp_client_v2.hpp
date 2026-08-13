#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_TCP_CLIENT_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_TCP_CLIENT_V2_HPP_

#include "chronos/cluster/distributed_vector_aggregate_query_tls_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace chronos::cluster {

struct DistributedVectorAggregateQueryTcpClientConfigV2 {
  network::Ipv4Endpoint remote_endpoint;
  const network::TlsClientContext* tls_context{};
  DistributedVectorAggregateQueryTlsClientConfigV2 carrier;
  std::chrono::milliseconds connect_timeout{5000};
};

enum class DistributedVectorAggregateQueryTcpClientStateV2 : std::uint8_t {
  kConnecting = 1,
  kExchanging = 2,
  kComplete = 3,
  kFailed = 4,
};

// Owns one nonblocking TCP connection, the exact aggregate definitions and query resources, and
// one definition-bound aggregate query v2 TLS exchange. One event-loop thread serializes calls.
// The TLS context, authenticator, and node authorizer are borrowed and outlive the client.
class DistributedVectorAggregateQueryTcpClientV2 {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorAggregateQueryTcpClientV2() = delete;
  ~DistributedVectorAggregateQueryTcpClientV2();
  DistributedVectorAggregateQueryTcpClientV2(const DistributedVectorAggregateQueryTcpClientV2&) =
      delete;
  DistributedVectorAggregateQueryTcpClientV2&
  operator=(const DistributedVectorAggregateQueryTcpClientV2&) = delete;
  DistributedVectorAggregateQueryTcpClientV2(DistributedVectorAggregateQueryTcpClientV2&&) noexcept;
  DistributedVectorAggregateQueryTcpClientV2&
  operator=(DistributedVectorAggregateQueryTcpClientV2&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorAggregateQueryTcpClientV2>
  begin(DistributedVectorAggregateQueryAttemptV2 attempt,
        std::vector<query::VectorAggregateDefinition>&& definitions,
        query::QueryResourceContext resources,
        DistributedVectorAggregateQueryTcpClientConfigV2 config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);

  [[nodiscard]] DistributedVectorAggregateQueryTcpClientStateV2 state() const noexcept;
  [[nodiscard]] DistributedVectorAggregateQueryTlsInterestV2 interest() const noexcept;
  [[nodiscard]] int descriptor() const noexcept;
  [[nodiscard]] common::Result<std::span<const DistributedVectorAggregateQueryResponseV2>>
  responses() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorAggregateQueryTcpClientV2(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_TCP_CLIENT_V2_HPP_
