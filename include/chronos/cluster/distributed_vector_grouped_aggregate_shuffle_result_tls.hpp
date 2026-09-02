#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_TLS_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_TLS_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_ack.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_stream.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tls_socket.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct DistributedVectorGroupedAggregateShuffleResultTlsLimits {
  std::chrono::milliseconds handshake_timeout{5000};
  std::chrono::milliseconds exchange_timeout{30000};
  DistributedVectorGroupedAggregateShuffleResultStreamLimits stream{};
};

struct DistributedVectorGroupedAggregateShuffleResultTlsClientConfig {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::array<std::uint8_t, 4> peer_ipv4_address{};
  DistributedVectorGroupedAggregateShuffleResultTlsLimits limits;
};

struct DistributedVectorGroupedAggregateShuffleResultTlsServerConfig {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  const DistributedVectorGroupedAggregateShuffleAuthority* authority{};
  const query::DistributedVectorResultSchema* result_schema{};
  raft::NodeId coordinator_node_id{};
  std::array<std::uint8_t, 4> peer_ipv4_address{};
  DistributedVectorGroupedAggregateShuffleResultTlsLimits limits;
};

enum class DistributedVectorGroupedAggregateShuffleResultTlsState : std::uint8_t {
  kHandshaking = 1,
  kWritingStream = 2,
  kReadingStream = 3,
  kWritingAck = 4,
  kReadingAck = 5,
  kComplete = 6,
  kFailed = 7,
};

struct DistributedVectorGroupedAggregateShuffleResultTlsInterest {
  bool want_read{};
  bool want_write{};
};

// Owns one already-connected nonblocking mutual-TLS result-return attempt. The reducer
// authenticates and authorizes the coordinator before writing, and completes only after the exact
// application receipt. Borrowed authority, schema, and security dependencies outlive it.
class DistributedVectorGroupedAggregateShuffleResultTlsClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorGroupedAggregateShuffleResultTlsClient() = delete;
  ~DistributedVectorGroupedAggregateShuffleResultTlsClient();
  DistributedVectorGroupedAggregateShuffleResultTlsClient(
      const DistributedVectorGroupedAggregateShuffleResultTlsClient&) = delete;
  DistributedVectorGroupedAggregateShuffleResultTlsClient&
  operator=(const DistributedVectorGroupedAggregateShuffleResultTlsClient&) = delete;
  DistributedVectorGroupedAggregateShuffleResultTlsClient(
      DistributedVectorGroupedAggregateShuffleResultTlsClient&&) noexcept;
  DistributedVectorGroupedAggregateShuffleResultTlsClient&
  operator=(DistributedVectorGroupedAggregateShuffleResultTlsClient&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleResultTlsClient>
  create(network::TlsSocket socket,
         DistributedVectorGroupedAggregateShuffleResultStreamSender sender,
         const DistributedVectorGroupedAggregateShuffleAuthority& authority,
         const query::DistributedVectorResultSchema& result_schema,
         raft::NodeId coordinator_node_id,
         DistributedVectorGroupedAggregateShuffleResultTlsClientConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleResultTlsState state() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleResultTlsInterest interest() const noexcept;
  [[nodiscard]] TimePoint deadline() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleResultTlsClient(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

// Authenticates the reducer before application reads, privately retains one complete authorized
// partition, and publishes it only after the correlated receipt is fully written.
class DistributedVectorGroupedAggregateShuffleResultTlsServer {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorGroupedAggregateShuffleResultTlsServer() = delete;
  ~DistributedVectorGroupedAggregateShuffleResultTlsServer();
  DistributedVectorGroupedAggregateShuffleResultTlsServer(
      const DistributedVectorGroupedAggregateShuffleResultTlsServer&) = delete;
  DistributedVectorGroupedAggregateShuffleResultTlsServer&
  operator=(const DistributedVectorGroupedAggregateShuffleResultTlsServer&) = delete;
  DistributedVectorGroupedAggregateShuffleResultTlsServer(
      DistributedVectorGroupedAggregateShuffleResultTlsServer&&) noexcept;
  DistributedVectorGroupedAggregateShuffleResultTlsServer&
  operator=(DistributedVectorGroupedAggregateShuffleResultTlsServer&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleResultTlsServer>
  create(network::TlsSocket socket,
         DistributedVectorGroupedAggregateShuffleResultTlsServerConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleResultTlsState state() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleResultTlsInterest interest() const noexcept;
  [[nodiscard]] TimePoint deadline() const noexcept;
  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleCompleteResultStream>
  take_complete_stream();
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleResultTlsServer(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_TLS_HPP_
