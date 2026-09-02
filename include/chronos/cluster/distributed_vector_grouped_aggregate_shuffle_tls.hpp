#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_TLS_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_TLS_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_ack.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_stream.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tls_socket.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct DistributedVectorGroupedAggregateShuffleTlsLimits {
  std::chrono::milliseconds handshake_timeout{5000};
  std::chrono::milliseconds exchange_timeout{30000};
  DistributedVectorGroupedAggregateShuffleStreamLimits stream{};
};

struct DistributedVectorGroupedAggregateShuffleTlsClientConfig {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::array<std::uint8_t, 4> peer_ipv4_address{};
  DistributedVectorGroupedAggregateShuffleTlsLimits limits;
};

struct DistributedVectorGroupedAggregateShuffleTlsServerConfig {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  const DistributedVectorGroupedAggregateShuffleAuthority* authority{};
  raft::NodeId local_node_id{};
  std::array<std::uint8_t, 4> peer_ipv4_address{};
  DistributedVectorGroupedAggregateShuffleTlsLimits limits;
};

enum class DistributedVectorGroupedAggregateShuffleTlsState : std::uint8_t {
  kHandshaking = 1,
  kWritingStream = 2,
  kReadingStream = 3,
  kWritingAck = 4,
  kReadingAck = 5,
  kComplete = 6,
  kFailed = 7,
};

struct DistributedVectorGroupedAggregateShuffleTlsInterest {
  bool want_read{};
  bool want_write{};
};

// Owns one already-connected nonblocking mutual-TLS attempt. It authenticates and node-authorizes
// the destination before writing the all-or-none stream and completes only after one exact receipt.
class DistributedVectorGroupedAggregateShuffleTlsClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorGroupedAggregateShuffleTlsClient() = delete;
  ~DistributedVectorGroupedAggregateShuffleTlsClient();
  DistributedVectorGroupedAggregateShuffleTlsClient(
      const DistributedVectorGroupedAggregateShuffleTlsClient&) = delete;
  DistributedVectorGroupedAggregateShuffleTlsClient&
  operator=(const DistributedVectorGroupedAggregateShuffleTlsClient&) = delete;
  DistributedVectorGroupedAggregateShuffleTlsClient(
      DistributedVectorGroupedAggregateShuffleTlsClient&&) noexcept;
  DistributedVectorGroupedAggregateShuffleTlsClient&
  operator=(DistributedVectorGroupedAggregateShuffleTlsClient&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleTlsClient>
  create(network::TlsSocket socket, DistributedVectorGroupedAggregateShuffleStreamSender sender,
         const DistributedVectorGroupedAggregateShuffleAuthority& authority,
         DistributedVectorGroupedAggregateShuffleTlsClientConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleTlsState state() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleTlsInterest interest() const noexcept;
  [[nodiscard]] TimePoint deadline() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleTlsClient(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

// Authenticates before application reads, authorizes the claimed source through the stream owner,
// retains one complete stream, and emits its exact receipt. Borrowed dependencies outlive it.
class DistributedVectorGroupedAggregateShuffleTlsServer {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorGroupedAggregateShuffleTlsServer() = delete;
  ~DistributedVectorGroupedAggregateShuffleTlsServer();
  DistributedVectorGroupedAggregateShuffleTlsServer(
      const DistributedVectorGroupedAggregateShuffleTlsServer&) = delete;
  DistributedVectorGroupedAggregateShuffleTlsServer&
  operator=(const DistributedVectorGroupedAggregateShuffleTlsServer&) = delete;
  DistributedVectorGroupedAggregateShuffleTlsServer(
      DistributedVectorGroupedAggregateShuffleTlsServer&&) noexcept;
  DistributedVectorGroupedAggregateShuffleTlsServer&
  operator=(DistributedVectorGroupedAggregateShuffleTlsServer&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleTlsServer>
  create(network::TlsSocket socket, query::QueryResourceContext resources,
         DistributedVectorGroupedAggregateShuffleTlsServerConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleTlsState state() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleTlsInterest interest() const noexcept;
  [[nodiscard]] TimePoint deadline() const noexcept;
  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleCompleteStream>
  take_complete_stream();
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleTlsServer(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_TLS_HPP_
