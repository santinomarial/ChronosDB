#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_CONTROL_TLS_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_CONTROL_TLS_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_transport.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_service.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tls_socket.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct DistributedVectorGroupedAggregateShuffleJobControlTlsLimits {
  std::chrono::milliseconds handshake_timeout{5000};
  std::chrono::milliseconds exchange_timeout{30000};
  DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits request;
};

struct DistributedVectorGroupedAggregateShuffleJobControlTlsInterest {
  bool want_read{};
  bool want_write{};
};

struct DistributedVectorGroupedAggregateShuffleJobControlTlsClientConfig {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::array<std::uint8_t, 4U> peer_ipv4_address{};
  DistributedVectorGroupedAggregateShuffleJobControlRequest request;
  DistributedVectorGroupedAggregateShuffleJobControlTlsLimits limits;
};

enum class DistributedVectorGroupedAggregateShuffleJobControlTlsClientState : std::uint8_t {
  kHandshaking = 1,
  kWritingRequest = 2,
  kReadingResponse = 3,
  kComplete = 4,
  kFailed = 5,
};

// Owns one exact reducer-job request/response exchange over an already-connected mTLS socket.
// The descriptor, authenticator, and authorizer are borrowed and must outlive the client.
class DistributedVectorGroupedAggregateShuffleJobControlTlsClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorGroupedAggregateShuffleJobControlTlsClient() = delete;
  ~DistributedVectorGroupedAggregateShuffleJobControlTlsClient();
  DistributedVectorGroupedAggregateShuffleJobControlTlsClient(
      const DistributedVectorGroupedAggregateShuffleJobControlTlsClient&) = delete;
  DistributedVectorGroupedAggregateShuffleJobControlTlsClient&
  operator=(const DistributedVectorGroupedAggregateShuffleJobControlTlsClient&) = delete;
  DistributedVectorGroupedAggregateShuffleJobControlTlsClient(
      DistributedVectorGroupedAggregateShuffleJobControlTlsClient&&) noexcept;
  DistributedVectorGroupedAggregateShuffleJobControlTlsClient&
  operator=(DistributedVectorGroupedAggregateShuffleJobControlTlsClient&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleJobControlTlsClient>
  create(network::TlsSocket socket,
         DistributedVectorGroupedAggregateShuffleJobControlTlsClientConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleJobControlTlsClientState
  state() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleJobControlTlsInterest
  interest() const noexcept;
  [[nodiscard]] TimePoint deadline() const noexcept;
  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponse>
  result() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleJobControlTlsClient(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

struct DistributedVectorGroupedAggregateShuffleJobControlTlsServerConfig {
  network::ConnectionAuthenticator* authenticator{};
  DistributedVectorGroupedAggregateShuffleJobService* service{};
  std::array<std::uint8_t, 4U> peer_ipv4_address{};
  DistributedVectorGroupedAggregateShuffleJobControlTlsLimits limits;
};

enum class DistributedVectorGroupedAggregateShuffleJobControlTlsServerState : std::uint8_t {
  kHandshaking = 1,
  kReadingRequest = 2,
  kWritingResponse = 3,
  kComplete = 4,
  kFailed = 5,
};

// Authenticates before reading one exact request, invokes the bounded job service, and publishes
// one complete correlated response. The descriptor, authenticator, and service are borrowed.
class DistributedVectorGroupedAggregateShuffleJobControlTlsServer {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorGroupedAggregateShuffleJobControlTlsServer() = delete;
  ~DistributedVectorGroupedAggregateShuffleJobControlTlsServer();
  DistributedVectorGroupedAggregateShuffleJobControlTlsServer(
      const DistributedVectorGroupedAggregateShuffleJobControlTlsServer&) = delete;
  DistributedVectorGroupedAggregateShuffleJobControlTlsServer&
  operator=(const DistributedVectorGroupedAggregateShuffleJobControlTlsServer&) = delete;
  DistributedVectorGroupedAggregateShuffleJobControlTlsServer(
      DistributedVectorGroupedAggregateShuffleJobControlTlsServer&&) noexcept;
  DistributedVectorGroupedAggregateShuffleJobControlTlsServer&
  operator=(DistributedVectorGroupedAggregateShuffleJobControlTlsServer&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleJobControlTlsServer>
  create(network::TlsSocket socket,
         DistributedVectorGroupedAggregateShuffleJobControlTlsServerConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleJobControlTlsServerState
  state() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleJobControlTlsInterest
  interest() const noexcept;
  [[nodiscard]] TimePoint deadline() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleJobControlTlsServer(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_CONTROL_TLS_HPP_
