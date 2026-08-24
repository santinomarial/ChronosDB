#ifndef CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_QUERY_TLS_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_QUERY_TLS_HPP_

#include "chronos/cluster/distributed_mutable_vector_query_transport.hpp"
#include "chronos/cluster/distributed_vector_query_tls_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tls_socket.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <span>

namespace chronos::cluster {

using DistributedMutableVectorQueryTlsLimits = DistributedVectorQueryTlsLimitsV2;
using DistributedMutableVectorQueryTlsState = DistributedVectorQueryTlsStateV2;
using DistributedMutableVectorQueryTlsInterest = DistributedVectorQueryTlsInterestV2;
using DistributedMutableVectorQueryTlsClientConfig = DistributedVectorQueryTlsClientConfigV2;

struct DistributedMutableVectorQueryTlsServerConfig {
  network::ConnectionAuthenticator* authenticator{};
  DistributedMutableVectorQueryReceiver* receiver{};
  std::array<std::uint8_t, 4> peer_ipv4_address{};
  DistributedMutableVectorQueryTlsLimits limits;
};

// Owns one authenticated mutable-fragment request and its complete correlated response stream.
// The TLS context outlives the socket. One event-loop thread serializes readiness calls.
class DistributedMutableVectorQueryTlsClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedMutableVectorQueryTlsClient() = delete;
  ~DistributedMutableVectorQueryTlsClient();
  DistributedMutableVectorQueryTlsClient(const DistributedMutableVectorQueryTlsClient&) = delete;
  DistributedMutableVectorQueryTlsClient&
  operator=(const DistributedMutableVectorQueryTlsClient&) = delete;
  DistributedMutableVectorQueryTlsClient(DistributedMutableVectorQueryTlsClient&&) noexcept;
  DistributedMutableVectorQueryTlsClient&
  operator=(DistributedMutableVectorQueryTlsClient&&) noexcept;

  [[nodiscard]] static common::Result<DistributedMutableVectorQueryTlsClient>
  create(network::TlsSocket socket, DistributedMutableVectorQueryAttempt attempt,
         DistributedMutableVectorQueryTlsClientConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedMutableVectorQueryTlsState state() const noexcept;
  [[nodiscard]] DistributedMutableVectorQueryTlsInterest interest() const noexcept;
  [[nodiscard]] common::Result<std::span<const DistributedVectorQueryResponseV2>> responses() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedMutableVectorQueryTlsClient(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

// Authenticates one inbound peer before reading the distinct mutable request, invokes the receiver
// once, and writes only its complete bounded response vector. One event-loop thread owns calls.
class DistributedMutableVectorQueryTlsServer {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedMutableVectorQueryTlsServer() = delete;
  ~DistributedMutableVectorQueryTlsServer();
  DistributedMutableVectorQueryTlsServer(const DistributedMutableVectorQueryTlsServer&) = delete;
  DistributedMutableVectorQueryTlsServer&
  operator=(const DistributedMutableVectorQueryTlsServer&) = delete;
  DistributedMutableVectorQueryTlsServer(DistributedMutableVectorQueryTlsServer&&) noexcept;
  DistributedMutableVectorQueryTlsServer&
  operator=(DistributedMutableVectorQueryTlsServer&&) noexcept;

  [[nodiscard]] static common::Result<DistributedMutableVectorQueryTlsServer>
  create(network::TlsSocket socket, DistributedMutableVectorQueryTlsServerConfig config,
         TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedMutableVectorQueryTlsState state() const noexcept;
  [[nodiscard]] DistributedMutableVectorQueryTlsInterest interest() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedMutableVectorQueryTlsServer(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_QUERY_TLS_HPP_
