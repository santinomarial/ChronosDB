#ifndef CHRONOS_CLUSTER_DISTRIBUTED_GROUPED_QUERY_TLS_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_GROUPED_QUERY_TLS_HPP_

#include "chronos/cluster/distributed_grouped_query_transport.hpp"
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

struct DistributedGroupedQueryAttempt {
  std::size_t attempt_number{};
  raft::NodeId target_node_id{};
  std::vector<std::byte> request_bytes;
};

struct DistributedGroupedQueryTlsLimits {
  std::chrono::milliseconds handshake_timeout{5000};
  std::chrono::milliseconds exchange_timeout{30000};
  std::size_t maximum_response_frames{1024U};
};

struct DistributedGroupedQueryTlsClientConfig {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::array<std::uint8_t, 4> peer_ipv4_address{};
  DistributedGroupedQueryTlsLimits limits;
};

struct DistributedGroupedQueryTlsServerConfig {
  network::ConnectionAuthenticator* authenticator{};
  DistributedGroupedQueryReceiver* receiver{};
  std::array<std::uint8_t, 4> peer_ipv4_address{};
  DistributedGroupedQueryTlsLimits limits;
};

enum class DistributedGroupedQueryTlsState : std::uint8_t {
  kHandshaking = 1,
  kWritingRequest = 2,
  kReadingRequest = 3,
  kReadingResponses = 4,
  kWritingResponses = 5,
  kComplete = 6,
  kFailed = 7,
};

struct DistributedGroupedQueryTlsInterest {
  bool want_read{};
  bool want_write{};
};

// Owns one authenticated grouped request and its complete ordered response stream over one
// connected nonblocking TLS socket. One event-loop thread serializes calls.
class DistributedGroupedQueryTlsClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedGroupedQueryTlsClient() = delete;
  ~DistributedGroupedQueryTlsClient();
  DistributedGroupedQueryTlsClient(const DistributedGroupedQueryTlsClient&) = delete;
  DistributedGroupedQueryTlsClient& operator=(const DistributedGroupedQueryTlsClient&) = delete;
  DistributedGroupedQueryTlsClient(DistributedGroupedQueryTlsClient&&) noexcept;
  DistributedGroupedQueryTlsClient& operator=(DistributedGroupedQueryTlsClient&&) noexcept;

  [[nodiscard]] static common::Result<DistributedGroupedQueryTlsClient>
  create(network::TlsSocket socket, DistributedGroupedQueryAttempt attempt,
         DistributedGroupedQueryTlsClientConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedGroupedQueryTlsState state() const noexcept;
  [[nodiscard]] DistributedGroupedQueryTlsInterest interest() const noexcept;
  [[nodiscard]] common::Result<std::span<const DistributedGroupedQueryResponse>> responses() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedGroupedQueryTlsClient(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

// Owns one authenticated inbound grouped request and writes every receiver-produced response in
// exact order before completion. One event-loop thread serializes calls.
class DistributedGroupedQueryTlsServer {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedGroupedQueryTlsServer() = delete;
  ~DistributedGroupedQueryTlsServer();
  DistributedGroupedQueryTlsServer(const DistributedGroupedQueryTlsServer&) = delete;
  DistributedGroupedQueryTlsServer& operator=(const DistributedGroupedQueryTlsServer&) = delete;
  DistributedGroupedQueryTlsServer(DistributedGroupedQueryTlsServer&&) noexcept;
  DistributedGroupedQueryTlsServer& operator=(DistributedGroupedQueryTlsServer&&) noexcept;

  [[nodiscard]] static common::Result<DistributedGroupedQueryTlsServer>
  create(network::TlsSocket socket, DistributedGroupedQueryTlsServerConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedGroupedQueryTlsState state() const noexcept;
  [[nodiscard]] DistributedGroupedQueryTlsInterest interest() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedGroupedQueryTlsServer(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_GROUPED_QUERY_TLS_HPP_
