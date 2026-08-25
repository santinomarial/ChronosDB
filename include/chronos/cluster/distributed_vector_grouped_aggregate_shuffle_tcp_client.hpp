#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_TCP_CLIENT_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_TCP_CLIENT_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_retry.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tls.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct DistributedVectorGroupedAggregateShuffleTcpClientConfig {
  network::Ipv4Endpoint remote_endpoint;
  const network::TlsClientContext* tls_context{};
  DistributedVectorGroupedAggregateShuffleTlsClientConfig carrier;
  std::chrono::milliseconds connect_timeout{5000};
};

enum class DistributedVectorGroupedAggregateShuffleTcpClientState : std::uint8_t {
  kConnecting = 1,
  kExchanging = 2,
  kComplete = 3,
  kFailed = 4,
};

// Owns one immutable grouped-shuffle attempt through nonblocking TCP connect and mutual TLS. One
// event-loop thread serializes calls. The authority, TLS context, authenticator, and node
// authorizer are borrowed and must outlive the client.
class DistributedVectorGroupedAggregateShuffleTcpClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorGroupedAggregateShuffleTcpClient() = delete;
  ~DistributedVectorGroupedAggregateShuffleTcpClient();
  DistributedVectorGroupedAggregateShuffleTcpClient(
      const DistributedVectorGroupedAggregateShuffleTcpClient&) = delete;
  DistributedVectorGroupedAggregateShuffleTcpClient&
  operator=(const DistributedVectorGroupedAggregateShuffleTcpClient&) = delete;
  DistributedVectorGroupedAggregateShuffleTcpClient(
      DistributedVectorGroupedAggregateShuffleTcpClient&&) noexcept;
  DistributedVectorGroupedAggregateShuffleTcpClient&
  operator=(DistributedVectorGroupedAggregateShuffleTcpClient&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleTcpClient>
  begin(DistributedVectorGroupedAggregateShuffleAttempt attempt,
        const DistributedVectorGroupedAggregateShuffleAuthority& authority,
        DistributedVectorGroupedAggregateShuffleTcpClientConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);

  [[nodiscard]] DistributedVectorGroupedAggregateShuffleTcpClientState state() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleTlsInterest interest() const noexcept;
  [[nodiscard]] int descriptor() const noexcept;
  [[nodiscard]] std::size_t attempt_number() const noexcept;
  [[nodiscard]] raft::NodeId target_node_id() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleTcpClient(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_TCP_CLIENT_HPP_
