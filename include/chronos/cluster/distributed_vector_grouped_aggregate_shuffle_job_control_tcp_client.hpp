#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_CONTROL_TCP_CLIENT_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_CONTROL_TCP_CLIENT_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_tls.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>

namespace chronos::cluster {

struct DistributedVectorGroupedAggregateShuffleJobControlTcpClientConfig {
  network::Ipv4Endpoint remote_endpoint;
  const network::TlsClientContext* tls_context{};
  DistributedVectorGroupedAggregateShuffleJobControlTlsClientConfig carrier;
  std::chrono::milliseconds connect_timeout{5000};
};

enum class DistributedVectorGroupedAggregateShuffleJobControlTcpClientState : std::uint8_t {
  kConnecting = 1,
  kExchanging = 2,
  kComplete = 3,
  kFailed = 4,
};

// Owns one nonblocking TCP connection and one exact reducer-job control exchange. One event-loop
// thread serializes calls. TLS, authentication, and node-authorization dependencies are borrowed
// and must outlive the client. Validation completes before socket acquisition.
class DistributedVectorGroupedAggregateShuffleJobControlTcpClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorGroupedAggregateShuffleJobControlTcpClient() = delete;
  ~DistributedVectorGroupedAggregateShuffleJobControlTcpClient();
  DistributedVectorGroupedAggregateShuffleJobControlTcpClient(
      const DistributedVectorGroupedAggregateShuffleJobControlTcpClient&) = delete;
  DistributedVectorGroupedAggregateShuffleJobControlTcpClient&
  operator=(const DistributedVectorGroupedAggregateShuffleJobControlTcpClient&) = delete;
  DistributedVectorGroupedAggregateShuffleJobControlTcpClient(
      DistributedVectorGroupedAggregateShuffleJobControlTcpClient&&) noexcept;
  DistributedVectorGroupedAggregateShuffleJobControlTcpClient&
  operator=(DistributedVectorGroupedAggregateShuffleJobControlTcpClient&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleJobControlTcpClient>
  begin(DistributedVectorGroupedAggregateShuffleJobControlTcpClientConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleJobControlTcpClientState
  state() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleJobControlTlsInterest
  interest() const noexcept;
  [[nodiscard]] int descriptor() const noexcept;
  [[nodiscard]] std::optional<TimePoint> deadline() const noexcept;
  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponse>
  result() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleJobControlTcpClient(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_CONTROL_TCP_CLIENT_HPP_
