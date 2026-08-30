#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_CONTROL_TCP_ACQUISITION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_CONTROL_TCP_ACQUISITION_HPP_

#include "chronos/cluster/distributed_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_tcp_client.hpp"
#include "chronos/common/result.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

namespace chronos::cluster {

struct DistributedVectorGroupedAggregateShuffleJobControlTcpRetryLimits {
  std::size_t maximum_attempts{5U};
  std::chrono::milliseconds initial_backoff{50};
  std::chrono::milliseconds maximum_backoff{5000};
};

struct DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionConfig {
  DistributedQueryNodeRoute route;
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  DistributedVectorGroupedAggregateShuffleJobControlRequest request;
  DistributedVectorGroupedAggregateShuffleJobControlTlsLimits carrier_limits;
  std::chrono::milliseconds connect_timeout{5000};
  DistributedVectorGroupedAggregateShuffleJobControlTcpRetryLimits retry;
  std::optional<std::chrono::steady_clock::time_point> execution_deadline;
  bool retry_unavailable_response{};
};

struct DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionMetrics {
  std::uint64_t attempts_started{};
  std::uint64_t retries_started{};
  std::uint64_t completed_attempts{};
  std::uint64_t failed_attempts{};
  std::size_t active_attempts{};
};

enum class DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
  kCancelled = 4,
};

// Owns finite whole-exchange retries for one immutable PREPARE, INSTALL_ROUTES, SEAL, CANCEL, or
// RENEW_LEASE.
// Address rotation never
// changes node, TLS, query, coordinator, target, authority, schema, or action. Only SEAL may opt
// into retrying a correlated UNAVAILABLE response while the reducer drains admitted sources.
class DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition() noexcept;
  ~DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition();
  DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition(
      const DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition&) = delete;
  DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition&
  operator=(const DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition&) = delete;
  DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition(
      DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition&&) noexcept;
  DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition&
  operator=(DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition&&) noexcept;

  [[nodiscard]] static common::Result<
      DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition>
  create(DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState
  state() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionMetrics
  metrics() const noexcept;
  [[nodiscard]] int descriptor() const noexcept;
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleJobControlTlsInterest
  interest() const noexcept;
  [[nodiscard]] std::optional<TimePoint> wake_deadline() const noexcept;
  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponse>
  result() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_CONTROL_TCP_ACQUISITION_HPP_
