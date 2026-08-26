#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_SERVICE_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_SERVICE_HPP_

#include "chronos/cluster/distributed_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_destination_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tcp_execution.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

namespace chronos::cluster {

inline constexpr std::size_t kMaximumDistributedVectorGroupedAggregateShuffleJobQueryMemoryBytes =
    std::size_t{1024U} * 1024U * 1024U;

struct DistributedVectorGroupedAggregateShuffleJobServiceConfig {
  raft::NodeId local_node_id{};
  network::TcpListenerConfig shuffle_listener;
  network::TlsServerConfig shuffle_tls;
  network::ConnectionAuthenticator* shuffle_authenticator{};
  network::ConnectionAuthenticator* result_authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::span<const DistributedQueryNodeTlsContext> result_tls_contexts;
  DistributedVectorGroupedAggregateShuffleTlsLimits shuffle_carrier_limits;
  DistributedVectorGroupedAggregateShuffleReducerLimits reducer_limits;
  DistributedVectorGroupedAggregateShuffleResultRetryLimits result_retry_limits;
  DistributedVectorGroupedAggregateShuffleResultTlsLimits result_carrier_limits;
  network::QueryResultLimits result_batch_limits;
  std::chrono::milliseconds result_connect_timeout{5000};
  std::size_t maximum_jobs{64U};
  std::size_t maximum_job_query_memory_bytes{std::size_t{256U} * 1024U * 1024U};
  std::size_t maximum_retained_streams_per_job{1024U};
  std::size_t maximum_accepts_per_job_poll{32U};
  std::size_t maximum_reducer_admissions_per_job_poll{1024U};
};

struct DistributedVectorGroupedAggregateShuffleJobServiceMetrics {
  std::uint64_t prepare_requests{};
  std::uint64_t duplicate_prepares{};
  std::uint64_t conflicting_prepares{};
  std::uint64_t seal_requests{};
  std::uint64_t completed_jobs{};
  std::uint64_t failed_jobs{};
  std::uint64_t cancelled_jobs{};
  std::size_t active_jobs{};
  std::size_t transmitting_jobs{};
};

// Owns a finite set of reducer jobs after an authenticated control carrier has decoded a request.
// One caller thread serializes receive, local delivery, polling, and cancellation. Configuration
// security/TLS dependencies are borrowed and must outlive the service.
class DistributedVectorGroupedAggregateShuffleJobService {
public:
  DistributedVectorGroupedAggregateShuffleJobService() noexcept;
  ~DistributedVectorGroupedAggregateShuffleJobService();
  DistributedVectorGroupedAggregateShuffleJobService(
      const DistributedVectorGroupedAggregateShuffleJobService&) = delete;
  DistributedVectorGroupedAggregateShuffleJobService&
  operator=(const DistributedVectorGroupedAggregateShuffleJobService&) = delete;
  DistributedVectorGroupedAggregateShuffleJobService(
      DistributedVectorGroupedAggregateShuffleJobService&&) noexcept;
  DistributedVectorGroupedAggregateShuffleJobService&
  operator=(DistributedVectorGroupedAggregateShuffleJobService&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleJobService>
  create(DistributedVectorGroupedAggregateShuffleJobServiceConfig config);

  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponse>
  receive(DistributedVectorGroupedAggregateShuffleJobControlRequest request,
          const network::PeerAuthenticationResult& authenticated_peer,
          std::chrono::steady_clock::time_point now);
  [[nodiscard]] common::Status
  accept_local_stream(const common::Uuid& query_id,
                      const DistributedVectorGroupedAggregateShuffleCompleteStream& stream);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait,
                                         std::chrono::steady_clock::time_point now);
  [[nodiscard]] common::Status cancel(const common::Uuid& query_id);

  [[nodiscard]] DistributedVectorGroupedAggregateShuffleJobServiceMetrics metrics() const noexcept;

private:
  class Impl;
  explicit DistributedVectorGroupedAggregateShuffleJobService(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_SERVICE_HPP_
