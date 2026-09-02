#ifndef CHRONOS_CLUSTER_RAFT_OBSERVATION_TCP_BATCH_ACQUISITION_HPP_
#define CHRONOS_CLUSTER_RAFT_OBSERVATION_TCP_BATCH_ACQUISITION_HPP_

#include "chronos/cluster/raft_observation_tcp_pair_acquisition.hpp"
#include "chronos/query/distributed_vector_fragment.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace chronos::cluster {

struct RaftObservationTcpBatchAcquisitionConfig {
  // Canonical unique order by leader.request.group_id.
  std::vector<RaftObservationTcpPairAcquisitionConfig> pairs;
  std::size_t maximum_pairs{query::DistributedPlanLimits{}.maximum_fragments};
};

struct RaftObservationTcpBatchConstructionConfig {
  raft::NodeId source_node_id{};
  std::uint64_t first_correlation_id{1U};
  std::span<const RaftObservationNodeTlsContext> tls_contexts;
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  RaftObservationRouteResolutionLimits route_limits{};
  RaftObservationTlsClientLimits carrier_limits{};
  std::chrono::milliseconds connect_timeout{5000};
  RaftObservationTcpRetryLimits retry{};
  std::size_t maximum_pairs{query::DistributedPlanLimits{}.maximum_fragments};
};

// Selects one follower per planned group from committed stable placement, resolves every unique
// target once, and packages a canonical batch without opening sockets. The source node is preferred
// when it is a nonleader replica; otherwise the lowest nonleader replica is selected.
[[nodiscard]] common::Result<RaftObservationTcpBatchAcquisitionConfig>
construct_raft_observation_tcp_batch(const query::DistributedAggregatePlan& plan,
                                     const raft::MetadataCatalogSnapshot& catalog,
                                     const RaftObservationTcpBatchConstructionConfig& config);

[[nodiscard]] common::Result<RaftObservationTcpBatchAcquisitionConfig>
construct_raft_observation_tcp_batch(const query::DistributedVectorQueryPlan& plan,
                                     const raft::MetadataCatalogSnapshot& catalog,
                                     const RaftObservationTcpBatchConstructionConfig& config);

struct RaftObservationTcpBatchAcquisitionMetrics {
  std::size_t total_pairs{};
  std::size_t completed_pairs{};
  std::size_t active_pairs{};
};

enum class RaftObservationTcpBatchAcquisitionState : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
  kCancelled = 4,
};

// Single-threaded all-groups owner. Every selected pair begins before the batch blocks. Failure of
// any pair cancels all survivors; results publish only as one complete canonical authority vector.
class RaftObservationTcpBatchAcquisition {
public:
  RaftObservationTcpBatchAcquisition() noexcept;
  ~RaftObservationTcpBatchAcquisition();
  RaftObservationTcpBatchAcquisition(const RaftObservationTcpBatchAcquisition&) = delete;
  RaftObservationTcpBatchAcquisition& operator=(const RaftObservationTcpBatchAcquisition&) = delete;
  RaftObservationTcpBatchAcquisition(RaftObservationTcpBatchAcquisition&&) noexcept;
  RaftObservationTcpBatchAcquisition& operator=(RaftObservationTcpBatchAcquisition&&) noexcept;

  [[nodiscard]] static common::Result<RaftObservationTcpBatchAcquisition>
  create(RaftObservationTcpBatchAcquisitionConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();

  [[nodiscard]] RaftObservationTcpBatchAcquisitionState state() const noexcept;
  [[nodiscard]] RaftObservationTcpBatchAcquisitionMetrics metrics() const noexcept;
  [[nodiscard]] common::Result<std::vector<query::DistributedAggregateFollowerReadAuthority>>
  result() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit RaftObservationTcpBatchAcquisition(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_OBSERVATION_TCP_BATCH_ACQUISITION_HPP_
