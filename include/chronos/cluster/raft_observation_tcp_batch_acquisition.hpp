#ifndef CHRONOS_CLUSTER_RAFT_OBSERVATION_TCP_BATCH_ACQUISITION_HPP_
#define CHRONOS_CLUSTER_RAFT_OBSERVATION_TCP_BATCH_ACQUISITION_HPP_

#include "chronos/cluster/raft_observation_tcp_pair_acquisition.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::cluster {

struct RaftObservationTcpBatchAcquisitionConfig {
  // Canonical unique order by leader.request.group_id.
  std::vector<RaftObservationTcpPairAcquisitionConfig> pairs;
  std::size_t maximum_pairs{query::DistributedPlanLimits{}.maximum_fragments};
};

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
