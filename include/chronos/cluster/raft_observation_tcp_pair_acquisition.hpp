#ifndef CHRONOS_CLUSTER_RAFT_OBSERVATION_TCP_PAIR_ACQUISITION_HPP_
#define CHRONOS_CLUSTER_RAFT_OBSERVATION_TCP_PAIR_ACQUISITION_HPP_

#include "chronos/cluster/raft_observation_tcp_acquisition.hpp"
#include "chronos/query/distributed_fragment_binding.hpp"

#include <chrono>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct RaftObservationTcpPairAcquisitionConfig {
  RaftObservationTcpAcquisitionConfig leader;
  RaftObservationTcpAcquisitionConfig follower;
};

struct RaftObservationTcpPairAcquisitionMetrics {
  RaftObservationTcpAcquisitionMetrics leader;
  RaftObservationTcpAcquisitionMetrics follower;
};

enum class RaftObservationTcpPairAcquisitionState : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
  kCancelled = 4,
};

// Single-threaded poll owner for one selected leader/follower observation pair. Both independent
// finite acquisitions are driven concurrently. No observation escapes until the complete pair
// satisfies the bounded-stale authority correlation contract.
class RaftObservationTcpPairAcquisition {
public:
  RaftObservationTcpPairAcquisition() noexcept;
  ~RaftObservationTcpPairAcquisition();
  RaftObservationTcpPairAcquisition(const RaftObservationTcpPairAcquisition&) = delete;
  RaftObservationTcpPairAcquisition& operator=(const RaftObservationTcpPairAcquisition&) = delete;
  RaftObservationTcpPairAcquisition(RaftObservationTcpPairAcquisition&&) noexcept;
  RaftObservationTcpPairAcquisition& operator=(RaftObservationTcpPairAcquisition&&) noexcept;

  [[nodiscard]] static common::Result<RaftObservationTcpPairAcquisition>
  create(RaftObservationTcpPairAcquisitionConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();

  [[nodiscard]] RaftObservationTcpPairAcquisitionState state() const noexcept;
  [[nodiscard]] RaftObservationTcpPairAcquisitionMetrics metrics() const noexcept;
  [[nodiscard]] common::Result<query::DistributedAggregateFollowerReadAuthority> result() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit RaftObservationTcpPairAcquisition(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_OBSERVATION_TCP_PAIR_ACQUISITION_HPP_
