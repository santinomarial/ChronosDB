#ifndef CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TCP_BATCH_ACQUISITION_HPP_
#define CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TCP_BATCH_ACQUISITION_HPP_

#include "chronos/cluster/raft_read_authority_tcp_acquisition.hpp"
#include "chronos/query/distributed_fragment_binding.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::cluster {

struct RaftReadAuthorityTcpBatchAcquisitionConfig {
  // Canonical unique order by request.group_id. Every request has one common source node and a
  // unique correlation identifier.
  std::vector<RaftReadAuthorityTcpAcquisitionConfig> groups;
  std::size_t maximum_groups{query::DistributedPlanLimits{}.maximum_fragments};
};

struct RaftReadAuthorityTcpBatchAcquisitionMetrics {
  std::size_t total_groups{};
  std::size_t completed_groups{};
  std::size_t active_groups{};
};

enum class RaftReadAuthorityTcpBatchAcquisitionState : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
  kCancelled = 4,
};

// Single-threaded all-groups owner. Every acquisition starts before the batch blocks. One child
// failure cancels every survivor, and no authority is published until all groups complete.
class RaftReadAuthorityTcpBatchAcquisition {
public:
  RaftReadAuthorityTcpBatchAcquisition() noexcept;
  ~RaftReadAuthorityTcpBatchAcquisition();
  RaftReadAuthorityTcpBatchAcquisition(const RaftReadAuthorityTcpBatchAcquisition&) = delete;
  RaftReadAuthorityTcpBatchAcquisition&
  operator=(const RaftReadAuthorityTcpBatchAcquisition&) = delete;
  RaftReadAuthorityTcpBatchAcquisition(RaftReadAuthorityTcpBatchAcquisition&&) noexcept;
  RaftReadAuthorityTcpBatchAcquisition& operator=(RaftReadAuthorityTcpBatchAcquisition&&) noexcept;

  [[nodiscard]] static common::Result<RaftReadAuthorityTcpBatchAcquisition>
  create(RaftReadAuthorityTcpBatchAcquisitionConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();

  [[nodiscard]] RaftReadAuthorityTcpBatchAcquisitionState state() const noexcept;
  [[nodiscard]] RaftReadAuthorityTcpBatchAcquisitionMetrics metrics() const noexcept;
  [[nodiscard]] common::Result<std::vector<query::DistributedVectorGroupReadAuthority>>
  result() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit RaftReadAuthorityTcpBatchAcquisition(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TCP_BATCH_ACQUISITION_HPP_
