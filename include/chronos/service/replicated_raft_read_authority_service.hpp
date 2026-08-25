#ifndef CHRONOS_SERVICE_REPLICATED_RAFT_READ_AUTHORITY_SERVICE_HPP_
#define CHRONOS_SERVICE_REPLICATED_RAFT_READ_AUTHORITY_SERVICE_HPP_

#include "chronos/cluster/raft_read_authority_transport.hpp"
#include "chronos/common/result.hpp"
#include "chronos/service/replicated_read_barrier.hpp"

namespace chronos::service {

// Synchronous adapter for one authenticated remote request. The borrowed read-barrier owner must
// outlive this service. acquire() runs on a non-poll thread because the transport poll owner must
// continue driving the quorum operation. Concurrent callers retain the barrier's one-waiter bound.
class ReplicatedRaftReadAuthorityService final : public cluster::RaftReadAuthorityService {
public:
  ReplicatedRaftReadAuthorityService() = delete;

  [[nodiscard]] static common::Result<ReplicatedRaftReadAuthorityService>
  create(ReplicatedReadBarrier* read_barrier);
  [[nodiscard]] common::Result<cluster::RaftReadAuthority>
  acquire(const raft::GroupId& group_id) override;

private:
  explicit ReplicatedRaftReadAuthorityService(ReplicatedReadBarrier* read_barrier) noexcept;
  ReplicatedReadBarrier* read_barrier_{};
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_RAFT_READ_AUTHORITY_SERVICE_HPP_
