#include "chronos/service/replicated_raft_read_authority_service.hpp"

#include <utility>

namespace chronos::service {

ReplicatedRaftReadAuthorityService::ReplicatedRaftReadAuthorityService(
    ReplicatedReadBarrier* const read_barrier) noexcept
    : read_barrier_(read_barrier) {}

common::Result<ReplicatedRaftReadAuthorityService>
ReplicatedRaftReadAuthorityService::create(ReplicatedReadBarrier* const read_barrier) {
  if (read_barrier == nullptr) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInvalidArgument, "replicated Raft read-authority barrier is null"});
  }
  return ReplicatedRaftReadAuthorityService{read_barrier};
}

common::Result<cluster::RaftReadAuthority>
ReplicatedRaftReadAuthorityService::acquire(const raft::GroupId& group_id) {
  auto authority = read_barrier_->await_group_authority(group_id);
  if (!authority.has_value())
    return common::make_unexpected(authority.error());
  return cluster::RaftReadAuthority{.barrier = authority->barrier,
                                    .observation = std::move(authority->observation)};
}

} // namespace chronos::service
