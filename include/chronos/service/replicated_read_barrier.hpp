#ifndef CHRONOS_SERVICE_REPLICATED_READ_BARRIER_HPP_
#define CHRONOS_SERVICE_REPLICATED_READ_BARRIER_HPP_

#include "chronos/cluster/raft_transport_runtime.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_fragment_binding.hpp"
#include "chronos/raft/async_durable_runtime.hpp"

#include <chrono>
#include <cstddef>
#include <memory>
#include <span>
#include <vector>

namespace chronos::service {

class ReplicatedRaftTransportRuntime;

struct ReplicatedReadBarrierLimits {
  std::size_t maximum_groups{4096U};
  std::chrono::milliseconds request_timeout{5000};
};

using ReplicatedReadAuthority = query::DistributedAggregateGroupReadAuthority;

// Bounded synchronous query gate for one stable vector of per-group leader read indexes. Local
// mode submits directly to an asynchronous durable runtime and is valid only for groups that can
// complete a barrier in that submission (normally one-voter groups). Transported mode has one
// query-thread waiter and one poll owner: poll_owner_drive() admits ordered application work and
// poll_owner_observe() correlates its completion plus later peer responses by exact group, term,
// and context. A completed vector still requires the application snapshot to prove applied-index
// coverage before rows become visible.
class ReplicatedReadBarrier {
public:
  ReplicatedReadBarrier() = delete;
  ~ReplicatedReadBarrier();
  ReplicatedReadBarrier(const ReplicatedReadBarrier&) = delete;
  ReplicatedReadBarrier& operator=(const ReplicatedReadBarrier&) = delete;
  ReplicatedReadBarrier(ReplicatedReadBarrier&&) noexcept;
  ReplicatedReadBarrier& operator=(ReplicatedReadBarrier&&) noexcept;

  [[nodiscard]] static common::Result<ReplicatedReadBarrier>
  create_local(raft::AsyncDurableMultiRaftRuntime* runtime, std::vector<raft::GroupId> groups,
               ReplicatedReadBarrierLimits limits = {});
  [[nodiscard]] static common::Result<ReplicatedReadBarrier>
  create_transported(std::vector<raft::GroupId> groups, ReplicatedReadBarrierLimits limits = {});

  // Query-thread-only. At most one call may be active. The returned group vector is sorted and
  // contains exactly the configured groups.
  [[nodiscard]] common::Result<std::vector<raft::GroupReadBarrier>> await();
  // Captures the ordered current-leader observation that exact-validated each completed barrier.
  // Existing barrier-only callers do not allocate or retain these observation copies.
  [[nodiscard]] common::Result<std::vector<ReplicatedReadAuthority>> await_authority();

  // Transport-poll-owner-only. drive() is nonblocking and admits at most one operation per group
  // per call. observe() must receive every completed transport result in FIFO order.
  [[nodiscard]] common::Status poll_owner_drive(ReplicatedRaftTransportRuntime& transport);
  [[nodiscard]] common::Status
  poll_owner_observe(const cluster::RaftTransportRuntimeResult& result);

  // Idempotently rejects new waits and wakes the current waiter. The owner must call this before
  // destroying the borrowed durable or transport runtime.
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] std::span<const raft::GroupId> groups() const noexcept;

private:
  class Impl;
  explicit ReplicatedReadBarrier(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_READ_BARRIER_HPP_
