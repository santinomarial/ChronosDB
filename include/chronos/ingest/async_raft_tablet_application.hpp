#ifndef CHRONOS_INGEST_ASYNC_RAFT_TABLET_APPLICATION_HPP_
#define CHRONOS_INGEST_ASYNC_RAFT_TABLET_APPLICATION_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/raft_tablet_state_machine.hpp"
#include "chronos/raft/async_durable_runtime.hpp"

#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace chronos::ingest {

namespace detail {
class AsyncRaftTabletQuorumCompletionState;
}

struct AsyncRaftTabletApplicationConfig {
  raft::GroupId group_id;
  std::optional<RaftTabletSnapshotStorage> snapshot_storage;
  RetryDirectory retry_directory;
  TabletState tablet;
  std::vector<std::shared_ptr<const schema::TableSchema>> retained_schemas;
  ColumnarAppendDecodeLimits decode_limits;
};

struct AsyncRaftTabletApplicationLimits {
  std::size_t maximum_tablets{4096U};
  std::size_t maximum_pending_quorum_completions{65'536U};
};

// Single-consumer exact applied-quorum result. Dropping this owner cancels the request without
// retaining application capacity. wait() must not run on the durable Raft worker.
class AsyncRaftTabletQuorumCompletion {
public:
  AsyncRaftTabletQuorumCompletion() noexcept;
  ~AsyncRaftTabletQuorumCompletion();
  AsyncRaftTabletQuorumCompletion(const AsyncRaftTabletQuorumCompletion&) = delete;
  AsyncRaftTabletQuorumCompletion& operator=(const AsyncRaftTabletQuorumCompletion&) = delete;
  AsyncRaftTabletQuorumCompletion(AsyncRaftTabletQuorumCompletion&&) noexcept;
  AsyncRaftTabletQuorumCompletion& operator=(AsyncRaftTabletQuorumCompletion&&) noexcept;

  [[nodiscard]] bool is_valid() const noexcept;
  [[nodiscard]] bool is_ready() const;
  [[nodiscard]] const raft::GroupId& group_id() const noexcept;
  [[nodiscard]] raft::Term leader_term() const noexcept;
  [[nodiscard]] raft::LogIndex log_index() const noexcept;
  [[nodiscard]] common::Result<raft::QuorumSyncReceipt> wait();

private:
  AsyncRaftTabletQuorumCompletion(
      std::shared_ptr<detail::AsyncRaftTabletQuorumCompletionState> state, raft::GroupId group_id,
      raft::Term leader_term, raft::LogIndex log_index) noexcept;
  std::shared_ptr<detail::AsyncRaftTabletQuorumCompletionState> state_;
  raft::GroupId group_id_;
  raft::Term leader_term_{};
  raft::LogIndex log_index_{};
  friend class AsyncRaftTabletApplication;
};

// Concrete worker extension that owns every configured tablet state machine on the same thread as
// DurableMultiRaftRuntime. External readers receive pinned immutable snapshots or copied receipts;
// no machine, mutable tablet, or synchronous runtime reference escapes the worker.
class AsyncRaftTabletApplication final : public raft::AsyncDurableRaftWorkerExtension {
public:
  AsyncRaftTabletApplication(const AsyncRaftTabletApplication&) = delete;
  AsyncRaftTabletApplication& operator=(const AsyncRaftTabletApplication&) = delete;
  ~AsyncRaftTabletApplication() override;

  [[nodiscard]] static common::Result<std::shared_ptr<AsyncRaftTabletApplication>>
  create(std::vector<AsyncRaftTabletApplicationConfig> tablets,
         AsyncRaftTabletApplicationLimits limits = {});

  [[nodiscard]] common::Result<TabletSnapshot> snapshot(const raft::GroupId& group_id) const;
  [[nodiscard]] std::optional<raft::QuorumSyncReceipt>
  latest_quorum_sync_receipt(const raft::GroupId& group_id) const;
  // Registers an exact receipt waiter before enqueuing an ordered observation. If the entry is
  // already applied, that observation resolves it; otherwise a later batch that commits/applies
  // this group does. Admission rejection registers no live waiter.
  [[nodiscard]] common::Result<AsyncRaftTabletQuorumCompletion>
  request_quorum_sync(raft::AsyncDurableMultiRaftRuntime& runtime, const raft::GroupId& group_id,
                      raft::Term required_leader_term, raft::LogIndex log_index);
  [[nodiscard]] std::size_t tablet_count() const;
  [[nodiscard]] std::size_t pending_quorum_completions() const;
  [[nodiscard]] bool initialized() const;
  [[nodiscard]] bool failed() const;
  [[nodiscard]] common::Status failure_status() const;

  [[nodiscard]] common::Status initialize(raft::DurableMultiRaftRuntime& runtime) override;
  [[nodiscard]] common::Result<std::unique_ptr<raft::AsyncDurableRaftWorkerBatchContext>>
  prepare_batch(raft::DurableMultiRaftRuntime& runtime,
                std::span<const raft::DurableRaftRequest> requests) override;
  [[nodiscard]] common::Status
  complete_batch(raft::DurableMultiRaftRuntime& runtime,
                 std::unique_ptr<raft::AsyncDurableRaftWorkerBatchContext> context,
                 std::span<const raft::DurableRaftResult> results) override;
  [[nodiscard]] common::Status shutdown(raft::DurableMultiRaftRuntime& runtime) override;

private:
  class Impl;
  explicit AsyncRaftTabletApplication(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_ASYNC_RAFT_TABLET_APPLICATION_HPP_
