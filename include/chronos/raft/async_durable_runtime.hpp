#ifndef CHRONOS_RAFT_ASYNC_DURABLE_RUNTIME_HPP_
#define CHRONOS_RAFT_ASYNC_DURABLE_RUNTIME_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/durable_runtime.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace chronos::raft {

// Opaque per-batch state prepared by a worker extension before the durable runtime consumes and
// moves request payloads. Its destructor runs on the durable worker unless preparation itself
// fails.
class AsyncDurableRaftWorkerBatchContext {
public:
  virtual ~AsyncDurableRaftWorkerBatchContext() = default;
};

// Optional worker-affine composition seam for committed application owners. Every method is
// invoked serially on the one thread that owns DurableMultiRaftRuntime. initialize() completes
// before create/open returns. prepare_batch() runs immediately before durable execution;
// complete_batch() runs after its persistence boundary and before the caller's completion is
// published. A failure from any hook fails the asynchronous owner closed. Implementations must not
// wait on this same asynchronous runtime from a hook; they may use the supplied synchronous owner.
class AsyncDurableRaftWorkerExtension {
public:
  virtual ~AsyncDurableRaftWorkerExtension() = default;

  // Immutable membership query used by higher-level owners to prove that their extension is
  // hosted by the same worker. Composite extensions override this for their direct children.
  [[nodiscard]] virtual bool
  contains_worker_extension(const AsyncDurableRaftWorkerExtension& candidate) const noexcept {
    return this == &candidate;
  }

  [[nodiscard]] virtual common::Status initialize(DurableMultiRaftRuntime& runtime) = 0;
  [[nodiscard]] virtual common::Result<std::unique_ptr<AsyncDurableRaftWorkerBatchContext>>
  prepare_batch(DurableMultiRaftRuntime& runtime, std::span<const DurableRaftRequest> requests) = 0;
  [[nodiscard]] virtual common::Status
  complete_batch(DurableMultiRaftRuntime& runtime,
                 std::unique_ptr<AsyncDurableRaftWorkerBatchContext> context,
                 std::span<const DurableRaftResult> results) = 0;
  [[nodiscard]] virtual common::Status shutdown(DurableMultiRaftRuntime& runtime) = 0;
};

struct AsyncDurableMultiRaftLimits {
  std::size_t maximum_pending_batches{1024U};
  std::size_t maximum_pending_operations{65'536U};
  DurableMultiRaftLimits durable;
};

struct AsyncDurableMultiRaftMetrics {
  std::uint64_t admitted_batches{};
  std::uint64_t rejected_batches{};
  std::uint64_t completed_batches{};
  std::uint64_t failed_batches{};
  std::uint64_t admitted_reclamations{};
  std::uint64_t rejected_reclamations{};
  std::uint64_t completed_reclamations{};
  std::uint64_t failed_reclamations{};
  // Worker notification attempts that either wrote one byte or coalesced against a full,
  // already-readable pipe. These counters do not publish completion state.
  std::uint64_t written_completion_notifications{};
  std::uint64_t coalesced_completion_notifications{};
  std::size_t pending_batches{};
  std::size_t pending_operations{};
  std::size_t high_water_pending_batches{};
  std::size_t high_water_pending_operations{};
  bool accepting{};
  bool terminal_failure{};
};

namespace detail {
class AsyncDurableRaftCompletionState;
class AsyncRaftLogReclamationCompletionState;
} // namespace detail

// Owning single-consumer completion for one admitted batch. It may outlive the worker. wait() is
// the acquire edge and moves the potentially large transition batch out exactly once.
class AsyncDurableRaftCompletion {
public:
  AsyncDurableRaftCompletion() noexcept;
  ~AsyncDurableRaftCompletion();
  AsyncDurableRaftCompletion(const AsyncDurableRaftCompletion&) = delete;
  AsyncDurableRaftCompletion& operator=(const AsyncDurableRaftCompletion&) = delete;
  AsyncDurableRaftCompletion(AsyncDurableRaftCompletion&&) noexcept;
  AsyncDurableRaftCompletion& operator=(AsyncDurableRaftCompletion&&) noexcept;

  [[nodiscard]] bool is_valid() const noexcept;
  [[nodiscard]] std::uint64_t submission_sequence() const noexcept;
  [[nodiscard]] bool is_ready() const;
  [[nodiscard]] common::Result<std::vector<DurableRaftResult>> wait();

private:
  explicit AsyncDurableRaftCompletion(
      std::shared_ptr<detail::AsyncDurableRaftCompletionState> state,
      std::uint64_t submission_sequence) noexcept;
  std::shared_ptr<detail::AsyncDurableRaftCompletionState> state_;
  std::uint64_t submission_sequence_{};
  friend class AsyncDurableMultiRaftRuntime;
};

// Single-consumer completion for one node-wide physical-log checkpoint/reclamation request.
// wait() must not run on the durable worker. Dropping the owner does not cancel admitted work.
class AsyncRaftLogReclamationCompletion {
public:
  AsyncRaftLogReclamationCompletion() noexcept;
  ~AsyncRaftLogReclamationCompletion();
  AsyncRaftLogReclamationCompletion(const AsyncRaftLogReclamationCompletion&) = delete;
  AsyncRaftLogReclamationCompletion& operator=(const AsyncRaftLogReclamationCompletion&) = delete;
  AsyncRaftLogReclamationCompletion(AsyncRaftLogReclamationCompletion&&) noexcept;
  AsyncRaftLogReclamationCompletion& operator=(AsyncRaftLogReclamationCompletion&&) noexcept;

  [[nodiscard]] bool is_valid() const noexcept;
  [[nodiscard]] std::uint64_t submission_sequence() const noexcept;
  [[nodiscard]] bool is_ready() const;
  [[nodiscard]] common::Result<RaftPersistentLogReclamation> wait();

private:
  explicit AsyncRaftLogReclamationCompletion(
      std::shared_ptr<detail::AsyncRaftLogReclamationCompletionState> state,
      std::uint64_t submission_sequence) noexcept;
  std::shared_ptr<detail::AsyncRaftLogReclamationCompletionState> state_;
  std::uint64_t submission_sequence_{};
  friend class AsyncDurableMultiRaftRuntime;
};

// One background thread exclusively owns DurableMultiRaftRuntime and its physical log. Producers
// publish complete batches through a bounded mutex-protected FIFO and never call the durable owner.
class AsyncDurableMultiRaftRuntime {
public:
  AsyncDurableMultiRaftRuntime() noexcept;
  ~AsyncDurableMultiRaftRuntime();
  AsyncDurableMultiRaftRuntime(const AsyncDurableMultiRaftRuntime&) = delete;
  AsyncDurableMultiRaftRuntime& operator=(const AsyncDurableMultiRaftRuntime&) = delete;
  AsyncDurableMultiRaftRuntime(AsyncDurableMultiRaftRuntime&&) noexcept;
  AsyncDurableMultiRaftRuntime& operator=(AsyncDurableMultiRaftRuntime&&) noexcept;

  [[nodiscard]] static common::Result<AsyncDurableMultiRaftRuntime>
  create_new(NodeId local_node_id, const RaftPersistentLogConfig& log_config,
             std::vector<RaftGroupConfiguration> groups, AsyncDurableMultiRaftLimits limits = {},
             std::shared_ptr<AsyncDurableRaftWorkerExtension> extension = nullptr);
  [[nodiscard]] static common::Result<AsyncDurableMultiRaftRuntime>
  open_existing(NodeId local_node_id, const RaftPersistentLogConfig& log_config,
                const RaftPersistentLogOpenOptions& open_options,
                std::vector<RaftGroupConfiguration> groups, AsyncDurableMultiRaftLimits limits = {},
                std::shared_ptr<AsyncDurableRaftWorkerExtension> extension = nullptr);

  // Admission is nonblocking. Invalid size or aggregate outbound reservation is rejected before
  // the operation vector moves into worker ownership.
  [[nodiscard]] common::Result<AsyncDurableRaftCompletion>
  try_submit(std::vector<DurableRaftRequest> requests);

  // Enqueues one bounded owning observation behind all previously admitted work. The returned
  // completion contains exactly one result with observation set and transition empty.
  [[nodiscard]] common::Result<AsyncDurableRaftCompletion>
  try_observe_group(const GroupId& group_id);

  // Enqueues one worker-affine node-wide full-state checkpoint. The durable runtime installs and
  // synchronizes the recovery anchor before removing older shared-log segments.
  [[nodiscard]] common::Result<AsyncRaftLogReclamationCompletion> try_checkpoint_and_reclaim();

  // Idempotently stops admission, drains all accepted batches in FIFO order, closes the log, and
  // joins the worker. A terminal worker failure rejects all not-yet-executed accepted batches.
  [[nodiscard]] common::Status shutdown();
  // Borrowed nonblocking descriptor readable after one or more completions are published. A single
  // event-loop consumer drains it, then inspects every completion owner that it coordinates.
  [[nodiscard]] int completion_descriptor() const noexcept;
  [[nodiscard]] common::Status drain_completion_notifications();
  [[nodiscard]] AsyncDurableMultiRaftMetrics metrics() const;
  [[nodiscard]] bool is_accepting() const;
  // Immutable identity check for higher-level owners that must enqueue follow-up work on the same
  // worker that hosts their extension.
  [[nodiscard]] bool
  owns_worker_extension(const AsyncDurableRaftWorkerExtension& extension) const noexcept;
  [[nodiscard]] common::Status terminal_status() const;

private:
  class Impl;
  explicit AsyncDurableMultiRaftRuntime(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_ASYNC_DURABLE_RUNTIME_HPP_
