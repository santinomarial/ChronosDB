#ifndef CHRONOS_RAFT_ASYNC_DURABLE_RUNTIME_HPP_
#define CHRONOS_RAFT_ASYNC_DURABLE_RUNTIME_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/durable_runtime.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::raft {

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
  std::size_t pending_batches{};
  std::size_t pending_operations{};
  std::size_t high_water_pending_batches{};
  std::size_t high_water_pending_operations{};
  bool accepting{};
  bool terminal_failure{};
};

namespace detail {
class AsyncDurableRaftCompletionState;
}

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
  [[nodiscard]] bool is_ready() const;
  [[nodiscard]] common::Result<std::vector<DurableRaftResult>> wait();

private:
  explicit AsyncDurableRaftCompletion(
      std::shared_ptr<detail::AsyncDurableRaftCompletionState> state) noexcept;
  std::shared_ptr<detail::AsyncDurableRaftCompletionState> state_;
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
             std::vector<RaftGroupConfiguration> groups, AsyncDurableMultiRaftLimits limits = {});
  [[nodiscard]] static common::Result<AsyncDurableMultiRaftRuntime>
  open_existing(NodeId local_node_id, const RaftPersistentLogConfig& log_config,
                const RaftPersistentLogOpenOptions& open_options,
                std::vector<RaftGroupConfiguration> groups,
                AsyncDurableMultiRaftLimits limits = {});

  // Admission is nonblocking. The operation vector is moved into worker ownership only on success.
  [[nodiscard]] common::Result<AsyncDurableRaftCompletion>
  try_submit(std::vector<DurableRaftRequest> requests);

  // Enqueues one bounded owning observation behind all previously admitted work. The returned
  // completion contains exactly one result with observation set and transition empty.
  [[nodiscard]] common::Result<AsyncDurableRaftCompletion>
  try_observe_group(const GroupId& group_id);

  // Idempotently stops admission, drains all accepted batches in FIFO order, closes the log, and
  // joins the worker. A terminal worker failure rejects all not-yet-executed accepted batches.
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] AsyncDurableMultiRaftMetrics metrics() const;
  [[nodiscard]] bool is_accepting() const;
  [[nodiscard]] common::Status terminal_status() const;

private:
  class Impl;
  explicit AsyncDurableMultiRaftRuntime(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_ASYNC_DURABLE_RUNTIME_HPP_
