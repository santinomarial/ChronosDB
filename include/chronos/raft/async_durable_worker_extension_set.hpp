#ifndef CHRONOS_RAFT_ASYNC_DURABLE_WORKER_EXTENSION_SET_HPP_
#define CHRONOS_RAFT_ASYNC_DURABLE_WORKER_EXTENSION_SET_HPP_

#include "chronos/raft/async_durable_runtime.hpp"

#include <cstddef>
#include <memory>
#include <span>
#include <vector>

namespace chronos::raft {

// A bounded, flat composition of worker extensions. Lifecycle and batch callbacks run in
// declaration order; shutdown runs in reverse order. The set and its lifecycle state are owned by
// the one durable worker. The child vector is immutable after construction, so membership queries
// are safe from producer threads.
class AsyncDurableRaftWorkerExtensionSet final : public AsyncDurableRaftWorkerExtension {
public:
  static constexpr std::size_t kMaximumExtensions = 64U;

  [[nodiscard]] static common::Result<std::shared_ptr<AsyncDurableRaftWorkerExtensionSet>>
  create(std::vector<std::shared_ptr<AsyncDurableRaftWorkerExtension>> extensions);

  [[nodiscard]] std::size_t size() const noexcept;
  [[nodiscard]] bool contains_worker_extension(
      const AsyncDurableRaftWorkerExtension& candidate) const noexcept override;

  [[nodiscard]] common::Status initialize(DurableMultiRaftRuntime& runtime) override;
  [[nodiscard]] common::Result<std::unique_ptr<AsyncDurableRaftWorkerBatchContext>>
  prepare_batch(DurableMultiRaftRuntime& runtime,
                std::span<const DurableRaftRequest> requests) override;
  [[nodiscard]] common::Status
  complete_batch(DurableMultiRaftRuntime& runtime,
                 std::unique_ptr<AsyncDurableRaftWorkerBatchContext> context,
                 std::span<const DurableRaftResult> results) override;
  [[nodiscard]] common::Status shutdown(DurableMultiRaftRuntime& runtime) override;

private:
  explicit AsyncDurableRaftWorkerExtensionSet(
      std::vector<std::shared_ptr<AsyncDurableRaftWorkerExtension>> extensions) noexcept;

  std::vector<std::shared_ptr<AsyncDurableRaftWorkerExtension>> extensions_;
  std::size_t attempted_initializations_{};
  bool initialized_{};
  bool shutdown_complete_{};
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_ASYNC_DURABLE_WORKER_EXTENSION_SET_HPP_
