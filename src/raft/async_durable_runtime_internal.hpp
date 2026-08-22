#ifndef CHRONOS_RAFT_ASYNC_DURABLE_RUNTIME_INTERNAL_HPP_
#define CHRONOS_RAFT_ASYNC_DURABLE_RUNTIME_INTERNAL_HPP_

#include "chronos/common/time_source.hpp"
#include "chronos/raft/async_durable_runtime.hpp"
#include "io/posix_syscalls.hpp"

#include <array>
#include <cstddef>
#include <sys/types.h>
#include <utility>

namespace chronos::raft::detail {

// Narrow completion-pipe boundary for deterministic syscall failure injection. The runtime borrows
// this owner for its complete lifetime; production uses the process-lifetime system implementation.
class AsyncDurableRaftCompletionIo {
public:
  virtual ~AsyncDurableRaftCompletionIo() = default;

  virtual int create_pipe(std::array<int, 2>& descriptors) = 0;
  virtual int get_status_flags(int descriptor) = 0;
  virtual int get_descriptor_flags(int descriptor) = 0;
  virtual int set_status_flags(int descriptor, int flags) = 0;
  virtual int set_descriptor_flags(int descriptor, int flags) = 0;
  virtual ssize_t write(int descriptor, const void* source, std::size_t size) = 0;
  virtual ssize_t read(int descriptor, void* destination, std::size_t size) = 0;
  virtual int close(int descriptor) = 0;
};

class AsyncDurableMultiRaftRuntimeTestAccess {
public:
  [[nodiscard]] static common::Result<AsyncDurableMultiRaftRuntime>
  start_with(DurableMultiRaftRuntime runtime, AsyncDurableMultiRaftLimits limits = {}) {
    return AsyncDurableMultiRaftRuntime::start_with(std::move(runtime), limits, nullptr,
                                                    common::system_time_source(),
                                                    system_completion_io(), nullptr, nullptr);
  }

  [[nodiscard]] static common::Result<AsyncDurableMultiRaftRuntime>
  start_with_completion_io(DurableMultiRaftRuntime runtime,
                           AsyncDurableRaftCompletionIo& completion_io,
                           AsyncDurableMultiRaftLimits limits = {},
                           std::shared_ptr<AsyncDurableRaftWorkerExtension> extension = nullptr) {
    return AsyncDurableMultiRaftRuntime::start_with(
        std::move(runtime), limits, std::move(extension), common::system_time_source(),
        completion_io, nullptr, nullptr);
  }

  [[nodiscard]] static common::Result<AsyncDurableMultiRaftRuntime>
  create_new(NodeId local_node_id, const RaftPersistentLogConfig& log_config,
             std::vector<RaftGroupConfiguration> groups, AsyncDurableMultiRaftLimits limits,
             std::shared_ptr<AsyncDurableRaftWorkerExtension> extension,
             io::detail::PosixSyscalls& syscalls) {
    return AsyncDurableMultiRaftRuntime::create_new_with(
        local_node_id, log_config, std::move(groups), limits, std::move(extension), syscalls,
        common::system_time_source(), nullptr, nullptr);
  }

  [[nodiscard]] static common::Result<AsyncDurableMultiRaftRuntime> create_new_with_time_source(
      NodeId local_node_id, const RaftPersistentLogConfig& log_config,
      std::vector<RaftGroupConfiguration> groups, AsyncDurableMultiRaftLimits limits,
      std::shared_ptr<AsyncDurableRaftWorkerExtension> extension,
      io::detail::PosixSyscalls& syscalls, const common::TimeSource& time_source) {
    return AsyncDurableMultiRaftRuntime::create_new_with(
        local_node_id, log_config, std::move(groups), limits, std::move(extension), syscalls,
        time_source, nullptr, nullptr);
  }

  [[nodiscard]] static common::Result<AsyncDurableMultiRaftRuntime>
  create_new_with_worker_start_hook(NodeId local_node_id, const RaftPersistentLogConfig& log_config,
                                    std::vector<RaftGroupConfiguration> groups,
                                    AsyncDurableMultiRaftLimits limits,
                                    std::shared_ptr<AsyncDurableRaftWorkerExtension> extension,
                                    void (*worker_start_hook)(void*), void* worker_start_context) {
    return AsyncDurableMultiRaftRuntime::create_new_with(
        local_node_id, log_config, std::move(groups), limits, std::move(extension),
        io::detail::system_posix_syscalls(), common::system_time_source(), worker_start_hook,
        worker_start_context);
  }

  [[nodiscard]] static common::Result<AsyncDurableMultiRaftRuntime>
  open_existing_with_worker_start_hook(NodeId local_node_id,
                                       const RaftPersistentLogConfig& log_config,
                                       const RaftPersistentLogOpenOptions& open_options,
                                       std::vector<RaftGroupConfiguration> groups,
                                       AsyncDurableMultiRaftLimits limits,
                                       std::shared_ptr<AsyncDurableRaftWorkerExtension> extension,
                                       void (*worker_start_hook)(void*),
                                       void* worker_start_context) {
    return AsyncDurableMultiRaftRuntime::open_existing_with(
        local_node_id, log_config, open_options, std::move(groups), limits, std::move(extension),
        common::system_time_source(), worker_start_hook, worker_start_context);
  }

private:
  [[nodiscard]] static AsyncDurableRaftCompletionIo& system_completion_io() noexcept;
};

} // namespace chronos::raft::detail

#endif // CHRONOS_RAFT_ASYNC_DURABLE_RUNTIME_INTERNAL_HPP_
