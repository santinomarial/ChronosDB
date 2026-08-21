#ifndef CHRONOS_RAFT_ASYNC_DURABLE_RUNTIME_INTERNAL_HPP_
#define CHRONOS_RAFT_ASYNC_DURABLE_RUNTIME_INTERNAL_HPP_

#include "chronos/raft/async_durable_runtime.hpp"
#include "io/posix_syscalls.hpp"

#include <utility>

namespace chronos::raft::detail {

class AsyncDurableMultiRaftRuntimeTestAccess {
public:
  [[nodiscard]] static common::Result<AsyncDurableMultiRaftRuntime>
  create_new(NodeId local_node_id, const RaftPersistentLogConfig& log_config,
             std::vector<RaftGroupConfiguration> groups, AsyncDurableMultiRaftLimits limits,
             std::shared_ptr<AsyncDurableRaftWorkerExtension> extension,
             io::detail::PosixSyscalls& syscalls) {
    return AsyncDurableMultiRaftRuntime::create_new_with(
        local_node_id, log_config, std::move(groups), limits, std::move(extension), syscalls);
  }
};

} // namespace chronos::raft::detail

#endif // CHRONOS_RAFT_ASYNC_DURABLE_RUNTIME_INTERNAL_HPP_
