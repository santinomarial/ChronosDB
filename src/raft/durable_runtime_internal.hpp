#ifndef CHRONOS_RAFT_DURABLE_RUNTIME_INTERNAL_HPP_
#define CHRONOS_RAFT_DURABLE_RUNTIME_INTERNAL_HPP_

#include "chronos/raft/durable_runtime.hpp"
#include "io/posix_syscalls.hpp"

#include <utility>
#include <vector>

namespace chronos::raft::detail {

class DurableMultiRaftRuntimeTestAccess {
public:
  [[nodiscard]] static common::Result<DurableMultiRaftRuntime>
  create_new(NodeId local_node_id, const RaftPersistentLogConfig& log_config,
             std::vector<RaftGroupConfiguration> groups, DurableMultiRaftLimits limits,
             io::detail::PosixSyscalls& syscalls) {
    return DurableMultiRaftRuntime::create_new_with(local_node_id, log_config, std::move(groups),
                                                    limits, syscalls);
  }

  [[nodiscard]] static common::Result<DurableMultiRaftRuntime>
  open_existing(NodeId local_node_id, const RaftPersistentLogConfig& log_config,
                const RaftPersistentLogOpenOptions& open_options,
                std::vector<RaftGroupConfiguration> groups, DurableMultiRaftLimits limits,
                io::detail::PosixSyscalls& syscalls) {
    return DurableMultiRaftRuntime::open_existing_with(local_node_id, log_config, open_options,
                                                       std::move(groups), limits, syscalls);
  }
};

} // namespace chronos::raft::detail

#endif // CHRONOS_RAFT_DURABLE_RUNTIME_INTERNAL_HPP_
