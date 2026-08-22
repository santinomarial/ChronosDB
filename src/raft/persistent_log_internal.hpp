#ifndef CHRONOS_RAFT_PERSISTENT_LOG_INTERNAL_HPP_
#define CHRONOS_RAFT_PERSISTENT_LOG_INTERNAL_HPP_

#include "chronos/raft/persistent_log.hpp"
#include "io/posix_syscalls.hpp"

namespace chronos::raft::detail {

class RaftPersistentLogTestAccess {
public:
  [[nodiscard]] static common::Result<RaftPersistentLog>
  create_new(const RaftPersistentLogConfig& config, io::detail::PosixSyscalls& syscalls) {
    return RaftPersistentLog::create_new_with(config, syscalls);
  }

  [[nodiscard]] static common::Result<RaftPersistentLog>
  open_existing(const RaftPersistentLogConfig& config, const RaftPersistentLogOpenOptions& options,
                io::detail::PosixSyscalls& syscalls) {
    return RaftPersistentLog::open_existing_with(config, options, syscalls);
  }
};

} // namespace chronos::raft::detail

#endif // CHRONOS_RAFT_PERSISTENT_LOG_INTERNAL_HPP_
