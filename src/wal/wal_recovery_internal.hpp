#ifndef CHRONOS_WAL_WAL_RECOVERY_INTERNAL_HPP_
#define CHRONOS_WAL_WAL_RECOVERY_INTERNAL_HPP_

#include "chronos/common/result.hpp"
#include "chronos/wal/wal_recovery.hpp"
#include "wal/wal_segment_internal.hpp"

namespace chronos::wal::detail {

struct RecoveredWalState {
  WalRecoveryReport report;
  io::PosixDirectory directory;
  io::PosixAdvisoryLock lock;
  ActiveWalSegment active_segment;
};

[[nodiscard]] common::Result<RecoveredWalState>
recover_existing_for_writer(const WalWriterConfig& config, const WalRecoveryOptions& options,
                            WalReplaySink& replay_sink, io::detail::PosixSyscalls& syscalls);

[[nodiscard]] common::Result<WalRecoveryReport>
recover_wal_with(const WalWriterConfig& config, const WalRecoveryOptions& options,
                 WalReplaySink& replay_sink, io::detail::PosixSyscalls& syscalls);

class WalRecoveryTestAccess {
public:
  [[nodiscard]] static common::Result<WalRecoveryReport>
  recover(const WalWriterConfig& config, const WalRecoveryOptions& options, WalReplaySink& sink,
          io::detail::PosixSyscalls& syscalls) {
    return recover_wal_with(config, options, sink, syscalls);
  }
};

} // namespace chronos::wal::detail

#endif // CHRONOS_WAL_WAL_RECOVERY_INTERNAL_HPP_
