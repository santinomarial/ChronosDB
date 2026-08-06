#include "chronos/wal/wal_writer.hpp"
#include "io/posix_syscalls.hpp"
#include "wal/wal_recovery_internal.hpp"

#include <utility>

namespace chronos::wal {

common::Result<WalWriter> WalWriter::open_existing(const WalWriterConfig& config,
                                                   const WalRecoveryOptions& options,
                                                   WalReplaySink& replay_sink) {
  return open_existing_with(config, options, replay_sink, io::detail::system_posix_syscalls());
}

common::Result<WalWriter> WalWriter::open_existing_from_checkpoint(
    const WalWriterConfig& config, const WalRecoveryOptions& options,
    const WalReplayCheckpoint& checkpoint, WalReplaySink& replay_sink) {
  return open_existing_with(config, options, replay_sink, io::detail::system_posix_syscalls(),
                            checkpoint);
}

common::Result<WalWriter>
WalWriter::open_existing_with(const WalWriterConfig& config, const WalRecoveryOptions& options,
                              WalReplaySink& replay_sink, io::detail::PosixSyscalls& syscalls,
                              const std::optional<WalReplayCheckpoint> checkpoint) {
  common::Result<detail::RecoveredWalState> recovered =
      detail::recover_existing_for_writer(config, options, replay_sink, syscalls, checkpoint);
  if (!recovered.has_value()) {
    return common::make_unexpected(recovered.error());
  }
  return from_recovered_state(config, std::move(*recovered));
}

} // namespace chronos::wal
