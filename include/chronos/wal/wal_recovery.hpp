#ifndef CHRONOS_WAL_WAL_RECOVERY_HPP_
#define CHRONOS_WAL_WAL_RECOVERY_HPP_

#include "chronos/common/result.hpp"
#include "chronos/wal/wal_recovery_report.hpp"
#include "chronos/wal/wal_replay_sink.hpp"
#include "chronos/wal/wal_writer_config.hpp"

#include <string_view>

namespace chronos::io::detail {
class PosixSyscalls;
}

namespace chronos::wal {

struct WalRecoveryOptions {
  bool repair_incomplete_final_tail{false};
};

// Locked and read-only. Unknown physical/application semantics may be accepted by an inspection
// sink, but replay callbacks still begin only after whole-log verification and preflight.
[[nodiscard]] common::Result<WalRecoveryReport> inspect_wal(std::string_view directory_path,
                                                            WalReplaySink& sink);

// Performs writer-startup recovery without returning a writer: verify, optional explicit repair,
// re-verify, semantic preflight, replay, and the WAL-directory namespace barrier.
[[nodiscard]] common::Result<WalRecoveryReport>
recover_wal(const WalWriterConfig& config, const WalRecoveryOptions& options, WalReplaySink& sink);

namespace detail {
class WalRecoveryTestAccess;
}

} // namespace chronos::wal

#endif // CHRONOS_WAL_WAL_RECOVERY_HPP_
