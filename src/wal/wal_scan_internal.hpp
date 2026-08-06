#ifndef CHRONOS_WAL_WAL_SCAN_INTERNAL_HPP_
#define CHRONOS_WAL_WAL_SCAN_INTERNAL_HPP_

#include "chronos/common/result.hpp"
#include "chronos/io/posix_io.hpp"
#include "chronos/wal/wal_recovery_report.hpp"
#include "chronos/wal/wal_replay_sink.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace chronos::io::detail {
class PosixSyscalls;
}

namespace chronos::wal::detail {

struct DiscoveredWalSegment {
  std::uint64_t number{};
  std::string file_name;
};

struct WalDiscovery {
  std::vector<DiscoveredWalSegment> segments;
  std::vector<std::string> temporary_file_names;
};

struct LockedWalDirectory {
  io::PosixDirectory directory;
  io::PosixAdvisoryLock lock;
  WalDiscovery discovery;
};

enum class ScanPass : std::uint8_t {
  kVerify,
  kPreflight,
  kReplay,
};

[[nodiscard]] common::Result<LockedWalDirectory>
open_locked_wal_directory(std::string_view directory_path, std::uint16_t lock_permissions,
                          bool create_lock, io::detail::PosixSyscalls& syscalls);
[[nodiscard]] common::Result<LockedWalDirectory>
open_locked_wal_directory_for_checkpoint(std::string_view directory_path,
                                         io::detail::PosixSyscalls& syscalls);
[[nodiscard]] common::Result<WalRecoveryReport>
scan_discovered_wal(io::PosixDirectory& directory, const WalDiscovery& discovery,
                    ScanPass pass = ScanPass::kVerify, WalReplaySink* sink = nullptr,
                    std::optional<WalReplayCheckpoint> checkpoint = std::nullopt);
[[nodiscard]] common::Status require_same_verified_history(const WalRecoveryReport& expected,
                                                           const WalRecoveryReport& observed);

} // namespace chronos::wal::detail

#endif // CHRONOS_WAL_WAL_SCAN_INTERNAL_HPP_
