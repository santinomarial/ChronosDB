#ifndef CHRONOS_WAL_WAL_SCAN_HPP_
#define CHRONOS_WAL_WAL_SCAN_HPP_

#include "chronos/common/result.hpp"
#include "chronos/wal/wal_recovery_report.hpp"

#include <string_view>

namespace chronos::wal {

// Performs a locked, read-only physical scan. The existing regular LOCK entry is required; this
// function never creates, truncates, synchronizes, removes, or renames a file.
[[nodiscard]] common::Result<WalRecoveryReport> scan_wal(std::string_view directory_path);

} // namespace chronos::wal

#endif // CHRONOS_WAL_WAL_SCAN_HPP_
