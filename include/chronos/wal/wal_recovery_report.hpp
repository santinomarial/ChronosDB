#ifndef CHRONOS_WAL_WAL_RECOVERY_REPORT_HPP_
#define CHRONOS_WAL_WAL_RECOVERY_REPORT_HPP_

#include "chronos/wal/types.hpp"

#include <cstdint>

namespace chronos::wal {

enum class WalScanClassification : std::uint8_t {
  kClean,
  kIncompleteFinalTail,
};

struct WalRecoveryReport {
  WalScanClassification classification{WalScanClassification::kClean};
  WalId wal_id;
  std::uint64_t segment_count{};
  std::uint64_t temporary_file_count{};
  std::uint64_t temporary_files_removed{};
  std::uint64_t record_count{};
  std::uint64_t physical_bytes{};
  PhysicalWalPosition valid_end;
  std::uint64_t observed_final_size{};
  std::uint64_t last_record_sequence{};
  bool sequence_exhausted{false};
  bool repaired{false};
  std::uint64_t repair_original_size{};
  std::uint64_t repair_new_size{};

  friend bool operator==(const WalRecoveryReport&, const WalRecoveryReport&) = default;
};

} // namespace chronos::wal

#endif // CHRONOS_WAL_WAL_RECOVERY_REPORT_HPP_
