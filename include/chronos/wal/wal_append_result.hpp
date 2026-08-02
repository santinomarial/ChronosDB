#ifndef CHRONOS_WAL_WAL_APPEND_RESULT_HPP_
#define CHRONOS_WAL_WAL_APPEND_RESULT_HPP_

#include "chronos/wal/types.hpp"

#include <cstdint>

namespace chronos::wal {

// A successful append has completed the operating-system write path for the entire record. It is
// not a stable-media acknowledgment until synchronize() covers record_end.
struct WalAppendResult {
  std::uint64_t record_sequence{};
  PhysicalWalPosition record_start;
  PhysicalWalPosition record_end;

  friend bool operator==(const WalAppendResult&, const WalAppendResult&) = default;
};

} // namespace chronos::wal

#endif // CHRONOS_WAL_WAL_APPEND_RESULT_HPP_
