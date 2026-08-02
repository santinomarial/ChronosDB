#ifndef CHRONOS_WAL_WAL_SEGMENT_HPP_
#define CHRONOS_WAL_WAL_SEGMENT_HPP_

#include "chronos/wal/types.hpp"

#include <cstdint>
#include <string>

namespace chronos::wal {

// Public, owning metadata for the active segment. The writer exclusively owns the file descriptor.
struct WalSegment {
  std::string file_name;
  SegmentHeader header;
  std::uint64_t end_offset{kSegmentHeaderSize};

  friend bool operator==(const WalSegment&, const WalSegment&) = default;
};

} // namespace chronos::wal

#endif // CHRONOS_WAL_WAL_SEGMENT_HPP_
