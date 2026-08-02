#ifndef CHRONOS_WAL_WAL_SEGMENT_INTERNAL_HPP_
#define CHRONOS_WAL_WAL_SEGMENT_INTERNAL_HPP_

#include "chronos/common/result.hpp"
#include "chronos/io/posix_io.hpp"
#include "chronos/wal/types.hpp"
#include "chronos/wal/wal_segment.hpp"

#include <cstdint>

namespace chronos::wal::detail {

struct ActiveWalSegment {
  WalSegment metadata;
  io::PosixFile file;
};

struct SegmentInstallRequest {
  WalId wal_id;
  std::uint64_t segment_number;
  std::uint64_t first_record_sequence;
  std::uint16_t file_permissions;
};

[[nodiscard]] common::Result<ActiveWalSegment>
install_segment(io::PosixDirectory& directory, const SegmentInstallRequest& request);

} // namespace chronos::wal::detail

#endif // CHRONOS_WAL_WAL_SEGMENT_INTERNAL_HPP_
