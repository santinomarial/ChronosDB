#ifndef CHRONOS_WAL_CODEC_INTERNAL_HPP_
#define CHRONOS_WAL_CODEC_INTERNAL_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/wal/types.hpp"

namespace chronos::wal::detail {

// Validates the common WAL v1 record header and returns its bounded framing fields without applying
// semantic support policy for nonzero required flags. Recovery uses this only to determine how many
// bytes must be read before decode_record() establishes complete-record integrity. Public callers
// use decode_record_header(), which continues to reject unsupported required flags immediately.
[[nodiscard]] common::Result<RecordHeader>
decode_record_header_for_physical_scan(common::ByteView encoded_bytes);

} // namespace chronos::wal::detail

#endif // CHRONOS_WAL_CODEC_INTERNAL_HPP_
