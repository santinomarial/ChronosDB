#ifndef CHRONOS_WAL_CODEC_HPP_
#define CHRONOS_WAL_CODEC_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/wal/types.hpp"

#include <cstddef>
#include <cstdint>

namespace chronos::wal {

// These helpers perform no disk I/O. Successful decoding returns borrowed views and allocates no
// payload storage. Error Status messages may allocate.
[[nodiscard]] common::Result<RecordLayout> calculate_record_layout(std::size_t payload_length);
[[nodiscard]] common::Status validate_segment_size(std::uint64_t segment_size);
[[nodiscard]] common::Status validate_physical_wal_position(const PhysicalWalPosition& position);
[[nodiscard]] common::Result<PhysicalWalPosition>
advance_physical_wal_position(const PhysicalWalPosition& position, std::uint32_t record_length);

[[nodiscard]] common::Result<EncodedSegmentHeader>
encode_segment_header(const SegmentHeader& header);

// encoded_bytes must contain at least the 64-byte compatibility prefix. Bytes after that prefix are
// ignored; validate_segment_size() separately checks the size of a complete segment image.
[[nodiscard]] common::Result<SegmentHeader> decode_segment_header(common::ByteView encoded_bytes);

// Constructs a supported WAL v1 outer header. Nonzero record types are structurally frameable; the
// WAL writer remains responsible for enforcing the specification's type-allocation policy.
[[nodiscard]] common::Result<RecordHeader>
make_record_header(std::uint16_t record_type, std::uint64_t record_sequence,
                   std::size_t payload_length);

[[nodiscard]] common::Result<EncodedRecordHeader>
encode_record_header(const RecordHeader& header);

// encoded_bytes must contain at least the 40-byte common header. A nonzero unknown record format or
// type remains structurally decodable. Unknown required outer flags return kNotSupported.
[[nodiscard]] common::Result<RecordHeader> decode_record_header(common::ByteView encoded_bytes);

// On failure, destination is unchanged. On success, exactly header.total_length bytes are written;
// extra destination capacity is untouched. Payload may alias destination.
[[nodiscard]] common::Result<std::size_t>
encode_record(const RecordHeader& header, common::ByteView payload,
              common::MutableByteView destination);

// Decodes the first complete record in encoded_bytes and ignores later bytes. Truncation returns
// kOutOfRange so the recovery layer can apply its final-segment tail rules. Integrity or structural
// contradictions return kCorruption. The returned payload view borrows encoded_bytes.
[[nodiscard]] common::Result<DecodedRecord> decode_record(common::ByteView encoded_bytes);

} // namespace chronos::wal

#endif // CHRONOS_WAL_CODEC_HPP_
