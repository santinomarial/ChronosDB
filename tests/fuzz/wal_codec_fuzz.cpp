#include "chronos/common/bytes.hpp"
#include "chronos/wal/codec.hpp"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <span>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const chronos::common::ByteView bytes =
      chronos::common::byte_view(std::span<const std::uint8_t>{data, size});

  const auto segment_header = chronos::wal::decode_segment_header(bytes);
  if (segment_header.has_value() && !segment_header->wal_id.is_valid()) {
    std::abort();
  }

  const auto record_header = chronos::wal::decode_record_header(bytes);
  if (record_header.has_value()) {
    if (record_header->total_length < chronos::wal::kMinimumRecordLength ||
        record_header->total_length > chronos::wal::kMaximumRecordLength ||
        record_header->payload_length > chronos::wal::kMaximumPayloadLength ||
        (record_header->total_length % 8U) != 0U) {
      std::abort();
    }
  }

  const auto record = chronos::wal::decode_record(bytes);
  if (record.has_value()) {
    if (record->header.total_length > size ||
        record->payload.size() != record->header.payload_length ||
        record->payload.data() != bytes.data() + chronos::wal::kRecordHeaderSize) {
      std::abort();
    }
  }

  if (size >= sizeof(std::uint64_t)) {
    std::uint64_t declared_payload = 0;
    for (std::size_t index = 0; index < sizeof(declared_payload); ++index) {
      declared_payload |= static_cast<std::uint64_t>(data[index]) << (index * 8U);
    }
    const auto layout = chronos::wal::calculate_record_layout(declared_payload);
    if (layout.has_value() && (layout->total_length > chronos::wal::kMaximumRecordLength ||
                               layout->payload_length > chronos::wal::kMaximumPayloadLength)) {
      std::abort();
    }
  }
  return 0;
}
