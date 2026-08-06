#include "chronos/common/bytes.hpp"
#include "chronos/wal/application.hpp"
#include "chronos/wal/codec.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <span>
#include <vector>

namespace {

void exercise(const chronos::common::ByteView bytes) {
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
    if (record->header.total_length > bytes.size() ||
        record->payload.size() != record->header.payload_length ||
        record->payload.data() != bytes.data() + chronos::wal::kRecordHeaderSize) {
      std::abort();
    }
  }
}

[[nodiscard]] chronos::wal::EncodedSegmentHeader structured_segment() {
  chronos::wal::WalId wal_id;
  wal_id.bytes.front() = std::byte{1U};
  return chronos::wal::encode_segment_header(
             {.wal_id = wal_id, .segment_number = 1U, .first_record_sequence = 1U})
      .value();
}

[[nodiscard]] std::vector<std::byte> structured_record() {
  constexpr std::array body{std::byte{0U}, std::byte{1U}, std::byte{0xffU}, std::byte{2U},
                            std::byte{3U}};
  const chronos::wal::EncodedApplicationPayload payload =
      chronos::wal::encode_application_payload({.application_format = 1U,
                                                .application_kind = 99U,
                                                .application_flags = 0U,
                                                .application_body = body})
          .value();
  const chronos::wal::RecordHeader header =
      chronos::wal::make_record_header({.record_type = chronos::wal::kApplicationEntryRecordType,
                                        .record_sequence = 1U,
                                        .payload_length = payload.size()})
          .value();
  std::vector<std::byte> encoded(header.total_length);
  static_cast<void>(chronos::wal::encode_record(header, payload.bytes(), encoded).value());
  return encoded;
}

void exercise_structured(std::vector<std::byte> bytes, const std::uint8_t* data,
                         const std::size_t size) {
  exercise(bytes);
  if (size != 0U) {
    const std::size_t offset = static_cast<std::size_t>(data[0]) % bytes.size();
    bytes[offset] ^=
        std::byte{size > 1U ? static_cast<std::uint8_t>(data[1] | 1U) : std::uint8_t{1U}};
    if (size > 2U) {
      bytes.resize(static_cast<std::size_t>(data[2]) * bytes.size() / 255U);
    }
    exercise(bytes);
  }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const chronos::common::ByteView bytes =
      chronos::common::byte_view(std::span<const std::uint8_t>{data, size});
  exercise(bytes);

  static const chronos::wal::EncodedSegmentHeader segment = structured_segment();
  exercise_structured({segment.begin(), segment.end()}, data, size);
  static const std::vector<std::byte> record = structured_record();
  exercise_structured(record, data, size);

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
