#include "chronos/wal/codec.hpp"

#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <utility>

namespace chronos::wal {
namespace {

constexpr std::size_t kSegmentHeaderCrcOffset = 60;
constexpr std::size_t kRecordHeaderCrcOffset = 36;

[[nodiscard]] common::Status corruption(std::string message) {
  return common::Status{common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status out_of_range(std::string message) {
  return common::Status{common::StatusCode::kOutOfRange, std::move(message)};
}

[[nodiscard]] common::Status not_supported(std::string message) {
  return common::Status{common::StatusCode::kNotSupported, std::move(message)};
}

template <std::size_t Size>
[[nodiscard]] bool bytes_equal(const common::ByteView bytes, const std::size_t offset,
                               const std::array<std::byte, Size>& expected) noexcept {
  return std::equal(expected.begin(), expected.end(), bytes.subspan(offset, Size).begin());
}

[[nodiscard]] std::uint16_t load_u16_le(const common::ByteView bytes,
                                        const std::size_t offset) noexcept {
  return static_cast<std::uint16_t>(std::to_integer<std::uint16_t>(bytes[offset]) |
                                    (std::to_integer<std::uint16_t>(bytes[offset + 1U]) << 8U));
}

[[nodiscard]] std::uint32_t load_u32_le(const common::ByteView bytes,
                                        const std::size_t offset) noexcept {
  std::uint32_t value = 0;
  for (std::size_t index = 0; index < sizeof(value); ++index) {
    value |= std::to_integer<std::uint32_t>(bytes[offset + index]) << (index * 8U);
  }
  return value;
}

[[nodiscard]] std::uint64_t load_u64_le(const common::ByteView bytes,
                                        const std::size_t offset) noexcept {
  std::uint64_t value = 0;
  for (std::size_t index = 0; index < sizeof(value); ++index) {
    value |= std::to_integer<std::uint64_t>(bytes[offset + index]) << (index * 8U);
  }
  return value;
}

void store_u16_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint16_t value) noexcept {
  for (std::size_t index = 0; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
}

void store_u32_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint32_t value) noexcept {
  for (std::size_t index = 0; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
}

void store_u64_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint64_t value) noexcept {
  for (std::size_t index = 0; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
}

[[nodiscard]] common::Status validate_segment_header_for_encoding(const SegmentHeader& header) {
  if (!header.wal_id.is_valid()) {
    return common::Status{common::StatusCode::kInvalidArgument, "WAL identity must be nonzero"};
  }
  if (header.segment_number == 0U) {
    return common::Status{common::StatusCode::kInvalidArgument, "segment number must be nonzero"};
  }
  if (header.first_record_sequence == 0U) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "first record sequence must be nonzero"};
  }
  if (header.segment_number == kFirstSegmentNumber &&
      header.first_record_sequence != kFirstRecordSequence) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "segment 1 must begin with record sequence 1"};
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<RecordHeader>
parse_record_header(const common::ByteView encoded_bytes) {
  if (encoded_bytes.size() < kRecordHeaderSize) {
    return common::make_unexpected(out_of_range("WAL record header requires 40 available bytes"));
  }
  const common::ByteView bytes = encoded_bytes.first(kRecordHeaderSize);
  if (!bytes_equal(bytes, 8U, kRecordMagic)) {
    return common::make_unexpected(corruption("WAL record magic mismatch"));
  }

  const std::uint32_t stored_header_crc = load_u32_le(bytes, kRecordHeaderCrcOffset);
  if (common::crc32c(bytes.first(kRecordHeaderCrcOffset)) != stored_header_crc) {
    return common::make_unexpected(corruption("WAL record header CRC32C mismatch"));
  }

  RecordHeader header{
      .total_length = load_u32_le(bytes, 0U),
      .record_format = load_u16_le(bytes, 12U),
      .record_type = load_u16_le(bytes, 14U),
      .record_flags = load_u32_le(bytes, 16U),
      .record_sequence = load_u64_le(bytes, 24U),
      .payload_length = load_u32_le(bytes, 32U),
  };
  if ((header.total_length ^ load_u32_le(bytes, 4U)) != std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(corruption("WAL record length complement mismatch"));
  }
  if (load_u32_le(bytes, 20U) != kRecordHeaderSize) {
    return common::make_unexpected(corruption("WAL record header length is not 40"));
  }
  if (header.record_format == 0U) {
    return common::make_unexpected(corruption("WAL record format zero is invalid"));
  }
  if (header.record_type == 0U) {
    return common::make_unexpected(corruption("WAL record type zero is invalid"));
  }
  if (header.record_sequence == 0U) {
    return common::make_unexpected(corruption("WAL record sequence zero is invalid"));
  }

  const common::Result<RecordLayout> layout = calculate_record_layout(header.payload_length);
  if (!layout.has_value()) {
    return common::make_unexpected(corruption("WAL record payload length exceeds v1 bounds"));
  }
  if (header.total_length != layout->total_length) {
    return common::make_unexpected(corruption("WAL record total length does not match payload"));
  }
  return header;
}

[[nodiscard]] common::Status validate_application_envelope(const common::ByteView payload) {
  if (payload.size() < kApplicationEnvelopeSize) {
    return corruption("WAL application entry payload is shorter than 16 bytes");
  }
  if (load_u32_le(payload, 0U) == 0U) {
    return corruption("WAL application format zero is invalid");
  }
  if (load_u32_le(payload, 4U) == 0U) {
    return corruption("WAL application kind zero is invalid");
  }
  return common::Status::ok();
}

} // namespace

bool WalId::is_valid() const noexcept {
  return std::any_of(bytes.begin(), bytes.end(),
                     [](const std::byte byte) { return byte != std::byte{0}; });
}

common::Result<RecordLayout> calculate_record_layout(const std::size_t payload_length) {
  if (payload_length > kMaximumPayloadLength ||
      payload_length > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(out_of_range("WAL record payload exceeds 16777172 bytes"));
  }

  const auto payload = static_cast<std::uint32_t>(payload_length);
  const std::uint32_t padding = static_cast<std::uint32_t>((4U - (payload % 8U)) % 8U);
  const std::uint64_t total = static_cast<std::uint64_t>(kRecordHeaderSize) + payload + padding +
                              static_cast<std::uint64_t>(kRecordTrailerSize);
  if (total < kMinimumRecordLength || total > kMaximumRecordLength) {
    return common::make_unexpected(out_of_range("WAL record total length is outside v1 bounds"));
  }
  return RecordLayout{.payload_length = payload,
                      .padding_length = padding,
                      .total_length = static_cast<std::uint32_t>(total)};
}

common::Status validate_segment_size(const std::uint64_t segment_size) {
  if (segment_size < kSegmentHeaderSize) {
    return out_of_range("WAL segment is shorter than its 64-byte header");
  }
  if (segment_size > kSegmentSizeLimit) {
    return out_of_range("WAL segment exceeds the 64 MiB v1 limit");
  }
  return common::Status::ok();
}

common::Status validate_physical_wal_position(const PhysicalWalPosition& position) {
  if (!position.wal_id.is_valid()) {
    return common::Status{common::StatusCode::kInvalidArgument, "WAL identity must be nonzero"};
  }
  if (position.segment_number == 0U) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "WAL position segment number must be nonzero"};
  }
  if (position.byte_offset < kSegmentHeaderSize || position.byte_offset > kSegmentSizeLimit) {
    return out_of_range("WAL position offset is outside the segment bounds");
  }
  if ((position.byte_offset % 8U) != 0U) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "WAL position offset is not an 8-byte record boundary"};
  }
  return common::Status::ok();
}

common::Result<PhysicalWalPosition>
advance_physical_wal_position(const PhysicalWalPosition& position,
                              const std::uint32_t record_length) {
  const common::Status position_status = validate_physical_wal_position(position);
  if (!position_status.is_ok()) {
    return common::make_unexpected(position_status);
  }
  if (record_length < kMinimumRecordLength || record_length > kMaximumRecordLength ||
      (record_length % 8U) != 0U) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInvalidArgument, "record length is not a valid WAL v1 frame size"});
  }
  if (record_length > kSegmentSizeLimit - position.byte_offset) {
    return common::make_unexpected(out_of_range("WAL record would cross the segment boundary"));
  }
  PhysicalWalPosition advanced = position;
  advanced.byte_offset += record_length;
  return advanced;
}

common::Result<EncodedSegmentHeader> encode_segment_header(const SegmentHeader& header) {
  const common::Status validation = validate_segment_header_for_encoding(header);
  if (!validation.is_ok()) {
    return common::make_unexpected(validation);
  }

  EncodedSegmentHeader bytes{};
  const common::MutableByteView output{bytes};
  std::copy(kSegmentMagic.begin(), kSegmentMagic.end(), bytes.begin());
  store_u16_le(output, 8U, kSegmentFormatMajor);
  store_u16_le(output, 10U, kSegmentFormatMinor);
  store_u32_le(output, 12U, static_cast<std::uint32_t>(kSegmentHeaderSize));
  std::copy(header.wal_id.bytes.begin(), header.wal_id.bytes.end(), bytes.begin() + 16U);
  store_u64_le(output, 32U, header.segment_number);
  store_u64_le(output, 40U, header.first_record_sequence);
  store_u64_le(output, 48U, kSegmentSizeLimit);
  store_u32_le(output, 56U, 0U);
  store_u32_le(output, kSegmentHeaderCrcOffset,
               common::crc32c(common::ByteView{bytes}.first(kSegmentHeaderCrcOffset)));
  return bytes;
}

common::Result<SegmentHeader> decode_segment_header(const common::ByteView encoded_bytes) {
  if (encoded_bytes.size() < kSegmentHeaderSize) {
    return common::make_unexpected(out_of_range("WAL segment header requires 64 available bytes"));
  }
  const common::ByteView bytes = encoded_bytes.first(kSegmentHeaderSize);
  if (!bytes_equal(bytes, 0U, kSegmentMagic)) {
    return common::make_unexpected(corruption("WAL segment magic mismatch"));
  }
  const std::uint32_t stored_crc = load_u32_le(bytes, kSegmentHeaderCrcOffset);
  if (common::crc32c(bytes.first(kSegmentHeaderCrcOffset)) != stored_crc) {
    return common::make_unexpected(corruption("WAL segment header CRC32C mismatch"));
  }

  const std::uint16_t major = load_u16_le(bytes, 8U);
  const std::uint16_t minor = load_u16_le(bytes, 10U);
  if (major == 0U) {
    return common::make_unexpected(corruption("WAL segment format zero is invalid"));
  }
  if (major != kSegmentFormatMajor || minor != kSegmentFormatMinor) {
    return common::make_unexpected(not_supported("WAL segment format is not supported"));
  }
  if (load_u32_le(bytes, 12U) != kSegmentHeaderSize) {
    return common::make_unexpected(corruption("WAL segment header length is not 64"));
  }
  if (load_u32_le(bytes, 56U) != 0U) {
    return common::make_unexpected(not_supported("WAL segment required flags are not supported"));
  }

  SegmentHeader header;
  std::copy_n(bytes.begin() + 16U, kWalIdSize, header.wal_id.bytes.begin());
  header.segment_number = load_u64_le(bytes, 32U);
  header.first_record_sequence = load_u64_le(bytes, 40U);
  if (!header.wal_id.is_valid()) {
    return common::make_unexpected(corruption("WAL segment identity is all zero"));
  }
  if (header.segment_number == 0U) {
    return common::make_unexpected(corruption("WAL segment number zero is invalid"));
  }
  if (header.first_record_sequence == 0U) {
    return common::make_unexpected(corruption("WAL first record sequence zero is invalid"));
  }
  if (header.segment_number == kFirstSegmentNumber &&
      header.first_record_sequence != kFirstRecordSequence) {
    return common::make_unexpected(corruption("WAL segment 1 does not begin at sequence 1"));
  }
  if (load_u64_le(bytes, 48U) != kSegmentSizeLimit) {
    return common::make_unexpected(corruption("WAL segment size limit is not the v1 value"));
  }
  return header;
}

common::Result<RecordHeader> make_record_header(const RecordHeaderInput& input) {
  if (input.record_type == 0U) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument, "record type must be nonzero"});
  }
  if (input.record_sequence == 0U) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument, "record sequence must be nonzero"});
  }
  const common::Result<RecordLayout> layout = calculate_record_layout(input.payload_length);
  if (!layout.has_value()) {
    return common::make_unexpected(layout.error());
  }
  return RecordHeader{.total_length = layout->total_length,
                      .record_format = kRecordFormat,
                      .record_type = input.record_type,
                      .record_flags = 0U,
                      .record_sequence = input.record_sequence,
                      .payload_length = layout->payload_length};
}

common::Result<EncodedRecordHeader> encode_record_header(const RecordHeader& header) {
  if (header.record_format != kRecordFormat) {
    return common::make_unexpected(not_supported("only WAL record format 1 can be encoded"));
  }
  if (header.record_flags != 0U) {
    return common::make_unexpected(not_supported("WAL record required flags cannot be encoded"));
  }
  if (header.record_type == 0U || header.record_sequence == 0U) {
    return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                  "record type and sequence must be nonzero"});
  }
  const common::Result<RecordLayout> layout = calculate_record_layout(header.payload_length);
  if (!layout.has_value()) {
    return common::make_unexpected(layout.error());
  }
  if (header.total_length != layout->total_length) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInvalidArgument, "record total length does not match payload length"});
  }

  EncodedRecordHeader bytes{};
  const common::MutableByteView output{bytes};
  store_u32_le(output, 0U, header.total_length);
  store_u32_le(output, 4U, header.total_length ^ std::numeric_limits<std::uint32_t>::max());
  std::copy(kRecordMagic.begin(), kRecordMagic.end(), bytes.begin() + 8U);
  store_u16_le(output, 12U, header.record_format);
  store_u16_le(output, 14U, header.record_type);
  store_u32_le(output, 16U, header.record_flags);
  store_u32_le(output, 20U, static_cast<std::uint32_t>(kRecordHeaderSize));
  store_u64_le(output, 24U, header.record_sequence);
  store_u32_le(output, 32U, header.payload_length);
  store_u32_le(output, kRecordHeaderCrcOffset,
               common::crc32c(common::ByteView{bytes}.first(kRecordHeaderCrcOffset)));
  return bytes;
}

common::Result<RecordHeader> decode_record_header(const common::ByteView encoded_bytes) {
  common::Result<RecordHeader> header = parse_record_header(encoded_bytes);
  if (!header.has_value()) {
    return header;
  }
  if (header->record_flags != 0U) {
    return common::make_unexpected(not_supported("WAL record required flags are not supported"));
  }
  return header;
}

common::Result<std::size_t> encode_record(const RecordHeader& header,
                                          const common::ByteView payload,
                                          const common::MutableByteView destination) {
  const common::Result<EncodedRecordHeader> encoded_header = encode_record_header(header);
  if (!encoded_header.has_value()) {
    return common::make_unexpected(encoded_header.error());
  }
  if (payload.size() != header.payload_length) {
    return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                  "payload size does not match record header"});
  }
  if (header.record_type == kApplicationEntryRecordType) {
    const common::Status application_status = validate_application_envelope(payload);
    if (!application_status.is_ok()) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInvalidArgument, application_status.message()});
    }
  }
  const auto total_length = static_cast<std::size_t>(header.total_length);
  if (destination.size() < total_length) {
    return common::make_unexpected(out_of_range("destination cannot hold complete WAL record"));
  }

  const common::Result<RecordLayout> layout = calculate_record_layout(payload.size());
  if (!layout.has_value()) {
    return common::make_unexpected(layout.error());
  }

  // Copy the potentially aliased payload before overwriting the header region. memmove handles
  // overlap, and every validation that can fail has completed before this first destination write.
  if (!payload.empty()) {
    std::memmove(destination.data() + kRecordHeaderSize, payload.data(), payload.size());
  }
  std::copy(encoded_header->begin(), encoded_header->end(), destination.begin());
  const std::size_t padding_start = kRecordHeaderSize + payload.size();
  std::fill_n(destination.data() + padding_start, layout->padding_length, std::byte{0});
  const std::size_t trailer_offset = total_length - kRecordTrailerSize;
  const std::uint32_t checksum = common::crc32c(destination.first(trailer_offset));
  store_u32_le(destination, trailer_offset, checksum);
  return total_length;
}

common::Result<DecodedRecord> decode_record(const common::ByteView encoded_bytes) {
  common::Result<RecordHeader> header = parse_record_header(encoded_bytes);
  if (!header.has_value()) {
    return common::make_unexpected(header.error());
  }
  const auto total_length = static_cast<std::size_t>(header->total_length);
  if (encoded_bytes.size() < total_length) {
    if (header->record_flags != 0U) {
      return common::make_unexpected(not_supported("WAL record required flags are not supported"));
    }
    return common::make_unexpected(out_of_range("complete WAL record extends beyond input"));
  }

  const common::Result<RecordLayout> layout = calculate_record_layout(header->payload_length);
  if (!layout.has_value()) {
    return common::make_unexpected(corruption("WAL record layout is invalid"));
  }
  const common::ByteView record = encoded_bytes.first(total_length);
  const common::ByteView payload = record.subspan(kRecordHeaderSize, header->payload_length);
  const std::size_t padding_start = kRecordHeaderSize + header->payload_length;
  const common::ByteView padding = record.subspan(padding_start, layout->padding_length);
  if (std::any_of(padding.begin(), padding.end(),
                  [](const std::byte byte) { return byte != std::byte{0}; })) {
    return common::make_unexpected(corruption("WAL record padding is nonzero"));
  }

  const std::size_t trailer_offset = total_length - kRecordTrailerSize;
  const std::uint32_t stored_crc = load_u32_le(record, trailer_offset);
  if (common::crc32c(record.first(trailer_offset)) != stored_crc) {
    return common::make_unexpected(corruption("WAL complete-record CRC32C mismatch"));
  }
  if (header->record_flags != 0U) {
    return common::make_unexpected(not_supported("WAL record required flags are not supported"));
  }
  if (header->record_type == kApplicationEntryRecordType) {
    const common::Status application_status = validate_application_envelope(payload);
    if (!application_status.is_ok()) {
      return common::make_unexpected(application_status);
    }
  }
  return DecodedRecord{.header = *header, .payload = payload, .record_crc32c = stored_crc};
}

} // namespace chronos::wal
