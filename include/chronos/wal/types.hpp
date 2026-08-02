#ifndef CHRONOS_WAL_TYPES_HPP_
#define CHRONOS_WAL_TYPES_HPP_

#include "chronos/common/bytes.hpp"

#include <array>
#include <cstddef>
#include <cstdint>

namespace chronos::wal {

inline constexpr std::size_t kWalIdSize = 16;
inline constexpr std::size_t kSegmentHeaderSize = 64;
inline constexpr std::uint64_t kSegmentSizeLimit = 67'108'864;
inline constexpr std::size_t kRecordHeaderSize = 40;
inline constexpr std::size_t kRecordTrailerSize = 4;
inline constexpr std::uint32_t kMinimumRecordLength = 48;
inline constexpr std::uint32_t kMaximumRecordLength = 16'777'216;
inline constexpr std::uint32_t kMaximumPayloadLength = 16'777'172;
inline constexpr std::uint16_t kSegmentFormatMajor = 1;
inline constexpr std::uint16_t kSegmentFormatMinor = 0;
inline constexpr std::uint16_t kRecordFormat = 1;
inline constexpr std::uint16_t kApplicationEntryRecordType = 1;
inline constexpr std::size_t kApplicationEnvelopeSize = 16;
inline constexpr std::uint64_t kFirstSegmentNumber = 1;
inline constexpr std::uint64_t kFirstRecordSequence = 1;

inline constexpr std::array<std::byte, 8> kSegmentMagic{
    std::byte{0x43}, std::byte{0x48}, std::byte{0x52}, std::byte{0x4e},
    std::byte{0x57}, std::byte{0x41}, std::byte{0x4c}, std::byte{0x00},
};
inline constexpr std::array<std::byte, 4> kRecordMagic{std::byte{0x52}, std::byte{0x45},
                                                       std::byte{0x43}, std::byte{0x31}};

struct WalId {
  std::array<std::byte, kWalIdSize> bytes{};

  [[nodiscard]] bool is_valid() const noexcept;

  friend bool operator==(const WalId&, const WalId&) = default;
};

// A position is an in-memory coordinate, not a separately serialized WAL v1 structure. It names a
// record boundary within one WAL identity. Positions borrow no storage and are ordinary values.
struct PhysicalWalPosition {
  WalId wal_id;
  std::uint64_t segment_number{};
  std::uint64_t byte_offset{};

  friend bool operator==(const PhysicalWalPosition&, const PhysicalWalPosition&) = default;
};

struct SegmentHeader {
  WalId wal_id;
  std::uint64_t segment_number{};
  std::uint64_t first_record_sequence{};

  friend bool operator==(const SegmentHeader&, const SegmentHeader&) = default;
};

struct RecordLayout {
  std::uint32_t payload_length{};
  std::uint32_t padding_length{};
  std::uint32_t total_length{};

  friend bool operator==(const RecordLayout&, const RecordLayout&) = default;
};

struct RecordHeaderInput {
  std::uint16_t record_type{};
  std::uint64_t record_sequence{};
  std::size_t payload_length{};
};

struct RecordHeader {
  std::uint32_t total_length{};
  std::uint16_t record_format{};
  std::uint16_t record_type{};
  std::uint32_t record_flags{};
  std::uint64_t record_sequence{};
  std::uint32_t payload_length{};

  friend bool operator==(const RecordHeader&, const RecordHeader&) = default;
};

// DecodedRecord borrows payload bytes from the input passed to decode_record(). The input must
// outlive the value and every copy of its payload view.
struct DecodedRecord {
  RecordHeader header;
  common::ByteView payload;
  std::uint32_t record_crc32c{};
};

using EncodedSegmentHeader = std::array<std::byte, kSegmentHeaderSize>;
using EncodedRecordHeader = std::array<std::byte, kRecordHeaderSize>;

} // namespace chronos::wal

#endif // CHRONOS_WAL_TYPES_HPP_
