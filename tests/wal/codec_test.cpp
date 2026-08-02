#include "chronos/common/crc32c.hpp"
#include "chronos/wal/codec.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <random>
#include <vector>

namespace chronos::wal {
namespace {

constexpr std::array<std::uint8_t, kSegmentHeaderSize> kGoldenSegmentHeader{
    0x43, 0x48, 0x52, 0x4e, 0x57, 0x41, 0x4c, 0x00, 0x01, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00,
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x53, 0x5e, 0x9e,
};

constexpr std::array<std::uint8_t, 64> kGoldenRecord{
    0x40, 0x00, 0x00, 0x00, 0xbf, 0xff, 0xff, 0xff, 0x52, 0x45, 0x43, 0x31, 0x01, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x28, 0x00, 0x00, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x13, 0x00, 0x00, 0x00, 0x79, 0xea, 0x85, 0x9b, 0x01, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00,
    0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0xde, 0xad, 0xbe, 0x00, 0x04, 0x2c, 0x86, 0xe6,
};

[[nodiscard]] WalId test_wal_id() {
  WalId id;
  for (std::size_t index = 0; index < id.bytes.size(); ++index) {
    id.bytes[index] = static_cast<std::byte>(index);
  }
  return id;
}

template <std::size_t Size>
[[nodiscard]] std::array<std::byte, Size> as_bytes(const std::array<std::uint8_t, Size>& input) {
  std::array<std::byte, Size> output{};
  std::transform(input.begin(), input.end(), output.begin(),
                 [](const std::uint8_t byte) { return static_cast<std::byte>(byte); });
  return output;
}

void store_u16_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint16_t value) {
  bytes[offset] = static_cast<std::byte>(value & 0xffU);
  bytes[offset + 1U] = static_cast<std::byte>((value >> 8U) & 0xffU);
}

void store_u32_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t index = 0; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
}

void refresh_segment_crc(EncodedSegmentHeader& bytes) {
  store_u32_le(bytes, 60U, common::crc32c(common::ByteView{bytes}.first(60U)));
}

void refresh_record_header_crc(const common::MutableByteView bytes) {
  store_u32_le(bytes, 36U, common::crc32c(common::ByteView{bytes}.first(36U)));
}

void refresh_record_crc(const common::MutableByteView bytes) {
  store_u32_le(bytes, bytes.size() - 4U,
               common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

[[nodiscard]] std::vector<std::byte> golden_application_payload() {
  const auto record = as_bytes(kGoldenRecord);
  return {record.begin() + static_cast<std::ptrdiff_t>(kRecordHeaderSize), record.end() - 5};
}

TEST(WalTypesTest, ConstantsMatchTheAcceptedFormat) {
  static_assert(kSegmentHeaderSize == 64U);
  static_assert(kSegmentSizeLimit == 64ULL * 1024ULL * 1024ULL);
  static_assert(kRecordHeaderSize == 40U);
  static_assert(kMaximumRecordLength == 16U * 1024U * 1024U);
  static_assert(kMaximumPayloadLength == kMaximumRecordLength - 44U);
  EXPECT_EQ(kSegmentMagic.front(), std::byte{0x43});
  EXPECT_EQ(kSegmentMagic.back(), std::byte{0x00});
  EXPECT_EQ(kRecordMagic, (std::array<std::byte, 4>{std::byte{0x52}, std::byte{0x45},
                                                    std::byte{0x43}, std::byte{0x31}}));
}

TEST(WalTypesTest, IdentityAndPhysicalPositionValidationIsExplicit) {
  WalId zero;
  EXPECT_FALSE(zero.is_valid());
  const WalId id = test_wal_id();
  EXPECT_TRUE(id.is_valid());

  const PhysicalWalPosition start{.wal_id = id, .segment_number = 3U, .byte_offset = 64U};
  EXPECT_TRUE(validate_physical_wal_position(start).is_ok());
  const auto advanced = advance_physical_wal_position(start, 48U);
  ASSERT_TRUE(advanced.has_value());
  EXPECT_EQ(advanced->byte_offset, 112U);
  EXPECT_EQ(advanced->wal_id, id);

  PhysicalWalPosition invalid = start;
  invalid.wal_id = zero;
  EXPECT_EQ(validate_physical_wal_position(invalid).code(), common::StatusCode::kInvalidArgument);
  invalid = start;
  invalid.byte_offset = 65U;
  EXPECT_EQ(validate_physical_wal_position(invalid).code(), common::StatusCode::kInvalidArgument);
  invalid = start;
  invalid.byte_offset = kSegmentSizeLimit;
  const auto crosses = advance_physical_wal_position(invalid, 48U);
  ASSERT_FALSE(crosses.has_value());
  EXPECT_EQ(crosses.error().code(), common::StatusCode::kOutOfRange);
  EXPECT_FALSE(advance_physical_wal_position(start, 49U).has_value());
}

TEST(WalLayoutTest, CalculatesEveryPaddingClassAndBoundaryExactly) {
  for (std::size_t payload = 0; payload < 16U; ++payload) {
    SCOPED_TRACE(payload);
    const auto layout = calculate_record_layout(payload);
    ASSERT_TRUE(layout.has_value());
    EXPECT_EQ(layout->padding_length, (4U - (payload % 8U)) % 8U);
    EXPECT_EQ(layout->total_length,
              kRecordHeaderSize + payload + layout->padding_length + kRecordTrailerSize);
    EXPECT_EQ(layout->total_length % 8U, 0U);
  }

  const auto maximum = calculate_record_layout(kMaximumPayloadLength);
  ASSERT_TRUE(maximum.has_value());
  EXPECT_EQ(maximum->padding_length, 0U);
  EXPECT_EQ(maximum->total_length, kMaximumRecordLength);
  EXPECT_FALSE(
      calculate_record_layout(static_cast<std::size_t>(kMaximumPayloadLength) + 1U).has_value());
  EXPECT_FALSE(calculate_record_layout(std::numeric_limits<std::size_t>::max()).has_value());

  EXPECT_EQ(validate_segment_size(63U).code(), common::StatusCode::kOutOfRange);
  EXPECT_TRUE(validate_segment_size(64U).is_ok());
  EXPECT_TRUE(validate_segment_size(kSegmentSizeLimit).is_ok());
  EXPECT_EQ(validate_segment_size(kSegmentSizeLimit + 1U).code(), common::StatusCode::kOutOfRange);
}

TEST(WalSegmentHeaderTest, MatchesIndependentGoldenBytesAndRoundTrips) {
  const SegmentHeader header{
      .wal_id = test_wal_id(), .segment_number = 1U, .first_record_sequence = 1U};
  const auto encoded = encode_segment_header(header);
  ASSERT_TRUE(encoded.has_value());
  EXPECT_EQ(*encoded, as_bytes(kGoldenSegmentHeader));

  std::array<std::byte, kSegmentHeaderSize + 2U> unaligned_storage{};
  std::copy(encoded->begin(), encoded->end(), unaligned_storage.begin() + 1);
  const auto decoded =
      decode_segment_header(common::ByteView{unaligned_storage}.subspan(1U, kSegmentHeaderSize));
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(*decoded, header);
}

TEST(WalSegmentHeaderTest, RejectsTruncationBeforeReadingPastInput) {
  const auto bytes = as_bytes(kGoldenSegmentHeader);
  for (std::size_t size = 0; size < bytes.size(); ++size) {
    SCOPED_TRACE(size);
    const auto decoded = decode_segment_header(common::ByteView{bytes}.first(size));
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error().code(), common::StatusCode::kOutOfRange);
  }
}

TEST(WalSegmentHeaderTest, DistinguishesCorruptionFromUnsupportedFeatures) {
  EncodedSegmentHeader bytes = as_bytes(kGoldenSegmentHeader);
  bytes[0] ^= std::byte{1};
  EXPECT_EQ(decode_segment_header(bytes).error().code(), common::StatusCode::kCorruption);

  bytes = as_bytes(kGoldenSegmentHeader);
  bytes[20] ^= std::byte{1};
  EXPECT_EQ(decode_segment_header(bytes).error().code(), common::StatusCode::kCorruption);

  bytes = as_bytes(kGoldenSegmentHeader);
  store_u16_le(bytes, 8U, 2U);
  refresh_segment_crc(bytes);
  EXPECT_EQ(decode_segment_header(bytes).error().code(), common::StatusCode::kNotSupported);

  bytes = as_bytes(kGoldenSegmentHeader);
  store_u16_le(bytes, 10U, 1U);
  refresh_segment_crc(bytes);
  EXPECT_EQ(decode_segment_header(bytes).error().code(), common::StatusCode::kNotSupported);

  bytes = as_bytes(kGoldenSegmentHeader);
  store_u32_le(bytes, 56U, 1U);
  refresh_segment_crc(bytes);
  EXPECT_EQ(decode_segment_header(bytes).error().code(), common::StatusCode::kNotSupported);

  bytes = as_bytes(kGoldenSegmentHeader);
  std::fill_n(bytes.begin() + 16, kWalIdSize, std::byte{0});
  refresh_segment_crc(bytes);
  EXPECT_EQ(decode_segment_header(bytes).error().code(), common::StatusCode::kCorruption);

  bytes = as_bytes(kGoldenSegmentHeader);
  store_u32_le(bytes, 12U, 63U);
  refresh_segment_crc(bytes);
  EXPECT_EQ(decode_segment_header(bytes).error().code(), common::StatusCode::kCorruption);
}

TEST(WalSegmentHeaderTest, EncoderRejectsInvalidIdentityAndFirstSegmentSequence) {
  SegmentHeader header{.wal_id = {}, .segment_number = 1U, .first_record_sequence = 1U};
  EXPECT_FALSE(encode_segment_header(header).has_value());
  header.wal_id = test_wal_id();
  header.first_record_sequence = 2U;
  EXPECT_FALSE(encode_segment_header(header).has_value());
  header.segment_number = 0U;
  header.first_record_sequence = 1U;
  EXPECT_FALSE(encode_segment_header(header).has_value());
}

TEST(WalRecordTest, MatchesIndependentGoldenBytesAndDecodesBorrowedPayload) {
  const std::vector<std::byte> payload = golden_application_payload();
  const auto header = make_record_header({.record_type = kApplicationEntryRecordType,
                                          .record_sequence = 42U,
                                          .payload_length = payload.size()});
  ASSERT_TRUE(header.has_value());
  EXPECT_EQ(header->total_length, 64U);

  const auto encoded_header = encode_record_header(*header);
  ASSERT_TRUE(encoded_header.has_value());
  const auto golden = as_bytes(kGoldenRecord);
  EXPECT_TRUE(std::equal(encoded_header->begin(), encoded_header->end(), golden.begin()));

  std::array<std::byte, 72> destination{};
  destination.fill(std::byte{0xa5});
  const auto encoded = encode_record(*header, payload, destination);
  ASSERT_TRUE(encoded.has_value());
  EXPECT_EQ(*encoded, 64U);
  EXPECT_TRUE(std::equal(golden.begin(), golden.end(), destination.begin()));
  EXPECT_TRUE(std::all_of(destination.begin() + 64, destination.end(),
                          [](const std::byte byte) { return byte == std::byte{0xa5}; }));

  const auto decoded = decode_record(destination);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->header, *header);
  EXPECT_EQ(decoded->payload.data(), destination.data() + kRecordHeaderSize);
  EXPECT_TRUE(std::equal(decoded->payload.begin(), decoded->payload.end(), payload.begin()));
  EXPECT_EQ(decoded->record_crc32c, 0xe6862c04U);
}

TEST(WalRecordTest, SupportsUnalignedInputAndOutput) {
  const std::vector<std::byte> payload = golden_application_payload();
  const auto header = make_record_header({.record_type = kApplicationEntryRecordType,
                                          .record_sequence = 42U,
                                          .payload_length = payload.size()});
  ASSERT_TRUE(header.has_value());
  std::array<std::byte, 66> storage{};
  const common::MutableByteView unaligned = common::MutableByteView{storage}.subspan(1U, 64U);
  ASSERT_TRUE(encode_record(*header, payload, unaligned).has_value());
  const auto decoded = decode_record(common::ByteView{unaligned});
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->header.record_sequence, 42U);
}

TEST(WalRecordTest, SupportsPayloadAliasingDestination) {
  const auto expected = as_bytes(kGoldenRecord);
  const std::vector<std::byte> expected_payload = golden_application_payload();
  std::array<std::byte, 64> storage{};
  std::copy(expected.begin() + static_cast<std::ptrdiff_t>(kRecordHeaderSize), expected.end() - 5,
            storage.begin());
  const common::ByteView aliased_payload{storage.data(), expected_payload.size()};
  const auto header = make_record_header({.record_type = kApplicationEntryRecordType,
                                          .record_sequence = 42U,
                                          .payload_length = expected_payload.size()});
  ASSERT_TRUE(header.has_value());
  ASSERT_TRUE(encode_record(*header, aliased_payload, storage).has_value());

  const auto decoded = decode_record(storage);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_TRUE(
      std::equal(decoded->payload.begin(), decoded->payload.end(), expected_payload.begin()));
}

TEST(WalRecordTest, EveryTruncationIsBoundedAndReportedAsOutOfRange) {
  const auto bytes = as_bytes(kGoldenRecord);
  for (std::size_t size = 0; size < bytes.size(); ++size) {
    SCOPED_TRACE(size);
    const auto decoded = decode_record(common::ByteView{bytes}.first(size));
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error().code(), common::StatusCode::kOutOfRange);
  }
}

TEST(WalRecordTest, EncodesAndDecodesTheMaximumRecordWithoutLengthWraparound) {
  std::vector<std::byte> payload(kMaximumPayloadLength, std::byte{0x5a});
  store_u32_le(payload, 0U, 1U);
  store_u32_le(payload, 4U, 1U);
  const auto header =
      make_record_header({.record_type = kApplicationEntryRecordType,
                          .record_sequence = std::numeric_limits<std::uint64_t>::max(),
                          .payload_length = payload.size()});
  ASSERT_TRUE(header.has_value());
  EXPECT_EQ(header->total_length, kMaximumRecordLength);

  std::vector<std::byte> encoded(kMaximumRecordLength);
  const auto written = encode_record(*header, payload, encoded);
  ASSERT_TRUE(written.has_value());
  EXPECT_EQ(*written, kMaximumRecordLength);
  const auto decoded = decode_record(encoded);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->header, *header);
  EXPECT_EQ(decoded->payload.size(), kMaximumPayloadLength);
  EXPECT_EQ(decoded->payload.front(), std::byte{0x01});
  EXPECT_EQ(decoded->payload.back(), std::byte{0x5a});
}

TEST(WalRecordTest, DetectsHeaderLengthAndIntegrityCorruption) {
  auto bytes = as_bytes(kGoldenRecord);
  bytes[8] ^= std::byte{1};
  EXPECT_EQ(decode_record(bytes).error().code(), common::StatusCode::kCorruption);

  bytes = as_bytes(kGoldenRecord);
  bytes[4] ^= std::byte{1};
  refresh_record_header_crc(bytes);
  EXPECT_EQ(decode_record(bytes).error().code(), common::StatusCode::kCorruption);

  bytes = as_bytes(kGoldenRecord);
  store_u32_le(bytes, 32U, 8U);
  refresh_record_header_crc(bytes);
  EXPECT_EQ(decode_record(bytes).error().code(), common::StatusCode::kCorruption);

  bytes = as_bytes(kGoldenRecord);
  bytes[63] ^= std::byte{1};
  EXPECT_EQ(decode_record(bytes).error().code(), common::StatusCode::kCorruption);

  bytes = as_bytes(kGoldenRecord);
  bytes[59] = std::byte{1};
  refresh_record_crc(bytes);
  EXPECT_EQ(decode_record(bytes).error().code(), common::StatusCode::kCorruption);
}

TEST(WalRecordTest, ValidatesAssignedApplicationEnvelopeAfterChecksums) {
  auto bytes = as_bytes(kGoldenRecord);
  store_u32_le(bytes, 40U, 0U);
  refresh_record_crc(bytes);
  EXPECT_EQ(decode_record(bytes).error().code(), common::StatusCode::kCorruption);

  bytes = as_bytes(kGoldenRecord);
  store_u32_le(bytes, 44U, 0U);
  refresh_record_crc(bytes);
  EXPECT_EQ(decode_record(bytes).error().code(), common::StatusCode::kCorruption);

  std::array<std::byte, 8> short_payload{};
  store_u32_le(short_payload, 0U, 1U);
  store_u32_le(short_payload, 4U, 1U);
  const auto short_header = make_record_header(
      {.record_type = 2U, .record_sequence = 1U, .payload_length = short_payload.size()});
  ASSERT_TRUE(short_header.has_value());
  std::array<std::byte, 56> short_record{};
  ASSERT_TRUE(encode_record(*short_header, short_payload, short_record).has_value());
  store_u16_le(short_record, 14U, kApplicationEntryRecordType);
  refresh_record_header_crc(short_record);
  refresh_record_crc(short_record);
  EXPECT_EQ(decode_record(short_record).error().code(), common::StatusCode::kCorruption);
}

TEST(WalRecordTest, StructurallyDecodesUnknownFormatsAndTypesButRejectsRequiredFlags) {
  auto bytes = as_bytes(kGoldenRecord);
  store_u16_le(bytes, 12U, 2U);
  refresh_record_header_crc(bytes);
  refresh_record_crc(bytes);
  const auto unknown_format = decode_record(bytes);
  ASSERT_TRUE(unknown_format.has_value());
  EXPECT_EQ(unknown_format->header.record_format, 2U);

  bytes = as_bytes(kGoldenRecord);
  store_u16_le(bytes, 14U, 9U);
  refresh_record_header_crc(bytes);
  refresh_record_crc(bytes);
  const auto unknown_type = decode_record(bytes);
  ASSERT_TRUE(unknown_type.has_value());
  EXPECT_EQ(unknown_type->header.record_type, 9U);

  bytes = as_bytes(kGoldenRecord);
  store_u32_le(bytes, 16U, 1U);
  refresh_record_header_crc(bytes);
  refresh_record_crc(bytes);
  const auto flags = decode_record(bytes);
  ASSERT_FALSE(flags.has_value());
  EXPECT_EQ(flags.error().code(), common::StatusCode::kNotSupported);

  bytes[63] ^= std::byte{1};
  const auto corrupt_flagged_record = decode_record(bytes);
  ASSERT_FALSE(corrupt_flagged_record.has_value());
  EXPECT_EQ(corrupt_flagged_record.error().code(), common::StatusCode::kCorruption);
}

TEST(WalRecordTest, FailedEncodingLeavesDestinationUnchanged) {
  const std::vector<std::byte> payload = golden_application_payload();
  const auto header = make_record_header({.record_type = kApplicationEntryRecordType,
                                          .record_sequence = 42U,
                                          .payload_length = payload.size()});
  ASSERT_TRUE(header.has_value());

  std::array<std::byte, 64> destination{};
  destination.fill(std::byte{0xa5});
  const auto original = destination;
  EXPECT_FALSE(
      encode_record(*header, payload, common::MutableByteView{destination}.first(63U)).has_value());
  EXPECT_EQ(destination, original);

  RecordHeader inconsistent = *header;
  inconsistent.payload_length -= 1U;
  EXPECT_FALSE(encode_record(inconsistent, payload, destination).has_value());
  EXPECT_EQ(destination, original);

  std::vector<std::byte> invalid_payload = payload;
  std::fill_n(invalid_payload.begin(), 4U, std::byte{0});
  EXPECT_FALSE(encode_record(*header, invalid_payload, destination).has_value());
  EXPECT_EQ(destination, original);
}

TEST(WalRecordTest, DeterministicPropertyRoundTripsPayloadsAndPaddingClasses) {
  constexpr std::uint64_t kSeed = 0x39f07c2db1846ea5ULL;
  // Determinism is required so a reported seed and iteration reproduce a failure.
  // NOLINTNEXTLINE(bugprone-random-generator-seed)
  std::mt19937_64 random{kSeed};
  for (std::size_t iteration = 0; iteration < 1000U; ++iteration) {
    SCOPED_TRACE(::testing::Message() << "seed=" << kSeed << " iteration=" << iteration);
    const std::size_t body_size = static_cast<std::size_t>(random() % 257U);
    std::vector<std::byte> payload(kApplicationEnvelopeSize + body_size);
    std::uint32_t application_format = static_cast<std::uint32_t>(random());
    std::uint32_t application_kind = static_cast<std::uint32_t>(random());
    application_format = application_format == 0U ? 1U : application_format;
    application_kind = application_kind == 0U ? 1U : application_kind;
    store_u32_le(payload, 0U, application_format);
    store_u32_le(payload, 4U, application_kind);
    for (std::size_t index = 8U; index < payload.size(); ++index) {
      payload[index] = static_cast<std::byte>(random() & 0xffU);
    }
    std::uint64_t sequence = random();
    sequence = sequence == 0U ? 1U : sequence;
    const auto header = make_record_header({.record_type = kApplicationEntryRecordType,
                                            .record_sequence = sequence,
                                            .payload_length = payload.size()});
    ASSERT_TRUE(header.has_value());
    std::vector<std::byte> encoded(static_cast<std::size_t>(header->total_length) + 7U,
                                   std::byte{0xa5});
    const auto written = encode_record(*header, payload, encoded);
    ASSERT_TRUE(written.has_value());
    EXPECT_EQ(*written, header->total_length);
    const auto decoded = decode_record(encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->header, *header);
    EXPECT_TRUE(std::equal(decoded->payload.begin(), decoded->payload.end(), payload.begin()));
    EXPECT_TRUE(std::all_of(encoded.begin() + static_cast<std::ptrdiff_t>(*written), encoded.end(),
                            [](const std::byte byte) { return byte == std::byte{0xa5}; }));
  }
}

} // namespace
} // namespace chronos::wal
