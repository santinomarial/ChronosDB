#include "chronos/ingest/columnar_append.hpp"
#include "ingest/ingest_test_support.hpp"

#include <array>
#include <cstddef>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::ingest {
namespace {

void expect_kind(const common::ByteView bytes, const ColumnarAppendDecodeErrorKind kind) {
  const auto decoded = decode_columnar_append_v1_exact(bytes);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().kind(), kind) << decoded.error().status().to_string();
}

TEST(ColumnarAppendCorruptionTest, RejectsUnsupportedEnvelopeAndCommandSemantics) {
  using namespace columnar_append_v1;
  constexpr std::array<std::size_t, 6U> kU32Offsets{
      wal::kApplicationFormatOffset, wal::kApplicationKindOffset,
      wal::kApplicationEnvelopeSize + kCommandFlagsOffset,
      wal::kApplicationEnvelopeSize + kMutationKindOffset,
      wal::kApplicationEnvelopeSize + kDigestAlgorithmOffset,
      wal::kApplicationEnvelopeSize + kOutcomeCodeOffset};
  for (const std::size_t offset : kU32Offsets) {
    std::vector<std::byte> bytes = test::command_bytes();
    test::store_u32_le(bytes, offset, 9U);
    expect_kind(bytes, ColumnarAppendDecodeErrorKind::kUnsupported);
  }
  std::vector<std::byte> bytes = test::command_bytes();
  bytes[wal::kApplicationFlagsOffset] = std::byte{1U};
  expect_kind(bytes, ColumnarAppendDecodeErrorKind::kUnsupported);
}

TEST(ColumnarAppendCorruptionTest, RejectsHostileHeaderDigestAndEmbeddedBatchDamage) {
  using namespace columnar_append_v1;
  for (const std::size_t offset : {
           wal::kApplicationEnvelopeSize + kCommandHeaderLengthOffset,
           wal::kApplicationEnvelopeSize + kReservedOffset,
           wal::kApplicationEnvelopeSize + kTableIdOffset + 15U,
           wal::kApplicationEnvelopeSize + kSchemaVersionOffset,
           wal::kApplicationEnvelopeSize + kRowCountOffset,
           wal::kApplicationEnvelopeSize + kRequestDigestOffset,
           kApplicationPayloadHeaderLength + 200U,
       }) {
    SCOPED_TRACE(offset);
    std::vector<std::byte> bytes = test::command_bytes();
    bytes[offset] ^= std::byte{1U};
    expect_kind(bytes, ColumnarAppendDecodeErrorKind::kCorruption);
  }

  std::vector<std::byte> length_bytes = test::command_bytes();
  length_bytes[wal::kApplicationEnvelopeSize + kBatchLengthOffset] ^= std::byte{1U};
  expect_kind(length_bytes, ColumnarAppendDecodeErrorKind::kIncomplete);

  std::vector<std::byte> bytes = test::command_bytes();
  std::fill_n(bytes.begin() + static_cast<std::ptrdiff_t>(wal::kApplicationEnvelopeSize +
                                                          kClientIdOffset),
              16U, std::byte{0U});
  expect_kind(bytes, ColumnarAppendDecodeErrorKind::kCorruption);
}

TEST(ColumnarAppendCorruptionTest, RejectsMismatchedRecordMetadataBeforeCommandDecoding) {
  const wal::EncodedApplicationPayload encoded = test::encoded_command();
  wal::DecodedRecord record{.header = {.total_length = 0U,
                                      .record_format = wal::kRecordFormat,
                                      .record_type = wal::kApplicationEntryRecordType,
                                      .record_flags = 0U,
                                      .record_sequence = 1U,
                                      .payload_length =
                                          static_cast<std::uint32_t>(encoded.size() - 1U)},
                            .payload = encoded.bytes(),
                            .record_crc32c = 0U};
  expect_kind(record.payload.first(0U), ColumnarAppendDecodeErrorKind::kIncomplete);
  const auto decoded = decode_columnar_append_v1_record(record);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().kind(), ColumnarAppendDecodeErrorKind::kCorruption);
  record.header.payload_length = static_cast<std::uint32_t>(encoded.size());
  record.header.record_type = 9U;
  const auto unsupported_record = decode_columnar_append_v1_record(record);
  ASSERT_FALSE(unsupported_record.has_value());
  EXPECT_EQ(unsupported_record.error().kind(), ColumnarAppendDecodeErrorKind::kUnsupported);
}

} // namespace
} // namespace chronos::ingest
