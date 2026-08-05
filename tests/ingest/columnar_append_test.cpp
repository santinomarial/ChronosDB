#include "chronos/ingest/columnar_append.hpp"
#include "chronos/wal/codec.hpp"
#include "ingest/ingest_test_support.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <ranges>
#include <vector>

namespace chronos::ingest {
namespace {

// Generated independently from ADR-0015 with Python struct.pack and hashlib.sha256. The embedded
// 400-byte suffix is the independently reviewed Columnar Batch v1 golden fixture in the columnar
// codec tests; this literal covers every envelope and command-header byte around that suffix.
constexpr std::array<std::uint8_t, 176U> kGoldenCommandPrefix{
    0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0xa0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
    0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x90, 0x01, 0x00, 0x00,
    0xef, 0x60, 0x3d, 0x8a, 0x85, 0xac, 0xba, 0x37, 0xab, 0xc4, 0x0a, 0x7f, 0xe4, 0x3e, 0x3c, 0x9a,
    0x37, 0x78, 0x7d, 0xcb, 0x32, 0x2d, 0x4b, 0x76, 0x63, 0xfa, 0xb0, 0x1c, 0x41, 0xfe, 0xa8, 0x39,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
};

[[nodiscard]] std::array<std::byte, kGoldenCommandPrefix.size()> golden_prefix() {
  std::array<std::byte, kGoldenCommandPrefix.size()> bytes{};
  std::transform(kGoldenCommandPrefix.begin(), kGoldenCommandPrefix.end(), bytes.begin(),
                 [](const std::uint8_t value) { return static_cast<std::byte>(value); });
  return bytes;
}

TEST(ColumnarAppendFormatTest, ConstantsMatchTheFrozenContractsAndWALBoundary) {
  using namespace columnar_append_v1;
  static_assert(kCommandHeaderLength == 160U);
  static_assert(kApplicationPayloadHeaderLength == 176U);
  static_assert(kMaximumApplicationPayloadLength == 16'777'168U);
  static_assert(kMaximumApplicationPayloadLength + 4U == wal::kMaximumPayloadLength);
  static_assert(kBatchOffset == 160U);
  static_assert(kRequestDigestDomain.size() == 28U);
}

TEST(ColumnarAppendCodecTest, MatchesIndependentGoldenHeaderAndDecodesBorrowedViews) {
  const wal::EncodedApplicationPayload encoded = test::encoded_command();
  ASSERT_EQ(encoded.size(), 576U);
  EXPECT_TRUE(std::ranges::equal(encoded.bytes().first(176U), golden_prefix()));

  const auto decoded = decode_columnar_append_v1_exact(encoded.bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().status().to_string();
  EXPECT_EQ(decoded->client_id(), test::request_id<ClientId>(0x10U));
  EXPECT_EQ(decoded->client_batch_id(), test::request_id<ClientBatchId>(0x20U));
  EXPECT_EQ(decoded->table_id(), columnar::test::id<schema::TableId>(50U));
  EXPECT_EQ(decoded->tablet_id(), columnar::test::id<schema::TabletId>(52U));
  EXPECT_EQ(decoded->schema_id(), columnar::test::id<schema::SchemaId>(51U));
  EXPECT_EQ(decoded->schema_version(), schema::SchemaVersion::initial());
  EXPECT_EQ(decoded->row_count(), 2U);
  EXPECT_EQ(decoded->encoded_payload().data(), encoded.bytes().data());
  EXPECT_EQ(decoded->batch().encoded_bytes().data(), encoded.bytes().data() + 176U);
  EXPECT_TRUE(validate_columnar_append_schema(*decoded, *columnar::test::batch_schema()).is_ok());
}

TEST(ColumnarAppendCodecTest, PrefixExactAndRecordAdaptersPreserveClassificationsAndOwnership) {
  const wal::EncodedApplicationPayload encoded = test::encoded_command();
  for (std::size_t size = 0U; size < encoded.size(); ++size) {
    SCOPED_TRACE(size);
    const auto decoded = decode_columnar_append_v1_prefix(encoded.bytes().first(size));
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error().kind(), ColumnarAppendDecodeErrorKind::kIncomplete);
    if (size < 16U) {
      EXPECT_EQ(decoded.error().required_size(), 16U);
    } else if (size < 176U) {
      EXPECT_EQ(decoded.error().required_size(), 176U);
    } else {
      EXPECT_EQ(decoded.error().required_size(), 576U);
    }
  }

  std::vector<std::byte> with_suffix(encoded.bytes().begin(), encoded.bytes().end());
  with_suffix.push_back(std::byte{0x55});
  ASSERT_TRUE(decode_columnar_append_v1_prefix(with_suffix).has_value());
  const auto exact = decode_columnar_append_v1_exact(with_suffix);
  ASSERT_FALSE(exact.has_value());
  EXPECT_EQ(exact.error().kind(), ColumnarAppendDecodeErrorKind::kCorruption);

  const auto header = wal::make_record_header(
      {.record_type = wal::kApplicationEntryRecordType,
       .record_sequence = 7U,
       .payload_length = encoded.size()});
  ASSERT_TRUE(header.has_value());
  std::vector<std::byte> record_bytes(header->total_length);
  ASSERT_TRUE(wal::encode_record(*header, encoded.bytes(), record_bytes).has_value());
  const auto record = wal::decode_record(record_bytes);
  ASSERT_TRUE(record.has_value());
  const auto from_record = decode_columnar_append_v1_record(*record);
  ASSERT_TRUE(from_record.has_value()) << from_record.error().status().to_string();
  EXPECT_EQ(from_record->encoded_payload().data(), record->payload.data());
}

TEST(ColumnarAppendCodecTest, EnforcesApplicationAndNestedBatchLimits) {
  const wal::EncodedApplicationPayload encoded = test::encoded_command();
  for (const ColumnarAppendDecodeLimits limits : {
           ColumnarAppendDecodeLimits{.max_application_payload_length = 575U},
           ColumnarAppendDecodeLimits{
               .max_application_payload_length = 576U,
               .batch = {.max_batch_length = 399U, .max_rows = 2U, .max_columns = 3U}},
           ColumnarAppendDecodeLimits{
               .max_application_payload_length = 576U,
               .batch = {.max_batch_length = 400U, .max_rows = 1U, .max_columns = 3U}},
       }) {
    const auto decoded = decode_columnar_append_v1_exact(encoded.bytes(), limits);
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error().kind(), ColumnarAppendDecodeErrorKind::kResourceLimit);
  }
}

} // namespace
} // namespace chronos::ingest
