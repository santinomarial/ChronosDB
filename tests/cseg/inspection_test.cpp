#include "chronos/common/crc32c.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/inspection.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "cseg_test_fixture.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::cseg {
namespace {

void store_u16(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint16_t value) {
  bytes[offset] = std::byte{static_cast<std::uint8_t>(value)};
  bytes[offset + 1U] = std::byte{static_cast<std::uint8_t>(value >> 8U)};
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
  }
}

TEST(CsegInspectionTest, ReportsOwnedValidatedMetadataAndPageAccounting) {
  CsegInspectionReport report = [&] {
    const EncodedCsegPart encoded = test::make_valid_part();
    return inspect_cseg_v1_part(encoded.bytes()).value();
  }();

  EXPECT_EQ(report.part_id, test::identifier<PartId>(1U));
  EXPECT_EQ(report.table_id, test::identifier<schema::TableId>(2U));
  EXPECT_EQ(report.tablet_id, test::identifier<schema::TabletId>(3U));
  EXPECT_EQ(report.schema_id, test::identifier<schema::SchemaId>(4U));
  EXPECT_EQ(report.schema_version, schema::SchemaVersion::initial());
  EXPECT_EQ(report.row_count, 2U);
  EXPECT_EQ(report.event_time_column_ordinal, 0U);
  EXPECT_EQ(report.ordering_column_count, 1U);
  EXPECT_EQ(report.minimum_event_time, -5);
  EXPECT_EQ(report.maximum_event_time, 10);
  EXPECT_EQ(report.columns.size(), 5U);
  EXPECT_EQ(report.granules.size(), 1U);
  ASSERT_EQ(report.pages.size(), 5U);
  EXPECT_EQ(report.raw_page_count, 5U);
  EXPECT_EQ(report.zstd_page_count, 0U);
  std::uint64_t stored = 0U;
  std::uint64_t uncompressed = 0U;
  for (const CsegPageDescriptor& page : report.pages) {
    stored += page.stored_length;
    uncompressed += page.uncompressed_length;
  }
  EXPECT_EQ(report.stored_page_bytes, stored);
  EXPECT_EQ(report.uncompressed_page_bytes, uncompressed);
  EXPECT_EQ(report.columns.front().column_id, test::identifier<schema::ColumnId>(5U));
}

TEST(CsegInspectionTest, AccountsForZstdPages) {
  const EncodedCsegPart encoded = test::make_valid_part(PageCompression::kZstd);
  const CsegInspectionResult report = inspect_cseg_v1_part(encoded.bytes());

  ASSERT_TRUE(report.has_value()) << report.error().status().to_string();
  EXPECT_GT(report->zstd_page_count, 0U);
  EXPECT_EQ(report->raw_page_count + report->zstd_page_count, 5U);
}

TEST(CsegInspectionTest, DistinguishesIncompleteCorruptionAndResourceLimits) {
  const EncodedCsegPart encoded = test::make_valid_part();
  const CsegInspectionResult incomplete =
      inspect_cseg_v1_part(encoded.bytes().first(encoded.size() - 1U));
  ASSERT_FALSE(incomplete.has_value());
  EXPECT_EQ(incomplete.error().kind(), CsegInspectionErrorKind::kIncomplete);
  EXPECT_EQ(incomplete.error().required_size(), encoded.size());

  std::vector<std::byte> corrupt(encoded.bytes().begin(), encoded.bytes().end());
  corrupt.back() ^= std::byte{0x80U};
  const CsegInspectionResult corruption = inspect_cseg_v1_part(corrupt);
  ASSERT_FALSE(corruption.has_value());
  EXPECT_EQ(corruption.error().kind(), CsegInspectionErrorKind::kCorruption);

  const CsegInspectionResult limited = inspect_cseg_v1_part(
      encoded.bytes(), {.decode = {.max_file_length = encoded.size() - 1U}, .validation = {}});
  ASSERT_FALSE(limited.has_value());
  EXPECT_EQ(limited.error().kind(), CsegInspectionErrorKind::kResourceLimit);

  const CsegInspectionResult invalid = inspect_cseg_v1_part(
      encoded.bytes(), {.decode = {}, .validation = {.max_working_bytes = 0U}});
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().kind(), CsegInspectionErrorKind::kInvalidArgument);
}

TEST(CsegInspectionTest, RejectsTrailingBytesAsCorruption) {
  const EncodedCsegPart encoded = test::make_valid_part();
  std::vector<std::byte> suffixed(encoded.bytes().begin(), encoded.bytes().end());
  suffixed.push_back(std::byte{});

  const CsegInspectionResult report = inspect_cseg_v1_part(suffixed);

  ASSERT_FALSE(report.has_value());
  EXPECT_EQ(report.error().kind(), CsegInspectionErrorKind::kCorruption);
}

TEST(CsegInspectionTest, PreservesUnsupportedClassificationForAuthenticatedVersions) {
  const EncodedCsegPart encoded = test::make_valid_part();
  const CsegPartDecodeResult original = decode_cseg_v1_part_exact(encoded.bytes());
  ASSERT_TRUE(original.has_value());
  const std::size_t metadata_size = original->metadata().encoded_metadata().size();
  std::vector<std::byte> bytes(encoded.bytes().begin(), encoded.bytes().end());
  store_u16(bytes, format::kFormatMinorOffset, 1U);
  store_u32(bytes, format::kHeaderCrc32cOffset,
            common::crc32c(common::ByteView{bytes}.first(format::kHeaderCrc32cOffset)));
  const std::size_t metadata_crc_offset = metadata_size - format::kMetadataCrc32cLength;
  store_u32(bytes, metadata_crc_offset,
            common::crc32c(common::ByteView{bytes}.first(metadata_crc_offset)));

  const CsegInspectionResult report = inspect_cseg_v1_part(bytes);

  ASSERT_FALSE(report.has_value());
  EXPECT_EQ(report.error().kind(), CsegInspectionErrorKind::kUnsupported);
}

} // namespace
} // namespace chronos::cseg
