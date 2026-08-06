#include "chronos/common/crc32c.hpp"
#include "chronos/common/status.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/metadata_codec.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <span>
#include <vector>

namespace chronos::cseg {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    bytes[index] = std::byte{static_cast<std::uint8_t>(seed + index)};
  }
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

struct MetadataFixture {
  PartId part_id{id<PartId>(0x10U)};
  schema::TableId table_id{id<schema::TableId>(0x20U)};
  schema::TabletId tablet_id{id<schema::TabletId>(0x30U)};
  schema::SchemaId schema_id{id<schema::SchemaId>(0x40U)};
  std::vector<CsegColumnDescriptor> columns{
      CsegColumnDescriptor{.column_id = id<schema::ColumnId>(0x50U),
                           .storage_kind = StorageKind::kUser,
                           .logical_type = type(schema::LogicalTypeKind::kTimestampNs),
                           .nullable = false,
                           .event_time = true,
                           .schema_ordinal = 0U,
                           .ordering_ordinal = 0U},
      CsegColumnDescriptor{.column_id = std::nullopt,
                           .storage_kind = StorageKind::kWalId,
                           .logical_type = type(schema::LogicalTypeKind::kUuid),
                           .nullable = false,
                           .event_time = false,
                           .schema_ordinal = std::nullopt,
                           .ordering_ordinal = std::nullopt},
      CsegColumnDescriptor{.column_id = std::nullopt,
                           .storage_kind = StorageKind::kRecordSequence,
                           .logical_type = type(schema::LogicalTypeKind::kUInt64),
                           .nullable = false,
                           .event_time = false,
                           .schema_ordinal = std::nullopt,
                           .ordering_ordinal = std::nullopt},
      CsegColumnDescriptor{.column_id = std::nullopt,
                           .storage_kind = StorageKind::kRowOrdinal,
                           .logical_type = type(schema::LogicalTypeKind::kUInt32),
                           .nullable = false,
                           .event_time = false,
                           .schema_ordinal = std::nullopt,
                           .ordering_ordinal = std::nullopt},
      CsegColumnDescriptor{.column_id = std::nullopt,
                           .storage_kind = StorageKind::kOperation,
                           .logical_type = type(schema::LogicalTypeKind::kUInt8),
                           .nullable = false,
                           .event_time = false,
                           .schema_ordinal = std::nullopt,
                           .ordering_ordinal = std::nullopt},
  };
  std::vector<CsegGranuleDescriptor> granules{
      CsegGranuleDescriptor{.first_row = 0U,
                            .row_count = 2U,
                            .first_page_index = 0U,
                            .minimum_event_time = -5,
                            .maximum_event_time = 10},
  };
  std::vector<CsegPageMetadataInput> pages{
      page(16U, 0x11111111U), page(32U, 0x22222222U), page(16U, 0x33333333U),
      page(8U, 0x44444444U),  page(2U, 0x55555555U),
  };

  [[nodiscard]] static CsegPageMetadataInput page(const std::uint64_t length,
                                                  const std::uint32_t crc) {
    return CsegPageMetadataInput{.compression = PageCompression::kNone,
                                 .row_count = 2U,
                                 .null_count = 0U,
                                 .stored_length = length,
                                 .uncompressed_length = length,
                                 .validity_length = 0U,
                                 .offsets_length = 0U,
                                 .values_length = length,
                                 .page_crc32c = crc};
  }

  [[nodiscard]] CsegMetadataEncodeInput input() const {
    return CsegMetadataEncodeInput{.part_id = part_id,
                                   .table_id = table_id,
                                   .tablet_id = tablet_id,
                                   .schema_id = schema_id,
                                   .schema_version = schema::SchemaVersion::initial(),
                                   .row_count = 2U,
                                   .event_time_column_ordinal = 0U,
                                   .ordering_column_count = 1U,
                                   .minimum_event_time = -5,
                                   .maximum_event_time = 10,
                                   .columns = columns,
                                   .granules = granules,
                                   .pages = pages};
  }
};

void store_u16(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint16_t value) {
  bytes[offset] = std::byte{static_cast<std::uint8_t>(value)};
  bytes[offset + 1U] = std::byte{static_cast<std::uint8_t>(value >> 8U)};
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
  }
}

void store_u64(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint64_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
  }
}

[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes, const std::size_t offset) {
  std::uint32_t value = 0U;
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= std::to_integer<std::uint32_t>(bytes[offset + index]) << (index * 8U);
  }
  return value;
}

void refresh_metadata_crc(std::vector<std::byte>& bytes) {
  const std::size_t offset = bytes.size() - format::kMetadataCrc32cLength;
  store_u32(bytes, offset, common::crc32c(common::ByteView{bytes}.first(offset)));
}

void refresh_header_and_metadata_crc(std::vector<std::byte>& bytes) {
  store_u32(bytes, format::kHeaderCrc32cOffset,
            common::crc32c(common::ByteView{bytes}.first(format::kHeaderCrc32cOffset)));
  refresh_metadata_crc(bytes);
}

[[nodiscard]] std::vector<std::byte> encoded_fixture() {
  MetadataFixture fixture;
  const common::Result<EncodedCsegMetadata> encoded = encode_cseg_v1_metadata(fixture.input());
  return {encoded->bytes().begin(), encoded->bytes().end()};
}

TEST(CsegMetadataCodecTest, EncodesCanonicalDirectoryAndDecodesBorrowedMetadata) {
  MetadataFixture fixture;
  const common::Result<EncodedCsegMetadata> encoded = encode_cseg_v1_metadata(fixture.input());
  ASSERT_TRUE(encoded.has_value());
  EXPECT_EQ(encoded->size(), 1'208U);
  EXPECT_EQ(encoded->total_length(), 1'288U);
  // Independently generated with Python struct packing and a tableless reflected CRC32C, without
  // calling the production encoder or CRC implementation.
  EXPECT_EQ(load_u32(encoded->bytes(), format::kHeaderCrc32cOffset), 0x04d99ea6U);
  EXPECT_EQ(load_u32(encoded->bytes(), encoded->size() - format::kMetadataCrc32cLength),
            0xd0d3e30eU);

  const CsegMetadataDecodeResult decoded = decode_cseg_v1_metadata_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->part_id(), fixture.part_id);
  EXPECT_EQ(decoded->table_id(), fixture.table_id);
  EXPECT_EQ(decoded->tablet_id(), fixture.tablet_id);
  EXPECT_EQ(decoded->schema_id(), fixture.schema_id);
  EXPECT_EQ(decoded->schema_version(), schema::SchemaVersion::initial());
  EXPECT_EQ(decoded->row_count(), 2U);
  EXPECT_EQ(decoded->minimum_event_time(), -5);
  EXPECT_EQ(decoded->maximum_event_time(), 10);
  EXPECT_TRUE(std::ranges::equal(decoded->columns(), fixture.columns));
  EXPECT_TRUE(std::ranges::equal(decoded->granules(), fixture.granules));
  ASSERT_EQ(decoded->pages().size(), 5U);
  EXPECT_EQ(decoded->pages()[0].page_offset, 1'208U);
  EXPECT_EQ(decoded->pages()[1].page_offset, 1'224U);
  EXPECT_EQ(decoded->pages()[2].page_offset, 1'256U);
  EXPECT_EQ(decoded->pages()[3].page_offset, 1'272U);
  EXPECT_EQ(decoded->pages()[4].page_offset, 1'280U);
  EXPECT_EQ(decoded->pages()[4].page_crc32c, 0x55555555U);
  EXPECT_EQ(decoded->encoded_metadata().data(), encoded->bytes().data());
}

TEST(CsegMetadataCodecTest, PrefixClassifiesEveryTruncationAndExactRejectsSuffix) {
  const std::vector<std::byte> bytes = encoded_fixture();
  for (std::size_t size = 0U; size < bytes.size(); ++size) {
    const auto decoded = decode_cseg_v1_metadata_prefix(common::ByteView{bytes}.first(size));
    ASSERT_FALSE(decoded.has_value()) << size;
    EXPECT_EQ(decoded.error().kind(), CsegMetadataDecodeErrorKind::kIncomplete) << size;
    const std::uint64_t expected =
        size < format::kMagic.size()
            ? format::kMagic.size()
            : (size < format::kFileHeaderLength ? format::kFileHeaderLength : bytes.size());
    EXPECT_EQ(decoded.error().required_size(), expected) << size;
  }

  std::vector<std::byte> suffixed = bytes;
  suffixed.push_back(std::byte{0x7f});
  EXPECT_TRUE(decode_cseg_v1_metadata_prefix(suffixed).has_value());
  const auto exact = decode_cseg_v1_metadata_exact(suffixed);
  ASSERT_FALSE(exact.has_value());
  EXPECT_EQ(exact.error().kind(), CsegMetadataDecodeErrorKind::kCorruption);
}

TEST(CsegMetadataCodecTest, HeaderAndMetadataIntegrityFailClosed) {
  std::vector<std::byte> bytes = encoded_fixture();
  bytes[format::kTotalLengthOffset] ^= std::byte{1U};
  auto decoded = decode_cseg_v1_metadata_exact(bytes);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().kind(), CsegMetadataDecodeErrorKind::kCorruption);

  bytes = encoded_fixture();
  bytes[format::kColumnsOffset] ^= std::byte{1U};
  decoded = decode_cseg_v1_metadata_exact(bytes);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().kind(), CsegMetadataDecodeErrorKind::kCorruption);
}

TEST(CsegMetadataCodecPropertyTest, EverySingleByteMutationFailsIntegrityValidation) {
  const std::vector<std::byte> golden = encoded_fixture();
  for (std::size_t offset = 0U; offset < golden.size(); ++offset) {
    std::vector<std::byte> mutated = golden;
    mutated[offset] ^= std::byte{1U};
    const auto decoded = decode_cseg_v1_metadata_exact(mutated);
    EXPECT_FALSE(decoded.has_value()) << offset;
  }
}

TEST(CsegMetadataCodecTest, DistinguishesUnsupportedRegistriesFromInvalidZeroCodes) {
  const std::vector<std::byte> golden = encoded_fixture();
  const std::size_t first_page = 800U;
  const auto classify_u16 = [&](const std::size_t offset, const std::uint16_t value,
                                const bool header, const CsegMetadataDecodeErrorKind expected) {
    std::vector<std::byte> bytes = golden;
    store_u16(bytes, offset, value);
    header ? refresh_header_and_metadata_crc(bytes) : refresh_metadata_crc(bytes);
    const auto decoded = decode_cseg_v1_metadata_exact(bytes);
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error().kind(), expected);
  };
  const auto classify_u32 = [&](const std::size_t offset, const std::uint32_t value,
                                const bool header, const CsegMetadataDecodeErrorKind expected) {
    std::vector<std::byte> bytes = golden;
    store_u32(bytes, offset, value);
    header ? refresh_header_and_metadata_crc(bytes) : refresh_metadata_crc(bytes);
    const auto decoded = decode_cseg_v1_metadata_exact(bytes);
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error().kind(), expected);
  };

  classify_u16(format::kFormatMinorOffset, 1U, true, CsegMetadataDecodeErrorKind::kUnsupported);
  classify_u32(format::kFileFlagsOffset, 1U, true, CsegMetadataDecodeErrorKind::kUnsupported);
  classify_u16(256U + format::kStorageKindOffset, 6U, false,
               CsegMetadataDecodeErrorKind::kUnsupported);
  classify_u16(256U + format::kStorageKindOffset, 0U, false,
               CsegMetadataDecodeErrorKind::kCorruption);
  classify_u16(256U + format::kLogicalTypeOffset, 19U, false,
               CsegMetadataDecodeErrorKind::kUnsupported);
  classify_u16(256U + format::kLogicalTypeOffset, 0U, false,
               CsegMetadataDecodeErrorKind::kCorruption);
  classify_u32(256U + format::kColumnFlagsOffset, 8U, false,
               CsegMetadataDecodeErrorKind::kUnsupported);
  classify_u16(first_page + format::kPagePhysicalEncodingOffset, 2U, false,
               CsegMetadataDecodeErrorKind::kUnsupported);
  classify_u16(first_page + format::kPagePhysicalEncodingOffset, 0U, false,
               CsegMetadataDecodeErrorKind::kCorruption);
  classify_u16(first_page + format::kPageCompressionOffset, 3U, false,
               CsegMetadataDecodeErrorKind::kUnsupported);
  classify_u16(first_page + format::kPageCompressionOffset, 0U, false,
               CsegMetadataDecodeErrorKind::kCorruption);
  classify_u32(first_page + format::kPageFlagsOffset, 1U, false,
               CsegMetadataDecodeErrorKind::kUnsupported);
}

TEST(CsegMetadataCodecTest, RejectsChecksumValidCrossFieldContradictions) {
  std::vector<std::byte> bytes = encoded_fixture();
  const std::size_t first_page = 800U;
  store_u64(bytes, first_page + format::kPageOffsetFieldOffset, 1'216U);
  refresh_metadata_crc(bytes);
  auto decoded = decode_cseg_v1_metadata_exact(bytes);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().kind(), CsegMetadataDecodeErrorKind::kCorruption);

  bytes = encoded_fixture();
  store_u32(bytes, 736U + format::kGranuleRowCountOffset, 1U);
  refresh_metadata_crc(bytes);
  decoded = decode_cseg_v1_metadata_exact(bytes);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().kind(), CsegMetadataDecodeErrorKind::kCorruption);

  bytes = encoded_fixture();
  store_u32(bytes, 256U + format::kOrderingOrdinalOffset, format::kAbsentOrdinal);
  refresh_metadata_crc(bytes);
  decoded = decode_cseg_v1_metadata_exact(bytes);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().kind(), CsegMetadataDecodeErrorKind::kCorruption);
}

TEST(CsegMetadataCodecTest, AppliesRuntimeLimitsBeforeDescriptorAllocation) {
  const std::vector<std::byte> bytes = encoded_fixture();
  CsegMetadataDecodeLimits limits;
  limits.max_metadata_length = 1'000U;
  auto decoded = decode_cseg_v1_metadata_prefix(bytes, limits);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().kind(), CsegMetadataDecodeErrorKind::kResourceLimit);

  limits = {};
  limits.max_pages = 4U;
  decoded = decode_cseg_v1_metadata_prefix(bytes, limits);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().kind(), CsegMetadataDecodeErrorKind::kResourceLimit);
}

TEST(CsegMetadataCodecTest, BindsExactSchemaIdentityOrdinalsTypesAndRoles) {
  MetadataFixture fixture;
  const common::Result<EncodedCsegMetadata> encoded = encode_cseg_v1_metadata(fixture.input());
  ASSERT_TRUE(encoded.has_value());
  const CsegMetadataDecodeResult decoded = decode_cseg_v1_metadata_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value());

  const schema::ColumnId event_id = id<schema::ColumnId>(0x50U);
  std::vector<schema::ColumnDefinition> definitions;
  definitions.push_back(
      schema::ColumnDefinition::create(event_id, "event_time",
                                       type(schema::LogicalTypeKind::kTimestampNs), false)
          .value());
  const common::Result<schema::TableSchema> schema_value = schema::TableSchema::create(
      fixture.table_id, fixture.schema_id, schema::SchemaVersion::initial(), std::nullopt,
      std::move(definitions),
      schema::TableSchemaRoles{.event_time_column = event_id,
                               .physical_ordering_key = {event_id},
                               .partition_columns = {event_id},
                               .shard_key = {event_id},
                               .deduplication_key = {}});
  ASSERT_TRUE(schema_value.has_value());
  EXPECT_TRUE(validate_cseg_v1_metadata_schema(*decoded, *schema_value, fixture.tablet_id).is_ok());
  EXPECT_EQ(
      validate_cseg_v1_metadata_schema(*decoded, *schema_value, id<schema::TabletId>(0x70U)).code(),
      common::StatusCode::kInvalidArgument);
}

TEST(CsegMetadataCodecTest, EncoderRejectsNoncanonicalSystemAndPageMetadata) {
  MetadataFixture fixture;
  fixture.columns.back().nullable = true;
  auto encoded = encode_cseg_v1_metadata(fixture.input());
  ASSERT_FALSE(encoded.has_value());
  EXPECT_EQ(encoded.error().code(), common::StatusCode::kInvalidArgument);

  fixture = MetadataFixture{};
  fixture.pages.front().values_length = 8U;
  encoded = encode_cseg_v1_metadata(fixture.input());
  ASSERT_FALSE(encoded.has_value());
  EXPECT_EQ(encoded.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cseg
