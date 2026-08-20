#include "chronos/common/crc32c.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/metadata_codec.hpp"
#include "chronos/cseg/temporal_format.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::cseg {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] CsegColumnDescriptor user_column() {
  return CsegColumnDescriptor{.column_id = id<schema::ColumnId>(5U),
                              .storage_kind = StorageKind::kUser,
                              .logical_type = type(schema::LogicalTypeKind::kTimestampNs),
                              .nullable = false,
                              .event_time = true,
                              .schema_ordinal = 0U,
                              .ordering_ordinal = 0U};
}

[[nodiscard]] CsegColumnDescriptor system_column(const StorageKind kind,
                                                 const schema::LogicalTypeKind logical_type) {
  return CsegColumnDescriptor{.column_id = std::nullopt,
                              .storage_kind = kind,
                              .logical_type = type(logical_type),
                              .nullable = false,
                              .event_time = false,
                              .schema_ordinal = std::nullopt,
                              .ordering_ordinal = std::nullopt};
}

[[nodiscard]] CsegPageMetadataInput fixed_page(const std::uint64_t length,
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

[[nodiscard]] std::uint16_t load_u16(const common::ByteView bytes, const std::size_t offset) {
  return static_cast<std::uint16_t>(std::to_integer<std::uint16_t>(bytes[offset]) |
                                    (std::to_integer<std::uint16_t>(bytes[offset + 1U]) << 8U));
}

[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes, const std::size_t offset) {
  std::uint32_t value = 0U;
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= std::to_integer<std::uint32_t>(bytes[offset + index]) << (index * 8U);
  }
  return value;
}

[[nodiscard]] std::uint64_t load_u64(const common::ByteView bytes, const std::size_t offset) {
  std::uint64_t value = 0U;
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= std::to_integer<std::uint64_t>(bytes[offset + index]) << (index * 8U);
  }
  return value;
}

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

void refresh_metadata_crc(std::vector<std::byte>& bytes) {
  const std::size_t offset = bytes.size() - format::kMetadataCrc32cLength;
  store_u32(bytes, offset, common::crc32c(common::ByteView{bytes}.first(offset)));
}

void refresh_header_and_metadata_crc(std::vector<std::byte>& bytes) {
  store_u32(bytes, format::kHeaderCrc32cOffset,
            common::crc32c(common::ByteView{bytes}.first(format::kHeaderCrc32cOffset)));
  refresh_metadata_crc(bytes);
}

struct TemporalMetadataFixture {
  PartId part_id{id<PartId>(1U)};
  schema::TableId table_id{id<schema::TableId>(2U)};
  schema::TabletId tablet_id{id<schema::TabletId>(3U)};
  schema::SchemaId schema_id{id<schema::SchemaId>(4U)};
  std::vector<CsegColumnDescriptor> columns{
      user_column(),
      system_column(StorageKind::kCommitSource, schema::LogicalTypeKind::kUInt8),
      system_column(StorageKind::kSourceId, schema::LogicalTypeKind::kUuid),
      system_column(StorageKind::kCommitPosition, schema::LogicalTypeKind::kUInt64),
      system_column(StorageKind::kTemporalRowOrdinal, schema::LogicalTypeKind::kUInt32),
      system_column(StorageKind::kTemporalOperation, schema::LogicalTypeKind::kUInt8),
      system_column(StorageKind::kLogicalIdentity, schema::LogicalTypeKind::kBinary),
      system_column(StorageKind::kReceiveTime, schema::LogicalTypeKind::kTimestampNs),
      system_column(StorageKind::kSystemCommitTime, schema::LogicalTypeKind::kTimestampNs),
  };
  std::vector<CsegGranuleDescriptor> granules{{.first_row = 0U,
                                               .row_count = 2U,
                                               .first_page_index = 0U,
                                               .minimum_event_time = 10,
                                               .maximum_event_time = 20}};
  std::vector<CsegPageMetadataInput> pages{
      fixed_page(16U, 1U),
      fixed_page(2U, 2U),
      fixed_page(32U, 3U),
      fixed_page(16U, 4U),
      fixed_page(8U, 5U),
      fixed_page(2U, 6U),
      CsegPageMetadataInput{.compression = PageCompression::kNone,
                            .row_count = 2U,
                            .null_count = 0U,
                            .stored_length = 14U,
                            .uncompressed_length = 14U,
                            .validity_length = 0U,
                            .offsets_length = 12U,
                            .values_length = 2U,
                            .page_crc32c = 7U},
      fixed_page(16U, 8U),
      fixed_page(16U, 9U),
  };

  [[nodiscard]] CsegMetadataEncodeInput input() const {
    return CsegMetadataEncodeInput{.part_id = part_id,
                                   .table_id = table_id,
                                   .tablet_id = tablet_id,
                                   .schema_id = schema_id,
                                   .schema_version = schema::SchemaVersion::initial(),
                                   .row_count = 2U,
                                   .event_time_column_ordinal = 0U,
                                   .ordering_column_count = 1U,
                                   .minimum_event_time = 10,
                                   .maximum_event_time = 20,
                                   .columns = columns,
                                   .granules = granules,
                                   .pages = pages};
  }

  [[nodiscard]] std::shared_ptr<const schema::TableSchema> schema_value() const {
    std::vector<schema::ColumnDefinition> definitions;
    definitions.push_back(
        schema::ColumnDefinition::create(id<schema::ColumnId>(5U), "event_time",
                                         type(schema::LogicalTypeKind::kTimestampNs), false)
            .value());
    return std::make_shared<const schema::TableSchema>(
        schema::TableSchema::create(table_id, schema_id, schema::SchemaVersion::initial(),
                                    std::nullopt, std::move(definitions),
                                    {.event_time_column = id<schema::ColumnId>(5U),
                                     .physical_ordering_key = {id<schema::ColumnId>(5U)},
                                     .partition_columns = {id<schema::ColumnId>(5U)},
                                     .shard_key = {id<schema::ColumnId>(5U)},
                                     .deduplication_key = {id<schema::ColumnId>(5U)}})
            .value());
  }
};

TEST(TemporalMetadataCodecTest, EmitsIndependentFieldLevelGoldenBytes) {
  TemporalMetadataFixture fixture;
  const auto encoded = encode_cseg_v2_temporal_metadata(fixture.input());
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  ASSERT_EQ(encoded->size(), 1'912U);
  EXPECT_EQ(encoded->total_length(), 2'048U);

  // These absolute offsets and values were independently generated from the accepted byte layout
  // with Python struct packing and a tableless reflected CRC32C implementation.
  constexpr std::array<std::uint8_t, 8U> kMagic{0x43U, 0x48U, 0x52U, 0x4eU,
                                                0x43U, 0x53U, 0x45U, 0x47U};
  const auto expect_bytes = [&](const std::size_t offset, const auto& expected) {
    for (std::size_t index = 0U; index < expected.size(); ++index) {
      EXPECT_EQ(std::to_integer<std::uint8_t>(encoded->bytes()[offset + index]), expected[index])
          << offset + index;
    }
  };
  expect_bytes(0U, kMagic);
  EXPECT_EQ(load_u16(encoded->bytes(), 8U), 2U);
  EXPECT_EQ(load_u16(encoded->bytes(), 10U), 0U);
  EXPECT_EQ(load_u32(encoded->bytes(), 12U), 256U);
  EXPECT_EQ(load_u32(encoded->bytes(), 16U), 0U);
  EXPECT_EQ(load_u32(encoded->bytes(), 20U), 0U);
  EXPECT_EQ(load_u64(encoded->bytes(), 24U), 2'048U);
  EXPECT_EQ(load_u64(encoded->bytes(), 32U), 1'912U);
  EXPECT_EQ(load_u64(encoded->bytes(), 40U), 2U);
  EXPECT_EQ(load_u32(encoded->bytes(), 48U), 1U);
  EXPECT_EQ(load_u32(encoded->bytes(), 52U), 9U);
  EXPECT_EQ(load_u32(encoded->bytes(), 56U), 1U);
  EXPECT_EQ(load_u32(encoded->bytes(), 60U), 9U);

  constexpr std::array<std::size_t, 4U> kIdentityOffsets{64U, 80U, 96U, 112U};
  for (std::size_t index = 0U; index < kIdentityOffsets.size(); ++index) {
    const std::size_t offset = kIdentityOffsets[index];
    EXPECT_EQ(encoded->bytes()[offset], std::byte{static_cast<std::uint8_t>(index + 1U)});
    EXPECT_TRUE(std::ranges::all_of(encoded->bytes().subspan(offset + 1U, 15U),
                                    [](const std::byte value) { return value == std::byte{0U}; }));
  }
  EXPECT_EQ(load_u64(encoded->bytes(), 128U), 1U);
  EXPECT_EQ(load_u64(encoded->bytes(), 136U), 256U);
  EXPECT_EQ(load_u64(encoded->bytes(), 144U), 1'120U);
  EXPECT_EQ(load_u64(encoded->bytes(), 152U), 1'184U);
  EXPECT_EQ(load_u64(encoded->bytes(), 160U), 1'912U);
  EXPECT_EQ(load_u32(encoded->bytes(), 168U), 0U);
  EXPECT_EQ(load_u32(encoded->bytes(), 172U), 1U);
  EXPECT_EQ(load_u64(encoded->bytes(), 176U), 10U);
  EXPECT_EQ(load_u64(encoded->bytes(), 184U), 20U);
  EXPECT_TRUE(std::ranges::all_of(encoded->bytes().subspan(192U, 56U),
                                  [](const std::byte value) { return value == std::byte{0U}; }));
  EXPECT_EQ(load_u32(encoded->bytes(), 248U), 0x2e0f2f20U);
  EXPECT_EQ(load_u32(encoded->bytes(), 252U), 0U);

  constexpr std::array<std::size_t, 8U> kSystemDescriptorOffsets{352U, 448U, 544U, 640U,
                                                                 736U, 832U, 928U, 1'024U};
  constexpr std::array<std::array<std::uint8_t, 4U>, 8U> kRegistryCodes{{
      {0x02U, 0x00U, 0x06U, 0x00U},
      {0x03U, 0x00U, 0x12U, 0x00U},
      {0x04U, 0x00U, 0x09U, 0x00U},
      {0x05U, 0x00U, 0x08U, 0x00U},
      {0x06U, 0x00U, 0x06U, 0x00U},
      {0x07U, 0x00U, 0x11U, 0x00U},
      {0x08U, 0x00U, 0x0dU, 0x00U},
      {0x09U, 0x00U, 0x0dU, 0x00U},
  }};
  for (std::size_t index = 0U; index < kSystemDescriptorOffsets.size(); ++index) {
    const std::size_t offset = kSystemDescriptorOffsets[index];
    EXPECT_TRUE(std::ranges::all_of(encoded->bytes().subspan(offset, 16U),
                                    [](const std::byte value) { return value == std::byte{0U}; }));
    expect_bytes(offset + 16U, kRegistryCodes[index]);
    EXPECT_TRUE(std::ranges::all_of(encoded->bytes().subspan(offset + 20U, 8U),
                                    [](const std::byte value) { return value == std::byte{0U}; }));
    EXPECT_TRUE(
        std::ranges::all_of(encoded->bytes().subspan(offset + 28U, 8U),
                            [](const std::byte value) { return value == std::byte{0xffU}; }));
    EXPECT_TRUE(std::ranges::all_of(encoded->bytes().subspan(offset + 36U, 60U),
                                    [](const std::byte value) { return value == std::byte{0U}; }));
  }
  EXPECT_EQ(load_u32(encoded->bytes(), 1'908U), 0x5d84d7acU);
}

TEST(TemporalMetadataCodecTest, RoundTripsV2AndKeepsV1Strict) {
  TemporalMetadataFixture fixture;
  auto encoded = encode_cseg_v2_temporal_metadata(fixture.input());
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(std::to_integer<std::uint8_t>(encoded->bytes()[format::kFormatMajorOffset]), 2U);
  auto decoded = decode_cseg_v2_temporal_metadata_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().status().to_string();
  EXPECT_EQ(decoded->format_major(), temporal_format::kFormatMajor);
  EXPECT_EQ(decoded->format_minor(), temporal_format::kFormatMinor);
  EXPECT_EQ(decoded->part_id(), fixture.part_id);
  EXPECT_EQ(decoded->table_id(), fixture.table_id);
  EXPECT_EQ(decoded->tablet_id(), fixture.tablet_id);
  EXPECT_EQ(decoded->schema_id(), fixture.schema_id);
  EXPECT_EQ(decoded->schema_version(), schema::SchemaVersion::initial());
  EXPECT_EQ(decoded->total_length(), encoded->total_length());
  EXPECT_EQ(decoded->row_count(), 2U);
  EXPECT_EQ(decoded->event_time_column_ordinal(), 0U);
  EXPECT_EQ(decoded->ordering_column_count(), 1U);
  EXPECT_EQ(decoded->minimum_event_time(), 10);
  EXPECT_EQ(decoded->maximum_event_time(), 20);
  EXPECT_TRUE(std::ranges::equal(decoded->columns(), fixture.columns));
  EXPECT_TRUE(std::ranges::equal(decoded->granules(), fixture.granules));
  EXPECT_EQ(decoded->pages().size(), 9U);
  EXPECT_TRUE(validate_cseg_v2_temporal_metadata_schema(*decoded, *fixture.schema_value(),
                                                        fixture.tablet_id)
                  .is_ok());

  auto v1 = decode_cseg_v1_metadata_exact(encoded->bytes());
  ASSERT_FALSE(v1.has_value());
  EXPECT_EQ(v1.error().kind(), CsegMetadataDecodeErrorKind::kUnsupported);
  EXPECT_FALSE(encode_cseg_v1_metadata(fixture.input()).has_value());
}

TEST(TemporalMetadataCodecTest, RejectsV1RegistryAndUnknownV2Registry) {
  TemporalMetadataFixture fixture;
  while (fixture.columns.size() > 1U + format::kSystemColumnCount) {
    fixture.columns.pop_back();
  }
  EXPECT_FALSE(encode_cseg_v2_temporal_metadata(fixture.input()).has_value());

  fixture = TemporalMetadataFixture{};
  const auto encoded = encode_cseg_v2_temporal_metadata(fixture.input());
  ASSERT_TRUE(encoded.has_value());
  std::vector<std::byte> unknown_kind{encoded->bytes().begin(), encoded->bytes().end()};
  const std::size_t final_column_offset =
      format::kColumnsOffset + (fixture.columns.size() - 1U) * format::kColumnDescriptorLength;
  store_u16(unknown_kind, final_column_offset + format::kStorageKindOffset, 10U);
  refresh_metadata_crc(unknown_kind);
  const auto decoded = decode_cseg_v2_temporal_metadata_exact(unknown_kind);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().kind(), CsegMetadataDecodeErrorKind::kUnsupported);
}

TEST(TemporalMetadataCodecTest,
     ClassifiesChecksumValidHostileRegistriesReservedBytesAndContradictions) {
  TemporalMetadataFixture fixture;
  const auto encoded = encode_cseg_v2_temporal_metadata(fixture.input());
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const std::vector<std::byte> golden{encoded->bytes().begin(), encoded->bytes().end()};
  const std::size_t final_system_column =
      format::kColumnsOffset + (fixture.columns.size() - 1U) * format::kColumnDescriptorLength;
  const std::size_t granule =
      format::kColumnsOffset + fixture.columns.size() * format::kColumnDescriptorLength;
  const std::size_t first_page = granule + format::kGranuleDescriptorLength;

  const auto expect_u16 = [&](const std::size_t offset, const std::uint16_t value,
                              const bool header, const CsegMetadataDecodeErrorKind expected) {
    std::vector<std::byte> bytes = golden;
    store_u16(bytes, offset, value);
    header ? refresh_header_and_metadata_crc(bytes) : refresh_metadata_crc(bytes);
    const auto decoded = decode_cseg_v2_temporal_metadata_exact(bytes);
    EXPECT_FALSE(decoded.has_value()) << offset;
    if (!decoded.has_value())
      EXPECT_EQ(decoded.error().kind(), expected) << offset;
  };
  const auto expect_u32 = [&](const std::size_t offset, const std::uint32_t value,
                              const bool header, const CsegMetadataDecodeErrorKind expected) {
    std::vector<std::byte> bytes = golden;
    store_u32(bytes, offset, value);
    header ? refresh_header_and_metadata_crc(bytes) : refresh_metadata_crc(bytes);
    const auto decoded = decode_cseg_v2_temporal_metadata_exact(bytes);
    EXPECT_FALSE(decoded.has_value()) << offset;
    if (!decoded.has_value())
      EXPECT_EQ(decoded.error().kind(), expected) << offset;
  };
  const auto expect_byte = [&](const std::size_t offset,
                               const CsegMetadataDecodeErrorKind expected) {
    std::vector<std::byte> bytes = golden;
    bytes[offset] = std::byte{1U};
    const bool header = offset < format::kFileHeaderLength;
    header ? refresh_header_and_metadata_crc(bytes) : refresh_metadata_crc(bytes);
    const auto decoded = decode_cseg_v2_temporal_metadata_exact(bytes);
    EXPECT_FALSE(decoded.has_value()) << offset;
    if (!decoded.has_value())
      EXPECT_EQ(decoded.error().kind(), expected) << offset;
  };

  expect_u16(format::kFormatMajorOffset, 3U, true, CsegMetadataDecodeErrorKind::kUnsupported);
  expect_u16(format::kFormatMinorOffset, 1U, true, CsegMetadataDecodeErrorKind::kUnsupported);
  expect_u32(format::kFileFlagsOffset, 1U, true, CsegMetadataDecodeErrorKind::kUnsupported);
  expect_u32(format::kStoredColumnCountOffset, 8U, true, CsegMetadataDecodeErrorKind::kCorruption);
  expect_u32(format::kPageCountOffset, 8U, true, CsegMetadataDecodeErrorKind::kCorruption);
  expect_byte(format::kHeaderReserved0Offset, CsegMetadataDecodeErrorKind::kCorruption);
  expect_byte(format::kHeaderReserved1Offset, CsegMetadataDecodeErrorKind::kCorruption);
  expect_byte(format::kHeaderReserved2Offset, CsegMetadataDecodeErrorKind::kCorruption);

  expect_u16(final_system_column + format::kStorageKindOffset, 10U, false,
             CsegMetadataDecodeErrorKind::kUnsupported);
  expect_u16(final_system_column + format::kStorageKindOffset, 0U, false,
             CsegMetadataDecodeErrorKind::kCorruption);
  expect_u16(final_system_column + format::kStorageKindOffset,
             temporal_format::kReceiveTimeStorageKind, false,
             CsegMetadataDecodeErrorKind::kCorruption);
  expect_u16(final_system_column + format::kLogicalTypeOffset,
             static_cast<std::uint16_t>(schema::LogicalTypeKind::kUInt64), false,
             CsegMetadataDecodeErrorKind::kCorruption);
  expect_u32(final_system_column + format::kColumnFlagsOffset, format::kNullableColumnFlag, false,
             CsegMetadataDecodeErrorKind::kCorruption);
  expect_u32(final_system_column + format::kSchemaOrdinalOffset, 0U, false,
             CsegMetadataDecodeErrorKind::kCorruption);
  expect_u32(final_system_column + format::kOrderingOrdinalOffset, 0U, false,
             CsegMetadataDecodeErrorKind::kCorruption);
  expect_byte(final_system_column + format::kColumnIdOffset,
              CsegMetadataDecodeErrorKind::kCorruption);
  expect_byte(final_system_column + format::kColumnReservedOffset,
              CsegMetadataDecodeErrorKind::kCorruption);
  expect_byte(granule + format::kGranuleReservedOffset, CsegMetadataDecodeErrorKind::kCorruption);

  expect_u16(first_page + format::kPagePhysicalEncodingOffset, 2U, false,
             CsegMetadataDecodeErrorKind::kUnsupported);
  expect_u16(first_page + format::kPagePhysicalEncodingOffset, 0U, false,
             CsegMetadataDecodeErrorKind::kCorruption);
  expect_u16(first_page + format::kPageCompressionOffset, 3U, false,
             CsegMetadataDecodeErrorKind::kUnsupported);
  expect_u16(first_page + format::kPageCompressionOffset, 0U, false,
             CsegMetadataDecodeErrorKind::kCorruption);
  expect_u32(first_page + format::kPageFlagsOffset, 1U, false,
             CsegMetadataDecodeErrorKind::kUnsupported);
  expect_byte(first_page + format::kPageReservedOffset, CsegMetadataDecodeErrorKind::kCorruption);

  const auto canonical = decode_cseg_v2_temporal_metadata_exact(golden);
  ASSERT_TRUE(canonical.has_value()) << canonical.error().status().to_string();
  ASSERT_GE(canonical->pages().size(), 2U);
  std::vector<std::byte> overlapping_page = golden;
  store_u64(overlapping_page, first_page + format::kPageOffsetFieldOffset,
            canonical->pages()[1U].page_offset);
  refresh_metadata_crc(overlapping_page);
  const auto decoded = decode_cseg_v2_temporal_metadata_exact(overlapping_page);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().kind(), CsegMetadataDecodeErrorKind::kCorruption);
}

} // namespace
} // namespace chronos::cseg
