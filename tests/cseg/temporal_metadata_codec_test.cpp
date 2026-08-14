#include "chronos/common/crc32c.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/metadata_codec.hpp"
#include "chronos/cseg/temporal_format.hpp"

#include <algorithm>
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

void store_u16(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint16_t value) {
  bytes[offset] = std::byte{static_cast<std::uint8_t>(value)};
  bytes[offset + 1U] = std::byte{static_cast<std::uint8_t>(value >> 8U)};
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
  }
}

void refresh_metadata_crc(std::vector<std::byte>& bytes) {
  const std::size_t offset = bytes.size() - format::kMetadataCrc32cLength;
  store_u32(bytes, offset, common::crc32c(common::ByteView{bytes}.first(offset)));
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

} // namespace
} // namespace chronos::cseg
