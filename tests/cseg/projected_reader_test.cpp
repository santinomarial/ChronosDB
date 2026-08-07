#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/cseg/projected_reader.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
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

template <typename Integer> void append_le(std::vector<std::byte>& bytes, const Integer value) {
  using Unsigned = std::make_unsigned_t<Integer>;
  const Unsigned encoded = std::bit_cast<Unsigned>(value);
  for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
    bytes.push_back(std::byte{static_cast<std::uint8_t>(encoded >> (index * 8U))});
  }
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
  }
}

struct Schemas {
  schema::TableId table_id{id<schema::TableId>(1U)};
  schema::SchemaId v1_id{id<schema::SchemaId>(2U)};
  schema::SchemaId v2_id{id<schema::SchemaId>(3U)};
  schema::ColumnId event_id{id<schema::ColumnId>(4U)};
  schema::ColumnId payload_id{id<schema::ColumnId>(5U)};
  schema::ColumnId added_fixed_id{id<schema::ColumnId>(6U)};
  schema::ColumnId added_text_id{id<schema::ColumnId>(7U)};

  [[nodiscard]] schema::TableSchema v1() const {
    std::vector<schema::ColumnDefinition> columns;
    columns.push_back(schema::ColumnDefinition::create(event_id, "event_time",
                                                       type(schema::LogicalTypeKind::kTimestampNs),
                                                       false)
                          .value());
    columns.push_back(schema::ColumnDefinition::create(
                          payload_id, "payload", type(schema::LogicalTypeKind::kString), false)
                          .value());
    return schema::TableSchema::create(table_id, v1_id, schema::SchemaVersion::initial(),
                                       std::nullopt, std::move(columns),
                                       {.event_time_column = event_id,
                                        .physical_ordering_key = {event_id},
                                        .partition_columns = {event_id},
                                        .shard_key = {event_id},
                                        .deduplication_key = {}})
        .value();
  }

  [[nodiscard]] schema::TableSchema v2() const {
    std::vector<schema::ColumnDefinition> columns;
    columns.push_back(schema::ColumnDefinition::create(event_id, "event_time",
                                                       type(schema::LogicalTypeKind::kTimestampNs),
                                                       false)
                          .value());
    columns.push_back(schema::ColumnDefinition::create(payload_id, "renamed_payload",
                                                       type(schema::LogicalTypeKind::kString),
                                                       false)
                          .value());
    columns.push_back(schema::ColumnDefinition::create(added_fixed_id, "added_fixed",
                                                       type(schema::LogicalTypeKind::kUInt32), true)
                          .value());
    columns.push_back(schema::ColumnDefinition::create(added_text_id, "added_text",
                                                       type(schema::LogicalTypeKind::kString), true)
                          .value());
    return schema::TableSchema::create(table_id, v2_id,
                                       schema::SchemaVersion::initial().next().value(), v1_id,
                                       std::move(columns),
                                       {.event_time_column = event_id,
                                        .physical_ordering_key = {event_id},
                                        .partition_columns = {event_id},
                                        .shard_key = {event_id},
                                        .deduplication_key = {}})
        .value();
  }

  [[nodiscard]] schema::SchemaLineage lineage() const {
    schema::SchemaLineage result = schema::SchemaLineage::create(v1()).value();
    EXPECT_TRUE(result.append(v2()).is_ok());
    return result;
  }
};

[[nodiscard]] EncodedCsegPage encode_page(const schema::LogicalType logical_type,
                                          const std::uint32_t row_count,
                                          const common::ByteView offsets,
                                          const common::ByteView values,
                                          const PageCompression compression) {
  const auto physical = columnar::PhysicalColumnView::create(
      {.type = logical_type, .nullable = false, .row_count = row_count, .null_count = 0U},
      {.validity = {}, .offsets = offsets, .values = values});
  return encode_cseg_v1_page(*physical, compression).value();
}

struct PartFixture {
  Schemas schemas;
  schema::TabletId tablet_id{id<schema::TabletId>(8U)};
  std::uint32_t row_count;
  std::uint32_t granule_row_count;
  EncodedCsegPart encoded;

  explicit PartFixture(const std::uint32_t rows = 8U,
                       const PageCompression compression = PageCompression::kNone,
                       const std::uint32_t rows_per_granule = 0U)
      : row_count(rows), granule_row_count(rows_per_granule == 0U ? rows : rows_per_granule),
        encoded(make_part(compression)) {}

  [[nodiscard]] schema::SchemaLineage lineage() const {
    return schemas.lineage();
  }

private:
  [[nodiscard]] EncodedCsegPart make_part(const PageCompression compression) const {
    std::vector<CsegColumnDescriptor> columns{
        {.column_id = schemas.event_id,
         .storage_kind = StorageKind::kUser,
         .logical_type = type(schema::LogicalTypeKind::kTimestampNs),
         .nullable = false,
         .event_time = true,
         .schema_ordinal = 0U,
         .ordering_ordinal = 0U},
        {.column_id = schemas.payload_id,
         .storage_kind = StorageKind::kUser,
         .logical_type = type(schema::LogicalTypeKind::kString),
         .nullable = false,
         .event_time = false,
         .schema_ordinal = 1U,
         .ordering_ordinal = std::nullopt},
        {.column_id = std::nullopt,
         .storage_kind = StorageKind::kWalId,
         .logical_type = type(schema::LogicalTypeKind::kUuid),
         .nullable = false,
         .event_time = false,
         .schema_ordinal = std::nullopt,
         .ordering_ordinal = std::nullopt},
        {.column_id = std::nullopt,
         .storage_kind = StorageKind::kRecordSequence,
         .logical_type = type(schema::LogicalTypeKind::kUInt64),
         .nullable = false,
         .event_time = false,
         .schema_ordinal = std::nullopt,
         .ordering_ordinal = std::nullopt},
        {.column_id = std::nullopt,
         .storage_kind = StorageKind::kRowOrdinal,
         .logical_type = type(schema::LogicalTypeKind::kUInt32),
         .nullable = false,
         .event_time = false,
         .schema_ordinal = std::nullopt,
         .ordering_ordinal = std::nullopt},
        {.column_id = std::nullopt,
         .storage_kind = StorageKind::kOperation,
         .logical_type = type(schema::LogicalTypeKind::kUInt8),
         .nullable = false,
         .event_time = false,
         .schema_ordinal = std::nullopt,
         .ordering_ordinal = std::nullopt},
    };

    const common::Uuid::Bytes wal = id<schema::SchemaId>(0x90U).bytes();
    std::vector<CsegGranuleDescriptor> granules;
    std::vector<EncodedCsegPage> pages;
    for (std::uint32_t first_row = 0U; first_row < row_count; first_row += granule_row_count) {
      const std::uint32_t granule_rows = std::min(granule_row_count, row_count - first_row);
      std::vector<std::byte> event;
      std::vector<std::byte> payload_offsets;
      std::vector<std::byte> payload;
      std::vector<std::byte> wal_id;
      std::vector<std::byte> sequence;
      std::vector<std::byte> ordinal;
      std::vector<std::byte> operation;
      append_le(payload_offsets, std::uint32_t{0U});
      for (std::uint32_t local_row = 0U; local_row < granule_rows; ++local_row) {
        const std::uint32_t row = first_row + local_row;
        append_le(event, std::int64_t{100} + static_cast<std::int64_t>(row));
        const std::string value = "payload-" + std::to_string(row % 4U);
        for (const char character : value) {
          payload.push_back(std::byte{static_cast<std::uint8_t>(character)});
        }
        append_le(payload_offsets, static_cast<std::uint32_t>(payload.size()));
        wal_id.insert(wal_id.end(), wal.begin(), wal.end());
        append_le(sequence, std::uint64_t{7U});
        append_le(ordinal, row);
        operation.push_back(std::byte{format::kAppendRowsOperation});
      }
      granules.push_back(
          {.first_row = first_row,
           .row_count = granule_rows,
           .first_page_index = pages.size(),
           .minimum_event_time = 100 + static_cast<std::int64_t>(first_row),
           .maximum_event_time = 99 + static_cast<std::int64_t>(first_row + granule_rows)});
      pages.push_back(encode_page(columns[0].logical_type, granule_rows, {}, event, compression));
      pages.push_back(encode_page(columns[1].logical_type, granule_rows, payload_offsets, payload,
                                  compression));
      pages.push_back(encode_page(columns[2].logical_type, granule_rows, {}, wal_id, compression));
      pages.push_back(
          encode_page(columns[3].logical_type, granule_rows, {}, sequence, compression));
      pages.push_back(encode_page(columns[4].logical_type, granule_rows, {}, ordinal, compression));
      pages.push_back(
          encode_page(columns[5].logical_type, granule_rows, {}, operation, compression));
    }
    return encode_cseg_v1_part({.part_id = id<PartId>(9U),
                                .table_id = schemas.table_id,
                                .tablet_id = tablet_id,
                                .schema_id = schemas.v1_id,
                                .schema_version = schema::SchemaVersion::initial(),
                                .row_count = row_count,
                                .event_time_column_ordinal = 0U,
                                .ordering_column_count = 1U,
                                .minimum_event_time = 100,
                                .maximum_event_time = 99 + static_cast<std::int64_t>(row_count),
                                .columns = columns,
                                .granules = granules,
                                .pages = pages})
        .value();
  }
};

[[nodiscard]] std::vector<std::byte> mutable_bytes(const EncodedCsegPart& encoded) {
  return {encoded.bytes().begin(), encoded.bytes().end()};
}

void authenticate_page_mutation(std::vector<std::byte>& bytes, const std::size_t page_index) {
  const auto metadata = decode_cseg_v1_metadata_prefix(bytes);
  ASSERT_TRUE(metadata.has_value());
  const CsegPageDescriptor& page = metadata->pages()[page_index];
  const common::ByteView stored = common::ByteView{bytes}.subspan(
      static_cast<std::size_t>(page.page_offset), static_cast<std::size_t>(page.stored_length));
  constexpr std::size_t page_descriptor_base = format::kFileHeaderLength +
                                               6U * format::kColumnDescriptorLength +
                                               format::kGranuleDescriptorLength;
  store_u32(bytes,
            page_descriptor_base + page_index * format::kPageDescriptorLength +
                format::kPageCrc32cOffset,
            common::crc32c(stored));
  const std::size_t metadata_crc =
      metadata->encoded_metadata().size() - format::kMetadataCrc32cLength;
  store_u32(bytes, metadata_crc, common::crc32c(common::ByteView{bytes}.first(metadata_crc)));
}

TEST(CsegProjectedReaderTest, OpensPrefixAndExactWithoutTouchingUnselectedPages) {
  const PartFixture fixture;
  const schema::SchemaLineage lineage = fixture.lineage();
  std::vector<std::byte> bytes = mutable_bytes(fixture.encoded);
  const auto metadata = decode_cseg_v1_metadata_prefix(bytes);
  ASSERT_TRUE(metadata.has_value());
  const CsegPageDescriptor& payload = metadata->pages()[1U];
  bytes[static_cast<std::size_t>(payload.page_offset)] ^= std::byte{0x80U};
  bytes.push_back(std::byte{0x55U});

  const auto prefix = open_cseg_v1_projected_reader_prefix(bytes, lineage, fixture.schemas.v2_id,
                                                           fixture.tablet_id);
  ASSERT_TRUE(prefix.has_value());
  EXPECT_EQ(prefix->encoded_part().size(), fixture.encoded.size());
  const std::array<std::uint32_t, 1> event_only{0U};
  EXPECT_TRUE(prefix->read_granule(0U, event_only).has_value());
  const std::array<std::uint32_t, 1> payload_only{1U};
  EXPECT_EQ(prefix->read_granule(0U, payload_only).error().code(), common::StatusCode::kCorruption);

  const auto exact =
      open_cseg_v1_projected_reader_exact(bytes, lineage, fixture.schemas.v2_id, fixture.tablet_id);
  ASSERT_FALSE(exact.has_value());
  EXPECT_EQ(exact.error().kind(), CsegProjectedReaderOpenErrorKind::kCorruption);
}

TEST(CsegProjectedReaderTest, ClassifiesEveryTruncationWithAnExactRequiredBoundary) {
  const PartFixture fixture;
  const schema::SchemaLineage lineage = fixture.lineage();
  for (std::size_t size = 0U; size < fixture.encoded.size(); ++size) {
    const auto opened = open_cseg_v1_projected_reader_prefix(
        fixture.encoded.bytes().first(size), lineage, fixture.schemas.v1_id, fixture.tablet_id);
    ASSERT_FALSE(opened.has_value()) << "size=" << size;
    EXPECT_EQ(opened.error().kind(), CsegProjectedReaderOpenErrorKind::kIncomplete)
        << "size=" << size;
    EXPECT_GT(opened.error().required_size(), size) << "size=" << size;
  }
}

TEST(CsegProjectedReaderTest, PreservesAuthenticatedUnsupportedAndResourceClassifications) {
  const PartFixture fixture;
  const schema::SchemaLineage lineage = fixture.lineage();
  std::vector<std::byte> unsupported = mutable_bytes(fixture.encoded);
  unsupported[format::kFormatMajorOffset] = std::byte{2U};
  unsupported[format::kFormatMajorOffset + 1U] = std::byte{0U};
  store_u32(unsupported, format::kHeaderCrc32cOffset,
            common::crc32c(common::ByteView{unsupported}.first(format::kHeaderCrc32cOffset)));
  const auto metadata = decode_cseg_v1_metadata_prefix(fixture.encoded.bytes());
  ASSERT_TRUE(metadata.has_value());
  const std::size_t metadata_crc =
      metadata->encoded_metadata().size() - format::kMetadataCrc32cLength;
  store_u32(unsupported, metadata_crc,
            common::crc32c(common::ByteView{unsupported}.first(metadata_crc)));
  const auto unknown = open_cseg_v1_projected_reader_exact(
      unsupported, lineage, fixture.schemas.v1_id, fixture.tablet_id);
  ASSERT_FALSE(unknown.has_value());
  EXPECT_EQ(unknown.error().kind(), CsegProjectedReaderOpenErrorKind::kUnsupported);
  EXPECT_EQ(unknown.error().status().code(), common::StatusCode::kNotSupported);

  const auto limited = open_cseg_v1_projected_reader_exact(
      fixture.encoded.bytes(), lineage, fixture.schemas.v1_id, fixture.tablet_id,
      {.metadata = {.max_file_length = fixture.encoded.size() - 1U}});
  ASSERT_FALSE(limited.has_value());
  EXPECT_EQ(limited.error().kind(), CsegProjectedReaderOpenErrorKind::kResourceLimit);
  EXPECT_EQ(limited.error().status().code(), common::StatusCode::kResourceExhausted);
}

TEST(CsegProjectedReaderTest, PlansExactRawOwnedAndSynthesizedBytesBeforeExecution) {
  const PartFixture fixture;
  const schema::SchemaLineage lineage = fixture.lineage();
  const auto reader = open_cseg_v1_projected_reader_exact(fixture.encoded.bytes(), lineage,
                                                          fixture.schemas.v2_id, fixture.tablet_id);
  ASSERT_TRUE(reader.has_value());
  const std::array<std::uint32_t, 4> requested{3U, 1U, 2U, 0U};
  const auto plan = reader->plan_granule(0U, requested);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ(plan->granule_ordinal(), 0U);
  EXPECT_EQ(plan->first_row(), 0U);
  EXPECT_EQ(plan->row_count(), fixture.row_count);
  EXPECT_TRUE(std::ranges::equal(plan->destination_column_ordinals(), requested));
  EXPECT_EQ(plan->source_user_page_count(), 2U);
  EXPECT_EQ(plan->synthesized_column_count(), 2U);
  EXPECT_EQ(plan->decoded_page_count(), 2U + format::kSystemColumnCount);

  std::uint64_t borrowed_bytes = 0U;
  for (const CsegPageDescriptor& page : reader->metadata().pages())
    borrowed_bytes += page.uncompressed_length;
  const std::uint64_t validity = columnar::bitmap_size(fixture.row_count);
  const std::uint64_t synthesized_bytes =
      validity + (static_cast<std::uint64_t>(fixture.row_count) + 1U) * sizeof(std::uint32_t) +
      validity + static_cast<std::uint64_t>(fixture.row_count) * sizeof(std::uint32_t);
  EXPECT_EQ(plan->borrowed_buffer_bytes(), borrowed_bytes);
  EXPECT_EQ(plan->owned_buffer_bytes(), synthesized_bytes);
  EXPECT_EQ(plan->decoded_buffer_bytes(), borrowed_bytes + synthesized_bytes);

  const auto granule = reader->read_granule(*plan);
  ASSERT_TRUE(granule.has_value());
  ASSERT_EQ(granule->columns().size(), requested.size());
  EXPECT_EQ(granule->columns()[0].column_id(), fixture.schemas.added_text_id);
  EXPECT_EQ(granule->columns()[1].column_id(), fixture.schemas.payload_id);
  EXPECT_EQ(granule->columns()[2].column_id(), fixture.schemas.added_fixed_id);
  EXPECT_EQ(granule->columns()[3].column_id(), fixture.schemas.event_id);
}

TEST(CsegProjectedReaderTest, RejectsAPlanFromAnotherReaderBeforePageAccess) {
  const PartFixture fixture;
  const schema::SchemaLineage lineage = fixture.lineage();
  const auto first = open_cseg_v1_projected_reader_exact(fixture.encoded.bytes(), lineage,
                                                         fixture.schemas.v1_id, fixture.tablet_id);
  const auto second = open_cseg_v1_projected_reader_exact(fixture.encoded.bytes(), lineage,
                                                          fixture.schemas.v1_id, fixture.tablet_id);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  const std::array<std::uint32_t, 1> requested{0U};
  const auto plan = first->plan_granule(0U, requested);
  ASSERT_TRUE(plan.has_value());
  const auto result = second->read_granule(*plan);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(CsegProjectedReaderTest, ClassifiesCompressedAndFallbackPageOwnershipFromMetadata) {
  bool saw_owned = false;
  bool saw_borrowed = false;
  for (const std::uint32_t rows : {1U, 1'024U}) {
    const PartFixture fixture{rows, PageCompression::kZstd};
    const schema::SchemaLineage lineage = fixture.lineage();
    const auto reader = open_cseg_v1_projected_reader_exact(
        fixture.encoded.bytes(), lineage, fixture.schemas.v1_id, fixture.tablet_id);
    ASSERT_TRUE(reader.has_value());
    const std::array<std::uint32_t, 2> requested{0U, 1U};
    const auto plan = reader->plan_granule(0U, requested);
    ASSERT_TRUE(plan.has_value());

    std::uint64_t expected_owned = 0U;
    std::uint64_t expected_borrowed = 0U;
    for (const CsegPageDescriptor& page : reader->metadata().pages()) {
      std::uint64_t& expected =
          page.compression == PageCompression::kNone ? expected_borrowed : expected_owned;
      expected += page.uncompressed_length;
    }
    saw_owned = saw_owned || expected_owned != 0U;
    saw_borrowed = saw_borrowed || expected_borrowed != 0U;
    EXPECT_EQ(plan->owned_buffer_bytes(), expected_owned);
    EXPECT_EQ(plan->borrowed_buffer_bytes(), expected_borrowed);
    EXPECT_EQ(plan->decoded_buffer_bytes(), expected_owned + expected_borrowed);
  }
  EXPECT_TRUE(saw_owned);
  EXPECT_TRUE(saw_borrowed);
}

TEST(CsegProjectedReaderTest, ProjectsInCallerOrderAndSynthesizesCanonicalNullableTails) {
  const PartFixture fixture;
  const schema::SchemaLineage lineage = fixture.lineage();
  const auto reader = open_cseg_v1_projected_reader_exact(fixture.encoded.bytes(), lineage,
                                                          fixture.schemas.v2_id, fixture.tablet_id);
  ASSERT_TRUE(reader.has_value());
  const std::array<std::uint32_t, 4> requested{3U, 1U, 2U, 0U};
  auto granule = reader->read_granule(0U, requested);
  ASSERT_TRUE(granule.has_value());
  EXPECT_EQ(granule->schema_ptr()->schema_id(), fixture.schemas.v2_id);
  EXPECT_EQ(granule->first_row(), 0U);
  EXPECT_EQ(granule->row_count(), fixture.row_count);
  ASSERT_EQ(granule->columns().size(), requested.size());
  EXPECT_EQ(granule->columns()[0].column_id(), fixture.schemas.added_text_id);
  EXPECT_EQ(granule->columns()[1].column_id(), fixture.schemas.payload_id);
  EXPECT_EQ(granule->columns()[2].column_id(), fixture.schemas.added_fixed_id);
  EXPECT_EQ(granule->columns()[3].column_id(), fixture.schemas.event_id);
  EXPECT_EQ(granule->columns()[0].physical().null_count(), fixture.row_count);
  EXPECT_EQ(granule->columns()[0].physical().offsets().size(),
            (static_cast<std::size_t>(fixture.row_count) + 1U) * sizeof(std::uint32_t));
  EXPECT_TRUE(std::ranges::all_of(granule->columns()[0].physical().offsets(),
                                  [](const std::byte value) { return value == std::byte{0}; }));
  EXPECT_TRUE(granule->columns()[0].physical().values().empty());
  EXPECT_EQ(granule->columns()[2].physical().null_count(), fixture.row_count);
  EXPECT_EQ(granule->columns()[2].physical().values().size(),
            static_cast<std::size_t>(fixture.row_count) * sizeof(std::uint32_t));
  ASSERT_NE(granule->column(1U), nullptr);
  EXPECT_EQ(granule->column(99U), nullptr);
  EXPECT_EQ(granule->wal_id().row_count(), fixture.row_count);
  EXPECT_EQ(granule->record_sequence().row_count(), fixture.row_count);
  EXPECT_EQ(granule->row_ordinal().row_count(), fixture.row_count);
  EXPECT_EQ(granule->operation().row_count(), fixture.row_count);

  ProjectedCsegGranule moved = std::move(*granule);
  const auto payload = moved.columns()[1].physical().cell(0U);
  ASSERT_TRUE(payload.has_value());
  ASSERT_TRUE(payload->bytes().has_value());
  EXPECT_EQ(payload->bytes()->size(), std::string{"payload-0"}.size());
}

TEST(CsegProjectedReaderTest, ReadsCanonicalGranuleLocalPageRangesAndGlobalRows) {
  const PartFixture fixture{9U, PageCompression::kNone, 4U};
  const schema::SchemaLineage lineage = fixture.lineage();
  const auto reader = open_cseg_v1_projected_reader_exact(fixture.encoded.bytes(), lineage,
                                                          fixture.schemas.v1_id, fixture.tablet_id);
  ASSERT_TRUE(reader.has_value());
  ASSERT_EQ(reader->metadata().granules().size(), 3U);
  const std::array<std::uint32_t, 1> event_only{0U};
  for (std::size_t granule_ordinal = 0U; granule_ordinal < 3U; ++granule_ordinal) {
    const auto granule = reader->read_granule(granule_ordinal, event_only);
    ASSERT_TRUE(granule.has_value());
    const std::uint64_t expected_first = granule_ordinal * 4U;
    EXPECT_EQ(granule->first_row(), expected_first);
    EXPECT_EQ(granule->row_count(), granule_ordinal == 2U ? 1U : 4U);
    const auto cell = granule->columns()[0].physical().cell(0U);
    ASSERT_TRUE(cell.has_value());
    ASSERT_TRUE(cell->bytes().has_value());
    EXPECT_EQ(std::to_integer<std::uint8_t>(cell->bytes()->front()), 100U + expected_first);
  }
}

TEST(CsegProjectedReaderTest, EmptyProjectionStillAuthenticatesEverySystemPageAndSemantics) {
  const PartFixture fixture;
  const schema::SchemaLineage lineage = fixture.lineage();
  for (const std::size_t system_page : {2U, 3U, 4U, 5U}) {
    std::vector<std::byte> bytes = mutable_bytes(fixture.encoded);
    const auto metadata = decode_cseg_v1_metadata_prefix(bytes);
    ASSERT_TRUE(metadata.has_value());
    const CsegPageDescriptor& page = metadata->pages()[system_page];
    bytes[static_cast<std::size_t>(page.page_offset)] ^= std::byte{0x01U};
    const auto reader = open_cseg_v1_projected_reader_exact(bytes, lineage, fixture.schemas.v1_id,
                                                            fixture.tablet_id);
    ASSERT_TRUE(reader.has_value());
    EXPECT_EQ(reader->read_granule(0U, {}).error().code(), common::StatusCode::kCorruption);
  }

  for (const std::uint8_t operation : {std::uint8_t{0U}, std::uint8_t{2U}}) {
    std::vector<std::byte> bytes = mutable_bytes(fixture.encoded);
    const auto metadata = decode_cseg_v1_metadata_prefix(bytes);
    ASSERT_TRUE(metadata.has_value());
    bytes[static_cast<std::size_t>(metadata->pages()[5U].page_offset)] = std::byte{operation};
    authenticate_page_mutation(bytes, 5U);
    const auto reader = open_cseg_v1_projected_reader_exact(bytes, lineage, fixture.schemas.v1_id,
                                                            fixture.tablet_id);
    ASSERT_TRUE(reader.has_value());
    const common::Status error = reader->read_granule(0U, {}).error();
    EXPECT_EQ(error.code(), operation == 0U ? common::StatusCode::kCorruption
                                            : common::StatusCode::kNotSupported);
  }
}

TEST(CsegProjectedReaderTest, ValidatesOnlyTheSelectedPageAlignment) {
  const PartFixture fixture;
  const schema::SchemaLineage lineage = fixture.lineage();
  std::vector<std::byte> bytes = mutable_bytes(fixture.encoded);
  const auto metadata = decode_cseg_v1_metadata_prefix(bytes);
  ASSERT_TRUE(metadata.has_value());
  const CsegPageDescriptor& payload = metadata->pages()[1U];
  const std::size_t padding = static_cast<std::size_t>(metadata->pages()[2U].page_offset -
                                                       payload.page_offset - payload.stored_length);
  ASSERT_GT(padding, 0U);
  bytes[static_cast<std::size_t>(payload.page_offset + payload.stored_length)] = std::byte{1U};
  const auto reader =
      open_cseg_v1_projected_reader_exact(bytes, lineage, fixture.schemas.v1_id, fixture.tablet_id);
  ASSERT_TRUE(reader.has_value());
  const std::array<std::uint32_t, 1> event_only{0U};
  EXPECT_TRUE(reader->read_granule(0U, event_only).has_value());
  const std::array<std::uint32_t, 1> payload_only{1U};
  EXPECT_EQ(reader->read_granule(0U, payload_only).error().code(), common::StatusCode::kCorruption);
}

TEST(CsegProjectedReaderTest, RejectsCatalogBindingProjectionAndRequestErrorsPrecisely) {
  const PartFixture fixture;
  const schema::SchemaLineage lineage = fixture.lineage();
  const auto wrong_tablet = open_cseg_v1_projected_reader_exact(
      fixture.encoded.bytes(), lineage, fixture.schemas.v1_id, id<schema::TabletId>(0x44U));
  ASSERT_FALSE(wrong_tablet.has_value());
  EXPECT_EQ(wrong_tablet.error().kind(), CsegProjectedReaderOpenErrorKind::kInvalidArgument);

  const auto missing = open_cseg_v1_projected_reader_exact(
      fixture.encoded.bytes(), lineage, id<schema::SchemaId>(0x45U), fixture.tablet_id);
  ASSERT_FALSE(missing.has_value());
  EXPECT_EQ(missing.error().kind(), CsegProjectedReaderOpenErrorKind::kNotFound);

  const auto reader = open_cseg_v1_projected_reader_exact(
      fixture.encoded.bytes(), lineage, fixture.schemas.v2_id, fixture.tablet_id,
      {.max_decoded_buffer_bytes = 1U, .max_projected_columns = 1U});
  ASSERT_TRUE(reader.has_value());
  EXPECT_EQ(reader->read_granule(1U, {}).error().code(), common::StatusCode::kOutOfRange);
  EXPECT_EQ(reader->read_granule(0U, {}).error().code(), common::StatusCode::kResourceExhausted);
  const std::array<std::uint32_t, 2> too_many{0U, 1U};
  EXPECT_EQ(reader->read_granule(0U, too_many).error().code(),
            common::StatusCode::kResourceExhausted);

  const auto ordinary_reader = open_cseg_v1_projected_reader_exact(
      fixture.encoded.bytes(), lineage, fixture.schemas.v2_id, fixture.tablet_id);
  ASSERT_TRUE(ordinary_reader.has_value());
  const std::array<std::uint32_t, 2> duplicate{1U, 1U};
  EXPECT_EQ(ordinary_reader->read_granule(0U, duplicate).error().code(),
            common::StatusCode::kInvalidArgument);
  const std::array<std::uint32_t, 1> outside{4U};
  EXPECT_EQ(ordinary_reader->read_granule(0U, outside).error().code(),
            common::StatusCode::kInvalidArgument);

  const auto invalid_limits =
      open_cseg_v1_projected_reader_exact(fixture.encoded.bytes(), lineage, fixture.schemas.v1_id,
                                          fixture.tablet_id, {.max_decoded_buffer_bytes = 0U});
  ASSERT_FALSE(invalid_limits.has_value());
  EXPECT_EQ(invalid_limits.error().kind(), CsegProjectedReaderOpenErrorKind::kInvalidArgument);
}

TEST(CsegProjectedReaderPropertyTest, GeneratedRowsPoliciesAndProjectionsAreDeterministic) {
  for (const std::uint32_t rows : {1U, 2U, 7U, 64U, 1024U}) {
    for (const PageCompression compression : {PageCompression::kNone, PageCompression::kZstd}) {
      const PartFixture fixture{rows, compression};
      const schema::SchemaLineage lineage = fixture.lineage();
      const auto reader = open_cseg_v1_projected_reader_exact(
          fixture.encoded.bytes(), lineage, fixture.schemas.v2_id, fixture.tablet_id);
      ASSERT_TRUE(reader.has_value()) << "rows=" << rows;
      for (const std::vector<std::uint32_t>& projection :
           {std::vector<std::uint32_t>{}, std::vector<std::uint32_t>{0U},
            std::vector<std::uint32_t>{1U}, std::vector<std::uint32_t>{3U, 2U, 1U, 0U}}) {
        const auto plan = reader->plan_granule(0U, projection);
        ASSERT_TRUE(plan.has_value()) << "rows=" << rows;
        EXPECT_EQ(plan->decoded_buffer_bytes(),
                  plan->owned_buffer_bytes() + plan->borrowed_buffer_bytes());
        const auto first = reader->read_granule(*plan);
        const auto second = reader->read_granule(0U, projection);
        ASSERT_TRUE(first.has_value()) << "rows=" << rows;
        ASSERT_TRUE(second.has_value()) << "rows=" << rows;
        ASSERT_EQ(first->columns().size(), second->columns().size());
        for (std::size_t column = 0U; column < first->columns().size(); ++column) {
          EXPECT_EQ(first->columns()[column].column_id(), second->columns()[column].column_id());
          EXPECT_TRUE(std::ranges::equal(first->columns()[column].physical().validity(),
                                         second->columns()[column].physical().validity()));
          EXPECT_TRUE(std::ranges::equal(first->columns()[column].physical().offsets(),
                                         second->columns()[column].physical().offsets()));
          EXPECT_TRUE(std::ranges::equal(first->columns()[column].physical().values(),
                                         second->columns()[column].physical().values()));
        }
      }
    }
  }
}

} // namespace
} // namespace chronos::cseg
