#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/cseg/validator.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
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

struct SemanticValues {
  std::vector<std::int64_t> event_time{-5, 10};
  std::vector<common::Uuid::Bytes> wal_id{id<schema::SchemaId>(0x70U).bytes(),
                                          id<schema::SchemaId>(0x70U).bytes()};
  std::vector<std::uint64_t> record_sequence{7U, 7U};
  std::vector<std::uint32_t> row_ordinal{0U, 1U};
  std::vector<std::uint8_t> operation{format::kAppendRowsOperation, format::kAppendRowsOperation};
};

[[nodiscard]] SemanticValues generated_values(const std::uint32_t row_count) {
  SemanticValues values;
  values.event_time.clear();
  values.wal_id.clear();
  values.record_sequence.clear();
  values.row_ordinal.clear();
  values.operation.clear();
  values.event_time.reserve(row_count);
  values.wal_id.reserve(row_count);
  values.record_sequence.reserve(row_count);
  values.row_ordinal.reserve(row_count);
  values.operation.reserve(row_count);
  const auto wal_id = id<schema::SchemaId>(0x70U).bytes();
  for (std::uint32_t row = 0U; row < row_count; ++row) {
    values.event_time.push_back(static_cast<std::int64_t>(row) - 100);
    values.wal_id.push_back(wal_id);
    values.record_sequence.push_back(7U);
    values.row_ordinal.push_back(row);
    values.operation.push_back(format::kAppendRowsOperation);
  }
  return values;
}

struct SemanticFixture {
  PartId part_id{id<PartId>(1U)};
  schema::TableId table_id{id<schema::TableId>(2U)};
  schema::TabletId tablet_id{id<schema::TabletId>(3U)};
  schema::SchemaId schema_id{id<schema::SchemaId>(4U)};
  schema::ColumnId event_id{id<schema::ColumnId>(5U)};
  std::vector<CsegColumnDescriptor> columns{
      {.column_id = event_id,
       .storage_kind = StorageKind::kUser,
       .logical_type = type(schema::LogicalTypeKind::kTimestampNs),
       .nullable = false,
       .event_time = true,
       .schema_ordinal = 0U,
       .ordering_ordinal = 0U},
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
  SemanticValues values;
  std::vector<CsegGranuleDescriptor> granules;
  std::vector<EncodedCsegPage> pages;

  explicit SemanticFixture(SemanticValues input = {},
                           const PageCompression policy = PageCompression::kNone)
      : values(std::move(input)) {
    const std::uint32_t rows = static_cast<std::uint32_t>(values.event_time.size());
    granules.push_back({.first_row = 0U,
                        .row_count = rows,
                        .first_page_index = 0U,
                        .minimum_event_time = *std::ranges::min_element(values.event_time),
                        .maximum_event_time = *std::ranges::max_element(values.event_time)});
    std::vector<std::byte> event;
    std::vector<std::byte> wal;
    std::vector<std::byte> sequence;
    std::vector<std::byte> ordinal;
    std::vector<std::byte> operation;
    for (std::uint32_t row = 0U; row < rows; ++row) {
      append_le(event, values.event_time[row]);
      wal.insert(wal.end(), values.wal_id[row].begin(), values.wal_id[row].end());
      append_le(sequence, values.record_sequence[row]);
      append_le(ordinal, values.row_ordinal[row]);
      append_le(operation, values.operation[row]);
    }
    pages.reserve(columns.size());
    pages.push_back(encode(columns[0].logical_type, rows, event, policy));
    pages.push_back(encode(columns[1].logical_type, rows, wal, policy));
    pages.push_back(encode(columns[2].logical_type, rows, sequence, policy));
    pages.push_back(encode(columns[3].logical_type, rows, ordinal, policy));
    pages.push_back(encode(columns[4].logical_type, rows, operation, policy));
  }

  [[nodiscard]] EncodedCsegPart encoded() const {
    return encode_cseg_v1_part({.part_id = part_id,
                                .table_id = table_id,
                                .tablet_id = tablet_id,
                                .schema_id = schema_id,
                                .schema_version = schema::SchemaVersion::initial(),
                                .row_count = values.event_time.size(),
                                .event_time_column_ordinal = 0U,
                                .ordering_column_count = 1U,
                                .minimum_event_time = granules.front().minimum_event_time,
                                .maximum_event_time = granules.front().maximum_event_time,
                                .columns = columns,
                                .granules = granules,
                                .pages = pages})
        .value();
  }

  [[nodiscard]] schema::TableSchema schema_value() const {
    std::vector<schema::ColumnDefinition> definitions;
    definitions.push_back(
        schema::ColumnDefinition::create(event_id, "event_time", columns[0].logical_type, false)
            .value());
    return schema::TableSchema::create(table_id, schema_id, schema::SchemaVersion::initial(),
                                       std::nullopt, std::move(definitions),
                                       {.event_time_column = event_id,
                                        .physical_ordering_key = {event_id},
                                        .partition_columns = {event_id},
                                        .shard_key = {event_id},
                                        .deduplication_key = {}})
        .value();
  }

private:
  [[nodiscard]] static EncodedCsegPage encode(const schema::LogicalType logical_type,
                                              const std::uint32_t rows,
                                              const common::ByteView values,
                                              const PageCompression policy) {
    const auto physical = columnar::PhysicalColumnView::create(
        {.type = logical_type, .nullable = false, .row_count = rows, .null_count = 0U},
        {.validity = {}, .offsets = {}, .values = values});
    return encode_cseg_v1_page(*physical, policy).value();
  }
};

struct OrderingBuffers {
  schema::LogicalType logical_type;
  bool nullable{};
  std::uint32_t null_count{};
  std::vector<std::byte> validity;
  std::vector<std::byte> offsets;
  std::vector<std::byte> values;
};

void append_offset(std::vector<std::byte>& offsets, const std::uint32_t value) {
  append_le(offsets, value);
}

[[nodiscard]] OrderingBuffers ordering_buffers(const schema::LogicalTypeKind kind,
                                               const bool reversed) {
  OrderingBuffers result{.logical_type = kind == schema::LogicalTypeKind::kDecimal
                                             ? schema::LogicalType::decimal(10U, 2U).value()
                                             : type(kind)};
  const auto first = [reversed]<typename Value>(const Value low, const Value high) {
    return reversed ? high : low;
  };
  const auto second = [reversed]<typename Value>(const Value low, const Value high) {
    return reversed ? low : high;
  };
  using schema::LogicalTypeKind;
  switch (kind) {
  case LogicalTypeKind::kBool:
    result.values.push_back(reversed ? std::byte{0x01U} : std::byte{0x02U});
    break;
  case LogicalTypeKind::kInt8:
    append_le(result.values, first(std::int8_t{-1}, std::int8_t{1}));
    append_le(result.values, second(std::int8_t{-1}, std::int8_t{1}));
    break;
  case LogicalTypeKind::kInt16:
    append_le(result.values, first(std::int16_t{-1}, std::int16_t{1}));
    append_le(result.values, second(std::int16_t{-1}, std::int16_t{1}));
    break;
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kDate:
    append_le(result.values, first(std::int32_t{-1}, std::int32_t{1}));
    append_le(result.values, second(std::int32_t{-1}, std::int32_t{1}));
    break;
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kTimestampNs:
    append_le(result.values, first(std::int64_t{-1}, std::int64_t{1}));
    append_le(result.values, second(std::int64_t{-1}, std::int64_t{1}));
    break;
  case LogicalTypeKind::kUInt8:
    append_le(result.values, first(std::uint8_t{1}, std::uint8_t{2}));
    append_le(result.values, second(std::uint8_t{1}, std::uint8_t{2}));
    break;
  case LogicalTypeKind::kUInt16:
    append_le(result.values, first(std::uint16_t{1}, std::uint16_t{2}));
    append_le(result.values, second(std::uint16_t{1}, std::uint16_t{2}));
    break;
  case LogicalTypeKind::kUInt32:
    append_le(result.values, first(std::uint32_t{1}, std::uint32_t{2}));
    append_le(result.values, second(std::uint32_t{1}, std::uint32_t{2}));
    break;
  case LogicalTypeKind::kUInt64:
    append_le(result.values, first(std::uint64_t{1}, std::uint64_t{2}));
    append_le(result.values, second(std::uint64_t{1}, std::uint64_t{2}));
    break;
  case LogicalTypeKind::kFloat32:
    append_le(result.values, std::bit_cast<std::uint32_t>(first(-1.0F, 1.0F)));
    append_le(result.values, std::bit_cast<std::uint32_t>(second(-1.0F, 1.0F)));
    break;
  case LogicalTypeKind::kFloat64:
    append_le(result.values, std::bit_cast<std::uint64_t>(first(-1.0, 1.0)));
    append_le(result.values, std::bit_cast<std::uint64_t>(second(-1.0, 1.0)));
    break;
  case LogicalTypeKind::kDecimal: {
    std::vector<std::byte> negative(16U, std::byte{0xffU});
    std::vector<std::byte> positive(16U, std::byte{0});
    positive.front() = std::byte{1U};
    const auto& left = reversed ? positive : negative;
    const auto& right = reversed ? negative : positive;
    result.values.insert(result.values.end(), left.begin(), left.end());
    result.values.insert(result.values.end(), right.begin(), right.end());
    break;
  }
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
  case LogicalTypeKind::kBinary:
    append_offset(result.offsets, 0U);
    append_offset(result.offsets, 1U);
    append_offset(result.offsets, 2U);
    result.values = reversed ? std::vector<std::byte>{std::byte{'b'}, std::byte{'a'}}
                             : std::vector<std::byte>{std::byte{'a'}, std::byte{'b'}};
    break;
  case LogicalTypeKind::kUuid:
    result.values.resize(32U, std::byte{0});
    result.values[reversed ? 0U : 16U] = std::byte{2U};
    result.values[reversed ? 16U : 0U] = std::byte{1U};
    break;
  }
  return result;
}

[[nodiscard]] common::Status validate_ordering_buffers(const OrderingBuffers& key) {
  const schema::LogicalType event_type = type(schema::LogicalTypeKind::kTimestampNs);
  const schema::LogicalType uuid_type = type(schema::LogicalTypeKind::kUuid);
  const schema::LogicalType uint64_type = type(schema::LogicalTypeKind::kUInt64);
  const schema::LogicalType uint32_type = type(schema::LogicalTypeKind::kUInt32);
  const schema::LogicalType uint8_type = type(schema::LogicalTypeKind::kUInt8);
  const std::vector<CsegColumnDescriptor> columns{
      {.column_id = id<schema::ColumnId>(6U),
       .storage_kind = StorageKind::kUser,
       .logical_type = key.logical_type,
       .nullable = key.nullable,
       .event_time = false,
       .schema_ordinal = 0U,
       .ordering_ordinal = 0U},
      {.column_id = id<schema::ColumnId>(5U),
       .storage_kind = StorageKind::kUser,
       .logical_type = event_type,
       .nullable = false,
       .event_time = true,
       .schema_ordinal = 1U,
       .ordering_ordinal = 1U},
      {.column_id = std::nullopt,
       .storage_kind = StorageKind::kWalId,
       .logical_type = uuid_type,
       .nullable = false,
       .event_time = false,
       .schema_ordinal = std::nullopt,
       .ordering_ordinal = std::nullopt},
      {.column_id = std::nullopt,
       .storage_kind = StorageKind::kRecordSequence,
       .logical_type = uint64_type,
       .nullable = false,
       .event_time = false,
       .schema_ordinal = std::nullopt,
       .ordering_ordinal = std::nullopt},
      {.column_id = std::nullopt,
       .storage_kind = StorageKind::kRowOrdinal,
       .logical_type = uint32_type,
       .nullable = false,
       .event_time = false,
       .schema_ordinal = std::nullopt,
       .ordering_ordinal = std::nullopt},
      {.column_id = std::nullopt,
       .storage_kind = StorageKind::kOperation,
       .logical_type = uint8_type,
       .nullable = false,
       .event_time = false,
       .schema_ordinal = std::nullopt,
       .ordering_ordinal = std::nullopt},
  };
  const std::vector<CsegGranuleDescriptor> granules{{.first_row = 0U,
                                                     .row_count = 2U,
                                                     .first_page_index = 0U,
                                                     .minimum_event_time = 0,
                                                     .maximum_event_time = 0}};
  std::vector<std::byte> event(16U, std::byte{0});
  std::vector<std::byte> wal;
  const common::Uuid::Bytes wal_value = id<schema::SchemaId>(7U).bytes();
  wal.insert(wal.end(), wal_value.begin(), wal_value.end());
  wal.insert(wal.end(), wal_value.begin(), wal_value.end());
  std::vector<std::byte> sequence;
  append_le(sequence, std::uint64_t{1U});
  append_le(sequence, std::uint64_t{1U});
  std::vector<std::byte> ordinal;
  append_le(ordinal, std::uint32_t{0U});
  append_le(ordinal, std::uint32_t{1U});
  const std::vector<std::byte> operation(2U, std::byte{format::kAppendRowsOperation});
  const auto encode = [](const schema::LogicalType logical_type, const bool nullable,
                         const std::uint32_t null_count,
                         const columnar::ColumnVectorBufferView buffers) {
    const auto physical = columnar::PhysicalColumnView::create(
        {.type = logical_type, .nullable = nullable, .row_count = 2U, .null_count = null_count},
        buffers);
    return encode_cseg_v1_page(*physical, PageCompression::kNone).value();
  };
  std::vector<EncodedCsegPage> pages;
  pages.reserve(columns.size());
  pages.push_back(encode(key.logical_type, key.nullable, key.null_count,
                         {.validity = key.validity, .offsets = key.offsets, .values = key.values}));
  pages.push_back(encode(event_type, false, 0U, {.validity = {}, .offsets = {}, .values = event}));
  pages.push_back(encode(uuid_type, false, 0U, {.validity = {}, .offsets = {}, .values = wal}));
  pages.push_back(
      encode(uint64_type, false, 0U, {.validity = {}, .offsets = {}, .values = sequence}));
  pages.push_back(
      encode(uint32_type, false, 0U, {.validity = {}, .offsets = {}, .values = ordinal}));
  pages.push_back(
      encode(uint8_type, false, 0U, {.validity = {}, .offsets = {}, .values = operation}));
  const EncodedCsegPart encoded =
      encode_cseg_v1_part({.part_id = id<PartId>(1U),
                           .table_id = id<schema::TableId>(2U),
                           .tablet_id = id<schema::TabletId>(3U),
                           .schema_id = id<schema::SchemaId>(4U),
                           .schema_version = schema::SchemaVersion::initial(),
                           .row_count = 2U,
                           .event_time_column_ordinal = 1U,
                           .ordering_column_count = 2U,
                           .minimum_event_time = 0,
                           .maximum_event_time = 0,
                           .columns = columns,
                           .granules = granules,
                           .pages = pages})
          .value();
  const auto decoded = decode_cseg_v1_part_exact(encoded.bytes());
  if (!decoded.has_value()) {
    return decoded.error().status();
  }
  return validate_cseg_v1_part_contents(*decoded);
}

[[nodiscard]] common::Status validate_two_granules(const bool reversed) {
  const auto one_row = [](const std::int64_t event_time, const std::uint32_t row_ordinal) {
    SemanticValues values;
    values.event_time = {event_time};
    values.wal_id = {id<schema::SchemaId>(0x70U).bytes()};
    values.record_sequence = {7U};
    values.row_ordinal = {row_ordinal};
    values.operation = {format::kAppendRowsOperation};
    return values;
  };
  const std::int64_t first_time = reversed ? 10 : -5;
  const std::int64_t second_time = reversed ? -5 : 10;
  SemanticFixture first{one_row(first_time, 0U)};
  SemanticFixture second{one_row(second_time, 1U)};
  std::vector<EncodedCsegPage> pages;
  pages.reserve(first.pages.size() + second.pages.size());
  for (EncodedCsegPage& page : first.pages) {
    pages.push_back(std::move(page));
  }
  for (EncodedCsegPage& page : second.pages) {
    pages.push_back(std::move(page));
  }
  const std::vector<CsegGranuleDescriptor> granules{
      {.first_row = 0U,
       .row_count = 1U,
       .first_page_index = 0U,
       .minimum_event_time = first_time,
       .maximum_event_time = first_time},
      {.first_row = 1U,
       .row_count = 1U,
       .first_page_index = first.columns.size(),
       .minimum_event_time = second_time,
       .maximum_event_time = second_time},
  };
  const EncodedCsegPart encoded =
      encode_cseg_v1_part({.part_id = first.part_id,
                           .table_id = first.table_id,
                           .tablet_id = first.tablet_id,
                           .schema_id = first.schema_id,
                           .schema_version = schema::SchemaVersion::initial(),
                           .row_count = 2U,
                           .event_time_column_ordinal = 0U,
                           .ordering_column_count = 1U,
                           .minimum_event_time = -5,
                           .maximum_event_time = 10,
                           .columns = first.columns,
                           .granules = granules,
                           .pages = pages})
          .value();
  const auto decoded = decode_cseg_v1_part_exact(encoded.bytes());
  return decoded.has_value() ? validate_cseg_v1_part_contents(*decoded) : decoded.error().status();
}

[[nodiscard]] common::Status validate_fixture(const SemanticFixture& fixture) {
  const EncodedCsegPart encoded = fixture.encoded();
  const auto decoded = decode_cseg_v1_part_exact(encoded.bytes());
  if (!decoded.has_value()) {
    return decoded.error().status();
  }
  return validate_cseg_v1_part_contents(*decoded);
}

TEST(CsegValidatorTest, AcceptsRawAndCompressedContentsAndExactSchemaBinding) {
  for (const PageCompression policy : {PageCompression::kNone, PageCompression::kZstd}) {
    SemanticFixture fixture{{}, policy};
    const EncodedCsegPart encoded = fixture.encoded();
    const auto decoded = decode_cseg_v1_part_exact(encoded.bytes());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(validate_cseg_v1_part_contents(*decoded).is_ok());
    EXPECT_TRUE(validate_cseg_v1_part(*decoded, fixture.schema_value(), fixture.tablet_id).is_ok());
    EXPECT_EQ(
        validate_cseg_v1_part(*decoded, fixture.schema_value(), id<schema::TabletId>(9U)).code(),
        common::StatusCode::kInvalidArgument);
  }
}

TEST(CsegValidatorTest, RejectsInvalidSystemValuesWithExactClassifications) {
  SemanticValues values;
  values.wal_id[0].fill(std::byte{0});
  EXPECT_EQ(validate_fixture(SemanticFixture{values}).code(), common::StatusCode::kCorruption);

  values = {};
  values.record_sequence[0] = 0U;
  EXPECT_EQ(validate_fixture(SemanticFixture{values}).code(), common::StatusCode::kCorruption);

  values = {};
  values.operation[0] = 0U;
  EXPECT_EQ(validate_fixture(SemanticFixture{values}).code(), common::StatusCode::kCorruption);

  values.operation[0] = 2U;
  EXPECT_EQ(validate_fixture(SemanticFixture{values}).code(), common::StatusCode::kNotSupported);
}

TEST(CsegValidatorTest, RejectsEventExtremaAndNonIncreasingCompleteTuples) {
  SemanticValues values;
  values.event_time = {10, -5};
  EXPECT_EQ(validate_fixture(SemanticFixture{values}).code(), common::StatusCode::kCorruption);

  values = {};
  values.event_time = {-5, -5};
  values.row_ordinal = {0U, 0U};
  EXPECT_EQ(validate_fixture(SemanticFixture{values}).code(), common::StatusCode::kCorruption);

  SemanticFixture extrema;
  extrema.granules[0].minimum_event_time -= 1;
  EXPECT_EQ(validate_fixture(extrema).code(), common::StatusCode::kCorruption);
}

TEST(CsegValidatorTest, AppliesExactOrderingRulesForEveryLogicalType) {
  using schema::LogicalTypeKind;
  constexpr std::array kinds{
      LogicalTypeKind::kBool,        LogicalTypeKind::kInt8,    LogicalTypeKind::kInt16,
      LogicalTypeKind::kInt32,       LogicalTypeKind::kInt64,   LogicalTypeKind::kUInt8,
      LogicalTypeKind::kUInt16,      LogicalTypeKind::kUInt32,  LogicalTypeKind::kUInt64,
      LogicalTypeKind::kFloat32,     LogicalTypeKind::kFloat64, LogicalTypeKind::kDecimal,
      LogicalTypeKind::kTimestampNs, LogicalTypeKind::kDate,    LogicalTypeKind::kSymbol,
      LogicalTypeKind::kString,      LogicalTypeKind::kBinary,  LogicalTypeKind::kUuid,
  };
  for (const LogicalTypeKind kind : kinds) {
    EXPECT_TRUE(validate_ordering_buffers(ordering_buffers(kind, false)).is_ok())
        << schema::logical_type_kind_name(kind);
    EXPECT_EQ(validate_ordering_buffers(ordering_buffers(kind, true)).code(),
              common::StatusCode::kCorruption)
        << schema::logical_type_kind_name(kind);
  }
}

TEST(CsegValidatorTest, OrdersNullFloatingZeroInfinityAndNanExactly) {
  OrderingBuffers nullable{.logical_type = type(schema::LogicalTypeKind::kUInt8),
                           .nullable = true,
                           .null_count = 1U,
                           .validity = {std::byte{0x01U}},
                           .offsets = {},
                           .values = {std::byte{1U}, std::byte{0U}}};
  EXPECT_TRUE(validate_ordering_buffers(nullable).is_ok());
  nullable.validity = {std::byte{0x02U}};
  nullable.values = {std::byte{0U}, std::byte{1U}};
  EXPECT_EQ(validate_ordering_buffers(nullable).code(), common::StatusCode::kCorruption);

  const auto floating = [](const std::uint64_t left, const std::uint64_t right) {
    OrderingBuffers buffers{.logical_type = type(schema::LogicalTypeKind::kFloat64)};
    append_le(buffers.values, left);
    append_le(buffers.values, right);
    return buffers;
  };
  EXPECT_TRUE(validate_ordering_buffers(floating(0x8000000000000000ULL, 0U)).is_ok());
  EXPECT_TRUE(
      validate_ordering_buffers(floating(0x7ff0000000000000ULL, 0x7ff8000000000001ULL)).is_ok());
  EXPECT_EQ(
      validate_ordering_buffers(floating(0x7ff8000000000001ULL, 0x7ff0000000000000ULL)).code(),
      common::StatusCode::kCorruption);
  EXPECT_TRUE(
      validate_ordering_buffers(floating(0x7ff8000000000001ULL, 0x7ff8000000000010ULL)).is_ok());
}

TEST(CsegValidatorTest, PreservesStrictOrderingAcrossGranuleBoundaries) {
  EXPECT_TRUE(validate_two_granules(false).is_ok());
  EXPECT_EQ(validate_two_granules(true).code(), common::StatusCode::kCorruption);
}

TEST(CsegValidatorPropertyTest, GeneratedSortedRowsAndCompressionPoliciesAreDeterministic) {
  for (std::uint32_t row_count = 1U; row_count <= 257U; row_count += 16U) {
    for (const PageCompression policy : {PageCompression::kNone, PageCompression::kZstd}) {
      SemanticValues values = generated_values(row_count);
      const SemanticFixture fixture{values, policy};
      EXPECT_TRUE(validate_fixture(fixture).is_ok()) << "row_count=" << row_count;

      if (row_count > 1U) {
        std::swap(values.event_time.front(), values.event_time.back());
        EXPECT_EQ(validate_fixture(SemanticFixture{std::move(values), policy}).code(),
                  common::StatusCode::kCorruption)
            << "row_count=" << row_count;
      }
    }
  }
}

TEST(CsegValidatorTest, EnforcesExplicitWorkingMemoryLimitBeforeSemanticDecompression) {
  SemanticFixture fixture;
  const EncodedCsegPart encoded = fixture.encoded();
  const auto decoded = decode_cseg_v1_part_exact(encoded.bytes());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(validate_cseg_v1_part_contents(*decoded, {.max_working_bytes = 1U}).code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(validate_cseg_v1_part_contents(*decoded, {.max_working_bytes = 0U}).code(),
            common::StatusCode::kInvalidArgument);

  std::uint64_t page_bytes = 0U;
  for (const CsegPageDescriptor& page : decoded->metadata().pages()) {
    page_bytes += page.uncompressed_length;
  }
  EXPECT_EQ(validate_cseg_v1_part_contents(*decoded, {.max_working_bytes = page_bytes}).code(),
            common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::cseg
