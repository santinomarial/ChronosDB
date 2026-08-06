#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/status.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/page_codec.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/manifest/compaction_equivalence.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"
#include "cseg/cseg_test_fixture.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

using cseg::test::identifier;

struct Row {
  std::int64_t event_time{};
  std::optional<std::uint64_t> value_bits;
  std::optional<std::string> payload;
  std::uint64_t record_sequence{};
  std::uint32_t row_ordinal{};
};

struct Fixture {
  schema::ColumnId event_id{identifier<schema::ColumnId>(5U)};
  schema::ColumnId value_id{identifier<schema::ColumnId>(6U)};
  schema::ColumnId payload_id{identifier<schema::ColumnId>(7U)};
  schema::TableId table_id{identifier<schema::TableId>(2U)};
  schema::TabletId tablet_id{identifier<schema::TabletId>(3U)};
  schema::SchemaId schema_id{identifier<schema::SchemaId>(4U)};
  wal::WalId wal_id{};
  schema::TableSchema schema;

  Fixture() : schema(make_schema()) {
    wal_id.bytes = identifier<schema::SchemaId>(0x70U).bytes();
  }

  [[nodiscard]] schema::TableSchema make_schema() const {
    std::vector<schema::ColumnDefinition> columns;
    columns.push_back(
        schema::ColumnDefinition::create(
            event_id, "event_time",
            schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(), false)
            .value());
    columns.push_back(schema::ColumnDefinition::create(
                          value_id, "value",
                          schema::LogicalType::create(schema::LogicalTypeKind::kFloat64).value(),
                          true)
                          .value());
    columns.push_back(schema::ColumnDefinition::create(
                          payload_id, "payload",
                          schema::LogicalType::create(schema::LogicalTypeKind::kBinary).value(),
                          true)
                          .value());
    return schema::TableSchema::create(table_id, schema_id, schema::SchemaVersion::initial(),
                                       std::nullopt, std::move(columns),
                                       {.event_time_column = event_id,
                                        .physical_ordering_key = {event_id},
                                        .partition_columns = {event_id},
                                        .shard_key = {event_id},
                                        .deduplication_key = {}})
        .value();
  }
};

void set_bit(std::vector<std::byte>& bytes, const std::uint32_t row) {
  bytes[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
}

template <typename Integer>
void store_little_endian(std::vector<std::byte>& bytes, const std::size_t offset,
                         const Integer value) {
  using Unsigned = std::make_unsigned_t<Integer>;
  const Unsigned encoded = std::bit_cast<Unsigned>(value);
  for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
    bytes[offset + index] = static_cast<std::byte>(encoded >> (index * 8U));
  }
}

[[nodiscard]] cseg::EncodedCsegPage encode_page(const schema::LogicalType type, const bool nullable,
                                                const std::uint32_t rows, const std::uint32_t nulls,
                                                const columnar::ColumnVectorBuffers& buffers,
                                                const cseg::PageCompression compression) {
  const common::Result<columnar::PhysicalColumnView> physical =
      columnar::PhysicalColumnView::create(
          {.type = type, .nullable = nullable, .row_count = rows, .null_count = nulls},
          {.validity = buffers.validity, .offsets = buffers.offsets, .values = buffers.values});
  EXPECT_TRUE(physical.has_value());
  return cseg::encode_cseg_v1_page(*physical, compression).value();
}

[[nodiscard]] cseg::EncodedCsegPart make_part(const Fixture& fixture, const std::uint8_t part_seed,
                                              const std::span<const Row> rows,
                                              const cseg::PageCompression compression) {
  const std::uint32_t count = static_cast<std::uint32_t>(rows.size());
  const auto timestamp = schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value();
  const auto floating = schema::LogicalType::create(schema::LogicalTypeKind::kFloat64).value();
  const auto binary = schema::LogicalType::create(schema::LogicalTypeKind::kBinary).value();
  const auto uuid = schema::LogicalType::create(schema::LogicalTypeKind::kUuid).value();
  const auto uint64 = schema::LogicalType::create(schema::LogicalTypeKind::kUInt64).value();
  const auto uint32 = schema::LogicalType::create(schema::LogicalTypeKind::kUInt32).value();
  const auto uint8 = schema::LogicalType::create(schema::LogicalTypeKind::kUInt8).value();

  columnar::ColumnVectorBuffers events;
  events.values.resize(rows.size() * sizeof(std::int64_t));
  columnar::ColumnVectorBuffers values;
  values.validity.resize(columnar::bitmap_size(count), std::byte{0});
  values.values.resize(rows.size() * sizeof(std::uint64_t), std::byte{0});
  columnar::ColumnVectorBuffers payloads;
  payloads.validity.resize(columnar::bitmap_size(count), std::byte{0});
  payloads.offsets.resize((rows.size() + 1U) * sizeof(std::uint32_t), std::byte{0});
  columnar::ColumnVectorBuffers wal;
  wal.values.reserve(rows.size() * fixture.wal_id.bytes.size());
  columnar::ColumnVectorBuffers sequences;
  sequences.values.resize(rows.size() * sizeof(std::uint64_t));
  columnar::ColumnVectorBuffers ordinals;
  ordinals.values.resize(rows.size() * sizeof(std::uint32_t));
  columnar::ColumnVectorBuffers operations;
  operations.values.assign(rows.size(), std::byte{cseg::format::kAppendRowsOperation});
  std::uint32_t value_nulls = 0U;
  std::uint32_t payload_nulls = 0U;
  for (std::uint32_t row = 0U; row < count; ++row) {
    store_little_endian(events.values, row * sizeof(std::int64_t), rows[row].event_time);
    if (rows[row].value_bits.has_value()) {
      set_bit(values.validity, row);
      store_little_endian(values.values, row * sizeof(std::uint64_t),
                          rows[row].value_bits.value_or(0U));
    } else {
      ++value_nulls;
    }
    if (rows[row].payload.has_value()) {
      set_bit(payloads.validity, row);
      const std::string payload = rows[row].payload.value_or(std::string{});
      for (const char byte : payload) {
        payloads.values.push_back(static_cast<std::byte>(static_cast<unsigned char>(byte)));
      }
    } else {
      ++payload_nulls;
    }
    store_little_endian(payloads.offsets, (row + 1U) * sizeof(std::uint32_t),
                        static_cast<std::uint32_t>(payloads.values.size()));
    wal.values.insert(wal.values.end(), fixture.wal_id.bytes.begin(), fixture.wal_id.bytes.end());
    store_little_endian(sequences.values, row * sizeof(std::uint64_t), rows[row].record_sequence);
    store_little_endian(ordinals.values, row * sizeof(std::uint32_t), rows[row].row_ordinal);
  }

  std::vector<cseg::EncodedCsegPage> pages;
  pages.push_back(encode_page(timestamp, false, count, 0U, events, compression));
  pages.push_back(encode_page(floating, true, count, value_nulls, values, compression));
  pages.push_back(encode_page(binary, true, count, payload_nulls, payloads, compression));
  pages.push_back(encode_page(uuid, false, count, 0U, wal, compression));
  pages.push_back(encode_page(uint64, false, count, 0U, sequences, compression));
  pages.push_back(encode_page(uint32, false, count, 0U, ordinals, compression));
  pages.push_back(encode_page(uint8, false, count, 0U, operations, compression));

  const std::vector<cseg::CsegColumnDescriptor> columns{
      {.column_id = fixture.event_id,
       .storage_kind = cseg::StorageKind::kUser,
       .logical_type = timestamp,
       .nullable = false,
       .event_time = true,
       .schema_ordinal = 0U,
       .ordering_ordinal = 0U},
      {.column_id = fixture.value_id,
       .storage_kind = cseg::StorageKind::kUser,
       .logical_type = floating,
       .nullable = true,
       .schema_ordinal = 1U},
      {.column_id = fixture.payload_id,
       .storage_kind = cseg::StorageKind::kUser,
       .logical_type = binary,
       .nullable = true,
       .schema_ordinal = 2U},
      {.column_id = std::nullopt,
       .storage_kind = cseg::StorageKind::kWalId,
       .logical_type = uuid,
       .nullable = false},
      {.column_id = std::nullopt,
       .storage_kind = cseg::StorageKind::kRecordSequence,
       .logical_type = uint64,
       .nullable = false},
      {.column_id = std::nullopt,
       .storage_kind = cseg::StorageKind::kRowOrdinal,
       .logical_type = uint32,
       .nullable = false},
      {.column_id = std::nullopt,
       .storage_kind = cseg::StorageKind::kOperation,
       .logical_type = uint8,
       .nullable = false},
  };
  const std::vector<cseg::CsegGranuleDescriptor> granules{{
      .first_row = 0U,
      .row_count = count,
      .first_page_index = 0U,
      .minimum_event_time = rows.front().event_time,
      .maximum_event_time = rows.back().event_time,
  }};
  return cseg::encode_cseg_v1_part({.part_id = identifier<cseg::PartId>(part_seed),
                                    .table_id = fixture.table_id,
                                    .tablet_id = fixture.tablet_id,
                                    .schema_id = fixture.schema_id,
                                    .schema_version = schema::SchemaVersion::initial(),
                                    .row_count = count,
                                    .event_time_column_ordinal = 0U,
                                    .ordering_column_count = 1U,
                                    .minimum_event_time = rows.front().event_time,
                                    .maximum_event_time = rows.back().event_time,
                                    .columns = columns,
                                    .granules = granules,
                                    .pages = pages})
      .value();
}

struct ImageSet {
  std::vector<cseg::EncodedCsegPart> owners;
  std::vector<CompactionPartImage> images;
};

[[nodiscard]] ImageSet make_images(const Fixture& fixture,
                                   const std::span<const std::vector<Row>> partitions,
                                   const std::uint8_t first_seed,
                                   const cseg::PageCompression compression) {
  ImageSet result;
  result.owners.reserve(partitions.size());
  result.images.reserve(partitions.size());
  for (std::size_t index = 0U; index < partitions.size(); ++index) {
    result.owners.push_back(make_part(fixture, static_cast<std::uint8_t>(first_seed + index),
                                      partitions[index], compression));
  }
  for (std::size_t index = 0U; index < result.owners.size(); ++index) {
    result.images.push_back(
        {.part_id = identifier<cseg::PartId>(static_cast<std::uint8_t>(first_seed + index)),
         .bytes = result.owners[index].bytes()});
  }
  return result;
}

[[nodiscard]] std::vector<Row> sample_rows() {
  return {{.event_time = -10,
           .value_bits = 0x7ff8000000000001ULL,
           .payload = std::string{"a\0b", 3U},
           .record_sequence = 7U,
           .row_ordinal = 0U},
          {.event_time = 0,
           .value_bits = std::nullopt,
           .payload = std::nullopt,
           .record_sequence = 8U,
           .row_ordinal = 0U},
          {.event_time = 5,
           .value_bits = 0x8000000000000000ULL,
           .payload = std::string{},
           .record_sequence = 9U,
           .row_ordinal = 0U},
          {.event_time = 20,
           .value_bits = 0x3ff0000000000000ULL,
           .payload = std::string{"xyz"},
           .record_sequence = 10U,
           .row_ordinal = 0U}};
}

TEST(CompactionEquivalenceTest, AcceptsInterleavedInputsAndRepartitionedCompressedOutput) {
  const Fixture fixture;
  const std::vector<Row> rows = sample_rows();
  const std::array input_partitions{std::vector<Row>{rows[0], rows[2]},
                                    std::vector<Row>{rows[1], rows[3]}};
  const std::array output_partitions{rows};
  const ImageSet inputs =
      make_images(fixture, input_partitions, 0x10U, cseg::PageCompression::kNone);
  const ImageSet outputs =
      make_images(fixture, output_partitions, 0x80U, cseg::PageCompression::kZstd);

  EXPECT_TRUE(validate_append_only_cseg_v1_equivalence(
                  inputs.images, outputs.images, fixture.schema, fixture.tablet_id, fixture.wal_id)
                  .is_ok());
}

TEST(CompactionEquivalenceTest, RejectsChangedCellMissingRowWrongWalAndReusedIdentity) {
  const Fixture fixture;
  const std::vector<Row> rows = sample_rows();
  const std::array one_partition{rows};
  const ImageSet inputs = make_images(fixture, one_partition, 0x10U, cseg::PageCompression::kNone);

  std::vector<Row> changed = rows;
  changed[2].value_bits = 0U;
  const std::array changed_partition{changed};
  const ImageSet changed_output =
      make_images(fixture, changed_partition, 0x80U, cseg::PageCompression::kNone);
  EXPECT_EQ(validate_append_only_cseg_v1_equivalence(inputs.images, changed_output.images,
                                                     fixture.schema, fixture.tablet_id,
                                                     fixture.wal_id)
                .code(),
            common::StatusCode::kInvalidArgument);

  const std::array missing_partition{std::vector<Row>{rows.begin(), rows.end() - 1U}};
  const ImageSet missing_output =
      make_images(fixture, missing_partition, 0x80U, cseg::PageCompression::kNone);
  EXPECT_EQ(validate_append_only_cseg_v1_equivalence(inputs.images, missing_output.images,
                                                     fixture.schema, fixture.tablet_id,
                                                     fixture.wal_id)
                .code(),
            common::StatusCode::kInvalidArgument);

  wal::WalId wrong_wal = fixture.wal_id;
  wrong_wal.bytes.back() = std::byte{0xffU};
  const ImageSet exact_output =
      make_images(fixture, one_partition, 0x80U, cseg::PageCompression::kNone);
  EXPECT_EQ(validate_append_only_cseg_v1_equivalence(inputs.images, exact_output.images,
                                                     fixture.schema, fixture.tablet_id, wrong_wal)
                .code(),
            common::StatusCode::kInvalidArgument);

  std::array reused_images{CompactionPartImage{.part_id = inputs.images.front().part_id,
                                               .bytes = exact_output.images.front().bytes}};
  EXPECT_EQ(validate_append_only_cseg_v1_equivalence(inputs.images, reused_images, fixture.schema,
                                                     fixture.tablet_id, fixture.wal_id)
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(validate_append_only_cseg_v1_equivalence(
                inputs.images, exact_output.images, fixture.schema, fixture.tablet_id,
                fixture.wal_id,
                {.max_parts_per_side = 1U, .max_rows_per_side = 4U, .max_resident_page_bytes = 1U})
                .code(),
            common::StatusCode::kResourceExhausted);
}

TEST(CompactionEquivalenceTest, RejectsCrossPartDuplicateTupleAndHostileBytes) {
  const Fixture fixture;
  const std::vector<Row> rows = sample_rows();
  const std::array duplicate_inputs{std::vector<Row>{rows[0]}, std::vector<Row>{rows[0]}};
  const std::array distinct_outputs{std::vector<Row>{rows[0], rows[1]}};
  const ImageSet inputs =
      make_images(fixture, duplicate_inputs, 0x10U, cseg::PageCompression::kNone);
  const ImageSet outputs =
      make_images(fixture, distinct_outputs, 0x80U, cseg::PageCompression::kNone);
  EXPECT_EQ(validate_append_only_cseg_v1_equivalence(inputs.images, outputs.images, fixture.schema,
                                                     fixture.tablet_id, fixture.wal_id)
                .code(),
            common::StatusCode::kCorruption);

  const std::array exact_partition{rows};
  const ImageSet exact_inputs =
      make_images(fixture, exact_partition, 0x10U, cseg::PageCompression::kNone);
  ImageSet corrupt_outputs =
      make_images(fixture, exact_partition, 0x80U, cseg::PageCompression::kNone);
  std::vector<std::byte> corrupt_bytes(corrupt_outputs.images.front().bytes.begin(),
                                       corrupt_outputs.images.front().bytes.end());
  corrupt_bytes.back() ^= std::byte{1U};
  corrupt_outputs.images.front().bytes = corrupt_bytes;
  EXPECT_EQ(validate_append_only_cseg_v1_equivalence(exact_inputs.images, corrupt_outputs.images,
                                                     fixture.schema, fixture.tablet_id,
                                                     fixture.wal_id)
                .code(),
            common::StatusCode::kCorruption);
}

TEST(CompactionEquivalenceTest, DeterministicPartitionPropertyMatrix) {
  const Fixture fixture;
  for (std::uint32_t seed = 1U; seed <= 32U; ++seed) {
    std::vector<Row> rows;
    rows.reserve(12U);
    for (std::uint32_t row = 0U; row < 12U; ++row) {
      rows.push_back(
          {.event_time = static_cast<std::int64_t>(row * 3U + seed),
           .value_bits =
               row % 3U == 0U
                   ? std::optional<std::uint64_t>{}
                   : std::optional<std::uint64_t>{(static_cast<std::uint64_t>(seed) << 32U) | row},
           .payload = row % 4U == 0U ? std::optional<std::string>{}
                                     : std::optional<std::string>{std::string(
                                           (row + seed) % 9U, static_cast<char>('a' + row))},
           .record_sequence = 100U + row,
           .row_ordinal = seed});
    }
    std::array<std::vector<Row>, 3U> input_partitions;
    for (std::size_t row = 0U; row < rows.size(); ++row) {
      input_partitions[row % input_partitions.size()].push_back(rows[row]);
    }
    const std::array output_partitions{
        std::vector<Row>{rows.begin(), rows.begin() + 5U},
        std::vector<Row>{rows.begin() + 5U, rows.end()},
    };
    const ImageSet inputs =
        make_images(fixture, input_partitions, 0x10U, cseg::PageCompression::kNone);
    const ImageSet outputs =
        make_images(fixture, output_partitions, 0x80U, cseg::PageCompression::kZstd);
    ASSERT_TRUE(validate_append_only_cseg_v1_equivalence(
                    inputs.images, outputs.images, fixture.schema, fixture.tablet_id,
                    fixture.wal_id, {.max_parts_per_side = 4U, .max_rows_per_side = 12U})
                    .is_ok())
        << "seed=" << seed;
  }
}

} // namespace
} // namespace chronos::manifest
