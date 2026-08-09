#ifndef CHRONOS_TESTS_CSEG_CSEG_TEST_FIXTURE_HPP_
#define CHRONOS_TESTS_CSEG_CSEG_TEST_FIXTURE_HPP_

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/cseg/temporal_format.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>
#include <vector>

namespace chronos::cseg::test {

template <typename Identifier> [[nodiscard]] Identifier identifier(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

template <typename Integer>
void append_little_endian(std::vector<std::byte>& bytes, Integer value) {
  using Unsigned = std::make_unsigned_t<Integer>;
  const Unsigned encoded = std::bit_cast<Unsigned>(value);
  for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
    bytes.push_back(std::byte{static_cast<std::uint8_t>(encoded >> (index * 8U))});
  }
}

[[nodiscard]] inline schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] inline EncodedCsegPage encode_fixed_page(const schema::LogicalType logical_type,
                                                       const std::uint32_t row_count,
                                                       const common::ByteView values,
                                                       const PageCompression compression) {
  const auto physical = columnar::PhysicalColumnView::create(
      {.type = logical_type, .nullable = false, .row_count = row_count, .null_count = 0U},
      {.validity = {}, .offsets = {}, .values = values});
  return encode_cseg_v1_page(*physical, compression).value();
}

[[nodiscard]] inline EncodedCsegPart
make_valid_part(const PageCompression compression = PageCompression::kNone) {
  constexpr std::uint32_t kRowCount = 2U;
  std::vector<CsegColumnDescriptor> columns{
      {.column_id = identifier<schema::ColumnId>(5U),
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

  std::vector<std::byte> event_time;
  append_little_endian(event_time, std::int64_t{-5});
  append_little_endian(event_time, std::int64_t{10});
  const common::Uuid::Bytes wal = identifier<schema::SchemaId>(0x70U).bytes();
  std::vector<std::byte> wal_id;
  wal_id.insert(wal_id.end(), wal.begin(), wal.end());
  wal_id.insert(wal_id.end(), wal.begin(), wal.end());
  std::vector<std::byte> record_sequence;
  append_little_endian(record_sequence, std::uint64_t{7U});
  append_little_endian(record_sequence, std::uint64_t{7U});
  std::vector<std::byte> row_ordinal;
  append_little_endian(row_ordinal, std::uint32_t{0U});
  append_little_endian(row_ordinal, std::uint32_t{1U});
  const std::vector<std::byte> operation(kRowCount, std::byte{format::kAppendRowsOperation});
  std::vector<EncodedCsegPage> pages;
  pages.push_back(encode_fixed_page(columns[0].logical_type, kRowCount, event_time, compression));
  pages.push_back(encode_fixed_page(columns[1].logical_type, kRowCount, wal_id, compression));
  pages.push_back(
      encode_fixed_page(columns[2].logical_type, kRowCount, record_sequence, compression));
  pages.push_back(encode_fixed_page(columns[3].logical_type, kRowCount, row_ordinal, compression));
  pages.push_back(encode_fixed_page(columns[4].logical_type, kRowCount, operation, compression));
  const std::vector<CsegGranuleDescriptor> granules{{.first_row = 0U,
                                                     .row_count = kRowCount,
                                                     .first_page_index = 0U,
                                                     .minimum_event_time = -5,
                                                     .maximum_event_time = 10}};
  return encode_cseg_v1_part({.part_id = identifier<PartId>(1U),
                              .table_id = identifier<schema::TableId>(2U),
                              .tablet_id = identifier<schema::TabletId>(3U),
                              .schema_id = identifier<schema::SchemaId>(4U),
                              .schema_version = schema::SchemaVersion::initial(),
                              .row_count = kRowCount,
                              .event_time_column_ordinal = 0U,
                              .ordering_column_count = 1U,
                              .minimum_event_time = -5,
                              .maximum_event_time = 10,
                              .columns = columns,
                              .granules = granules,
                              .pages = pages})
      .value();
}

struct PartFixtureOptions {
  std::uint8_t part_id_seed{1U};
  std::int64_t first_event_time{-100};
  std::uint64_t record_sequence{7U};
};

[[nodiscard]] inline EncodedCsegPart
make_valid_part_with_rows(const std::uint32_t rows, const std::uint32_t rows_per_granule,
                          const PageCompression compression = PageCompression::kNone,
                          const PartFixtureOptions options = {}) {
  const schema::LogicalType timestamp = type(schema::LogicalTypeKind::kTimestampNs);
  const schema::LogicalType uuid = type(schema::LogicalTypeKind::kUuid);
  const schema::LogicalType uint64 = type(schema::LogicalTypeKind::kUInt64);
  const schema::LogicalType uint32 = type(schema::LogicalTypeKind::kUInt32);
  const schema::LogicalType uint8 = type(schema::LogicalTypeKind::kUInt8);
  const std::vector<CsegColumnDescriptor> columns{{.column_id = identifier<schema::ColumnId>(5U),
                                                   .storage_kind = StorageKind::kUser,
                                                   .logical_type = timestamp,
                                                   .nullable = false,
                                                   .event_time = true,
                                                   .schema_ordinal = 0U,
                                                   .ordering_ordinal = 0U},
                                                  {.column_id = std::nullopt,
                                                   .storage_kind = StorageKind::kWalId,
                                                   .logical_type = uuid,
                                                   .nullable = false,
                                                   .event_time = false,
                                                   .schema_ordinal = std::nullopt,
                                                   .ordering_ordinal = std::nullopt},
                                                  {.column_id = std::nullopt,
                                                   .storage_kind = StorageKind::kRecordSequence,
                                                   .logical_type = uint64,
                                                   .nullable = false,
                                                   .event_time = false,
                                                   .schema_ordinal = std::nullopt,
                                                   .ordering_ordinal = std::nullopt},
                                                  {.column_id = std::nullopt,
                                                   .storage_kind = StorageKind::kRowOrdinal,
                                                   .logical_type = uint32,
                                                   .nullable = false,
                                                   .event_time = false,
                                                   .schema_ordinal = std::nullopt,
                                                   .ordering_ordinal = std::nullopt},
                                                  {.column_id = std::nullopt,
                                                   .storage_kind = StorageKind::kOperation,
                                                   .logical_type = uint8,
                                                   .nullable = false,
                                                   .event_time = false,
                                                   .schema_ordinal = std::nullopt,
                                                   .ordering_ordinal = std::nullopt}};
  std::vector<CsegGranuleDescriptor> granules;
  std::vector<EncodedCsegPage> pages;
  const common::Uuid::Bytes wal = identifier<schema::SchemaId>(0x70U).bytes();
  for (std::uint32_t first = 0U; first < rows; first += rows_per_granule) {
    const std::uint32_t count = std::min(rows_per_granule, rows - first);
    std::vector<std::byte> event_time;
    std::vector<std::byte> wal_id;
    std::vector<std::byte> record_sequence;
    std::vector<std::byte> row_ordinal;
    for (std::uint32_t local = 0U; local < count; ++local) {
      const std::uint32_t global = first + local;
      append_little_endian(event_time,
                           options.first_event_time + static_cast<std::int64_t>(global));
      wal_id.insert(wal_id.end(), wal.begin(), wal.end());
      append_little_endian(record_sequence, options.record_sequence);
      append_little_endian(row_ordinal, global);
    }
    const std::vector<std::byte> operation(count, std::byte{format::kAppendRowsOperation});
    granules.push_back(
        {.first_row = first,
         .row_count = count,
         .first_page_index = static_cast<std::uint64_t>(pages.size()),
         .minimum_event_time = options.first_event_time + static_cast<std::int64_t>(first),
         .maximum_event_time =
             options.first_event_time + static_cast<std::int64_t>(first + count - 1U)});
    pages.push_back(encode_fixed_page(timestamp, count, event_time, compression));
    pages.push_back(encode_fixed_page(uuid, count, wal_id, compression));
    pages.push_back(encode_fixed_page(uint64, count, record_sequence, compression));
    pages.push_back(encode_fixed_page(uint32, count, row_ordinal, compression));
    pages.push_back(encode_fixed_page(uint8, count, operation, compression));
  }
  return encode_cseg_v1_part(
             {.part_id = identifier<PartId>(options.part_id_seed),
              .table_id = identifier<schema::TableId>(2U),
              .tablet_id = identifier<schema::TabletId>(3U),
              .schema_id = identifier<schema::SchemaId>(4U),
              .schema_version = schema::SchemaVersion::initial(),
              .row_count = rows,
              .event_time_column_ordinal = 0U,
              .ordering_column_count = 1U,
              .minimum_event_time = options.first_event_time,
              .maximum_event_time = options.first_event_time + static_cast<std::int64_t>(rows - 1U),
              .columns = columns,
              .granules = granules,
              .pages = pages})
      .value();
}

[[nodiscard]] inline EncodedCsegPart
make_valid_temporal_part(const PageCompression compression = PageCompression::kNone) {
  constexpr std::uint32_t kRows = 2U;
  const schema::LogicalType timestamp = type(schema::LogicalTypeKind::kTimestampNs);
  const schema::LogicalType uuid = type(schema::LogicalTypeKind::kUuid);
  const schema::LogicalType uint64 = type(schema::LogicalTypeKind::kUInt64);
  const schema::LogicalType uint32 = type(schema::LogicalTypeKind::kUInt32);
  const schema::LogicalType uint8 = type(schema::LogicalTypeKind::kUInt8);
  const schema::LogicalType binary = type(schema::LogicalTypeKind::kBinary);
  const std::vector<CsegColumnDescriptor> columns{
      {.column_id = identifier<schema::ColumnId>(5U),
       .storage_kind = StorageKind::kUser,
       .logical_type = timestamp,
       .nullable = false,
       .event_time = true,
       .schema_ordinal = 0U,
       .ordering_ordinal = 0U},
      {.column_id = std::nullopt,
       .storage_kind = StorageKind::kCommitSource,
       .logical_type = uint8,
       .nullable = false},
      {.column_id = std::nullopt,
       .storage_kind = StorageKind::kSourceId,
       .logical_type = uuid,
       .nullable = false},
      {.column_id = std::nullopt,
       .storage_kind = StorageKind::kCommitPosition,
       .logical_type = uint64,
       .nullable = false},
      {.column_id = std::nullopt,
       .storage_kind = StorageKind::kTemporalRowOrdinal,
       .logical_type = uint32,
       .nullable = false},
      {.column_id = std::nullopt,
       .storage_kind = StorageKind::kTemporalOperation,
       .logical_type = uint8,
       .nullable = false},
      {.column_id = std::nullopt,
       .storage_kind = StorageKind::kLogicalIdentity,
       .logical_type = binary,
       .nullable = false},
      {.column_id = std::nullopt,
       .storage_kind = StorageKind::kReceiveTime,
       .logical_type = timestamp,
       .nullable = false},
      {.column_id = std::nullopt,
       .storage_kind = StorageKind::kSystemCommitTime,
       .logical_type = timestamp,
       .nullable = false},
  };

  std::vector<std::byte> event_times;
  append_little_endian(event_times, std::int64_t{10});
  append_little_endian(event_times, std::int64_t{20});
  const std::vector<std::byte> sources(
      kRows, std::byte{static_cast<std::uint8_t>(temporal_format::CommitSource::kWal)});
  const common::Uuid::Bytes source = identifier<schema::SchemaId>(8U).bytes();
  std::vector<std::byte> source_ids;
  source_ids.insert(source_ids.end(), source.begin(), source.end());
  source_ids.insert(source_ids.end(), source.begin(), source.end());
  std::vector<std::byte> positions;
  append_little_endian(positions, std::uint64_t{7U});
  append_little_endian(positions, std::uint64_t{9U});
  std::vector<std::byte> ordinals;
  append_little_endian(ordinals, std::uint32_t{0U});
  append_little_endian(ordinals, std::uint32_t{0U});
  const std::vector<std::byte> operations(
      kRows, std::byte{static_cast<std::uint8_t>(temporal_format::Operation::kOriginal)});
  std::vector<std::byte> identity_offsets;
  append_little_endian(identity_offsets, std::uint32_t{0U});
  append_little_endian(identity_offsets, std::uint32_t{1U});
  append_little_endian(identity_offsets, std::uint32_t{2U});
  const std::vector<std::byte> identities{std::byte{'a'}, std::byte{'b'}};
  std::vector<std::byte> receive_times;
  append_little_endian(receive_times, std::int64_t{100});
  append_little_endian(receive_times, std::int64_t{101});
  std::vector<std::byte> system_times;
  append_little_endian(system_times, std::int64_t{200});
  append_little_endian(system_times, std::int64_t{201});

  const auto encode_page = [&](const schema::LogicalType logical_type,
                               const common::ByteView offsets, const common::ByteView values) {
    const auto physical = columnar::PhysicalColumnView::create(
        {.type = logical_type, .nullable = false, .row_count = kRows, .null_count = 0U},
        {.validity = {}, .offsets = offsets, .values = values});
    return encode_cseg_v1_page(*physical, compression).value();
  };
  std::vector<EncodedCsegPage> pages;
  pages.push_back(encode_page(timestamp, {}, event_times));
  pages.push_back(encode_page(uint8, {}, sources));
  pages.push_back(encode_page(uuid, {}, source_ids));
  pages.push_back(encode_page(uint64, {}, positions));
  pages.push_back(encode_page(uint32, {}, ordinals));
  pages.push_back(encode_page(uint8, {}, operations));
  pages.push_back(encode_page(binary, identity_offsets, identities));
  pages.push_back(encode_page(timestamp, {}, receive_times));
  pages.push_back(encode_page(timestamp, {}, system_times));
  const std::vector<CsegGranuleDescriptor> granules{{.first_row = 0U,
                                                     .row_count = kRows,
                                                     .first_page_index = 0U,
                                                     .minimum_event_time = 10,
                                                     .maximum_event_time = 20}};
  return encode_cseg_v2_temporal_part({.part_id = identifier<PartId>(1U),
                                       .table_id = identifier<schema::TableId>(2U),
                                       .tablet_id = identifier<schema::TabletId>(3U),
                                       .schema_id = identifier<schema::SchemaId>(4U),
                                       .schema_version = schema::SchemaVersion::initial(),
                                       .row_count = kRows,
                                       .event_time_column_ordinal = 0U,
                                       .ordering_column_count = 1U,
                                       .minimum_event_time = 10,
                                       .maximum_event_time = 20,
                                       .columns = columns,
                                       .granules = granules,
                                       .pages = pages})
      .value();
}

} // namespace chronos::cseg::test

#endif // CHRONOS_TESTS_CSEG_CSEG_TEST_FIXTURE_HPP_
