#ifndef CHRONOS_TESTS_CSEG_CSEG_TEST_FIXTURE_HPP_
#define CHRONOS_TESTS_CSEG_CSEG_TEST_FIXTURE_HPP_

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"

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
       .nullable = false},
      {.column_id = std::nullopt,
       .storage_kind = StorageKind::kRecordSequence,
       .logical_type = type(schema::LogicalTypeKind::kUInt64),
       .nullable = false},
      {.column_id = std::nullopt,
       .storage_kind = StorageKind::kRowOrdinal,
       .logical_type = type(schema::LogicalTypeKind::kUInt32),
       .nullable = false},
      {.column_id = std::nullopt,
       .storage_kind = StorageKind::kOperation,
       .logical_type = type(schema::LogicalTypeKind::kUInt8),
       .nullable = false},
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

} // namespace chronos::cseg::test

#endif // CHRONOS_TESTS_CSEG_CSEG_TEST_FIXTURE_HPP_
