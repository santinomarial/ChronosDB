#ifndef CHRONOS_TESTS_QUERY_HEAD_SCAN_TEST_FIXTURE_HPP_
#define CHRONOS_TESTS_QUERY_HEAD_SCAN_TEST_FIXTURE_HPP_

#include "chronos/columnar/column_vector.hpp"
#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/head/mutable_head.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "columnar/columnar_test_support.hpp"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::query::test {

inline constexpr std::uint16_t kTableId = 500U;
inline constexpr std::uint16_t kInitialSchemaId = 501U;
inline constexpr std::uint16_t kSuccessorSchemaId = 502U;
inline constexpr std::uint16_t kTabletId = 503U;

[[nodiscard]] inline schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

inline void append_i64(std::vector<std::byte>& destination, const std::int64_t value) {
  const std::uint64_t bits = static_cast<std::uint64_t>(value);
  for (std::size_t index = 0U; index < sizeof(bits); ++index)
    destination.push_back(static_cast<std::byte>((bits >> (index * 8U)) & 0xffU));
}

inline void set_bit(std::vector<std::byte>& bitmap, const std::uint32_t row) {
  bitmap[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
}

[[nodiscard]] inline schema::SchemaLineage lineage() {
  const schema::ColumnId event = columnar::test::id<schema::ColumnId>(510U);
  const schema::ColumnId label = columnar::test::id<schema::ColumnId>(511U);
  const schema::ColumnId enabled = columnar::test::id<schema::ColumnId>(512U);
  const schema::ColumnId reading = columnar::test::id<schema::ColumnId>(513U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event, "event_time", type(schema::LogicalTypeKind::kTimestampNs), false)
                        .value());
  columns.push_back(
      schema::ColumnDefinition::create(label, "label", type(schema::LogicalTypeKind::kString), true)
          .value());
  columns.push_back(schema::ColumnDefinition::create(enabled, "enabled",
                                                     type(schema::LogicalTypeKind::kBool), true)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(reading, "reading",
                                                     type(schema::LogicalTypeKind::kInt64), true)
                        .value());
  schema::TableSchema initial =
      schema::TableSchema::create(columnar::test::id<schema::TableId>(kTableId),
                                  columnar::test::id<schema::SchemaId>(kInitialSchemaId),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns),
                                  {.event_time_column = event,
                                   .physical_ordering_key = {event},
                                   .partition_columns = {event},
                                   .shard_key = {event},
                                   .deduplication_key = {}})
          .value();
  schema::SchemaLineage result = schema::SchemaLineage::create(std::move(initial)).value();
  std::vector<schema::ColumnDefinition> successor_columns{result.current()->columns().begin(),
                                                          result.current()->columns().end()};
  successor_columns.push_back(
      schema::ColumnDefinition::create(columnar::test::id<schema::ColumnId>(514U), "payload",
                                       type(schema::LogicalTypeKind::kBinary), true)
          .value());
  schema::TableSchema successor =
      schema::TableSchema::create(result.table_id(),
                                  columnar::test::id<schema::SchemaId>(kSuccessorSchemaId),
                                  schema::SchemaVersion::from_value(2U).value(),
                                  result.current()->schema_id(), std::move(successor_columns),
                                  {.event_time_column = event,
                                   .physical_ordering_key = {event},
                                   .partition_columns = {event},
                                   .shard_key = {event},
                                   .deduplication_key = {}})
          .value();
  if (!result.append(std::move(successor)).is_ok())
    std::terminate();
  return result;
}

struct BatchRange {
  std::uint32_t first_row{};
  std::uint32_t row_count{};
};

[[nodiscard]] inline std::shared_ptr<const columnar::OwnedColumnarBatch>
batch(std::shared_ptr<const schema::TableSchema> schema_value, const BatchRange range) {
  const std::uint32_t first_row = range.first_row;
  const std::uint32_t rows = range.row_count;
  std::vector<columnar::OwnedColumnVector> columns;

  std::vector<std::byte> event_values;
  event_values.reserve(static_cast<std::size_t>(rows) * sizeof(std::int64_t));
  for (std::uint32_t row = 0U; row < rows; ++row)
    append_i64(event_values, static_cast<std::int64_t>(first_row + row) * 10);
  columns.push_back(columnar::test::fixed_vector(510U, type(schema::LogicalTypeKind::kTimestampNs),
                                                 false, rows, {}, 0U, std::move(event_values)));

  std::vector<std::byte> label_validity(columnar::bitmap_size(rows));
  std::vector<std::byte> label_offsets;
  std::vector<std::byte> label_values;
  label_offsets.reserve((static_cast<std::size_t>(rows) + 1U) * sizeof(std::uint32_t));
  columnar::test::append_u32(label_offsets, 0U);
  std::uint32_t label_nulls = 0U;
  for (std::uint32_t row = 0U; row < rows; ++row) {
    const std::uint32_t global = first_row + row;
    if ((global % 2U) == 0U) {
      set_bit(label_validity, row);
      label_values.push_back(static_cast<std::byte>('a' + (global % 26U)));
    } else {
      ++label_nulls;
    }
    columnar::test::append_u32(label_offsets, static_cast<std::uint32_t>(label_values.size()));
  }
  columns.push_back(
      columnar::OwnedColumnVector::create({.column_id = columnar::test::id<schema::ColumnId>(511U),
                                           .type = type(schema::LogicalTypeKind::kString),
                                           .nullable = true,
                                           .row_count = rows,
                                           .null_count = label_nulls},
                                          {.validity = std::move(label_validity),
                                           .offsets = std::move(label_offsets),
                                           .values = std::move(label_values)})
          .value());

  std::vector<std::byte> bool_validity(columnar::bitmap_size(rows));
  std::vector<std::byte> bool_values(columnar::bitmap_size(rows));
  std::uint32_t bool_nulls = 0U;
  for (std::uint32_t row = 0U; row < rows; ++row) {
    const std::uint32_t global = first_row + row;
    const bool valid = (global % 4U) != 1U;
    if (valid) {
      set_bit(bool_validity, row);
      if ((global % 3U) == 0U)
        set_bit(bool_values, row);
    } else {
      ++bool_nulls;
    }
  }
  columns.push_back(columnar::test::fixed_vector(512U, type(schema::LogicalTypeKind::kBool), true,
                                                 rows, std::move(bool_validity), bool_nulls,
                                                 std::move(bool_values)));

  std::vector<std::byte> reading_validity(columnar::bitmap_size(rows));
  std::vector<std::byte> reading_values;
  reading_values.reserve(static_cast<std::size_t>(rows) * sizeof(std::int64_t));
  std::uint32_t reading_nulls = 0U;
  for (std::uint32_t row = 0U; row < rows; ++row) {
    const std::uint32_t global = first_row + row;
    if ((global % 3U) == 1U) {
      ++reading_nulls;
      append_i64(reading_values, 0);
    } else {
      set_bit(reading_validity, row);
      append_i64(reading_values, static_cast<std::int64_t>(global) * 100 - 7);
    }
  }
  columns.push_back(columnar::test::fixed_vector(513U, type(schema::LogicalTypeKind::kInt64), true,
                                                 rows, std::move(reading_validity), reading_nulls,
                                                 std::move(reading_values)));

  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(std::move(schema_value), std::move(columns)).value());
}

[[nodiscard]] inline wal::WalId wal_id() {
  wal::WalId result{};
  result.bytes.back() = std::byte{0x5a};
  return result;
}

class HeadFixture {
public:
  struct Publication {
    BatchRange range;
    std::uint64_t record_sequence{};
  };

  explicit HeadFixture(const std::uint32_t row_capacity)
      : schemas_(lineage()),
        head_(head::MutableHead::create(schemas_.at(0U),
                                        columnar::test::id<schema::TabletId>(kTabletId), 1U,
                                        {.row_capacity = row_capacity,
                                         .variable_value_bytes = {0U, row_capacity, 0U, 0U}})
                  .value()) {}

  HeadFixture(const HeadFixture&) = delete;
  HeadFixture& operator=(const HeadFixture&) = delete;
  HeadFixture(HeadFixture&&) noexcept = default;
  HeadFixture& operator=(HeadFixture&&) noexcept = default;

  void publish(const Publication publication) {
    auto prepared = head_.prepare_append(batch(schemas_.at(0U), publication.range)).value();
    if (!prepared.mark_wal_started().is_ok())
      std::terminate();
    static_cast<void>(
        prepared.publish({.wal_id = wal_id(), .record_sequence = publication.record_sequence})
            .value());
  }

  [[nodiscard]] head::HeadSnapshot snapshot() const {
    return head_.snapshot().value();
  }

  [[nodiscard]] const schema::SchemaLineage& schemas() const noexcept {
    return schemas_;
  }

private:
  schema::SchemaLineage schemas_;
  head::MutableHead head_;
};

} // namespace chronos::query::test

#endif // CHRONOS_TESTS_QUERY_HEAD_SCAN_TEST_FIXTURE_HPP_
