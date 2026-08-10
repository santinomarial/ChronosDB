#include "chronos/common/uuid.hpp"
#include "chronos/query/scalar_snapshot_scan.hpp"
#include "chronos/query/temporal_snapshot.hpp"
#include "chronos/query/value.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace chronos::query {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] std::shared_ptr<const schema::TableSchema> scan_schema() {
  const schema::ColumnId timestamp = id<schema::ColumnId>(3U);
  const schema::ColumnId value = id<schema::ColumnId>(4U);
  const schema::ColumnId note = id<schema::ColumnId>(5U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        timestamp, "ts", type(schema::LogicalTypeKind::kTimestampNs), false)
                        .value());
  columns.push_back(
      schema::ColumnDefinition::create(value, "value", type(schema::LogicalTypeKind::kInt64), false)
          .value());
  columns.push_back(
      schema::ColumnDefinition::create(note, "note", type(schema::LogicalTypeKind::kString), true)
          .value());
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(id<schema::TableId>(1U), id<schema::SchemaId>(2U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns),
                                  {.event_time_column = timestamp,
                                   .physical_ordering_key = {timestamp},
                                   .partition_columns = {timestamp},
                                   .shard_key = {timestamp},
                                   .deduplication_key = {timestamp}})
          .value());
}

[[nodiscard]] common::Uuid source_id() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{9U};
  return common::Uuid{bytes};
}

[[nodiscard]] TemporalMutation
mutation(const char identity, const std::int64_t event_time, const std::int64_t value,
         const std::optional<std::string>& note, const TemporalMutationKind kind,
         const std::uint64_t sequence, const std::uint32_t row_ordinal) {
  return TemporalMutation{
      .logical_identity = {std::byte{static_cast<std::uint8_t>(identity)}},
      .columns = {ScalarValue::signed_value(type(schema::LogicalTypeKind::kTimestampNs), event_time)
                      .value(),
                  ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), value).value(),
                  note.has_value()
                      ? ScalarValue::text(type(schema::LogicalTypeKind::kString), *note).value()
                      : ScalarValue::null(type(schema::LogicalTypeKind::kString))},
      .event_time_ns = event_time,
      .receive_time_ns = event_time + 1,
      .wal_id = source_id(),
      .record_sequence = sequence,
      .row_ordinal = row_ordinal,
      .kind = kind};
}

[[nodiscard]] ScalarValue cell_value(const VectorChunk& chunk, const std::size_t column,
                                     const std::size_t row) {
  const columnar::PhysicalColumnView* physical = chunk.column(column);
  EXPECT_NE(physical, nullptr);
  return ScalarValue::from_column_cell(physical->type(), chunk.cell({column, row}).value()).value();
}

TEST(ScalarSnapshotScanTest, EmitsTemporalWinnersAsCanonicalBoundedChunks) {
  const std::shared_ptr<const schema::TableSchema> schema_value = scan_schema();
  auto provider = TemporalSnapshotProvider::create(schema_value);
  ASSERT_TRUE(provider.has_value());
  std::vector<TemporalMutation> originals;
  originals.push_back(
      mutation('a', 100, 10, std::string{"old"}, TemporalMutationKind::kOriginal, 1U, 0U));
  originals.push_back(
      mutation('b', 200, 20, std::string{"keep"}, TemporalMutationKind::kOriginal, 1U, 1U));
  ASSERT_TRUE((*provider)->apply_committed(1U, 1000, std::move(originals)).is_ok());
  ASSERT_TRUE((*provider)
                  ->apply_committed(2U, 2000,
                                    {mutation('a', 100, 30, std::nullopt,
                                              TemporalMutationKind::kCorrection, 2U, 0U)})
                  .is_ok());
  ASSERT_TRUE((*provider)
                  ->apply_committed(3U, 3000,
                                    {mutation('b', 200, 20, std::string{"keep"},
                                              TemporalMutationKind::kTombstone, 3U, 0U)})
                  .is_ok());

  auto historical = (*provider)->resolve(schema_value, 2500);
  ASSERT_TRUE(historical.has_value()) << historical.error().to_string();
  auto scan = ScalarSnapshotScanOperator::create(
      *historical, {.maximum_rows_per_chunk = 1U,
                    .chunk = {.maximum_rows = 1U,
                              .maximum_columns = 3U,
                              .maximum_buffer_bytes = 4096U,
                              .maximum_retained_buffer_bytes = 4096U}});
  ASSERT_TRUE(scan.has_value()) << scan.error().to_string();
  const QueryResourceContext tiny = QueryResourceContext::create(1U).value();
  const auto rejected = (*scan)->next(tiny);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kResourceExhausted);
  const QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();

  auto first = (*scan)->next(resources);
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_EQ(first->kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& first_chunk = first->chunk()->chunk();
  EXPECT_EQ(first_chunk.physical_row_count(), 1U);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(first_chunk, 0U, 0U).storage()), 100);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(first_chunk, 1U, 0U).storage()), 30);
  EXPECT_TRUE(cell_value(first_chunk, 2U, 0U).is_null());

  auto second = (*scan)->next(resources);
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  ASSERT_EQ(second->kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& second_chunk = second->chunk()->chunk();
  EXPECT_EQ(std::get<std::int64_t>(cell_value(second_chunk, 0U, 0U).storage()), 200);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(second_chunk, 1U, 0U).storage()), 20);
  EXPECT_EQ(std::get<std::string>(cell_value(second_chunk, 2U, 0U).storage()), "keep");

  EXPECT_EQ((*scan)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ((*scan)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);

  auto current = (*provider)->resolve(schema_value, std::nullopt);
  ASSERT_TRUE(current.has_value());
  ASSERT_EQ((*current)->rows().size(), 1U);
  auto cancelled_scan = ScalarSnapshotScanOperator::create(*current);
  ASSERT_TRUE(cancelled_scan.has_value());
  const QueryResourceContext cancelled = QueryResourceContext::create(4096U).value();
  EXPECT_TRUE(cancelled.request_cancel());
  const auto cancelled_step = (*cancelled_scan)->next(cancelled);
  ASSERT_FALSE(cancelled_step.has_value());
  EXPECT_EQ(cancelled_step.error().code(), common::StatusCode::kCancelled);

  auto empty_snapshot = ScalarTableSnapshot::create(schema_value, 3U, {});
  ASSERT_TRUE(empty_snapshot.has_value());
  auto empty_scan = ScalarSnapshotScanOperator::create(
      std::make_shared<const ScalarTableSnapshot>(std::move(*empty_snapshot)));
  ASSERT_TRUE(empty_scan.has_value());
  EXPECT_EQ((*empty_scan)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ((*empty_scan)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
}

} // namespace
} // namespace chronos::query
