#include "chronos/common/uuid.hpp"
#include "chronos/query/snapshot.hpp"
#include "chronos/query/value.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <utility>
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

[[nodiscard]] std::shared_ptr<const schema::TableSchema> schema(const bool deduplicated) {
  const schema::ColumnId timestamp = id<schema::ColumnId>(3U);
  const schema::ColumnId key = id<schema::ColumnId>(4U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        timestamp, "ts", type(schema::LogicalTypeKind::kTimestampNs), false)
                        .value());
  columns.push_back(
      schema::ColumnDefinition::create(key, "key", type(schema::LogicalTypeKind::kString), false)
          .value());
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(
          id<schema::TableId>(1U), id<schema::SchemaId>(2U), schema::SchemaVersion::initial(),
          std::nullopt, std::move(columns),
          {.event_time_column = timestamp,
           .physical_ordering_key = {key, timestamp},
           .partition_columns = {timestamp},
           .shard_key = {key},
           .deduplication_key = deduplicated ? std::vector{key} : std::vector<schema::ColumnId>{}})
          .value());
}

[[nodiscard]] ScalarInputRow row(const bool generated) {
  common::Uuid::Bytes wal_bytes{};
  wal_bytes.front() = std::byte{9U};
  return ScalarInputRow{
      .columns =
          {ScalarValue::signed_value(type(schema::LogicalTypeKind::kTimestampNs), 10).value(),
           ScalarValue::text(type(schema::LogicalTypeKind::kString), "a").value()},
      .generated_logical_identity =
          generated ? std::vector{std::byte{1U}} : std::vector<std::byte>{},
      .wal_id = common::Uuid{wal_bytes},
      .record_sequence = 7U,
      .system_commit_position = 8U,
      .row_ordinal = 2U,
  };
}

TEST(ScalarSnapshotTest, RetainsValidatedExactSchemaRowsAndCommitBoundary) {
  const auto table_schema = schema(true);
  auto snapshot = ScalarTableSnapshot::create(table_schema, 9U, {row(false)});
  ASSERT_TRUE(snapshot.has_value());
  EXPECT_EQ(snapshot->schema_ptr(), table_schema);
  EXPECT_EQ(snapshot->committed_position(), 9U);
  ASSERT_EQ(snapshot->rows().size(), 1U);
  EXPECT_EQ(snapshot->rows()[0].record_sequence, 7U);

  EXPECT_TRUE(ScalarTableSnapshot::create(schema(false), 9U, {row(true)}).has_value());
}

TEST(ScalarSnapshotTest, RejectsHostileRowsBeforeExecution) {
  const auto table_schema = schema(true);
  ScalarInputRow invalid = row(false);
  invalid.columns.pop_back();
  EXPECT_FALSE(ScalarTableSnapshot::create(table_schema, 9U, {std::move(invalid)}).has_value());

  invalid = row(false);
  invalid.columns[0] = ScalarValue::null(type(schema::LogicalTypeKind::kTimestampNs));
  EXPECT_FALSE(ScalarTableSnapshot::create(table_schema, 9U, {std::move(invalid)}).has_value());

  invalid = row(false);
  invalid.wal_id = {};
  EXPECT_FALSE(ScalarTableSnapshot::create(table_schema, 9U, {std::move(invalid)}).has_value());

  invalid = row(false);
  invalid.system_commit_position = 10U;
  EXPECT_FALSE(ScalarTableSnapshot::create(table_schema, 9U, {std::move(invalid)}).has_value());

  invalid = row(false);
  invalid.generated_logical_identity = {std::byte{1U}};
  EXPECT_FALSE(ScalarTableSnapshot::create(table_schema, 9U, {std::move(invalid)}).has_value());
}

} // namespace
} // namespace chronos::query
