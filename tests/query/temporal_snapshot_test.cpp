#include "chronos/query/temporal_snapshot.hpp"

#include "chronos/common/uuid.hpp"
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

[[nodiscard]] std::shared_ptr<const schema::TableSchema> table_schema() {
  const schema::ColumnId timestamp = id<schema::ColumnId>(3U);
  const schema::ColumnId value = id<schema::ColumnId>(4U);
  const schema::ColumnId key = id<schema::ColumnId>(5U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        timestamp, "ts", type(schema::LogicalTypeKind::kTimestampNs), false)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(
                        value, "value", type(schema::LogicalTypeKind::kInt64), false)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(
                        key, "key", type(schema::LogicalTypeKind::kString), false)
                        .value());
  return std::make_shared<const schema::TableSchema>(schema::TableSchema::create(
      id<schema::TableId>(1U), id<schema::SchemaId>(2U), schema::SchemaVersion::initial(),
      std::nullopt, std::move(columns),
      {.event_time_column = timestamp,
       .physical_ordering_key = {key, timestamp},
       .partition_columns = {timestamp},
       .shard_key = {key},
       .deduplication_key = {key}})
                                                        .value());
}

[[nodiscard]] common::Uuid wal_id() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{9U};
  return common::Uuid{bytes};
}

[[nodiscard]] TemporalMutation mutation(const std::int64_t value,
                                        const TemporalMutationKind kind,
                                        const std::uint64_t sequence) {
  return TemporalMutation{
      .logical_identity = {std::byte{1U}},
      .columns =
          {ScalarValue::signed_value(type(schema::LogicalTypeKind::kTimestampNs), 100).value(),
           ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), value).value(),
           ScalarValue::text(type(schema::LogicalTypeKind::kString), "a").value()},
      .event_time_ns = 100,
      .receive_time_ns = 110,
      .wal_id = wal_id(),
      .record_sequence = sequence,
      .row_ordinal = 0U,
      .kind = kind,
  };
}

[[nodiscard]] std::int64_t visible_value(const ScalarTableSnapshot& snapshot) {
  EXPECT_EQ(snapshot.rows().size(), 1U);
  return std::get<std::int64_t>(snapshot.rows().front().columns[1].storage());
}

TEST(TemporalSnapshotTest, ResolvesOriginalCorrectionAndTombstoneAtSystemTime) {
  const auto schema = table_schema();
  auto provider = TemporalSnapshotProvider::create(schema);
  ASSERT_TRUE(provider.has_value());
  ASSERT_TRUE((*provider)->apply_committed(1U, 1000, {mutation(10, TemporalMutationKind::kOriginal, 1U)})
                  .is_ok());
  ASSERT_TRUE((*provider)->apply_committed(
                          2U, 2000, {mutation(20, TemporalMutationKind::kCorrection, 2U)})
                  .is_ok());

  auto before = (*provider)->resolve(schema, 1500);
  ASSERT_TRUE(before.has_value());
  EXPECT_EQ((*before)->committed_position(), 1U);
  EXPECT_EQ(visible_value(**before), 10);
  auto current = (*provider)->resolve(schema, std::nullopt);
  ASSERT_TRUE(current.has_value());
  EXPECT_EQ(visible_value(**current), 20);

  ASSERT_TRUE((*provider)->apply_committed(
                          3U, 3000, {mutation(20, TemporalMutationKind::kTombstone, 3U)})
                  .is_ok());
  current = (*provider)->resolve(schema, std::nullopt);
  ASSERT_TRUE(current.has_value());
  EXPECT_TRUE((*current)->rows().empty());
  before = (*provider)->resolve(schema, 2500);
  ASSERT_TRUE(before.has_value());
  EXPECT_EQ(visible_value(**before), 20);
}

TEST(TemporalSnapshotTest, RetentionFailsClosedForExpiredHistory) {
  const auto schema = table_schema();
  auto provider = TemporalSnapshotProvider::create(schema);
  ASSERT_TRUE(provider.has_value());
  ASSERT_TRUE((*provider)->apply_committed(1U, 1000, {mutation(10, TemporalMutationKind::kOriginal, 1U)})
                  .is_ok());
  ASSERT_TRUE((*provider)->apply_committed(
                          2U, 2000, {mutation(20, TemporalMutationKind::kCorrection, 2U)})
                  .is_ok());
  ASSERT_TRUE((*provider)->compact_history(2U, 1500).is_ok());
  const auto expired = (*provider)->resolve(schema, 1000);
  ASSERT_FALSE(expired.has_value());
  EXPECT_EQ(expired.error().code(), common::StatusCode::kNotFound);
}

} // namespace
} // namespace chronos::query
