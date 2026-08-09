#include "chronos/common/uuid.hpp"
#include "chronos/query/temporal_snapshot.hpp"
#include "chronos/query/value.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"

#include <array>
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
  columns.push_back(
      schema::ColumnDefinition::create(value, "value", type(schema::LogicalTypeKind::kInt64), false)
          .value());
  columns.push_back(
      schema::ColumnDefinition::create(key, "key", type(schema::LogicalTypeKind::kString), false)
          .value());
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(id<schema::TableId>(1U), id<schema::SchemaId>(2U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns),
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

[[nodiscard]] TemporalMutation mutation(const std::int64_t value, const TemporalMutationKind kind,
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
  ASSERT_TRUE((*provider)
                  ->apply_committed(1U, 1000, {mutation(10, TemporalMutationKind::kOriginal, 1U)})
                  .is_ok());
  ASSERT_TRUE((*provider)
                  ->apply_committed(2U, 2000, {mutation(20, TemporalMutationKind::kCorrection, 2U)})
                  .is_ok());

  auto before = (*provider)->resolve(schema, 1500);
  ASSERT_TRUE(before.has_value());
  EXPECT_EQ((*before)->committed_position(), 1U);
  EXPECT_EQ(visible_value(**before), 10);
  auto current = (*provider)->resolve(schema, std::nullopt);
  ASSERT_TRUE(current.has_value());
  EXPECT_EQ(visible_value(**current), 20);

  ASSERT_TRUE((*provider)
                  ->apply_committed(3U, 3000, {mutation(20, TemporalMutationKind::kTombstone, 3U)})
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
  ASSERT_TRUE((*provider)
                  ->apply_committed(1U, 1000, {mutation(10, TemporalMutationKind::kOriginal, 1U)})
                  .is_ok());
  ASSERT_TRUE((*provider)
                  ->apply_committed(2U, 2000, {mutation(20, TemporalMutationKind::kCorrection, 2U)})
                  .is_ok());
  ASSERT_TRUE((*provider)->compact_history(2U, 1500).is_ok());
  const auto expired = (*provider)->resolve(schema, 1000);
  ASSERT_FALSE(expired.has_value());
  EXPECT_EQ(expired.error().code(), common::StatusCode::kNotFound);
}

TEST(TemporalSnapshotTest, AtomicallyRestoresCanonicalCompactedHistory) {
  const auto schema = table_schema();
  auto provider = TemporalSnapshotProvider::create(schema);
  ASSERT_TRUE(provider.has_value());
  std::vector<RetainedTemporalVersion> ahead;
  ahead.push_back({.system_commit_position = 2U,
                   .system_commit_time_ns = 2000,
                   .mutation = mutation(20, TemporalMutationKind::kOriginal, 2U)});
  EXPECT_EQ((*provider)->restore_retained_history(3000, std::move(ahead)).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ((*provider)->latest_commit_position(), 0U);

  std::vector<RetainedTemporalVersion> retained;
  retained.push_back({.system_commit_position = 7U,
                      .system_commit_time_ns = 7000,
                      .mutation = mutation(70, TemporalMutationKind::kCorrection, 7U)});
  retained.push_back({.system_commit_position = 9U,
                      .system_commit_time_ns = 9000,
                      .mutation = mutation(90, TemporalMutationKind::kReplacement, 9U)});
  ASSERT_TRUE((*provider)->restore_retained_history(8000, std::move(retained)).is_ok());
  EXPECT_EQ((*provider)->latest_commit_position(), 9U);
  EXPECT_EQ((*provider)->version_count(), 2U);

  const auto expired = (*provider)->resolve(schema, 7999);
  ASSERT_FALSE(expired.has_value());
  EXPECT_EQ(expired.error().code(), common::StatusCode::kNotFound);
  const auto historical = (*provider)->resolve(schema, 8000);
  ASSERT_TRUE(historical.has_value());
  EXPECT_EQ(visible_value(**historical), 70);
  const auto current = (*provider)->resolve(schema, std::nullopt);
  ASSERT_TRUE(current.has_value());
  EXPECT_EQ(visible_value(**current), 90);

  const std::array exact{mutation(90, TemporalMutationKind::kReplacement, 9U)};
  EXPECT_TRUE((*provider)->verify_retained_commit(9U, 9000, exact).is_ok());
  const std::array changed{mutation(91, TemporalMutationKind::kReplacement, 9U)};
  EXPECT_EQ((*provider)->verify_retained_commit(9U, 9000, changed).code(),
            common::StatusCode::kCorruption);
  auto extra = mutation(92, TemporalMutationKind::kOriginal, 9U);
  extra.logical_identity = {std::byte{2U}};
  const std::array expanded{exact.front(), std::move(extra)};
  EXPECT_EQ((*provider)->verify_retained_commit(9U, 9000, expanded).code(),
            common::StatusCode::kCorruption);
  const std::array expired_mutation{mutation(999, TemporalMutationKind::kCorrection, 7U)};
  EXPECT_TRUE((*provider)->verify_retained_commit(7U, 7000, expired_mutation).is_ok());

  EXPECT_TRUE(
      (*provider)
          ->apply_committed(10U, 10000, {mutation(90, TemporalMutationKind::kTombstone, 10U)})
          .is_ok());
  const auto tombstoned = (*provider)->resolve(schema, std::nullopt);
  ASSERT_TRUE(tombstoned.has_value());
  EXPECT_TRUE((*tombstoned)->rows().empty());
}

TEST(TemporalSnapshotTest, RejectsNoncanonicalRestoreWithoutPublishingPartialHistory) {
  const auto schema = table_schema();
  auto provider = TemporalSnapshotProvider::create(schema);
  ASSERT_TRUE(provider.has_value());
  std::vector<RetainedTemporalVersion> retained;
  retained.push_back({.system_commit_position = 2U,
                      .system_commit_time_ns = 2000,
                      .mutation = mutation(20, TemporalMutationKind::kOriginal, 2U)});
  retained.push_back({.system_commit_position = 1U,
                      .system_commit_time_ns = 1000,
                      .mutation = mutation(10, TemporalMutationKind::kCorrection, 1U)});
  EXPECT_EQ((*provider)->restore_retained_history(1000, std::move(retained)).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ((*provider)->latest_commit_position(), 0U);
  EXPECT_EQ((*provider)->version_count(), 0U);
  EXPECT_TRUE((*provider)
                  ->apply_committed(1U, 1000, {mutation(10, TemporalMutationKind::kOriginal, 1U)})
                  .is_ok());
}

} // namespace
} // namespace chronos::query
