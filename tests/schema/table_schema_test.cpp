#include "chronos/schema/table_schema.hpp"

#include "schema/schema_test_support.hpp"

#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace chronos::schema {
namespace {

static_assert(!std::is_default_constructible_v<TableSchema>);

[[nodiscard]] std::vector<ColumnDefinition> base_columns() {
  std::vector<ColumnDefinition> columns;
  columns.push_back(test::make_column(1, "ts", LogicalTypeKind::kTimestampNs, false));
  columns.push_back(test::make_column(2, "tenant", LogicalTypeKind::kSymbol, false));
  columns.push_back(test::make_column(3, "event_id", LogicalTypeKind::kUuid, false));
  columns.push_back(test::make_column(4, "value", LogicalTypeKind::kFloat64, true));
  return columns;
}

[[nodiscard]] TableSchemaRoles base_roles() {
  return TableSchemaRoles{
      .event_time_column = test::make_id<ColumnId>(1),
      .physical_ordering_key =
          {test::make_id<ColumnId>(2), test::make_id<ColumnId>(1),
           test::make_id<ColumnId>(3)},
      .partition_columns = {test::make_id<ColumnId>(1)},
      .shard_key = {test::make_id<ColumnId>(2)},
      .deduplication_key = {test::make_id<ColumnId>(2), test::make_id<ColumnId>(3)},
  };
}

[[nodiscard]] common::Result<TableSchema>
make_schema(std::vector<ColumnDefinition> columns = base_columns(),
            TableSchemaRoles roles = base_roles(), const std::uint16_t schema_id = 100,
            const std::uint64_t version = 1,
            std::optional<SchemaId> parent_schema_id = std::nullopt,
            const std::uint16_t table_id = 50) {
  return TableSchema::create(test::make_id<TableId>(table_id), test::make_id<SchemaId>(schema_id),
                             SchemaVersion::from_value(version).value(),
                             std::move(parent_schema_id), std::move(columns), std::move(roles));
}

TEST(TableSchemaTest, PreservesSchemaOrdinalOrderAndRoleOrder) {
  const TableSchema schema = make_schema().value();

  ASSERT_EQ(schema.columns().size(), 4U);
  EXPECT_EQ(schema.columns()[0].name(), "ts");
  EXPECT_EQ(schema.columns()[1].name(), "tenant");
  EXPECT_EQ(schema.columns()[2].name(), "event_id");
  EXPECT_EQ(schema.columns()[3].name(), "value");
  EXPECT_EQ(schema.physical_ordering_key()[0], test::make_id<ColumnId>(2));
  EXPECT_EQ(schema.physical_ordering_key()[1], test::make_id<ColumnId>(1));
  EXPECT_EQ(schema.physical_ordering_key()[2], test::make_id<ColumnId>(3));
  EXPECT_EQ(schema.version(), SchemaVersion::initial());
  EXPECT_FALSE(schema.parent_schema_id().has_value());
}

TEST(TableSchemaTest, SupportsDeterministicIdentityNameOrdinalAndRoleLookup) {
  const TableSchema schema = make_schema().value();

  ASSERT_NE(schema.find_column(test::make_id<ColumnId>(3)), nullptr);
  EXPECT_EQ(schema.find_column(test::make_id<ColumnId>(3))->name(), "event_id");
  ASSERT_NE(schema.find_column("value"), nullptr);
  EXPECT_EQ(schema.find_column("value")->id(), test::make_id<ColumnId>(4));
  EXPECT_EQ(schema.column_ordinal(test::make_id<ColumnId>(2)), 1U);
  EXPECT_FALSE(schema.column_ordinal(test::make_id<ColumnId>(99)).has_value());
  EXPECT_TRUE(schema.has_role(test::make_id<ColumnId>(1), ColumnRole::kEventTime));
  EXPECT_TRUE(schema.has_role(test::make_id<ColumnId>(2), ColumnRole::kPhysicalOrdering));
  EXPECT_TRUE(schema.has_role(test::make_id<ColumnId>(1), ColumnRole::kPartition));
  EXPECT_TRUE(schema.has_role(test::make_id<ColumnId>(2), ColumnRole::kShardKey));
  EXPECT_TRUE(schema.has_role(test::make_id<ColumnId>(3), ColumnRole::kDeduplicationKey));
  EXPECT_FALSE(schema.has_role(test::make_id<ColumnId>(4), ColumnRole::kShardKey));
}

TEST(TableSchemaTest, RejectsDuplicateColumnIdentityAndExactName) {
  std::vector<ColumnDefinition> duplicate_id = base_columns();
  duplicate_id[3] = test::make_column(3, "other", LogicalTypeKind::kFloat64, true);
  EXPECT_FALSE(make_schema(std::move(duplicate_id)).has_value());

  std::vector<ColumnDefinition> duplicate_name = base_columns();
  duplicate_name[3] = test::make_column(4, "tenant", LogicalTypeKind::kFloat64, true);
  EXPECT_FALSE(make_schema(std::move(duplicate_name)).has_value());

  std::vector<ColumnDefinition> byte_distinct_names = base_columns();
  byte_distinct_names[2] = test::make_column(3, "é", LogicalTypeKind::kUuid, false);
  byte_distinct_names[3] = test::make_column(4, "e\xcc\x81", LogicalTypeKind::kFloat64, true);
  EXPECT_TRUE(make_schema(std::move(byte_distinct_names)).has_value());
}

TEST(TableSchemaTest, EnforcesInitialAndSuccessorParentShape) {
  EXPECT_FALSE(make_schema(base_columns(), base_roles(), 100, 1,
                           test::make_id<SchemaId>(99))
                   .has_value());
  EXPECT_FALSE(make_schema(base_columns(), base_roles(), 100, 2).has_value());
  EXPECT_FALSE(make_schema(base_columns(), base_roles(), 100, 2,
                           test::make_id<SchemaId>(100))
                   .has_value());
  EXPECT_TRUE(make_schema(base_columns(), base_roles(), 101, 2,
                          test::make_id<SchemaId>(100))
                  .has_value());
}

TEST(TableSchemaTest, RequiresOneKnownNonNullTimestampEventColumn) {
  TableSchemaRoles missing = base_roles();
  missing.event_time_column = test::make_id<ColumnId>(99);
  EXPECT_FALSE(make_schema(base_columns(), std::move(missing)).has_value());

  std::vector<ColumnDefinition> wrong_type = base_columns();
  wrong_type[0] = test::make_column(1, "ts", LogicalTypeKind::kInt64, false);
  EXPECT_FALSE(make_schema(std::move(wrong_type)).has_value());

  std::vector<ColumnDefinition> nullable = base_columns();
  nullable[0] = test::make_column(1, "ts", LogicalTypeKind::kTimestampNs, true);
  EXPECT_FALSE(make_schema(std::move(nullable)).has_value());
}

TEST(TableSchemaTest, ValidatesAllRoleReferencesOrderAndDuplicates) {
  TableSchemaRoles empty_order = base_roles();
  empty_order.physical_ordering_key.clear();
  EXPECT_FALSE(make_schema(base_columns(), std::move(empty_order)).has_value());

  TableSchemaRoles missing_partition = base_roles();
  missing_partition.partition_columns = {test::make_id<ColumnId>(99)};
  EXPECT_FALSE(make_schema(base_columns(), std::move(missing_partition)).has_value());

  TableSchemaRoles duplicate_shard = base_roles();
  duplicate_shard.shard_key = {test::make_id<ColumnId>(2), test::make_id<ColumnId>(2)};
  EXPECT_FALSE(make_schema(base_columns(), std::move(duplicate_shard)).has_value());

  TableSchemaRoles no_event_order = base_roles();
  no_event_order.physical_ordering_key = {test::make_id<ColumnId>(2)};
  EXPECT_FALSE(make_schema(base_columns(), std::move(no_event_order)).has_value());

  TableSchemaRoles no_event_partition = base_roles();
  no_event_partition.partition_columns = {test::make_id<ColumnId>(2)};
  EXPECT_FALSE(make_schema(base_columns(), std::move(no_event_partition)).has_value());
}

TEST(TableSchemaTest, RequiresNonNullShardAndDedupKeysAndCoRoutingSubset) {
  TableSchemaRoles nullable_shard = base_roles();
  nullable_shard.shard_key = {test::make_id<ColumnId>(4)};
  nullable_shard.deduplication_key = {test::make_id<ColumnId>(4)};
  EXPECT_FALSE(make_schema(base_columns(), std::move(nullable_shard)).has_value());

  TableSchemaRoles nullable_dedup = base_roles();
  nullable_dedup.deduplication_key.push_back(test::make_id<ColumnId>(4));
  EXPECT_FALSE(make_schema(base_columns(), std::move(nullable_dedup)).has_value());

  TableSchemaRoles incompatible = base_roles();
  incompatible.shard_key = {test::make_id<ColumnId>(3)};
  incompatible.deduplication_key = {test::make_id<ColumnId>(2)};
  EXPECT_FALSE(make_schema(base_columns(), std::move(incompatible)).has_value());

  TableSchemaRoles no_dedup = base_roles();
  no_dedup.deduplication_key.clear();
  EXPECT_TRUE(make_schema(base_columns(), std::move(no_dedup)).has_value());
}

TEST(TableSchemaTest, EnforcesTheColumnarBatchV1ColumnLimit) {
  std::vector<ColumnDefinition> columns;
  columns.reserve(kMaximumSchemaColumnCount + 1U);
  columns.push_back(test::make_column(1, "ts", LogicalTypeKind::kTimestampNs, false));
  for (std::size_t index = 1; index <= kMaximumSchemaColumnCount; ++index) {
    columns.push_back(test::make_column(static_cast<std::uint16_t>(index + 1U),
                                        "column_" + std::to_string(index),
                                        LogicalTypeKind::kInt64, true));
  }
  EXPECT_FALSE(make_schema(std::move(columns)).has_value());
}

} // namespace
} // namespace chronos::schema
