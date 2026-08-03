#include "chronos/schema/schema_lineage.hpp"
#include "schema_test_support.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::schema {
namespace {

[[nodiscard]] std::vector<ColumnDefinition> initial_columns() {
  return {
      test::make_column(1, "ts", LogicalTypeKind::kTimestampNs, false),
      test::make_column(2, "tenant", LogicalTypeKind::kSymbol, false),
      test::make_column(3, "event_id", LogicalTypeKind::kUuid, false),
      test::make_column(4, "value", LogicalTypeKind::kFloat64, true),
  };
}

[[nodiscard]] TableSchemaRoles roles() {
  return TableSchemaRoles{
      .event_time_column = test::make_id<ColumnId>(1),
      .physical_ordering_key = {test::make_id<ColumnId>(2), test::make_id<ColumnId>(1),
                                test::make_id<ColumnId>(3)},
      .partition_columns = {test::make_id<ColumnId>(1)},
      .shard_key = {test::make_id<ColumnId>(2)},
      .deduplication_key = {test::make_id<ColumnId>(2), test::make_id<ColumnId>(3)},
  };
}

[[nodiscard]] TableSchema schema(const std::uint16_t schema_id, const std::uint64_t version,
                                 std::optional<SchemaId> parent,
                                 std::vector<ColumnDefinition> columns = initial_columns(),
                                 TableSchemaRoles schema_roles = roles(),
                                 const std::uint16_t table_id = 50) {
  return TableSchema::create(test::make_id<TableId>(table_id), test::make_id<SchemaId>(schema_id),
                             SchemaVersion::from_value(version).value(), parent, std::move(columns),
                             std::move(schema_roles))
      .value();
}

[[nodiscard]] TableSchema successor(const TableSchema& predecessor, const std::uint16_t schema_id,
                                    std::vector<ColumnDefinition> columns,
                                    TableSchemaRoles schema_roles = roles()) {
  return schema(schema_id, predecessor.version().value() + 1U, predecessor.schema_id(),
                std::move(columns), std::move(schema_roles));
}

[[nodiscard]] std::vector<ColumnDefinition> copy_columns(const TableSchema& source) {
  return {source.columns().begin(), source.columns().end()};
}

TEST(SchemaSuccessorTest, AcceptsRenameAndNullableTailAdditionTogether) {
  const TableSchema before = schema(100, 1, std::nullopt);
  std::vector<ColumnDefinition> columns = initial_columns();
  columns[3] = test::make_column(4, "measurement", LogicalTypeKind::kFloat64, true);
  columns.push_back(test::make_column(5, "region", LogicalTypeKind::kSymbol, true));
  const TableSchema after = successor(before, 101, std::move(columns));

  EXPECT_TRUE(validate_v1_successor(before, after).is_ok());
}

TEST(SchemaSuccessorTest, RejectsImmediateHistoricalNameReassignment) {
  const TableSchema before = schema(100, 1, std::nullopt);
  std::vector<ColumnDefinition> columns = initial_columns();
  columns[3] = test::make_column(4, "measurement", LogicalTypeKind::kFloat64, true);
  columns.push_back(test::make_column(5, "value", LogicalTypeKind::kString, true));
  const TableSchema after = successor(before, 101, std::move(columns));

  EXPECT_FALSE(validate_v1_successor(before, after).is_ok());
}

TEST(SchemaSuccessorTest, RejectsTableParentAndVersionDiscontinuity) {
  const TableSchema before = schema(100, 1, std::nullopt);
  const TableSchema wrong_table =
      schema(101, 2, before.schema_id(), initial_columns(), roles(), 51);
  EXPECT_FALSE(validate_v1_successor(before, wrong_table).is_ok());

  const TableSchema wrong_parent = schema(102, 2, test::make_id<SchemaId>(99), initial_columns());
  EXPECT_FALSE(validate_v1_successor(before, wrong_parent).is_ok());

  const TableSchema wrong_version = schema(103, 3, before.schema_id(), initial_columns());
  EXPECT_FALSE(validate_v1_successor(before, wrong_version).is_ok());
}

TEST(SchemaSuccessorTest, RejectsDropReorderIdentityTypeAndNullabilityChanges) {
  const TableSchema before = schema(100, 1, std::nullopt);

  std::vector<ColumnDefinition> dropped = initial_columns();
  dropped.pop_back();
  EXPECT_FALSE(validate_v1_successor(before, successor(before, 101, std::move(dropped))).is_ok());

  std::vector<ColumnDefinition> reordered = initial_columns();
  std::swap(reordered[1], reordered[2]);
  EXPECT_FALSE(validate_v1_successor(before, successor(before, 102, std::move(reordered))).is_ok());

  std::vector<ColumnDefinition> replaced = initial_columns();
  replaced[3] = test::make_column(9, "value", LogicalTypeKind::kFloat64, true);
  EXPECT_FALSE(validate_v1_successor(before, successor(before, 103, std::move(replaced))).is_ok());

  std::vector<ColumnDefinition> changed_type = initial_columns();
  changed_type[3] = test::make_column(4, "value", LogicalTypeKind::kInt64, true);
  EXPECT_FALSE(
      validate_v1_successor(before, successor(before, 104, std::move(changed_type))).is_ok());

  std::vector<ColumnDefinition> changed_nullability = initial_columns();
  changed_nullability[3] = test::make_column(4, "value", LogicalTypeKind::kFloat64, false);
  EXPECT_FALSE(validate_v1_successor(before, successor(before, 105, std::move(changed_nullability)))
                   .is_ok());
}

TEST(SchemaSuccessorTest, RejectsDecimalParameterChanges) {
  std::vector<ColumnDefinition> before_columns = initial_columns();
  before_columns[3] = ColumnDefinition::create(test::make_id<ColumnId>(4), "value",
                                               LogicalType::decimal(20, 8).value(), true)
                          .value();
  const TableSchema before = schema(100, 1, std::nullopt, before_columns);

  std::vector<ColumnDefinition> after_columns = std::move(before_columns);
  after_columns[3] = ColumnDefinition::create(test::make_id<ColumnId>(4), "value",
                                              LogicalType::decimal(20, 9).value(), true)
                         .value();
  EXPECT_FALSE(
      validate_v1_successor(before, successor(before, 101, std::move(after_columns))).is_ok());
}

TEST(SchemaSuccessorTest, RejectsNewNonNullColumnsAndEveryRoleOrKeyChange) {
  const TableSchema before = schema(100, 1, std::nullopt);
  std::vector<ColumnDefinition> nonnull = initial_columns();
  nonnull.push_back(test::make_column(5, "region", LogicalTypeKind::kSymbol, false));
  EXPECT_FALSE(validate_v1_successor(before, successor(before, 101, std::move(nonnull))).is_ok());

  TableSchemaRoles changed_order = roles();
  std::swap(changed_order.physical_ordering_key[0], changed_order.physical_ordering_key[1]);
  EXPECT_FALSE(validate_v1_successor(
                   before, successor(before, 102, initial_columns(), std::move(changed_order)))
                   .is_ok());

  TableSchemaRoles changed_partition = roles();
  changed_partition.partition_columns.push_back(test::make_id<ColumnId>(2));
  EXPECT_FALSE(validate_v1_successor(
                   before, successor(before, 103, initial_columns(), std::move(changed_partition)))
                   .is_ok());

  TableSchemaRoles changed_shard = roles();
  changed_shard.shard_key.push_back(test::make_id<ColumnId>(3));
  EXPECT_FALSE(validate_v1_successor(
                   before, successor(before, 104, initial_columns(), std::move(changed_shard)))
                   .is_ok());

  TableSchemaRoles changed_dedup = roles();
  changed_dedup.deduplication_key.clear();
  EXPECT_FALSE(validate_v1_successor(
                   before, successor(before, 105, initial_columns(), std::move(changed_dedup)))
                   .is_ok());

  std::vector<ColumnDefinition> two_timestamps = initial_columns();
  two_timestamps[3] = test::make_column(4, "alternate_ts", LogicalTypeKind::kTimestampNs, false);
  const TableSchema event_before = schema(200, 1, std::nullopt, two_timestamps);
  TableSchemaRoles changed_event = roles();
  changed_event.event_time_column = test::make_id<ColumnId>(4);
  changed_event.physical_ordering_key = {test::make_id<ColumnId>(2), test::make_id<ColumnId>(4),
                                         test::make_id<ColumnId>(3)};
  changed_event.partition_columns = {test::make_id<ColumnId>(4)};
  EXPECT_FALSE(
      validate_v1_successor(event_before, successor(event_before, 201, std::move(two_timestamps),
                                                    std::move(changed_event)))
          .is_ok());
}

TEST(SchemaLineageTest, IsLinearAndLeavesStateUntouchedOnRejectedBranch) {
  SchemaLineage lineage = SchemaLineage::create(schema(100, 1, std::nullopt)).value();
  const std::shared_ptr<const TableSchema> pinned_initial = lineage.current();

  std::vector<ColumnDefinition> second_columns = initial_columns();
  second_columns.push_back(test::make_column(5, "region", LogicalTypeKind::kSymbol, true));
  ASSERT_TRUE(lineage.append(successor(*pinned_initial, 101, std::move(second_columns))).is_ok());
  EXPECT_EQ(lineage.size(), 2U);
  EXPECT_EQ(pinned_initial->schema_id(), test::make_id<SchemaId>(100));

  const TableSchema branch = schema(102, 2, pinned_initial->schema_id(), initial_columns());
  const common::Status branch_status = lineage.append(branch);
  EXPECT_FALSE(branch_status.is_ok());
  EXPECT_EQ(lineage.size(), 2U);
  EXPECT_EQ(lineage.current()->schema_id(), test::make_id<SchemaId>(101));
}

TEST(SchemaLineageTest, RejectsHistoricalSchemaAndNameReuseButAllowsSameOwnerRenameBack) {
  SchemaLineage lineage = SchemaLineage::create(schema(100, 1, std::nullopt)).value();

  std::vector<ColumnDefinition> renamed = initial_columns();
  renamed[3] = test::make_column(4, "measurement", LogicalTypeKind::kFloat64, true);
  ASSERT_TRUE(lineage.append(successor(*lineage.current(), 101, std::move(renamed))).is_ok());

  std::vector<ColumnDefinition> reassigned = copy_columns(*lineage.current());
  reassigned.push_back(test::make_column(5, "value", LogicalTypeKind::kString, true));
  const common::Status name_reuse =
      lineage.append(successor(*lineage.current(), 102, std::move(reassigned)));
  EXPECT_FALSE(name_reuse.is_ok());
  EXPECT_EQ(lineage.size(), 2U);

  std::vector<ColumnDefinition> renamed_back = copy_columns(*lineage.current());
  renamed_back[3] = test::make_column(4, "value", LogicalTypeKind::kFloat64, true);
  ASSERT_TRUE(lineage.append(successor(*lineage.current(), 103, std::move(renamed_back))).is_ok());
  EXPECT_EQ(lineage.historical_column_id("value"), test::make_id<ColumnId>(4));

  const TableSchema reused_schema_id =
      schema(100, 4, lineage.current()->schema_id(), copy_columns(*lineage.current()));
  EXPECT_FALSE(lineage.append(reused_schema_id).is_ok());
}

TEST(SchemaLineageTest, BuildsExactAncestorToDescendantProjectionMetadata) {
  SchemaLineage lineage = SchemaLineage::create(schema(100, 1, std::nullopt)).value();
  std::vector<ColumnDefinition> second = initial_columns();
  second[3] = test::make_column(4, "measurement", LogicalTypeKind::kFloat64, true);
  second.push_back(test::make_column(5, "region", LogicalTypeKind::kSymbol, true));
  ASSERT_TRUE(lineage.append(successor(*lineage.current(), 101, std::move(second))).is_ok());

  std::vector<ColumnDefinition> third = copy_columns(*lineage.current());
  third[1] = test::make_column(2, "tenant_name", LogicalTypeKind::kSymbol, false);
  third.push_back(test::make_column(6, "note", LogicalTypeKind::kString, true));
  ASSERT_TRUE(lineage.append(successor(*lineage.current(), 102, std::move(third))).is_ok());

  const common::Result<SchemaProjection> projection =
      lineage.projection({.ancestor_schema_id = test::make_id<SchemaId>(100),
                          .descendant_schema_id = test::make_id<SchemaId>(102)});
  ASSERT_TRUE(projection.has_value());
  ASSERT_EQ(projection->entries().size(), 6U);
  for (std::size_t index = 0; index < 4U; ++index) {
    EXPECT_EQ(projection->entries()[index].ancestor_ordinal(), index);
    EXPECT_FALSE(projection->entries()[index].synthesizes_null());
  }
  for (std::size_t index = 4; index < 6U; ++index) {
    EXPECT_FALSE(projection->entries()[index].ancestor_ordinal().has_value());
    EXPECT_TRUE(projection->entries()[index].synthesizes_null());
  }
  EXPECT_EQ(projection->entries()[4].descendant_column_id(), test::make_id<ColumnId>(5));
  EXPECT_EQ(projection->entries()[5].descendant_column_id(), test::make_id<ColumnId>(6));

  EXPECT_FALSE(lineage
                   .projection({.ancestor_schema_id = test::make_id<SchemaId>(102),
                                .descendant_schema_id = test::make_id<SchemaId>(100)})
                   .has_value());
  const common::Result<SchemaProjection> missing =
      lineage.projection({.ancestor_schema_id = test::make_id<SchemaId>(99),
                          .descendant_schema_id = test::make_id<SchemaId>(102)});
  ASSERT_FALSE(missing.has_value());
  EXPECT_EQ(missing.error().code(), common::StatusCode::kNotFound);
}

TEST(SchemaLineageTest, DeterministicallyProjectsEveryGeneratedAncestorPrefix) {
  SchemaLineage lineage = SchemaLineage::create(schema(100, 1, std::nullopt)).value();
  constexpr std::size_t kSuccessorCount = 32;
  for (std::size_t step = 1; step <= kSuccessorCount; ++step) {
    std::vector<ColumnDefinition> columns = copy_columns(*lineage.current());
    const auto id = static_cast<std::uint16_t>(4U + step);
    columns.push_back(
        test::make_column(id, "added_" + std::to_string(step), LogicalTypeKind::kString, true));
    const auto schema_id = static_cast<std::uint16_t>(100U + step);
    ASSERT_TRUE(
        lineage.append(successor(*lineage.current(), schema_id, std::move(columns))).is_ok());
  }

  const SchemaId descendant = lineage.current()->schema_id();
  for (std::size_t ancestor_index = 0; ancestor_index < lineage.size(); ++ancestor_index) {
    const std::shared_ptr<const TableSchema> ancestor = lineage.at(ancestor_index);
    const common::Result<SchemaProjection> projection = lineage.projection(
        {.ancestor_schema_id = ancestor->schema_id(), .descendant_schema_id = descendant});
    ASSERT_TRUE(projection.has_value());
    ASSERT_EQ(projection->entries().size(), lineage.current()->columns().size());
    for (std::size_t column = 0; column < projection->entries().size(); ++column) {
      EXPECT_EQ(projection->entries()[column].ancestor_ordinal().has_value(),
                column < ancestor->columns().size());
    }
  }
}

} // namespace
} // namespace chronos::schema
