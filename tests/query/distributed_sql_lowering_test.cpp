#include "chronos/common/uuid.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/distributed_sql_lowering.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace chronos::query {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] std::shared_ptr<const QueryCatalogSnapshot> catalog() {
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(id<schema::ColumnId>(3U), "ts",
                                                     type(schema::LogicalTypeKind::kTimestampNs),
                                                     false)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(id<schema::ColumnId>(4U), "label",
                                                     type(schema::LogicalTypeKind::kString), true)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(id<schema::ColumnId>(5U), "value",
                                                     type(schema::LogicalTypeKind::kInt64), false)
                        .value());
  auto table = std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(id<schema::TableId>(1U), id<schema::SchemaId>(2U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns),
                                  {.event_time_column = id<schema::ColumnId>(3U),
                                   .physical_ordering_key = {id<schema::ColumnId>(3U)},
                                   .partition_columns = {id<schema::ColumnId>(3U)},
                                   .shard_key = {id<schema::ColumnId>(3U)},
                                   .deduplication_key = {id<schema::ColumnId>(3U)}})
          .value());
  const std::vector<QueryCatalogTableInput> tables{
      {.name = "metrics", .quoted = false, .schema = std::move(table)}};
  return std::make_shared<const QueryCatalogSnapshot>(
      QueryCatalogSnapshot::create(1U, tables).value());
}

[[nodiscard]] BoundSqlSelect bind(const std::string_view sql) {
  return bind_sql_v1_select(parse_sql_v1_select(sql).value(), catalog()).value();
}

TEST(DistributedSqlLoweringTest, OwnsCanonicalProjectionPredicateOrderLimitAndSchema) {
  BoundSqlSelect select = bind("SELECT value AS v, ts AS observed, value AS v_again FROM metrics "
                               "WHERE ts > TIMESTAMP '1970-01-01 00:00:00.000000001Z' "
                               "AND TIMESTAMP '1970-01-01 00:00:00.000000009Z' >= ts "
                               "ORDER BY v DESC, observed ASC NULLS FIRST, v ASC LIMIT 7");
  auto lowered = lower_bound_sql_select_to_distributed_vector_rows(select);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  EXPECT_EQ(lowered->table_id, id<schema::TableId>(1U));
  EXPECT_EQ(lowered->destination_schema_id, id<schema::SchemaId>(2U));
  EXPECT_EQ(lowered->destination_column_ordinals, (std::vector<std::uint32_t>{2U, 0U}));
  ASSERT_TRUE(lowered->event_time_predicate.has_value());
  ASSERT_TRUE(lowered->event_time_predicate->lower.has_value());
  EXPECT_EQ(lowered->event_time_predicate->lower,
            (cseg::EventTimeBound{.value = 1, .inclusive = false}));
  ASSERT_TRUE(lowered->event_time_predicate->upper.has_value());
  EXPECT_EQ(lowered->event_time_predicate->upper,
            (cseg::EventTimeBound{.value = 9, .inclusive = true}));
  EXPECT_EQ(lowered->intent.mode, DistributedVectorPlanMode::kRows);
  EXPECT_EQ(lowered->intent.row_output_indices, (std::vector<std::uint32_t>{0U, 1U, 0U}));
  ASSERT_EQ(lowered->intent.order_keys.size(), 2U);
  EXPECT_EQ(lowered->intent.order_keys[0],
            (DistributedVectorOrderKey{.output_index = 0U,
                                       .direction = PhysicalSortDirection::kDescending,
                                       .null_placement = ScalarNullPlacement::kFirst}));
  EXPECT_EQ(lowered->intent.order_keys[1],
            (DistributedVectorOrderKey{.output_index = 1U,
                                       .direction = PhysicalSortDirection::kAscending,
                                       .null_placement = ScalarNullPlacement::kFirst}));
  EXPECT_EQ(lowered->intent.limit, 7U);
  ASSERT_EQ(lowered->result_schema.columns.size(), 3U);
  EXPECT_EQ(lowered->result_schema.columns[0].name, "v");
  EXPECT_EQ(lowered->result_schema.columns[0].type.kind(), schema::LogicalTypeKind::kInt64);
  EXPECT_FALSE(lowered->result_schema.columns[0].nullable);
  EXPECT_EQ(lowered->result_schema.columns[1].name, "observed");
  EXPECT_EQ(lowered->result_schema.columns[1].type.kind(), schema::LogicalTypeKind::kTimestampNs);
  EXPECT_EQ(lowered->result_schema.columns[2].name, "v_again");
}

TEST(DistributedSqlLoweringTest, ExpandsStarsAndNormalizesTightestOpenBounds) {
  BoundSqlSelect select = bind("SELECT * FROM metrics WHERE "
                               "TIMESTAMP '1970-01-01 00:00:00.000000001Z' < ts "
                               "AND ts >= TIMESTAMP '1970-01-01 00:00:00.000000002Z' "
                               "AND ts <= TIMESTAMP '1970-01-01 00:00:00.000000010Z' "
                               "AND ts < TIMESTAMP '1970-01-01 00:00:00.000000009Z'");
  auto lowered = lower_bound_sql_select_to_distributed_vector_rows(select);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  EXPECT_EQ(lowered->destination_column_ordinals, (std::vector<std::uint32_t>{0U, 1U, 2U}));
  EXPECT_EQ(lowered->intent.row_output_indices, (std::vector<std::uint32_t>{0U, 1U, 2U}));
  ASSERT_TRUE(lowered->event_time_predicate.has_value());
  EXPECT_EQ(lowered->event_time_predicate->lower,
            (cseg::EventTimeBound{.value = 2, .inclusive = true}));
  EXPECT_EQ(lowered->event_time_predicate->upper,
            (cseg::EventTimeBound{.value = 9, .inclusive = false}));
  EXPECT_TRUE(lowered->intent.order_keys.empty());
  EXPECT_FALSE(lowered->intent.limit.has_value());
}

TEST(DistributedSqlLoweringTest, NormalizesInclusiveBetweenWithComparisonBounds) {
  BoundSqlSelect select = bind("SELECT value FROM metrics WHERE ts BETWEEN "
                               "TIMESTAMP '1970-01-01 00:00:00.000000002Z' AND "
                               "TIMESTAMP '1970-01-01 00:00:00.000000009Z' AND "
                               "ts > TIMESTAMP '1970-01-01 00:00:00.000000003Z'");
  auto lowered = lower_bound_sql_select_to_distributed_vector_rows(select);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  ASSERT_TRUE(lowered->event_time_predicate.has_value());
  EXPECT_EQ(lowered->event_time_predicate->lower,
            (cseg::EventTimeBound{.value = 3, .inclusive = false}));
  EXPECT_EQ(lowered->event_time_predicate->upper,
            (cseg::EventTimeBound{.value = 9, .inclusive = true}));

  BoundSqlSelect reversed = bind("SELECT value FROM metrics WHERE ts BETWEEN "
                                 "TIMESTAMP '1970-01-01 00:00:00.000000009Z' AND "
                                 "TIMESTAMP '1970-01-01 00:00:00.000000002Z'");
  auto empty = lower_bound_sql_select_to_distributed_vector_rows(reversed);
  ASSERT_TRUE(empty.has_value()) << empty.error().status().to_string();
  EXPECT_EQ(empty->event_time_predicate->lower,
            (cseg::EventTimeBound{.value = 9, .inclusive = true}));
  EXPECT_EQ(empty->event_time_predicate->upper,
            (cseg::EventTimeBound{.value = 2, .inclusive = true}));
}

TEST(DistributedSqlLoweringTest, CarriesAndHidesAnUnselectedDirectOrderColumn) {
  BoundSqlSelect select = bind("SELECT label FROM metrics ORDER BY ts DESC, ts ASC");
  auto lowered = lower_bound_sql_select_to_distributed_vector_rows(select);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  EXPECT_EQ(lowered->destination_column_ordinals, (std::vector<std::uint32_t>{1U, 0U}));
  EXPECT_EQ(lowered->intent.row_output_indices, (std::vector<std::uint32_t>{0U, 1U}));
  EXPECT_EQ(lowered->intent.visible_row_output_indices, (std::vector<std::uint32_t>{0U}));
  ASSERT_EQ(lowered->intent.order_keys.size(), 1U);
  EXPECT_EQ(lowered->intent.order_keys.front().output_index, 1U);
  ASSERT_EQ(lowered->result_schema.columns.size(), 2U);
  EXPECT_EQ(lowered->result_schema.columns[0].name, "label");
  EXPECT_EQ(lowered->result_schema.columns[1].name, "ts");
}

TEST(DistributedSqlLoweringTest, RejectsEveryUnsupportedSemanticWithoutFallback) {
  const std::vector<std::string_view> statements{
      "SELECT value + 1 AS v FROM metrics",
      "SELECT value FROM metrics WHERE value > 1",
      "SELECT count(*) AS n FROM metrics",
      "SELECT value FROM metrics WHERE ts NOT BETWEEN "
      "TIMESTAMP '1970-01-01 00:00:00Z' AND TIMESTAMP '1970-01-01 00:00:01Z'",
      "SELECT value FROM metrics WHERE value BETWEEN 1 AND 2",
      "SELECT value FROM metrics LATEST BY (value) ON ts",
      "SELECT value FROM metrics FOR SYSTEM_TIME AS OF "
      "TIMESTAMP '1970-01-01 00:00:00Z'"};
  for (const std::string_view statement : statements) {
    SCOPED_TRACE(statement);
    BoundSqlSelect select = bind(statement);
    const auto lowered = lower_bound_sql_select_to_distributed_vector_rows(select);
    ASSERT_FALSE(lowered.has_value());
    EXPECT_EQ(lowered.error().code(), SqlDiagnosticCode::kUnsupportedSyntax);
    EXPECT_EQ(lowered.error().status().code(), common::StatusCode::kNotSupported);
    EXPECT_GT(lowered.error().span().byte_length, 0U);
  }
}

TEST(DistributedSqlLoweringTest, EnforcesCallerBoundsBeforePublishingAPlan) {
  BoundSqlSelect select = bind("SELECT ts, label, value FROM metrics ORDER BY ts, label");
  auto projection =
      lower_bound_sql_select_to_distributed_vector_rows(select, {.maximum_projection_columns = 2U});
  ASSERT_FALSE(projection.has_value());
  EXPECT_EQ(projection.error().code(), SqlDiagnosticCode::kResourceLimit);
  EXPECT_EQ(projection.error().status().code(), common::StatusCode::kResourceExhausted);

  auto outputs =
      lower_bound_sql_select_to_distributed_vector_rows(select, {.maximum_output_columns = 2U});
  ASSERT_FALSE(outputs.has_value());
  EXPECT_EQ(outputs.error().status().code(), common::StatusCode::kResourceExhausted);

  auto order =
      lower_bound_sql_select_to_distributed_vector_rows(select, {.maximum_order_keys = 1U});
  ASSERT_FALSE(order.has_value());
  EXPECT_EQ(order.error().status().code(), common::StatusCode::kResourceExhausted);

  auto invalid =
      lower_bound_sql_select_to_distributed_vector_rows(select, {.maximum_result_name_bytes = 0U});
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().status().code(), common::StatusCode::kInvalidArgument);

  BoundSqlSelect hidden = bind("SELECT label FROM metrics ORDER BY ts");
  auto hidden_output =
      lower_bound_sql_select_to_distributed_vector_rows(hidden, {.maximum_output_columns = 1U});
  ASSERT_FALSE(hidden_output.has_value());
  EXPECT_EQ(hidden_output.error().status().code(), common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::query
