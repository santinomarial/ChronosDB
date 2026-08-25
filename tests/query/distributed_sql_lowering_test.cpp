#include "chronos/common/byte_reader.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/distributed_sql_lowering.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
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

TEST(DistributedSqlLoweringTest, OwnsSourceIndependentOutputsAndARealRowAnchor) {
  BoundSqlSelect mixed =
      bind("SELECT 7 AS seven, upper('ok') AS word, label AS source_label FROM metrics "
           "ORDER BY ts DESC LIMIT 2");
  auto lowered = lower_bound_sql_select_to_distributed_vector_rows(mixed);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  EXPECT_EQ(lowered->destination_column_ordinals, (std::vector<std::uint32_t>{1U, 0U}));
  EXPECT_EQ(lowered->intent.row_output_indices, (std::vector<std::uint32_t>{0U, 1U}));
  EXPECT_TRUE(lowered->intent.visible_row_output_indices.empty());
  ASSERT_EQ(lowered->intent.order_keys.size(), 1U);
  EXPECT_EQ(lowered->intent.order_keys.front().output_index, 1U);
  ASSERT_TRUE(lowered->coordinator_projection.has_value());
  const auto& projection = *lowered->coordinator_projection;
  ASSERT_EQ(projection.outputs.size(), 3U);
  const auto& seven = std::get<DistributedVectorRowConstantOutput>(projection.outputs[0]);
  common::ByteReader seven_reader{seven.canonical_value};
  EXPECT_FALSE(seven.is_null);
  EXPECT_EQ(seven_reader.read_i64_le(), 7);
  const auto& word = std::get<DistributedVectorRowConstantOutput>(projection.outputs[1]);
  EXPECT_EQ(word.canonical_value, (std::vector<std::byte>{std::byte{0x4f}, std::byte{0x4b}}));
  EXPECT_EQ(std::get<DistributedVectorRowSourceOutput>(projection.outputs[2]).worker_output_index,
            0U);
  ASSERT_EQ(projection.result_schema.columns.size(), 3U);
  EXPECT_EQ(projection.result_schema.columns[0].name, "seven");
  EXPECT_EQ(projection.result_schema.columns[1].name, "word");
  EXPECT_EQ(projection.result_schema.columns[2].name, "source_label");

  BoundSqlSelect constants =
      bind("SELECT CAST(42 AS INT32) AS answer, CAST(NULL AS STRING) AS missing FROM metrics");
  auto anchored = lower_bound_sql_select_to_distributed_vector_rows(constants);
  ASSERT_TRUE(anchored.has_value()) << anchored.error().status().to_string();
  EXPECT_EQ(anchored->destination_column_ordinals, (std::vector<std::uint32_t>{0U}));
  EXPECT_EQ(anchored->intent.row_output_indices, (std::vector<std::uint32_t>{0U}));
  ASSERT_EQ(anchored->result_schema.columns.size(), 1U);
  EXPECT_EQ(anchored->result_schema.columns.front().name, "ts");
  ASSERT_TRUE(anchored->coordinator_projection.has_value());
  const auto& missing =
      std::get<DistributedVectorRowConstantOutput>(anchored->coordinator_projection->outputs[1]);
  EXPECT_TRUE(missing.is_null);
  EXPECT_TRUE(missing.canonical_value.empty());
}

TEST(DistributedSqlLoweringTest, OwnsCheckedRowDependentCoordinatorExpressions) {
  BoundSqlSelect select = bind(
      "SELECT value + 2 AS shifted, lower(label) AS folded, value > 4 AS high, ts FROM metrics "
      "ORDER BY ts DESC LIMIT 2");
  auto lowered = lower_bound_sql_select_to_distributed_vector_rows(select);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  EXPECT_EQ(lowered->destination_column_ordinals, (std::vector<std::uint32_t>{0U, 1U, 2U}));
  EXPECT_EQ(lowered->intent.row_output_indices, (std::vector<std::uint32_t>{0U, 1U, 2U}));
  EXPECT_TRUE(lowered->intent.visible_row_output_indices.empty());
  ASSERT_EQ(lowered->intent.order_keys.size(), 1U);
  EXPECT_EQ(lowered->intent.order_keys.front().output_index, 0U);
  ASSERT_EQ(lowered->result_schema.columns.size(), 3U);
  EXPECT_EQ(lowered->result_schema.columns[1].name, "label");
  ASSERT_TRUE(lowered->coordinator_projection.has_value());
  const auto& projection = *lowered->coordinator_projection;
  ASSERT_EQ(projection.outputs.size(), 4U);
  const auto& shifted =
      std::get<DistributedVectorRowExpressionOutput>(projection.outputs[0]).expression;
  EXPECT_EQ(shifted.result_shape().type.kind(), schema::LogicalTypeKind::kInt64);
  EXPECT_FALSE(shifted.result_shape().nullable);
  const auto& folded =
      std::get<DistributedVectorRowExpressionOutput>(projection.outputs[1]).expression;
  EXPECT_EQ(folded.result_shape().type.kind(), schema::LogicalTypeKind::kString);
  EXPECT_TRUE(folded.result_shape().nullable);
  const auto& high =
      std::get<DistributedVectorRowExpressionOutput>(projection.outputs[2]).expression;
  EXPECT_EQ(high.result_shape().type.kind(), schema::LogicalTypeKind::kBool);
  EXPECT_EQ(std::get<DistributedVectorRowSourceOutput>(projection.outputs[3]).worker_output_index,
            0U);
  EXPECT_EQ(projection.result_schema.columns[0].name, "shifted");
  EXPECT_EQ(projection.result_schema.columns[1].name, "folded");

  const auto computed_order = lower_bound_sql_select_to_distributed_vector_rows(
      bind("SELECT value + 2 AS shifted FROM metrics ORDER BY shifted"));
  ASSERT_TRUE(computed_order.has_value()) << computed_order.error().status().to_string();
  EXPECT_EQ(computed_order->destination_column_ordinals, (std::vector<std::uint32_t>{0U, 1U, 2U}));
  EXPECT_TRUE(computed_order->intent.order_keys.empty());
  ASSERT_TRUE(computed_order->coordinator_projection.has_value());
  ASSERT_EQ(computed_order->coordinator_projection->order_keys.size(), 1U);
  EXPECT_EQ(computed_order->coordinator_projection->order_keys.front()
                .expression.result_shape()
                .type.kind(),
            schema::LogicalTypeKind::kInt64);

  const auto mixed_order = lower_bound_sql_select_to_distributed_vector_rows(
      bind("SELECT label FROM metrics ORDER BY lower(label) ASC NULLS FIRST, value + 1 DESC"));
  ASSERT_TRUE(mixed_order.has_value()) << mixed_order.error().status().to_string();
  EXPECT_TRUE(mixed_order->intent.order_keys.empty());
  ASSERT_TRUE(mixed_order->coordinator_projection.has_value());
  ASSERT_EQ(mixed_order->coordinator_projection->order_keys.size(), 2U);
  EXPECT_EQ(mixed_order->coordinator_projection->order_keys[0].null_placement,
            ScalarNullPlacement::kFirst);
  EXPECT_EQ(mixed_order->coordinator_projection->order_keys[1].direction,
            PhysicalSortDirection::kDescending);

  const auto constant_order = lower_bound_sql_select_to_distributed_vector_rows(
      bind("SELECT value FROM metrics ORDER BY lower('constant')"));
  ASSERT_TRUE(constant_order.has_value()) << constant_order.error().status().to_string();
  EXPECT_TRUE(constant_order->intent.order_keys.empty());
  EXPECT_FALSE(constant_order->coordinator_projection.has_value());

  const auto bounded_order = lower_bound_sql_select_to_distributed_vector_rows(
      bind("SELECT value FROM metrics ORDER BY value + 1"),
      {.maximum_expression_configuration_bytes = 1U});
  ASSERT_FALSE(bounded_order.has_value());
  EXPECT_EQ(bounded_order.error().code(), SqlDiagnosticCode::kResourceLimit);

  const auto bounded = lower_bound_sql_select_to_distributed_vector_rows(
      select, {.maximum_expression_configuration_bytes = 1U});
  ASSERT_FALSE(bounded.has_value());
  EXPECT_EQ(bounded.error().code(), SqlDiagnosticCode::kResourceLimit);
}

TEST(DistributedSqlLoweringTest, OwnsGeneralBooleanCoordinatorPredicates) {
  BoundSqlSelect direct_select = bind("SELECT value FROM metrics");
  const SqlExpression* direct_expression = direct_select.syntax().items().front().expression();
  ASSERT_NE(direct_expression, nullptr);
  auto direct_program = lower_bound_sql_scalar_expression(direct_select, *direct_expression);
  ASSERT_TRUE(direct_program.has_value()) << direct_program.error().status().to_string();
  ASSERT_EQ(direct_program->instructions().size(), 1U);
  EXPECT_EQ(
      std::get<VectorInputExpression>(direct_program->instructions().front()).input_column_ordinal,
      2U);

  BoundSqlSelect select =
      bind("SELECT value, label FROM metrics WHERE (value + 1 > 5 AND lower(label) = 'ok') "
           "OR label IS NULL ORDER BY value DESC LIMIT 2");
  auto lowered = lower_bound_sql_select_to_distributed_vector_rows(select);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  EXPECT_EQ(lowered->destination_column_ordinals, (std::vector<std::uint32_t>{0U, 1U, 2U}));
  EXPECT_EQ(lowered->intent.row_output_indices, (std::vector<std::uint32_t>{0U, 1U, 2U}));
  EXPECT_FALSE(lowered->event_time_predicate.has_value());
  ASSERT_TRUE(lowered->coordinator_projection.has_value());
  ASSERT_TRUE(lowered->coordinator_projection->predicate.has_value());
  EXPECT_EQ(lowered->coordinator_projection->predicate->result_shape().type.kind(),
            schema::LogicalTypeKind::kBool);
  EXPECT_TRUE(lowered->coordinator_projection->predicate->result_shape().nullable);
  ASSERT_EQ(lowered->intent.order_keys.size(), 1U);
  EXPECT_EQ(lowered->intent.order_keys.front().output_index, 2U);

  auto constant = lower_bound_sql_select_to_distributed_vector_rows(
      bind("SELECT label FROM metrics WHERE FALSE"));
  ASSERT_TRUE(constant.has_value()) << constant.error().status().to_string();
  EXPECT_EQ(constant->destination_column_ordinals, (std::vector<std::uint32_t>{1U}));
  ASSERT_TRUE(constant->coordinator_projection.has_value());
  ASSERT_TRUE(constant->coordinator_projection->predicate.has_value());
  EXPECT_EQ(constant->coordinator_projection->predicate->instructions().size(), 1U);

  auto disjoint_time = lower_bound_sql_select_to_distributed_vector_rows(
      bind("SELECT value FROM metrics WHERE ts NOT BETWEEN "
           "TIMESTAMP '1970-01-01 00:00:00Z' AND TIMESTAMP '1970-01-01 00:00:01Z'"));
  ASSERT_TRUE(disjoint_time.has_value()) << disjoint_time.error().status().to_string();
  EXPECT_FALSE(disjoint_time->event_time_predicate.has_value());
  ASSERT_TRUE(disjoint_time->coordinator_projection.has_value());
  EXPECT_TRUE(disjoint_time->coordinator_projection->predicate.has_value());

  const auto bounded = lower_bound_sql_select_to_distributed_vector_rows(
      select, {.maximum_expression_configuration_bytes = 1U});
  ASSERT_FALSE(bounded.has_value());
  EXPECT_EQ(bounded.error().code(), SqlDiagnosticCode::kResourceLimit);
}

TEST(DistributedSqlLoweringTest, RejectsEveryUnsupportedSemanticWithoutFallback) {
  const std::vector<std::string_view> statements{
      "SELECT count(*) AS n FROM metrics", "SELECT value FROM metrics LATEST BY (value) ON ts",
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

  BoundSqlSelect constant = bind("SELECT 'too large' AS value FROM metrics");
  auto constant_bytes =
      lower_bound_sql_select_to_distributed_vector_rows(constant, {.maximum_constant_bytes = 2U});
  ASSERT_FALSE(constant_bytes.has_value());
  EXPECT_EQ(constant_bytes.error().status().code(), common::StatusCode::kResourceExhausted);
}

TEST(DistributedSqlLoweringTest, OwnsCanonicalGlobalAggregateProjectionPredicateAndSchema) {
  BoundSqlSelect select =
      bind("SELECT count(*) AS n, count(value) AS present, sum(value) AS total, "
           "avg(value) AS mean_value, min(label) AS minimum_label, "
           "max(label) AS maximum_label, var_pop(value) AS population_variance, "
           "var_samp(value) AS sample_variance, "
           "sum(value) AS total_again FROM metrics WHERE ts BETWEEN "
           "TIMESTAMP '1970-01-01 00:00:00.000000002Z' AND "
           "TIMESTAMP '1970-01-01 00:00:00.000000009Z' LIMIT 1");
  auto lowered = lower_bound_sql_select_to_distributed_vector_aggregate(select);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  EXPECT_EQ(lowered->input_rows.table_id, id<schema::TableId>(1U));
  EXPECT_EQ(lowered->input_rows.destination_schema_id, id<schema::SchemaId>(2U));
  EXPECT_EQ(lowered->input_rows.destination_column_ordinals, (std::vector<std::uint32_t>{2U, 1U}));
  ASSERT_TRUE(lowered->input_rows.event_time_predicate.has_value());
  EXPECT_EQ(lowered->input_rows.event_time_predicate->lower,
            (cseg::EventTimeBound{.value = 2, .inclusive = true}));
  EXPECT_EQ(lowered->input_rows.event_time_predicate->upper,
            (cseg::EventTimeBound{.value = 9, .inclusive = true}));
  EXPECT_EQ(lowered->input_rows.intent.mode, DistributedVectorPlanMode::kRows);
  EXPECT_EQ(lowered->input_rows.intent.row_output_indices, (std::vector<std::uint32_t>{0U, 1U}));
  EXPECT_TRUE(lowered->input_rows.intent.visible_row_output_indices.empty());
  EXPECT_TRUE(lowered->input_rows.intent.order_keys.empty());
  EXPECT_FALSE(lowered->input_rows.intent.limit.has_value());
  ASSERT_EQ(lowered->input_rows.result_schema.columns.size(), 2U);
  EXPECT_EQ(lowered->input_rows.result_schema.columns[0].name, "value");
  EXPECT_EQ(lowered->input_rows.result_schema.columns[1].name, "label");
  EXPECT_EQ(lowered->intent.mode, DistributedVectorPlanMode::kUngroupedAggregate);
  EXPECT_EQ(lowered->intent.limit, 1U);
  EXPECT_TRUE(lowered->intent.row_output_indices.empty());
  EXPECT_TRUE(lowered->intent.order_keys.empty());
  ASSERT_EQ(lowered->intent.aggregates.size(), 9U);
  EXPECT_EQ(lowered->intent.aggregates[0],
            (DistributedVectorAggregateIntent{.operation = VectorAggregateOperation::kCountStar,
                                              .input_index = std::nullopt}));
  EXPECT_EQ(lowered->intent.aggregates[1],
            (DistributedVectorAggregateIntent{.operation = VectorAggregateOperation::kCount,
                                              .input_index = 0U}));
  EXPECT_EQ(lowered->intent.aggregates[2],
            (DistributedVectorAggregateIntent{.operation = VectorAggregateOperation::kSum,
                                              .input_index = 0U}));
  EXPECT_EQ(lowered->intent.aggregates[3],
            (DistributedVectorAggregateIntent{.operation = VectorAggregateOperation::kAverage,
                                              .input_index = 0U}));
  EXPECT_EQ(lowered->intent.aggregates[4],
            (DistributedVectorAggregateIntent{.operation = VectorAggregateOperation::kMinimum,
                                              .input_index = 1U}));
  EXPECT_EQ(lowered->intent.aggregates[5],
            (DistributedVectorAggregateIntent{.operation = VectorAggregateOperation::kMaximum,
                                              .input_index = 1U}));
  EXPECT_EQ(lowered->intent.aggregates[6],
            (DistributedVectorAggregateIntent{
                .operation = VectorAggregateOperation::kVariancePopulation, .input_index = 0U}));
  EXPECT_EQ(lowered->intent.aggregates[7],
            (DistributedVectorAggregateIntent{
                .operation = VectorAggregateOperation::kVarianceSample, .input_index = 0U}));
  EXPECT_EQ(lowered->intent.aggregates[8], lowered->intent.aggregates[2]);
  ASSERT_EQ(lowered->result_schema.columns.size(), 9U);
  EXPECT_EQ(lowered->result_schema.columns[0].name, "n");
  EXPECT_EQ(lowered->result_schema.columns[1].name, "present");
  EXPECT_EQ(lowered->result_schema.columns[2].name, "total");
  EXPECT_EQ(lowered->result_schema.columns[3].name, "mean_value");
  EXPECT_EQ(lowered->result_schema.columns[4].name, "minimum_label");
  EXPECT_EQ(lowered->result_schema.columns[5].name, "maximum_label");
  EXPECT_EQ(lowered->result_schema.columns[6].name, "population_variance");
  EXPECT_EQ(lowered->result_schema.columns[7].name, "sample_variance");
  EXPECT_EQ(lowered->result_schema.columns[8].name, "total_again");
}

TEST(DistributedSqlLoweringTest, AnchorsCountStarAndRejectsUnsupportedAggregateSemantics) {
  BoundSqlSelect count = bind("SELECT count(*) AS n FROM metrics ORDER BY n DESC LIMIT 0");
  auto lowered = lower_bound_sql_select_to_distributed_vector_aggregate(count);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  EXPECT_EQ(lowered->input_rows.destination_column_ordinals, (std::vector<std::uint32_t>{0U}));
  EXPECT_EQ(lowered->input_rows.intent.row_output_indices, (std::vector<std::uint32_t>{0U}));
  ASSERT_EQ(lowered->input_rows.result_schema.columns.size(), 1U);
  EXPECT_EQ(lowered->input_rows.result_schema.columns.front().name, "ts");
  EXPECT_FALSE(lowered->input_rows.intent.limit.has_value());
  EXPECT_EQ(lowered->intent.limit, 0U);
  EXPECT_TRUE(lowered->intent.order_keys.empty());

  const std::vector<std::string_view> statements{
      "SELECT value FROM metrics", "SELECT sum(value + 1) FROM metrics",
      "SELECT sum(value) FROM metrics GROUP BY label",
      "SELECT sum(value) AS total FROM metrics ORDER BY avg(value)",
      // NOLINTNEXTLINE(bugprone-suspicious-missing-comma)
      "SELECT sum(value) FROM metrics FOR SYSTEM_TIME AS OF "
      "TIMESTAMP '1970-01-01 00:00:00Z'"};
  for (const std::string_view statement : statements) {
    SCOPED_TRACE(statement);
    BoundSqlSelect unsupported_select = bind(statement);
    const auto unsupported_plan =
        lower_bound_sql_select_to_distributed_vector_aggregate(unsupported_select);
    ASSERT_FALSE(unsupported_plan.has_value());
    EXPECT_EQ(unsupported_plan.error().code(), SqlDiagnosticCode::kUnsupportedSyntax);
    EXPECT_EQ(unsupported_plan.error().status().code(), common::StatusCode::kNotSupported);
  }
}

TEST(DistributedSqlLoweringTest, LowersCheckedFinalGlobalAggregateExpressions) {
  BoundSqlSelect select =
      bind("SELECT sum(value) + 1 AS shifted, coalesce(min(label), 'none') AS minimum_label, "
           "count(*) * 2 AS doubled FROM metrics ORDER BY shifted DESC LIMIT 1");
  auto lowered = lower_bound_sql_select_to_distributed_vector_aggregate(select);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  EXPECT_EQ(lowered->input_rows.destination_column_ordinals, (std::vector<std::uint32_t>{2U, 1U}));
  ASSERT_EQ(lowered->intent.aggregates.size(), 3U);
  EXPECT_EQ(lowered->intent.aggregates[0].operation, VectorAggregateOperation::kSum);
  EXPECT_EQ(lowered->intent.aggregates[1].operation, VectorAggregateOperation::kMinimum);
  EXPECT_EQ(lowered->intent.aggregates[2].operation, VectorAggregateOperation::kCountStar);
  EXPECT_TRUE(lowered->intent.order_keys.empty());
  ASSERT_EQ(lowered->result_schema.columns.size(), 3U);
  EXPECT_EQ(lowered->result_schema.columns[0].name, "_");
  ASSERT_TRUE(lowered->coordinator_projection.has_value());
  ASSERT_EQ(lowered->coordinator_projection->outputs.size(), 3U);
  ASSERT_EQ(lowered->coordinator_projection->result_schema.columns.size(), 3U);
  EXPECT_EQ(lowered->coordinator_projection->result_schema.columns[0].name, "shifted");
  EXPECT_EQ(lowered->coordinator_projection->result_schema.columns[1].name, "minimum_label");
  EXPECT_EQ(lowered->coordinator_projection->result_schema.columns[2].name, "doubled");
  EXPECT_EQ(lowered->coordinator_projection->outputs[0].result_shape().type.kind(),
            schema::LogicalTypeKind::kInt64);
  EXPECT_EQ(lowered->coordinator_projection->outputs[1].result_shape().type.kind(),
            schema::LogicalTypeKind::kString);

  const auto bounded = lower_bound_sql_select_to_distributed_vector_aggregate(
      select, {.maximum_expression_configuration_bytes = 1U});
  ASSERT_FALSE(bounded.has_value());
  EXPECT_EQ(bounded.error().code(), SqlDiagnosticCode::kResourceLimit);
}

TEST(DistributedSqlLoweringTest, LowersCompleteSourceForCoordinatorGroupedExecution) {
  BoundSqlSelect select =
      bind("SELECT value % 5 AS bucket, count(*) AS rows, sum(value) AS total, "
           "max(label) AS maximum_label FROM metrics WHERE value < 18 "
           "GROUP BY value % 5 ORDER BY rows DESC, maximum_label ASC NULLS LAST, "
           "bucket DESC LIMIT 11");
  auto lowered = lower_bound_sql_select_to_distributed_vector_grouped(select);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  EXPECT_EQ(lowered->input_rows.destination_column_ordinals,
            (std::vector<std::uint32_t>{0U, 1U, 2U}));
  EXPECT_EQ(lowered->input_rows.intent.row_output_indices,
            (std::vector<std::uint32_t>{0U, 1U, 2U}));
  EXPECT_FALSE(lowered->input_rows.intent.limit.has_value());
  EXPECT_FALSE(lowered->input_rows.event_time_predicate.has_value());
  EXPECT_FALSE(lowered->input_rows.coordinator_projection.has_value());
  ASSERT_EQ(lowered->coordinator_pipeline.input_columns().size(), 3U);
  ASSERT_EQ(lowered->coordinator_pipeline.output_columns().size(), 4U);
  ASSERT_EQ(lowered->result_schema.columns.size(), 4U);
  EXPECT_EQ(lowered->result_schema.columns[0].name, "bucket");
  EXPECT_EQ(lowered->result_schema.columns[1].name, "rows");
  EXPECT_EQ(lowered->result_schema.columns[2].name, "total");
  EXPECT_EQ(lowered->result_schema.columns[3].name, "maximum_label");

  auto global =
      lower_bound_sql_select_to_distributed_vector_grouped(bind("SELECT count(*) FROM metrics"));
  ASSERT_FALSE(global.has_value());
  EXPECT_EQ(global.error().code(), SqlDiagnosticCode::kUnsupportedSyntax);
  auto bounded = lower_bound_sql_select_to_distributed_vector_grouped(
      select, {.rows = {.maximum_projection_columns = 1U}});
  ASSERT_FALSE(bounded.has_value());
  EXPECT_EQ(bounded.error().code(), SqlDiagnosticCode::kResourceLimit);
}

TEST(DistributedSqlLoweringTest, LowersDirectGroupedSufficientStateIntent) {
  BoundSqlSelect select =
      bind("SELECT label AS category, value AS raw_value, count(*) AS rows, "
           "sum(value) AS total, min(label) AS first_label FROM metrics WHERE ts BETWEEN "
           "TIMESTAMP '1970-01-01 00:00:00.000000002Z' AND "
           "TIMESTAMP '1970-01-01 00:00:00.000000009Z' GROUP BY label, value "
           "ORDER BY total DESC, category ASC NULLS FIRST, total ASC LIMIT 3");
  auto lowered = lower_bound_sql_select_to_distributed_vector_grouped_aggregate(select);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  EXPECT_EQ(lowered->table_id, id<schema::TableId>(1U));
  EXPECT_EQ(lowered->destination_schema_id, id<schema::SchemaId>(2U));
  EXPECT_EQ(lowered->destination_column_ordinals, (std::vector<std::uint32_t>{1U, 2U}));
  ASSERT_TRUE(lowered->event_time_predicate.has_value());
  EXPECT_EQ(lowered->event_time_predicate->lower,
            (cseg::EventTimeBound{.value = 2, .inclusive = true}));
  EXPECT_EQ(lowered->event_time_predicate->upper,
            (cseg::EventTimeBound{.value = 9, .inclusive = true}));
  EXPECT_EQ(lowered->intent.mode, DistributedVectorPlanMode::kGroupedAggregate);
  EXPECT_EQ(lowered->intent.group_key_input_indices, (std::vector<std::uint32_t>{0U, 1U}));
  ASSERT_EQ(lowered->intent.aggregates.size(), 3U);
  EXPECT_EQ(lowered->intent.aggregates[0],
            (DistributedVectorAggregateIntent{.operation = VectorAggregateOperation::kCountStar,
                                              .input_index = std::nullopt}));
  EXPECT_EQ(lowered->intent.aggregates[1],
            (DistributedVectorAggregateIntent{.operation = VectorAggregateOperation::kSum,
                                              .input_index = 1U}));
  EXPECT_EQ(lowered->intent.aggregates[2],
            (DistributedVectorAggregateIntent{.operation = VectorAggregateOperation::kMinimum,
                                              .input_index = 0U}));
  ASSERT_EQ(lowered->intent.order_keys.size(), 2U);
  EXPECT_EQ(lowered->intent.order_keys[0],
            (DistributedVectorOrderKey{.output_index = 3U,
                                       .direction = PhysicalSortDirection::kDescending,
                                       .null_placement = ScalarNullPlacement::kFirst}));
  EXPECT_EQ(lowered->intent.order_keys[1],
            (DistributedVectorOrderKey{.output_index = 0U,
                                       .direction = PhysicalSortDirection::kAscending,
                                       .null_placement = ScalarNullPlacement::kFirst}));
  EXPECT_EQ(lowered->intent.limit, 3U);
  EXPECT_FALSE(lowered->coordinator_projection.has_value());
  ASSERT_EQ(lowered->result_schema.columns.size(), 5U);
  EXPECT_EQ(lowered->result_schema.columns[0].name, "category");
  EXPECT_EQ(lowered->result_schema.columns[1].name, "raw_value");
  EXPECT_EQ(lowered->result_schema.columns[2].name, "rows");
  EXPECT_EQ(lowered->result_schema.columns[3].name, "total");
  EXPECT_EQ(lowered->result_schema.columns[4].name, "first_label");
  EXPECT_EQ(lowered->result_schema.columns[0].type.kind(), schema::LogicalTypeKind::kString);
  EXPECT_TRUE(lowered->result_schema.columns[0].nullable);
  EXPECT_EQ(lowered->result_schema.columns[2].type.kind(), schema::LogicalTypeKind::kInt64);
  EXPECT_FALSE(lowered->result_schema.columns[2].nullable);
}

TEST(DistributedSqlLoweringTest, SplitsRawGroupedStateFromCheckedFinalProjection) {
  BoundSqlSelect select =
      bind("SELECT sum(value) + 1 AS adjusted, label AS category, count(*) * 2 AS doubled "
           "FROM metrics GROUP BY label ORDER BY adjusted DESC, category LIMIT 3");
  auto lowered = lower_bound_sql_select_to_distributed_vector_grouped_aggregate(select);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  EXPECT_EQ(lowered->destination_column_ordinals, (std::vector<std::uint32_t>{1U, 2U}));
  EXPECT_EQ(lowered->intent.group_key_input_indices, (std::vector<std::uint32_t>{0U}));
  ASSERT_EQ(lowered->intent.aggregates.size(), 2U);
  EXPECT_EQ(lowered->intent.aggregates[0],
            (DistributedVectorAggregateIntent{.operation = VectorAggregateOperation::kSum,
                                              .input_index = 1U}));
  EXPECT_EQ(lowered->intent.aggregates[1],
            (DistributedVectorAggregateIntent{.operation = VectorAggregateOperation::kCountStar,
                                              .input_index = std::nullopt}));
  EXPECT_TRUE(lowered->intent.order_keys.empty());
  EXPECT_FALSE(lowered->intent.limit.has_value());
  ASSERT_EQ(lowered->result_schema.columns.size(), 3U);
  EXPECT_EQ(lowered->result_schema.columns[0].name, "_");
  EXPECT_EQ(lowered->result_schema.columns[1].type.kind(), schema::LogicalTypeKind::kInt64);
  EXPECT_EQ(lowered->result_schema.columns[2].type.kind(), schema::LogicalTypeKind::kInt64);
  ASSERT_TRUE(lowered->coordinator_projection.has_value());
  const DistributedVectorGroupedAggregateCoordinatorProjection& projection =
      *lowered->coordinator_projection;
  ASSERT_EQ(projection.outputs.size(), 3U);
  ASSERT_EQ(projection.result_schema.columns.size(), 3U);
  EXPECT_EQ(projection.result_schema.columns[0].name, "adjusted");
  EXPECT_EQ(projection.result_schema.columns[1].name, "category");
  EXPECT_EQ(projection.result_schema.columns[2].name, "doubled");
  ASSERT_EQ(projection.order_keys.size(), 2U);
  EXPECT_EQ(projection.order_keys[0].output_index, 0U);
  EXPECT_EQ(projection.order_keys[0].direction, PhysicalSortDirection::kDescending);
  EXPECT_EQ(projection.order_keys[1].output_index, 1U);
  EXPECT_EQ(projection.limit, 3U);
}

TEST(DistributedSqlLoweringTest, RejectsNonDirectGroupedSufficientStateSemanticsAndBounds) {
  const std::vector<std::string_view> statements{
      "SELECT value % 3 AS bucket, count(*) FROM metrics GROUP BY value % 3",
      "SELECT label, sum(value + 1) FROM metrics GROUP BY label",
      "SELECT label, count(*) FROM metrics WHERE value > 0 GROUP BY label",
      "SELECT label, count(*) FROM metrics GROUP BY label ORDER BY max(value)"};
  for (const std::string_view statement : statements) {
    SCOPED_TRACE(statement);
    auto lowered = lower_bound_sql_select_to_distributed_vector_grouped_aggregate(bind(statement));
    ASSERT_FALSE(lowered.has_value());
    EXPECT_EQ(lowered.error().code(), SqlDiagnosticCode::kUnsupportedSyntax);
    EXPECT_EQ(lowered.error().status().code(), common::StatusCode::kNotSupported);
  }

  BoundSqlSelect direct =
      bind("SELECT label, value, count(*) AS rows, sum(value) AS total FROM metrics "
           "GROUP BY label, value ORDER BY total, label");
  const std::vector<DistributedVectorGroupedAggregateSqlLoweringLimits> limits{
      {.maximum_projection_columns = 1U},
      {.maximum_group_keys = 1U},
      {.maximum_aggregates = 1U},
      {.maximum_order_keys = 1U},
      {.maximum_result_name_bytes = 1U},
      {.expression_limits = {.maximum_instructions = 0U}},
      {.maximum_expression_configuration_bytes = 0U}};
  for (const auto& limit : limits) {
    auto lowered = lower_bound_sql_select_to_distributed_vector_grouped_aggregate(direct, limit);
    ASSERT_FALSE(lowered.has_value());
    EXPECT_EQ(lowered.error().code(), SqlDiagnosticCode::kResourceLimit);
  }
}

TEST(DistributedSqlLoweringTest, OwnsGeneralGlobalAggregatePredicates) {
  BoundSqlSelect select =
      bind("SELECT count(*) AS n, sum(value) AS total, min(label) AS minimum_label FROM metrics "
           "WHERE value > 1 AND lower(label) = 'ok'");
  auto lowered = lower_bound_sql_select_to_distributed_vector_aggregate(select);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  EXPECT_EQ(lowered->input_rows.destination_column_ordinals,
            (std::vector<std::uint32_t>{0U, 1U, 2U}));
  EXPECT_EQ(lowered->input_rows.intent.row_output_indices,
            (std::vector<std::uint32_t>{0U, 1U, 2U}));
  EXPECT_FALSE(lowered->input_rows.event_time_predicate.has_value());
  ASSERT_TRUE(lowered->coordinator_predicate.has_value());
  EXPECT_EQ(lowered->coordinator_predicate->result_shape().type.kind(),
            schema::LogicalTypeKind::kBool);
  EXPECT_TRUE(lowered->coordinator_predicate->result_shape().nullable);
  ASSERT_EQ(lowered->intent.aggregates.size(), 3U);
  EXPECT_FALSE(lowered->intent.aggregates[0].input_index.has_value());
  EXPECT_EQ(lowered->intent.aggregates[1].input_index, 2U);
  EXPECT_EQ(lowered->intent.aggregates[2].input_index, 1U);

  auto constant = lower_bound_sql_select_to_distributed_vector_aggregate(
      bind("SELECT count(*) AS n FROM metrics WHERE FALSE"));
  ASSERT_TRUE(constant.has_value()) << constant.error().status().to_string();
  EXPECT_EQ(constant->input_rows.destination_column_ordinals, (std::vector<std::uint32_t>{0U}));
  ASSERT_TRUE(constant->coordinator_predicate.has_value());
  EXPECT_EQ(constant->coordinator_predicate->instructions().size(), 1U);

  auto disjoint_time = lower_bound_sql_select_to_distributed_vector_aggregate(
      bind("SELECT count(*) AS n FROM metrics WHERE ts NOT BETWEEN "
           "TIMESTAMP '1970-01-01 00:00:00Z' AND TIMESTAMP '1970-01-01 00:00:01Z'"));
  ASSERT_TRUE(disjoint_time.has_value()) << disjoint_time.error().status().to_string();
  EXPECT_FALSE(disjoint_time->input_rows.event_time_predicate.has_value());
  ASSERT_TRUE(disjoint_time->coordinator_predicate.has_value());
  EXPECT_EQ(disjoint_time->input_rows.destination_column_ordinals,
            (std::vector<std::uint32_t>{0U, 1U, 2U}));

  const auto bounded = lower_bound_sql_select_to_distributed_vector_aggregate(
      select, {.maximum_expression_configuration_bytes = 1U});
  ASSERT_FALSE(bounded.has_value());
  EXPECT_EQ(bounded.error().code(), SqlDiagnosticCode::kResourceLimit);
}

TEST(DistributedSqlLoweringTest, EnforcesGlobalAggregateCallerBounds) {
  BoundSqlSelect select =
      bind("SELECT sum(value) AS total, min(label) AS minimum_label FROM metrics");
  auto projection = lower_bound_sql_select_to_distributed_vector_aggregate(
      select, {.maximum_projection_columns = 1U});
  ASSERT_FALSE(projection.has_value());
  EXPECT_EQ(projection.error().status().code(), common::StatusCode::kResourceExhausted);

  auto aggregates =
      lower_bound_sql_select_to_distributed_vector_aggregate(select, {.maximum_aggregates = 1U});
  ASSERT_FALSE(aggregates.has_value());
  EXPECT_EQ(aggregates.error().status().code(), common::StatusCode::kResourceExhausted);

  auto names = lower_bound_sql_select_to_distributed_vector_aggregate(
      select, {.maximum_result_name_bytes = 4U});
  ASSERT_FALSE(names.has_value());
  EXPECT_EQ(names.error().status().code(), common::StatusCode::kResourceExhausted);

  auto invalid = lower_bound_sql_select_to_distributed_vector_aggregate(
      select, {.maximum_projection_columns = 0U});
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().status().code(), common::StatusCode::kInvalidArgument);

  auto invalid_expression = lower_bound_sql_select_to_distributed_vector_aggregate(
      select, {.expression_limits = {.maximum_instructions = 0U}});
  ASSERT_FALSE(invalid_expression.has_value());
  EXPECT_EQ(invalid_expression.error().status().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::query
