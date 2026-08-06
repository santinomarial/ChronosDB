#include "chronos/common/uuid.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/evaluator.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/value.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"

#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <memory>
#include <optional>
#include <string>
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

[[nodiscard]] std::shared_ptr<const QueryCatalogSnapshot> catalog() {
  const schema::ColumnId event_time = id<schema::ColumnId>(3U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event_time, "ts", type(schema::LogicalTypeKind::kTimestampNs), false)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(id<schema::ColumnId>(4U), "i",
                                                     type(schema::LogicalTypeKind::kInt64), true)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(id<schema::ColumnId>(5U), "u",
                                                     type(schema::LogicalTypeKind::kUInt64), false)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(id<schema::ColumnId>(6U), "f",
                                                     type(schema::LogicalTypeKind::kFloat64), true)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(id<schema::ColumnId>(7U), "s",
                                                     type(schema::LogicalTypeKind::kString), true)
                        .value());
  auto table = std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(id<schema::TableId>(1U), id<schema::SchemaId>(2U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns),
                                  {.event_time_column = event_time,
                                   .physical_ordering_key = {event_time},
                                   .partition_columns = {event_time},
                                   .shard_key = {event_time},
                                   .deduplication_key = {}})
          .value());
  const QueryCatalogTableInput input{.name = "t", .quoted = false, .schema = std::move(table)};
  QueryCatalogSnapshot snapshot = QueryCatalogSnapshot::create(1U, {&input, 1U}).value();
  return std::make_shared<const QueryCatalogSnapshot>(std::move(snapshot));
}

[[nodiscard]] BoundSqlSelect bind(const std::string_view sql) {
  ParsedSqlSelect parsed = parse_sql_v1_select(sql).value();
  return bind_sql_v1_select(std::move(parsed), catalog()).value();
}

[[nodiscard]] std::vector<ScalarValue> row(const bool null_integer = false) {
  std::vector<ScalarValue> values;
  values.push_back(
      ScalarValue::signed_value(type(schema::LogicalTypeKind::kTimestampNs), -61'000'000'000LL)
          .value());
  values.push_back(
      null_integer ? ScalarValue::null(type(schema::LogicalTypeKind::kInt64))
                   : ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), -7).value());
  values.push_back(ScalarValue::unsigned_value(type(schema::LogicalTypeKind::kUInt64), 9U).value());
  values.push_back(ScalarValue::float64(2.5).value());
  values.push_back(ScalarValue::text(type(schema::LogicalTypeKind::kString), "AbC").value());
  return values;
}

[[nodiscard]] ScalarEvaluationContext context(const std::vector<ScalarValue>& values,
                                              ScalarSourceRow& source) {
  source = ScalarSourceRow{values};
  return ScalarEvaluationContext{.sources = {&source, 1U}};
}

[[nodiscard]] std::int64_t small_decimal_coefficient(const ScalarValue& value) {
  const auto* decimal = std::get_if<Decimal128Value>(&value.storage());
  EXPECT_NE(decimal, nullptr);
  if (decimal == nullptr)
    return 0;
  std::uint64_t bits = 0U;
  for (std::size_t index = 0U; index < sizeof(bits); ++index) {
    bits |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(decimal->coefficient[index]))
            << (index * 8U);
  }
  return std::bit_cast<std::int64_t>(bits);
}

TEST(ScalarEvaluatorTest, EvaluatesBoundColumnsArithmeticAndScalarFunctions) {
  BoundSqlSelect plan = bind("SELECT i + 10 AS sum, abs(i) AS magnitude, lower(s) AS folded, "
                             "time_bucket(INTERVAL '1 minute', ts) AS bucket FROM t");
  const std::vector<ScalarValue> values = row();
  ScalarSourceRow source{{}};
  const ScalarEvaluationContext input = context(values, source);

  const auto sum = evaluate_sql_v1_expression(plan, *plan.syntax().items()[0].expression(), input);
  const auto magnitude =
      evaluate_sql_v1_expression(plan, *plan.syntax().items()[1].expression(), input);
  const auto folded =
      evaluate_sql_v1_expression(plan, *plan.syntax().items()[2].expression(), input);
  const auto bucket =
      evaluate_sql_v1_expression(plan, *plan.syntax().items()[3].expression(), input);
  ASSERT_TRUE(sum.has_value());
  ASSERT_TRUE(magnitude.has_value());
  ASSERT_TRUE(folded.has_value());
  ASSERT_TRUE(bucket.has_value());
  EXPECT_EQ(*std::get_if<std::int64_t>(&sum->storage()), 3);
  EXPECT_EQ(*std::get_if<std::int64_t>(&magnitude->storage()), 7);
  EXPECT_EQ(*std::get_if<std::string>(&folded->storage()), "abc");
  EXPECT_EQ(*std::get_if<std::int64_t>(&bucket->storage()), -120'000'000'000LL);
}

TEST(ScalarEvaluatorTest, ImplementsThreeValuedPredicatesBetweenAndIn) {
  BoundSqlSelect plan = bind("SELECT i BETWEEN -10 AND 0 AS bounded, i IN (1, NULL, 3) AS listed "
                             "FROM t WHERE i > 0 OR i IS NULL");
  std::vector<ScalarValue> values = row();
  ScalarSourceRow source{{}};
  ScalarEvaluationContext input = context(values, source);
  EXPECT_EQ(evaluate_sql_v1_predicate(plan, *plan.syntax().where(), input).value(),
            SqlTruthValue::kFalse);
  EXPECT_EQ(evaluate_sql_v1_predicate(plan, *plan.syntax().items()[0].expression(), input).value(),
            SqlTruthValue::kTrue);
  EXPECT_EQ(evaluate_sql_v1_predicate(plan, *plan.syntax().items()[1].expression(), input).value(),
            SqlTruthValue::kUnknown);

  values = row(true);
  input = context(values, source);
  EXPECT_EQ(evaluate_sql_v1_predicate(plan, *plan.syntax().where(), input).value(),
            SqlTruthValue::kTrue);
}

TEST(ScalarEvaluatorTest, DetectsIntegerFailuresAndPreservesFloatingIeeeRules) {
  BoundSqlSelect overflow = bind("SELECT i + 1 AS value FROM t");
  std::vector<ScalarValue> values = row();
  values[1] = ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64),
                                        std::numeric_limits<std::int64_t>::max())
                  .value();
  ScalarSourceRow source{{}};
  ScalarEvaluationContext input = context(values, source);
  EXPECT_EQ(evaluate_sql_v1_expression(overflow, *overflow.syntax().items()[0].expression(), input)
                .error()
                .status()
                .code(),
            common::StatusCode::kOutOfRange);

  BoundSqlSelect division = bind("SELECT i / 0 AS value FROM t");
  EXPECT_EQ(evaluate_sql_v1_expression(division, *division.syntax().items()[0].expression(), input)
                .error()
                .status()
                .code(),
            common::StatusCode::kInvalidArgument);

  BoundSqlSelect floating_plan = bind("SELECT f / CAST(0 AS FLOAT64) AS value FROM t");
  const auto floating_value = evaluate_sql_v1_expression(
      floating_plan, *floating_plan.syntax().items()[0].expression(), input);
  ASSERT_TRUE(floating_value.has_value());
  EXPECT_TRUE(std::isinf(*std::get_if<double>(&floating_value->storage())));

  BoundSqlSelect float32_plan =
      bind("SELECT CAST(1.0000001 AS FLOAT32) * CAST(1.0000001 AS FLOAT32) AS value FROM t");
  const auto float32_value = evaluate_sql_v1_expression(
      float32_plan, *float32_plan.syntax().items()[0].expression(), input);
  ASSERT_TRUE(float32_value.has_value());
  constexpr float kFloat32Operand = 1.0000001F;
  EXPECT_EQ(*std::get_if<float>(&float32_value->storage()), kFloat32Operand * kFloat32Operand);

  BoundSqlSelect short_circuit = bind("SELECT FALSE AND i / 0 = 1 AS value FROM t");
  EXPECT_EQ(evaluate_sql_v1_predicate(short_circuit,
                                      *short_circuit.syntax().items()[0].expression(), input)
                .value(),
            SqlTruthValue::kFalse);
}

TEST(ScalarEvaluatorTest, EvaluatesCastsAliasesAndAggregateOverrides) {
  BoundSqlSelect cast_plan =
      bind("SELECT CAST(i AS UINT8) AS small, CAST(DATE '1970-01-02' AS TIMESTAMP_NS) AS day "
           "FROM t ORDER BY small");
  std::vector<ScalarValue> values = row();
  values[1] = ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), 7).value();
  ScalarSourceRow source{{}};
  const ScalarEvaluationContext input = context(values, source);
  std::vector<ScalarValue> outputs;
  outputs.push_back(
      evaluate_sql_v1_expression(cast_plan, *cast_plan.syntax().items()[0].expression(), input)
          .value());
  outputs.push_back(
      evaluate_sql_v1_expression(cast_plan, *cast_plan.syntax().items()[1].expression(), input)
          .value());
  EXPECT_EQ(*std::get_if<std::uint64_t>(&outputs[0].storage()), 7U);
  EXPECT_EQ(*std::get_if<std::int64_t>(&outputs[1].storage()), 86'400'000'000'000LL);
  const ScalarEvaluationContext order_input{.sources = {&source, 1U}, .projected_outputs = outputs};
  const auto order = evaluate_sql_v1_expression(
      cast_plan, cast_plan.syntax().order_by()[0].expression, order_input);
  ASSERT_TRUE(order.has_value());
  EXPECT_EQ(*std::get_if<std::uint64_t>(&order->storage()), 7U);

  BoundSqlSelect aggregate = bind("SELECT count(*) AS n FROM t");
  const ScalarValue count =
      ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), 12).value();
  const ScalarExpressionOverride replacement{
      .expression_span = aggregate.syntax().items()[0].expression()->span(), .value = &count};
  const ScalarEvaluationContext aggregate_input{.sources = {&source, 1U},
                                                .overrides = {&replacement, 1U}};
  const auto result = evaluate_sql_v1_expression(
      aggregate, *aggregate.syntax().items()[0].expression(), aggregate_input);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*std::get_if<std::int64_t>(&result->storage()), 12);

  BoundSqlSelect float_cast = bind("SELECT CAST(CAST(7.9 AS FLOAT64) AS INT8) AS i FROM t");
  const auto cast_value =
      evaluate_sql_v1_expression(float_cast, *float_cast.syntax().items()[0].expression(), input);
  ASSERT_TRUE(cast_value.has_value());
  EXPECT_EQ(*std::get_if<std::int64_t>(&cast_value->storage()), 7);
}

TEST(ScalarEvaluatorTest, EvaluatesExactDecimalArithmeticRescalingAndCasts) {
  BoundSqlSelect plan = bind("SELECT "
                             "CAST(7 AS DECIMAL(6,2)) / CAST(2 AS DECIMAL(6,2)) AS quotient, "
                             "CAST(15 AS DECIMAL(6,1)) * CAST(2 AS DECIMAL(6,1)) AS product, "
                             "CAST(7 AS DECIMAL(6,2)) % CAST(2 AS DECIMAL(6,2)) AS remainder, "
                             "abs(-CAST(12 AS DECIMAL(6,2))) AS magnitude, "
                             "CAST(CAST(123 AS DECIMAL(6,2)) AS DECIMAL(4,1)) AS rescaled, "
                             "CAST(CAST(1.5 AS FLOAT64) AS DECIMAL(4,2)) AS exact_float, "
                             "CAST(CAST(-3.99 AS DECIMAL(4,2)) AS INT8) AS integral "
                             "FROM t");
  const std::vector<ScalarValue> values = row();
  ScalarSourceRow source{{}};
  const ScalarEvaluationContext input = context(values, source);
  std::vector<ScalarValue> results;
  for (const SqlSelectItem& item : plan.syntax().items()) {
    auto result = evaluate_sql_v1_expression(plan, *item.expression(), input);
    ASSERT_TRUE(result.has_value()) << result.error().status().message();
    results.push_back(std::move(*result));
  }
  EXPECT_EQ(small_decimal_coefficient(results[0]), 350);
  EXPECT_EQ(small_decimal_coefficient(results[1]), 300);
  EXPECT_EQ(small_decimal_coefficient(results[2]), 100);
  EXPECT_EQ(small_decimal_coefficient(results[3]), 1'200);
  EXPECT_EQ(small_decimal_coefficient(results[4]), 1'230);
  EXPECT_EQ(small_decimal_coefficient(results[5]), 150);
  EXPECT_EQ(*std::get_if<std::int64_t>(&results[6].storage()), -3);

  BoundSqlSelect overflow = bind("SELECT CAST(1000 AS DECIMAL(3,0)) AS value FROM t");
  const auto failed =
      evaluate_sql_v1_expression(overflow, *overflow.syntax().items()[0].expression(), input);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().status().code(), common::StatusCode::kOutOfRange);

  BoundSqlSelect divide_by_zero =
      bind("SELECT CAST(1 AS DECIMAL(3,0)) / CAST(0 AS DECIMAL(3,0)) AS value FROM t");
  const auto divided = evaluate_sql_v1_expression(
      divide_by_zero, *divide_by_zero.syntax().items()[0].expression(), input);
  ASSERT_FALSE(divided.has_value());
  EXPECT_EQ(divided.error().status().code(), common::StatusCode::kInvalidArgument);
}

TEST(ScalarEvaluatorPropertyTest, CheckedAdditionMatchesWideReferenceWithinDomain) {
  BoundSqlSelect plan = bind("SELECT i + 10 AS value FROM t");
  std::vector<ScalarValue> values = row();
  ScalarSourceRow source{{}};
  for (std::int64_t value = -100'000; value <= 100'000; value += 977) {
    values[1] = ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), value).value();
    const ScalarEvaluationContext input = context(values, source);
    const auto result =
        evaluate_sql_v1_expression(plan, *plan.syntax().items()[0].expression(), input);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*std::get_if<std::int64_t>(&result->storage()), value + 10);
  }
}

TEST(ScalarEvaluatorPropertyTest, DecimalArithmeticMatchesScaledIntegerReference) {
  BoundSqlSelect plan =
      bind("SELECT "
           "CAST(i AS DECIMAL(18,3)) + CAST(7 AS DECIMAL(18,3)) AS added, "
           "CAST(i AS DECIMAL(18,3)) * CAST(3 AS DECIMAL(18,3)) AS multiplied, "
           "CAST(i AS DECIMAL(18,3)) / CAST(7 AS DECIMAL(18,3)) AS divided, "
           "CAST(i AS DECIMAL(18,3)) % CAST(7 AS DECIMAL(18,3)) AS remainder FROM t");
  std::vector<ScalarValue> values = row();
  ScalarSourceRow source{{}};
  for (std::int64_t value = -100'000; value <= 100'000; value += 977) {
    values[1] = ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), value).value();
    const ScalarEvaluationContext input = context(values, source);
    std::array<std::int64_t, 4> coefficients{};
    for (std::size_t index = 0U; index < coefficients.size(); ++index) {
      const auto result =
          evaluate_sql_v1_expression(plan, *plan.syntax().items()[index].expression(), input);
      ASSERT_TRUE(result.has_value());
      coefficients[index] = small_decimal_coefficient(*result);
    }
    EXPECT_EQ(coefficients[0], (value + 7) * 1'000);
    EXPECT_EQ(coefficients[1], value * 3 * 1'000);
    EXPECT_EQ(coefficients[2], value * 1'000 / 7);
    EXPECT_EQ(coefficients[3], value % 7 * 1'000);
  }
}

TEST(ScalarEvaluatorTest, HandlesDecimalThirtyEightDigitIntermediatesAndOverflow) {
  BoundSqlSelect exact =
      bind("SELECT CAST((CAST(9000000000000000000 AS DECIMAL(38,0)) * "
           "CAST(9000000000000000000 AS DECIMAL(38,0))) / "
           "CAST(9000000000000000000 AS DECIMAL(38,0)) AS INT64) AS value FROM t");
  const std::vector<ScalarValue> values = row();
  ScalarSourceRow source{{}};
  const ScalarEvaluationContext input = context(values, source);
  const auto exact_result =
      evaluate_sql_v1_expression(exact, *exact.syntax().items()[0].expression(), input);
  ASSERT_TRUE(exact_result.has_value());
  EXPECT_EQ(*std::get_if<std::int64_t>(&exact_result->storage()), 9'000'000'000'000'000'000LL);

  BoundSqlSelect overflow = bind("SELECT "
                                 "CAST(9000000000000000000 AS DECIMAL(38,0)) * "
                                 "CAST(9000000000000000000 AS DECIMAL(38,0)) + "
                                 "CAST(9000000000000000000 AS DECIMAL(38,0)) * "
                                 "CAST(9000000000000000000 AS DECIMAL(38,0)) AS value FROM t");
  const auto overflow_result =
      evaluate_sql_v1_expression(overflow, *overflow.syntax().items()[0].expression(), input);
  ASSERT_FALSE(overflow_result.has_value());
  EXPECT_EQ(overflow_result.error().status().code(), common::StatusCode::kOutOfRange);
}

} // namespace
} // namespace chronos::query
