#include "chronos/cluster/distributed_vector_aggregate_finalization_v2.hpp"
#include "chronos/query/distributed_sql_lowering.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] query::Decimal128Value decimal(const std::int64_t value) {
  query::Decimal128Value result;
  result.coefficient.fill(value < 0 ? std::byte{0xff} : std::byte{});
  const std::uint64_t bits = std::bit_cast<std::uint64_t>(value);
  for (std::size_t index = 0U; index < sizeof(bits); ++index)
    result.coefficient[index] = static_cast<std::byte>((bits >> (index * 8U)) & 0xffU);
  return result;
}

struct AllTypesInput {
  query::DistributedVectorPlanIntent plan;
  query::DistributedVectorAggregateQueryResultV2 result;
};

[[nodiscard]] AllTypesInput all_types_input() {
  const std::array types{
      type(schema::LogicalTypeKind::kBool),        type(schema::LogicalTypeKind::kInt8),
      type(schema::LogicalTypeKind::kInt16),       type(schema::LogicalTypeKind::kInt32),
      type(schema::LogicalTypeKind::kInt64),       type(schema::LogicalTypeKind::kUInt8),
      type(schema::LogicalTypeKind::kUInt16),      type(schema::LogicalTypeKind::kUInt32),
      type(schema::LogicalTypeKind::kUInt64),      type(schema::LogicalTypeKind::kFloat32),
      type(schema::LogicalTypeKind::kFloat64),     schema::LogicalType::decimal(10U, 2U).value(),
      type(schema::LogicalTypeKind::kTimestampNs), type(schema::LogicalTypeKind::kDate),
      type(schema::LogicalTypeKind::kSymbol),      type(schema::LogicalTypeKind::kString),
      type(schema::LogicalTypeKind::kBinary),      type(schema::LogicalTypeKind::kUuid)};
  std::vector<query::ScalarValue> values;
  values.reserve(types.size());
  values.push_back(query::ScalarValue::boolean(true).value());
  values.push_back(query::ScalarValue::signed_value(types[1], -8).value());
  values.push_back(query::ScalarValue::signed_value(types[2], -16).value());
  values.push_back(query::ScalarValue::signed_value(types[3], -32).value());
  values.push_back(query::ScalarValue::signed_value(types[4], -64).value());
  values.push_back(query::ScalarValue::unsigned_value(types[5], 8U).value());
  values.push_back(query::ScalarValue::unsigned_value(types[6], 16U).value());
  values.push_back(query::ScalarValue::unsigned_value(types[7], 32U).value());
  values.push_back(query::ScalarValue::unsigned_value(types[8], 64U).value());
  values.push_back(query::ScalarValue::float32(1.25F).value());
  values.push_back(query::ScalarValue::float64(2.5).value());
  values.push_back(query::ScalarValue::decimal(types[11], decimal(1234)).value());
  values.push_back(query::ScalarValue::signed_value(types[12], 123456).value());
  values.push_back(query::ScalarValue::signed_value(types[13], 123).value());
  values.push_back(query::ScalarValue::text(types[14], "symbol").value());
  values.push_back(query::ScalarValue::null(types[15]));
  values.push_back(query::ScalarValue::binary({std::byte{1U}, std::byte{2U}}));
  common::Uuid::Bytes uuid_bytes{};
  uuid_bytes.back() = std::byte{9U};
  values.push_back(query::ScalarValue::uuid(common::Uuid{uuid_bytes}));

  AllTypesInput input{.plan = {.mode = query::DistributedVectorPlanMode::kUngroupedAggregate}};
  input.result.values = std::move(values);
  for (std::size_t ordinal = 0U; ordinal < types.size(); ++ordinal) {
    input.plan.aggregates.push_back({.operation = query::VectorAggregateOperation::kMinimum,
                                     .input_index = static_cast<std::uint32_t>(ordinal)});
    input.result.definitions.push_back(
        {.operation = query::VectorAggregateOperation::kMinimum,
         .input = query::VectorAggregateInput{
             .column_ordinal = ordinal, .type = types[ordinal], .nullable = false}});
    input.result.result_schema.columns.push_back(
        {.name = "c" + std::to_string(ordinal), .type = types[ordinal], .nullable = true});
  }
  return input;
}

TEST(DistributedVectorAggregateFinalizationV2Test,
     EncodesEveryScalarTypeIntoOneCanonicalNativeResultRow) {
  auto input = all_types_input();
  const auto expected_schema = input.result.result_schema;
  auto finalized = finalize_distributed_vector_aggregate_v2(input.plan, std::move(input.result));
  ASSERT_TRUE(finalized.has_value()) << finalized.error().to_string();
  EXPECT_EQ(finalized->row_count, 1U);
  EXPECT_EQ(finalized->encoded_bytes, finalized->encoded_batch.size());
  EXPECT_EQ(finalized->result_schema, expected_schema);
  auto decoded = network::decode_query_result_batch(finalized->encoded_batch);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->row_count(), 1U);
  ASSERT_EQ(decoded->columns().size(), expected_schema.columns.size());
  for (std::size_t ordinal = 0U; ordinal < expected_schema.columns.size(); ++ordinal) {
    const network::QueryResultCell* cell = decoded->cell(0U, ordinal);
    ASSERT_NE(cell, nullptr);
    const bool expected_null = ordinal == 15U;
    EXPECT_EQ(cell->is_null, expected_null);
    auto compared = query::compare_canonical_scalar_bytes(
        expected_schema.columns[ordinal].type, expected_null, cell->value, expected_null,
        cell->value, query::ScalarNullPlacement::kLast);
    ASSERT_TRUE(compared.has_value()) << compared.error().to_string();
    EXPECT_EQ(*compared, 0);
  }
}

TEST(DistributedVectorAggregateFinalizationV2Test,
     AppliesGlobalLimitAndRejectsAuthorityOrResourceMismatch) {
  auto zero = all_types_input();
  zero.plan.limit = 0U;
  auto zero_result = finalize_distributed_vector_aggregate_v2(zero.plan, std::move(zero.result));
  ASSERT_TRUE(zero_result.has_value()) << zero_result.error().to_string();
  EXPECT_EQ(zero_result->row_count, 0U);
  auto zero_batch = network::decode_query_result_batch(zero_result->encoded_batch);
  ASSERT_TRUE(zero_batch.has_value());
  EXPECT_EQ(zero_batch->row_count(), 0U);
  EXPECT_EQ(zero_batch->columns().size(), 18U);

  auto wrong_mode = all_types_input();
  wrong_mode.plan.mode = query::DistributedVectorPlanMode::kRows;
  EXPECT_EQ(finalize_distributed_vector_aggregate_v2(wrong_mode.plan, std::move(wrong_mode.result))
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto wrong_definition = all_types_input();
  wrong_definition.result.definitions[0].operation = query::VectorAggregateOperation::kMaximum;
  EXPECT_EQ(finalize_distributed_vector_aggregate_v2(wrong_definition.plan,
                                                     std::move(wrong_definition.result))
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto wrong_scalar = all_types_input();
  wrong_scalar.result.values[0] = query::ScalarValue::float64(1.0).value();
  EXPECT_EQ(
      finalize_distributed_vector_aggregate_v2(wrong_scalar.plan, std::move(wrong_scalar.result))
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);

  auto bounded = all_types_input();
  EXPECT_EQ(finalize_distributed_vector_aggregate_v2(bounded.plan, std::move(bounded.result),
                                                     {.maximum_working_bytes = 1U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
}

TEST(DistributedVectorAggregateFinalizationV2Test,
     EvaluatesCheckedVisibleExpressionsBeforeGlobalLimit) {
  const schema::LogicalType int64 = type(schema::LogicalTypeKind::kInt64);
  const schema::LogicalType symbol = type(schema::LogicalTypeKind::kSymbol);
  query::DistributedVectorAggregateCoordinatorProjection projection{
      .result_schema = {.columns = {{"shifted", int64, true}, {"upper_symbol", symbol, true}}}};
  projection.outputs.push_back(
      query::VectorExpression::create(
          {query::VectorInputExpression{
               .input_column_ordinal = 4U, .type = int64, .nullable = true},
           query::VectorConstantExpression{query::ScalarValue::signed_value(int64, 1).value()},
           query::VectorBinaryExpression{.operation = query::VectorBinaryOperation::kAdd,
                                         .left_instruction = 0U,
                                         .right_instruction = 1U}})
          .value());
  projection.outputs.push_back(
      query::VectorExpression::create(
          {query::VectorInputExpression{
               .input_column_ordinal = 14U, .type = symbol, .nullable = true},
           query::VectorUnaryExpression{.operation = query::VectorUnaryOperation::kUpperAscii,
                                        .operand_instruction = 0U}})
          .value());

  auto input = all_types_input();
  auto finalized = finalize_distributed_vector_aggregate_with_projection_v2(
      input.plan, std::move(input.result), projection);
  ASSERT_TRUE(finalized.has_value()) << finalized.error().to_string();
  EXPECT_EQ(finalized->result_schema, projection.result_schema);
  auto batch = network::decode_query_result_batch(finalized->encoded_batch);
  ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
  ASSERT_EQ(batch->row_count(), 1U);
  const network::QueryResultCell* shifted = batch->cell(0U, 0U);
  ASSERT_NE(shifted, nullptr);
  ASSERT_EQ(shifted->value.size(), 8U);
  std::array<std::byte, 8U> shifted_bytes{};
  std::ranges::copy(shifted->value, shifted_bytes.begin());
  EXPECT_EQ(std::bit_cast<std::int64_t>(shifted_bytes), -63);
  const network::QueryResultCell* upper = batch->cell(0U, 1U);
  ASSERT_NE(upper, nullptr);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(upper->value.data()), upper->value.size()),
            "SYMBOL");

  auto failing_input = all_types_input();
  failing_input.plan.limit = 0U;
  query::DistributedVectorAggregateCoordinatorProjection failing{
      .result_schema = {.columns = {{"broken", int64, true}}}};
  failing.outputs.push_back(
      query::VectorExpression::create(
          {query::VectorInputExpression{
               .input_column_ordinal = 4U, .type = int64, .nullable = true},
           query::VectorConstantExpression{query::ScalarValue::signed_value(int64, 0).value()},
           query::VectorBinaryExpression{.operation = query::VectorBinaryOperation::kDivide,
                                         .left_instruction = 0U,
                                         .right_instruction = 1U}})
          .value());
  EXPECT_EQ(finalize_distributed_vector_aggregate_with_projection_v2(
                failing_input.plan, std::move(failing_input.result), failing)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto stale_input = all_types_input();
  query::DistributedVectorAggregateCoordinatorProjection stale{
      .result_schema = {.columns = {{"stale", int64, true}}}};
  stale.outputs.push_back(query::VectorExpression::create(
                              {query::VectorInputExpression{
                                  .input_column_ordinal = 99U, .type = int64, .nullable = true}})
                              .value());
  EXPECT_EQ(finalize_distributed_vector_aggregate_with_projection_v2(
                stale_input.plan, std::move(stale_input.result), stale)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto bounded_input = all_types_input();
  EXPECT_EQ(finalize_distributed_vector_aggregate_with_projection_v2(
                bounded_input.plan, std::move(bounded_input.result), projection,
                {.maximum_working_bytes = 1U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::cluster
