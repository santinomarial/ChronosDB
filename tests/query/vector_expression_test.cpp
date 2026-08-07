#include "chronos/common/status.hpp"
#include "chronos/query/vector_expression.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

TEST(VectorExpressionTest, InfersExactShapesAndRetainsABoundedImmutableProgram) {
  std::vector<VectorExpressionInstruction> instructions;
  instructions.emplace_back(VectorInputExpression{
      .input_column_ordinal = 2U, .type = type(schema::LogicalTypeKind::kInt16), .nullable = true});
  instructions.emplace_back(VectorConstantExpression{
      ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), 9).value()});
  instructions.emplace_back(VectorBinaryExpression{
      .operation = VectorBinaryOperation::kAdd, .left_instruction = 0U, .right_instruction = 1U});
  instructions.emplace_back(VectorUnaryExpression{.operation = VectorUnaryOperation::kAbsolute,
                                                  .operand_instruction = 2U});
  instructions.emplace_back(VectorConstantExpression{
      ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), 100).value()});
  instructions.emplace_back(VectorBinaryExpression{
      .operation = VectorBinaryOperation::kLess, .left_instruction = 3U, .right_instruction = 4U});
  VectorExpression expression = VectorExpression::create(std::move(instructions)).value();

  ASSERT_EQ(expression.instructions().size(), 6U);
  ASSERT_EQ(expression.instruction_shapes().size(), 6U);
  EXPECT_EQ(expression.instruction_shapes()[2U].type.kind(), schema::LogicalTypeKind::kInt64);
  EXPECT_TRUE(expression.instruction_shapes()[2U].nullable);
  EXPECT_EQ(expression.result_shape().type.kind(), schema::LogicalTypeKind::kBool);
  EXPECT_TRUE(expression.result_shape().nullable);
  EXPECT_EQ(expression.maximum_depth(), 4U);
  EXPECT_GT(expression.retained_configuration_bytes(), 0U);

  VectorExpression copy = expression; // NOLINT(performance-unnecessary-copy-initialization)
  EXPECT_EQ(copy.result_shape(), expression.result_shape());
  EXPECT_EQ(copy.instructions().size(), expression.instructions().size());
  EXPECT_NE(copy.instructions().data(), expression.instructions().data());
}

TEST(VectorExpressionTest, RejectsHostileProgramsAndUnsupportedLeafTypes) {
  EXPECT_EQ(VectorExpression::create({}).error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(VectorExpression::create({VectorConstantExpression{ScalarValue::boolean(true).value()}},
                                     {.maximum_instructions = 0U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(VectorExpression::create({VectorConstantExpression{ScalarValue::untyped_null()}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(VectorExpression::create(
                {VectorConstantExpression{
                    ScalarValue::text(type(schema::LogicalTypeKind::kString), "x").value()}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(VectorExpression::create({VectorUnaryExpression{.operation = VectorUnaryOperation::kNot,
                                                            .operand_instruction = 0U}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(
      VectorExpression::create(
          {VectorConstantExpression{
               ScalarValue::unsigned_value(type(schema::LogicalTypeKind::kUInt64), 1U).value()},
           VectorUnaryExpression{.operation = VectorUnaryOperation::kNegative,
                                 .operand_instruction = 0U}})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_EQ(VectorExpression::create(
                {VectorConstantExpression{ScalarValue::boolean(true).value()},
                 VectorConstantExpression{
                     ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), 1).value()},
                 VectorBinaryExpression{.operation = VectorBinaryOperation::kAdd,
                                        .left_instruction = 0U,
                                        .right_instruction = 1U}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  std::vector<VectorExpressionInstruction> over_reserved;
  over_reserved.reserve(8U);
  over_reserved.emplace_back(VectorConstantExpression{ScalarValue::boolean(true).value()});
  EXPECT_EQ(VectorExpression::create(std::move(over_reserved), {.maximum_instructions = 1U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::query
