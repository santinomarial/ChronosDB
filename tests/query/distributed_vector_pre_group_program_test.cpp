#include "chronos/common/crc32c.hpp"
#include "chronos/query/distributed_vector_pre_group_program.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <string>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

void store_u32_le(std::vector<std::byte>& bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t index = 0U; index < 4U; ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

[[nodiscard]] DistributedVectorPreGroupProgram program() {
  std::vector<VectorExpressionInstruction> arithmetic{
      VectorInputExpression{2U, type(schema::LogicalTypeKind::kInt64), true},
      VectorConstantExpression{
          ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), 7).value()},
      VectorBinaryExpression{VectorBinaryOperation::kAdd, 0U, 1U},
      VectorUnaryExpression{VectorUnaryOperation::kAbsolute, 2U},
      VectorCastExpression{3U, type(schema::LogicalTypeKind::kFloat64)}};
  std::vector<VectorExpressionInstruction> text{
      VectorInputExpression{4U, type(schema::LogicalTypeKind::kString), true},
      VectorUnaryExpression{VectorUnaryOperation::kLowerAscii, 0U}};
  std::vector<VectorExpressionInstruction> nullable_constant{
      VectorConstantExpression{ScalarValue::null(type(schema::LogicalTypeKind::kTimestampNs))}};
  std::vector<VectorExpression> outputs;
  outputs.push_back(VectorExpression::create(std::move(arithmetic)).value());
  outputs.push_back(VectorExpression::create(std::move(text)).value());
  outputs.push_back(VectorExpression::create(std::move(nullable_constant)).value());
  return {.outputs = std::move(outputs)};
}

TEST(DistributedVectorPreGroupProgramTest, RoundTripsEveryInstructionVariantAndOwnedConstants) {
  const DistributedVectorPreGroupProgram expected = program();
  ASSERT_TRUE(validate_distributed_vector_pre_group_program(expected).is_ok());
  auto encoded = encode_distributed_vector_pre_group_program(expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto decoded = decode_distributed_vector_pre_group_program_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, expected);

  const auto& constant =
      std::get<VectorConstantExpression>(decoded->outputs.front().instructions()[1U]);
  ASSERT_TRUE(constant.value.type().has_value());
  EXPECT_EQ(constant.value.type(), type(schema::LogicalTypeKind::kInt64));
  EXPECT_EQ(std::get<std::int64_t>(constant.value.storage()), 7);
}

TEST(DistributedVectorPreGroupProgramTest, RejectsDamageVersionsAndCallerBoundExhaustion) {
  auto encoded = encode_distributed_vector_pre_group_program(program());
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();

  std::vector<std::byte> damaged(encoded->bytes().begin(), encoded->bytes().end());
  damaged[distributed_vector_pre_group_program_format::kHeaderLength + 20U] ^= std::byte{1U};
  EXPECT_EQ(decode_distributed_vector_pre_group_program_exact(damaged).error().code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> hostile(encoded->bytes().begin(), encoded->bytes().end());
  constexpr std::size_t kFirstExpressionStart = 80U;
  constexpr std::size_t kFirstExpressionLength = 168U;
  constexpr std::size_t kUnaryOperandOffset = kFirstExpressionStart + 32U + 40U + 32U + 12U;
  store_u32_le(hostile, kUnaryOperandOffset, 3U);
  store_u32_le(hostile, 72U,
               common::crc32c(common::ByteView{hostile}.subspan(kFirstExpressionStart,
                                                                kFirstExpressionLength)));
  store_u32_le(hostile, 36U,
               common::crc32c(common::ByteView{hostile}.subspan(
                   distributed_vector_pre_group_program_format::kHeaderLength,
                   hostile.size() - distributed_vector_pre_group_program_format::kHeaderLength -
                       distributed_vector_pre_group_program_format::kTrailerLength)));
  store_u32_le(hostile, 40U, common::crc32c(common::ByteView{hostile}.first(40U)));
  store_u32_le(hostile, hostile.size() - 4U,
               common::crc32c(common::ByteView{hostile}.first(hostile.size() - 4U)));
  EXPECT_EQ(decode_distributed_vector_pre_group_program_exact(hostile).error().code(),
            common::StatusCode::kCorruption);

  EXPECT_EQ(decode_distributed_vector_pre_group_program_exact(
                encoded->bytes(), {.maximum_frame_length = encoded->bytes().size() - 1U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(decode_distributed_vector_pre_group_program_exact(encoded->bytes(),
                                                              {.maximum_expressions = 2U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(decode_distributed_vector_pre_group_program_exact(
                encoded->bytes(), {.maximum_instructions_per_expression = 4U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);

  std::vector<std::byte> future(encoded->bytes().begin(), encoded->bytes().end());
  future[8U] = std::byte{2U};
  store_u32_le(future, 40U, common::crc32c(common::ByteView{future}.first(40U)));
  store_u32_le(future, future.size() - 4U,
               common::crc32c(common::ByteView{future}.first(future.size() - 4U)));
  EXPECT_EQ(decode_distributed_vector_pre_group_program_exact(future).error().code(),
            common::StatusCode::kNotSupported);
}

TEST(DistributedVectorPreGroupProgramTest, RejectsEmptyAndOversizedConstantPrograms) {
  const DistributedVectorPreGroupProgram empty;
  EXPECT_EQ(validate_distributed_vector_pre_group_program(empty).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(encode_distributed_vector_pre_group_program(empty).error().code(),
            common::StatusCode::kInvalidArgument);

  std::string large(distributed_vector_pre_group_program_format::kMaximumConstantPayloadBytes + 1U,
                    'x');
  std::vector<VectorExpressionInstruction> instructions{VectorConstantExpression{
      ScalarValue::text(type(schema::LogicalTypeKind::kString), std::move(large)).value()}};
  const DistributedVectorPreGroupProgram oversized{
      .outputs = {VectorExpression::create(
                      std::move(instructions),
                      {.maximum_retained_configuration_bytes = std::size_t{2U} * 1024U * 1024U})
                      .value()}};
  EXPECT_EQ(validate_distributed_vector_pre_group_program(oversized).code(),
            common::StatusCode::kResourceExhausted);
}

TEST(DistributedVectorPreGroupProgramTest,
     RoundTripsAboveTheLocalExpressionDefaultWithinWireBounds) {
  std::string payload(std::size_t{300U} * 1024U, 'z');
  std::vector<VectorExpressionInstruction> instructions{VectorConstantExpression{
      ScalarValue::text(type(schema::LogicalTypeKind::kString), std::move(payload)).value()}};
  const DistributedVectorPreGroupProgram expected{
      .outputs = {VectorExpression::create(
                      std::move(instructions),
                      {.maximum_retained_configuration_bytes = std::size_t{512U} * 1024U})
                      .value()}};
  auto encoded = encode_distributed_vector_pre_group_program(expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto decoded = decode_distributed_vector_pre_group_program_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, expected);
  EXPECT_EQ(decode_distributed_vector_pre_group_program_exact(
                encoded->bytes(), {.maximum_constant_payload_bytes = 128U * 1024U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::query
