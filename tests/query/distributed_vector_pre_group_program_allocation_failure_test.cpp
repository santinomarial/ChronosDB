#include "chronos/query/distributed_vector_pre_group_program.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <limits>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

template <typename Operation>
[[nodiscard]] auto run_failure(const std::size_t fail_after, Operation&& operation,
                               std::size_t& observed) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    observed = failure.observed_allocations();
    failure.disable();
  }
  return std::move(*result);
}

[[nodiscard]] DistributedVectorPreGroupProgram program() {
  const auto int64_type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  std::vector<VectorExpressionInstruction> instructions{
      VectorInputExpression{0U, int64_type, false},
      VectorConstantExpression{ScalarValue::signed_value(int64_type, 2).value()},
      VectorBinaryExpression{VectorBinaryOperation::kMultiply, 0U, 1U}};
  return {.outputs = {VectorExpression::create(std::move(instructions)).value()}};
}

TEST(DistributedVectorPreGroupProgramAllocationFailureTest, ClassifiesEveryOwnedCodecAllocation) {
  const DistributedVectorPreGroupProgram expected = program();
  const auto baseline = encode_distributed_vector_pre_group_program(expected);
  ASSERT_TRUE(baseline.has_value()) << baseline.error().to_string();
  const std::vector<std::byte> bytes(baseline->bytes().begin(), baseline->bytes().end());

  std::size_t observed = 0U;
  auto encoded = run_failure(
      std::numeric_limits<std::size_t>::max(),
      [&] { return encode_distributed_vector_pre_group_program(expected); }, observed);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const std::size_t encode_allocations = observed;
  for (std::size_t index = 0U; index < encode_allocations; ++index) {
    auto failed = run_failure(
        index, [&] { return encode_distributed_vector_pre_group_program(expected); }, observed);
    ASSERT_FALSE(failed.has_value()) << index;
    EXPECT_EQ(failed.error().code(), common::StatusCode::kResourceExhausted) << index;
  }

  auto decoded = run_failure(
      std::numeric_limits<std::size_t>::max(),
      [&] { return decode_distributed_vector_pre_group_program_exact(bytes); }, observed);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  const std::size_t decode_allocations = observed;
  for (std::size_t index = 0U; index < decode_allocations; ++index) {
    auto failed = run_failure(
        index, [&] { return decode_distributed_vector_pre_group_program_exact(bytes); }, observed);
    ASSERT_FALSE(failed.has_value()) << index;
    EXPECT_EQ(failed.error().code(), common::StatusCode::kResourceExhausted) << index;
  }
}

} // namespace
} // namespace chronos::query
