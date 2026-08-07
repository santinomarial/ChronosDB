#include "chronos/common/status.hpp"
#include "chronos/query/column_output.hpp"
#include "chronos/schema/logical_type.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

template <typename Operation>
[[nodiscard]] auto run_with_output_allocation_failure(const std::size_t fail_after,
                                                      std::size_t& observed,
                                                      Operation&& operation) {
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

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

void append_u32(std::vector<std::byte>& bytes, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes.push_back(static_cast<std::byte>((value >> (index * 8U)) & 0xffU));
}

[[nodiscard]] columnar::OwnedPhysicalColumn string_column() {
  columnar::ColumnVectorBuffers buffers;
  buffers.validity = {std::byte{0x05}};
  for (const std::uint32_t offset : {0U, 3U, 3U, 7U})
    append_u32(buffers.offsets, offset);
  for (const char value : {'a', 'b', 'c', 'd', 'e', 'f', 'g'})
    buffers.values.push_back(static_cast<std::byte>(value));
  return columnar::OwnedPhysicalColumn::create({.type = type(schema::LogicalTypeKind::kString),
                                                .nullable = true,
                                                .row_count = 3U,
                                                .null_count = 1U},
                                               std::move(buffers))
      .value();
}

class OneChunkSource final : public PhysicalOperator {
public:
  explicit OneChunkSource(AccountedVectorChunk chunk) : chunk_(std::move(chunk)) {}

  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    if (!chunk_.has_value())
      return PhysicalOperatorStep::end();
    AccountedVectorChunk chunk = std::move(*chunk_);
    chunk_.reset();
    return PhysicalOperatorStep::chunk(std::move(chunk));
  }

private:
  std::optional<AccountedVectorChunk> chunk_;
};

class EmptySource final : public PhysicalOperator {
public:
  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    return PhysicalOperatorStep::end();
  }
};

[[nodiscard]] AccountedVectorChunk chunk(const QueryResourceContext& resources) {
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(string_column());
  VectorChunk value =
      VectorChunk::create(std::move(columns), VectorSelection::from_indices(3U, {0U, 2U}).value())
          .value();
  return AccountedVectorChunk::create(std::move(value), resources.reserve(4'096U).value(),
                                      resources)
      .value();
}

[[nodiscard]] VectorExpression constant_expression() {
  std::vector<VectorExpressionInstruction> instructions;
  instructions.emplace_back(VectorConstantExpression{
      ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), 7).value()});
  instructions.emplace_back(VectorConstantExpression{
      ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), 5).value()});
  instructions.emplace_back(VectorBinaryExpression{.operation = VectorBinaryOperation::kMultiply,
                                                   .left_instruction = 0U,
                                                   .right_instruction = 1U});
  return VectorExpression::create(std::move(instructions)).value();
}

TEST(VectorExpressionAllocationFailureTest, CreationClassifiesEveryOwnedAllocationFailure) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::vector<VectorExpressionInstruction> instructions;
    instructions.emplace_back(VectorConstantExpression{
        ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), 7).value()});
    instructions.emplace_back(VectorUnaryExpression{.operation = VectorUnaryOperation::kAbsolute,
                                                    .operand_instruction = 0U});
    std::size_t observed = 0U;
    auto expression = run_with_output_allocation_failure(
        fail_after, observed, [&] { return VectorExpression::create(std::move(instructions)); });
    EXPECT_GT(observed, 0U);
    if (expression.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(expression.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(SourceColumnOutputAllocationFailureTest, CreationClassifiesOperatorAllocationFailure) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 8U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::unique_ptr<PhysicalOperator> child = std::make_unique<EmptySource>();
    std::vector<std::size_t> ordinals{0U, 0U};
    std::size_t observed = 0U;
    auto output = run_with_output_allocation_failure(fail_after, observed, [&] {
      return SourceColumnOutputOperator::create(std::move(child), std::move(ordinals));
    });
    EXPECT_GT(observed, 0U);
    if (output.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(output.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(SourceColumnOutputAllocationFailureTest,
     PullClassifiesEveryCanonicalOutputAllocationFailureAndReleasesCredit) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
    auto output = SourceColumnOutputOperator::create(
                      std::make_unique<OneChunkSource>(chunk(resources)), {0U, 0U})
                      .value();
    std::size_t observed = 0U;
    auto step = run_with_output_allocation_failure(fail_after, observed,
                                                   [&] { return output->next(resources); });
    EXPECT_GT(observed, 0U);
    if (step.has_value()) {
      reached_success = true;
      step = common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "drop source-column output step"});
      output.reset();
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      break;
    }
    EXPECT_EQ(step.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(resources.is_cancelled());
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(reached_success);
}

TEST(ColumnOutputAllocationFailureTest, CreationClassifiesOperatorAllocationFailure) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 8U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::unique_ptr<PhysicalOperator> child = std::make_unique<EmptySource>();
    std::vector<ColumnOutputPosition> positions;
    positions.emplace_back(ConstantColumnOutputPosition{
        ScalarValue::text(type(schema::LogicalTypeKind::kString), "constant").value()});
    positions.emplace_back(SourceColumnOutputPosition{0U});
    std::size_t observed = 0U;
    auto output = run_with_output_allocation_failure(fail_after, observed, [&] {
      return ColumnOutputOperator::create(std::move(child), std::move(positions));
    });
    EXPECT_GT(observed, 0U);
    if (output.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(output.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(ColumnOutputAllocationFailureTest,
     PullClassifiesEveryMixedOutputAllocationFailureAndReleasesCredit) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 96U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
    std::vector<ColumnOutputPosition> positions;
    positions.emplace_back(SourceColumnOutputPosition{0U});
    positions.emplace_back(ConstantColumnOutputPosition{
        ScalarValue::text(type(schema::LogicalTypeKind::kString), "constant").value()});
    positions.emplace_back(
        ConstantColumnOutputPosition{ScalarValue::null(type(schema::LogicalTypeKind::kBinary))});
    positions.emplace_back(ComputedColumnOutputPosition{constant_expression()});
    auto output = ColumnOutputOperator::create(std::make_unique<OneChunkSource>(chunk(resources)),
                                               std::move(positions))
                      .value();
    std::size_t observed = 0U;
    auto step = run_with_output_allocation_failure(fail_after, observed,
                                                   [&] { return output->next(resources); });
    EXPECT_GT(observed, 0U);
    if (step.has_value()) {
      reached_success = true;
      step = common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "drop column output step"});
      output.reset();
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      break;
    }
    EXPECT_EQ(step.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(resources.is_cancelled());
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(reached_success);
}

} // namespace
} // namespace chronos::query
