#include "chronos/cluster/distributed_vector_row_finalization_v2.hpp"
#include "chronos/query/distributed_sql_lowering.hpp"
#include "support/failing_allocator.hpp"

#include <array>
#include <cstddef>
#include <gtest/gtest.h>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

template <typename Operation>
[[nodiscard]] auto run_failure(const std::size_t fail_after, Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    failure.disable();
  }
  return std::move(*result);
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

[[nodiscard]] DistributedVectorQueryExecutionResultV2
make_input(const std::vector<std::byte>& batch, const schema::LogicalType type) {
  return {.plan = {.mode = query::DistributedVectorPlanMode::kRows,
                   .row_output_indices = {0U},
                   .visible_row_output_indices = {0U},
                   .order_keys = {{.output_index = 0U}}},
          .result = {.result_schema = {.columns = {{"value", type, false}}},
                     .messages = {{.query_id = uuid(1U),
                                   .tablet_id = schema::TabletId::from_uuid(uuid(2U)).value(),
                                   .sequence = 1U,
                                   .terminal = true,
                                   .encoded_result_batch = batch}}}};
}

TEST(DistributedVectorRowFinalizationV2AllocationFailureTest,
     ClassifiesEveryOwnedFinalizationAllocation) {
  const schema::LogicalType type =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  const std::array<network::QueryResultColumn, 1U> columns{
      network::QueryResultColumn{.name = "value", .type = type, .nullable = false}};
  const std::array<std::byte, 8U> value{std::byte{1U}};
  const std::array<network::QueryResultCell, 1U> cells{network::QueryResultCell{.value = value}};
  const std::vector<std::byte> batch =
      network::encode_query_result_batch(1U, columns, cells).value();

  bool succeeded = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    auto input = make_input(batch, type);
    auto result = run_failure(
        fail_after, [&] { return finalize_distributed_vector_rows_v2(std::move(input)); });
    if (result.has_value()) {
      succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(succeeded);
}

TEST(DistributedVectorRowFinalizationV2AllocationFailureTest,
     ClassifiesEveryCoordinatorProjectionAllocation) {
  const schema::LogicalType type =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  const std::array<network::QueryResultColumn, 1U> columns{
      network::QueryResultColumn{.name = "value", .type = type, .nullable = false}};
  const std::array<std::byte, 8U> value{std::byte{1U}};
  const std::array<network::QueryResultCell, 1U> cells{network::QueryResultCell{.value = value}};
  const std::vector<std::byte> batch =
      network::encode_query_result_batch(1U, columns, cells).value();
  std::vector<query::VectorExpressionInstruction> instructions;
  instructions.emplace_back(
      query::VectorInputExpression{.input_column_ordinal = 0U, .type = type, .nullable = false});
  instructions.emplace_back(
      query::VectorConstantExpression{.value = query::ScalarValue::signed_value(type, 3).value()});
  instructions.emplace_back(
      query::VectorBinaryExpression{.operation = query::VectorBinaryOperation::kAdd,
                                    .left_instruction = 0U,
                                    .right_instruction = 1U});
  const query::DistributedVectorRowCoordinatorProjection projection{
      .outputs = {query::DistributedVectorRowSourceOutput{.worker_output_index = 0U},
                  query::DistributedVectorRowConstantOutput{
                      .is_null = false,
                      .canonical_value = {std::byte{2U}, std::byte{0U}, std::byte{0U},
                                          std::byte{0U}, std::byte{0U}, std::byte{0U},
                                          std::byte{0U}, std::byte{0U}}},
                  query::DistributedVectorRowExpressionOutput{
                      .expression =
                          query::VectorExpression::create(std::move(instructions)).value()}},
      .result_schema = {.columns = {{"source", type, false},
                                    {"constant", type, false},
                                    {"computed", type, false}}}};

  bool succeeded = false;
  for (std::size_t fail_after = 0U; fail_after < 224U; ++fail_after) {
    auto input = make_input(batch, type);
    input.plan.visible_row_output_indices.clear();
    auto result = run_failure(fail_after, [&] {
      return finalize_distributed_vector_rows_with_projection_v2(std::move(input), projection);
    });
    if (result.has_value()) {
      succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(succeeded);
}

} // namespace
} // namespace chronos::cluster
