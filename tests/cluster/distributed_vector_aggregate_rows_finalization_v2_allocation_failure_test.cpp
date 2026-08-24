#include "chronos/cluster/distributed_vector_aggregate_rows_finalization_v2.hpp"
#include "support/failing_allocator.hpp"

#include <array>
#include <cstddef>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string>
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

struct Input {
  DistributedVectorQueryExecutionResultV2 execution;
  query::DistributedVectorPlanIntent aggregate;
  query::DistributedVectorResultSchema output;
};

[[nodiscard]] Input input(const std::vector<std::byte>& batch,
                          const schema::LogicalType string_type) {
  return {.execution = {.plan = {.mode = query::DistributedVectorPlanMode::kRows,
                                 .row_output_indices = {0U}},
                        .result = {.result_schema = {.columns = {{"value", string_type, false}}},
                                   .messages = {{.query_id = uuid(1U),
                                                 .tablet_id =
                                                     schema::TabletId::from_uuid(uuid(2U)).value(),
                                                 .sequence = 1U,
                                                 .terminal = true,
                                                 .encoded_result_batch = batch}}}},
          .aggregate = {.mode = query::DistributedVectorPlanMode::kUngroupedAggregate,
                        .aggregates = {{.operation = query::VectorAggregateOperation::kMaximum,
                                        .input_index = 0U}}},
          .output = {.columns = {{"maximum", string_type, true}}}};
}

TEST(DistributedVectorAggregateRowsFinalizationV2AllocationFailureTest,
     ClassifiesEveryOwnedAggregationAllocation) {
  const schema::LogicalType string_type =
      schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const std::array<network::QueryResultColumn, 1U> columns{
      network::QueryResultColumn{.name = "value", .type = string_type, .nullable = false}};
  const std::string value = "allocation-owned aggregate input larger than SSO";
  const std::array<network::QueryResultCell, 1U> cells{
      network::QueryResultCell{.value = std::as_bytes(std::span{value.data(), value.size()})}};
  const std::vector<std::byte> batch =
      network::encode_query_result_batch(1U, columns, cells).value();
  std::vector<query::VectorExpressionInstruction> predicate_instructions;
  predicate_instructions.emplace_back(query::VectorInputExpression{
      .input_column_ordinal = 0U, .type = string_type, .nullable = false});
  predicate_instructions.emplace_back(
      query::VectorConstantExpression{query::ScalarValue::text(string_type, value).value()});
  predicate_instructions.emplace_back(
      query::VectorBinaryExpression{.operation = query::VectorBinaryOperation::kEqual,
                                    .left_instruction = 0U,
                                    .right_instruction = 1U});
  const query::VectorExpression predicate =
      query::VectorExpression::create(std::move(predicate_instructions)).value();

  bool succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    auto value_input = input(batch, string_type);
    auto result = run_failure(fail_after, [&] {
      return finalize_distributed_vector_aggregate_rows_with_predicate_v2(
          std::move(value_input.execution), value_input.aggregate, std::move(value_input.output),
          predicate);
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
