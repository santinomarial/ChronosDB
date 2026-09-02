#include "chronos/cluster/distributed_vector_aggregate_finalization_v2.hpp"
#include "chronos/query/distributed_sql_lowering.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <optional>
#include <utility>

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

struct Input {
  query::DistributedVectorPlanIntent plan;
  query::DistributedVectorAggregateQueryResultV2 result;
};

[[nodiscard]] Input input() {
  const schema::LogicalType type =
      schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  Input value{.plan = {.mode = query::DistributedVectorPlanMode::kUngroupedAggregate,
                       .aggregates = {{.operation = query::VectorAggregateOperation::kMaximum,
                                       .input_index = 0U}}},
              .result = {.definitions = {{.operation = query::VectorAggregateOperation::kMaximum,
                                          .input = query::VectorAggregateInput{.column_ordinal = 0U,
                                                                               .type = type,
                                                                               .nullable = false}}},
                         .result_schema = {.columns = {{"maximum", type, true}}},
                         .values = {}}};
  value.result.values.push_back(
      query::ScalarValue::text(type, "allocation-owned aggregate result larger than SSO").value());
  return value;
}

TEST(DistributedVectorAggregateFinalizationV2AllocationFailureTest,
     ClassifiesEveryOwnedResultAllocation) {
  bool succeeded{};
  const schema::LogicalType string_type =
      schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  query::DistributedVectorAggregateCoordinatorProjection projection{
      .result_schema = {.columns = {{"upper_maximum", string_type, true}}}};
  projection.outputs.push_back(
      query::VectorExpression::create(
          {query::VectorInputExpression{
               .input_column_ordinal = 0U, .type = string_type, .nullable = true},
           query::VectorUnaryExpression{.operation = query::VectorUnaryOperation::kUpperAscii,
                                        .operand_instruction = 0U}})
          .value());
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    auto value = input();
    auto result = run_failure(fail_after, [&] {
      return finalize_distributed_vector_aggregate_with_projection_v2(
          value.plan, std::move(value.result), projection);
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
