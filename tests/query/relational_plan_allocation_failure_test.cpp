#include "chronos/query/relational_plan.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

template <typename Operation>
[[nodiscard]] auto run_with_allocation_failure(const std::size_t fail_after, std::size_t& observed,
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

class EmptySource final : public PhysicalOperator {
public:
  common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    return PhysicalOperatorStep::end();
  }
};

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] std::vector<PhysicalColumnShape> input_shape() {
  return {{.type = type(schema::LogicalTypeKind::kInt64), .nullable = false},
          {.type = type(schema::LogicalTypeKind::kTimestampNs), .nullable = false},
          {.type = type(schema::LogicalTypeKind::kUuid), .nullable = false},
          {.type = type(schema::LogicalTypeKind::kUInt64), .nullable = false},
          {.type = type(schema::LogicalTypeKind::kUInt32), .nullable = false},
          {.type = type(schema::LogicalTypeKind::kUInt8), .nullable = false}};
}

[[nodiscard]] VectorAsofJoinDefinition definition() {
  std::vector<VectorAsofColumnShape> left;
  std::vector<VectorAsofColumnShape> right;
  for (const PhysicalColumnShape& column : input_shape()) {
    left.push_back({.type = column.type, .nullable = column.nullable});
    right.push_back({.type = column.type, .nullable = column.nullable});
  }
  return {.left_input_columns = std::move(left),
          .right_input_columns = std::move(right),
          .equality_keys = {{.left_column_ordinal = 0U, .right_column_ordinal = 0U}},
          .left_timestamp_column_ordinal = 1U,
          .right_timestamp_column_ordinal = 1U,
          .right_physical_ordering_key_ordinals = {0U},
          .right_row_version_first_column_ordinal = 2U,
          .left_output_column_ordinals = {0U, 1U, 2U, 3U, 4U, 5U},
          .right_output_column_ordinals = {0U, 1U, 2U, 3U, 4U, 5U},
          .left_outer = true};
}

[[nodiscard]] common::Result<PhysicalAsofPlan> create_plan() {
  VectorAsofJoinDefinition configured = definition();
  std::vector<PhysicalColumnShape> joined;
  const std::vector<VectorAsofColumnShape> joined_shapes =
      vector_asof_join_output_shape(configured).value();
  joined.reserve(joined_shapes.size());
  for (const VectorAsofColumnShape& column : joined_shapes)
    joined.push_back({.type = column.type, .nullable = column.nullable});
  std::vector<PhysicalAsofPlanJoin> joins;
  joins.push_back({.left_preparation = PhysicalPipelinePlan::create(input_shape(), {}).value(),
                   .right_preparation = PhysicalPipelinePlan::create(input_shape(), {}).value(),
                   .definition = std::move(configured)});
  return PhysicalAsofPlan::create(std::move(joins),
                                  PhysicalPipelinePlan::create(std::move(joined), {}).value());
}

TEST(PhysicalAsofPlanAllocationFailureTest, CreationClassifiesEveryOwnedAllocation) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    VectorAsofJoinDefinition configured = definition();
    std::vector<PhysicalColumnShape> joined;
    const std::vector<VectorAsofColumnShape> joined_shapes =
        vector_asof_join_output_shape(configured).value();
    joined.reserve(joined_shapes.size());
    for (const VectorAsofColumnShape& column : joined_shapes)
      joined.push_back({.type = column.type, .nullable = column.nullable});
    std::vector<PhysicalAsofPlanJoin> joins;
    joins.push_back({.left_preparation = PhysicalPipelinePlan::create(input_shape(), {}).value(),
                     .right_preparation = PhysicalPipelinePlan::create(input_shape(), {}).value(),
                     .definition = std::move(configured)});
    PhysicalPipelinePlan final = PhysicalPipelinePlan::create(std::move(joined), {}).value();
    std::size_t observed = 0U;
    auto result = run_with_allocation_failure(fail_after, observed, [&] {
      return PhysicalAsofPlan::create(std::move(joins), std::move(final));
    });
    EXPECT_GT(observed, 0U);
    if (result.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(PhysicalAsofPlanAllocationFailureTest, InstantiationClassifiesEveryOwnedAllocation) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    PhysicalAsofPlan configured = create_plan().value();
    std::vector<std::unique_ptr<PhysicalOperator>> sources;
    sources.push_back(std::make_unique<EmptySource>());
    sources.push_back(std::make_unique<EmptySource>());
    std::size_t observed = 0U;
    auto result = run_with_allocation_failure(
        fail_after, observed, [&] { return configured.instantiate(std::move(sources)); });
    EXPECT_GT(observed, 0U);
    if (result.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

} // namespace
} // namespace chronos::query
