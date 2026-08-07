#include "chronos/common/status.hpp"
#include "chronos/query/aggregate.hpp"
#include "chronos/schema/logical_type.hpp"
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
  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    return PhysicalOperatorStep::end();
  }
};

[[nodiscard]] schema::LogicalType int64_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
}

[[nodiscard]] std::vector<VectorAggregateDefinition> definitions() {
  return {
      {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt},
      {.operation = VectorAggregateOperation::kSum,
       .input = VectorAggregateInput{.column_ordinal = 0U, .type = int64_type(), .nullable = true}},
      {.operation = VectorAggregateOperation::kAverage,
       .input = VectorAggregateInput{.column_ordinal = 0U, .type = int64_type(), .nullable = true}},
  };
}

TEST(UngroupedAggregateAllocationFailureTest, CreationClassifiesEveryOwnedAllocationFailure) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::unique_ptr<PhysicalOperator> input = std::make_unique<EmptySource>();
    std::vector<VectorAggregateDefinition> configured = definitions();
    std::size_t observed = 0U;
    auto aggregate = run_with_allocation_failure(fail_after, observed, [&] {
      return UngroupedAggregateOperator::create(std::move(input), configured);
    });
    EXPECT_GT(observed, 0U);
    if (aggregate.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(aggregate.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(UngroupedAggregateAllocationFailureTest,
     PullClassifiesEveryOutputAllocationFailureAndReleasesCredit) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
    auto aggregate =
        UngroupedAggregateOperator::create(std::make_unique<EmptySource>(), definitions()).value();
    std::size_t observed = 0U;
    auto step = run_with_allocation_failure(fail_after, observed,
                                            [&] { return aggregate->next(resources); });
    EXPECT_GT(observed, 0U);
    if (step.has_value()) {
      reached_success = true;
      step = common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "drop aggregate output step"});
      aggregate.reset();
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
