#include "chronos/query/physical_optimizer.hpp"
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

class EmptySource final : public PhysicalOperator {
public:
  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    return PhysicalOperatorStep::end();
  }
};

[[nodiscard]] PhysicalPipelinePlan plan() {
  SortLimits limits;
  limits.maximum_rows = 8U;
  limits.output_limits.maximum_rows = 8U;
  std::vector<PhysicalPipelineStage> stages;
  stages.emplace_back(SortStage{.keys = {{.column_ordinal = 0U}}, .limits = limits});
  return PhysicalPipelinePlan::create(
             {{.type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
               .nullable = false}},
             std::move(stages))
      .value();
}

[[nodiscard]] PhysicalExecutionStatistics statistics() {
  return {.source_task_count = 2U,
          .maximum_source_rows = 8U,
          .estimated_source_work_units = 1'024U,
          .source_merge_requirement = PhysicalSourceMergeRequirement::kPreserveTaskOrder,
          .sort_stages = {{.stage_index = 0U,
                           .maximum_rows = 8U,
                           .maximum_input_chunk_rows = 4U,
                           .maximum_output_logical_bytes = 64U,
                           .maximum_output_retained_bytes = 1'024U,
                           .maximum_spill_bytes = 2'048U,
                           .maximum_serialized_record_bytes = 64U}}};
}

[[nodiscard]] std::vector<std::unique_ptr<PhysicalOperator>> sources() {
  std::vector<std::unique_ptr<PhysicalOperator>> result;
  result.reserve(2U);
  result.push_back(std::make_unique<EmptySource>());
  result.push_back(std::make_unique<EmptySource>());
  return result;
}

TEST(PhysicalOptimizerAllocationFailureTest, CreationClassifiesEveryOwnedAllocation) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    PhysicalPipelinePlan input_plan = plan();
    PhysicalExecutionStatistics input_statistics = statistics();
    std::optional<common::Result<OptimizedPhysicalPipelinePlan>> optimized;
    std::size_t observed = 0U;
    {
      ::chronos::test::ScopedAllocationFailure failure{fail_after};
      optimized.emplace(OptimizedPhysicalPipelinePlan::create(std::move(input_plan),
                                                              std::move(input_statistics), {}, {}));
      observed = failure.observed_allocations();
      failure.disable();
    }
    EXPECT_GT(observed, 0U);
    if (optimized->has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(optimized->error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(PhysicalOptimizerAllocationFailureTest, SerialInstantiationReleasesCreditAndSources) {
  OptimizedPhysicalPipelinePlan optimized =
      OptimizedPhysicalPipelinePlan::create(plan(), statistics(), {}, {}).value();
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
    std::vector<std::unique_ptr<PhysicalOperator>> inputs = sources();
    std::optional<common::Result<std::unique_ptr<PhysicalOperator>>> instantiated;
    std::size_t observed = 0U;
    {
      ::chronos::test::ScopedAllocationFailure failure{fail_after};
      instantiated.emplace(optimized.instantiate(resources, std::move(inputs)));
      observed = failure.observed_allocations();
      failure.disable();
    }
    EXPECT_GT(observed, 0U);
    if (instantiated->has_value()) {
      reached_success = true;
      instantiated->value().reset();
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      break;
    }
    EXPECT_EQ(instantiated->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(reached_success);
}

} // namespace
} // namespace chronos::query
