#include "chronos/common/status.hpp"
#include "chronos/query/parallel_scheduler.hpp"
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

[[nodiscard]] std::vector<std::unique_ptr<PhysicalOperator>> make_tasks() {
  std::vector<std::unique_ptr<PhysicalOperator>> tasks;
  tasks.reserve(2U);
  tasks.push_back(std::make_unique<EmptySource>());
  tasks.push_back(std::make_unique<EmptySource>());
  return tasks;
}

TEST(ParallelSchedulerAllocationFailureTest, SharedReservationClassifiesAndRollsBackAllocation) {
  for (std::size_t fail_after = 0U; fail_after < 2U; ++fail_after) {
    QueryResourceContext resources = QueryResourceContext::create(4'096U).value();
    std::size_t observed = 0U;
    auto reservation = run_with_allocation_failure(
        fail_after, observed, [&] { return resources.reserve_shared(1'024U); });
    EXPECT_GT(observed, 0U);
    if (reservation.has_value()) {
      reservation->reset();
    } else {
      EXPECT_EQ(reservation.error().code(), common::StatusCode::kResourceExhausted);
    }
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
}

TEST(ParallelSchedulerAllocationFailureTest, CreationClassifiesEveryCallerOwnedAllocation) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
    std::vector<std::unique_ptr<PhysicalOperator>> tasks = make_tasks();
    std::size_t observed = 0U;
    auto scheduler = run_with_allocation_failure(fail_after, observed, [&] {
      return ParallelMergeOperator::create(resources, std::move(tasks),
                                           {.maximum_tasks = 2U,
                                            .maximum_workers = 2U,
                                            .maximum_ready_chunks = 2U,
                                            .maximum_retained_configuration_bytes = 1U << 20U});
    });
    EXPECT_GT(observed, 0U);
    if (scheduler.has_value()) {
      reached_success = true;
      scheduler->reset();
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      break;
    }
    EXPECT_TRUE(scheduler.error().code() == common::StatusCode::kResourceExhausted ||
                scheduler.error().code() == common::StatusCode::kUnavailable);
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(reached_success);
}

} // namespace
} // namespace chronos::query
