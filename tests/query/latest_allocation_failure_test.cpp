#include "chronos/common/status.hpp"
#include "chronos/query/latest.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <memory>
#include <optional>

namespace chronos::query {
namespace {

class EmptySource final : public PhysicalOperator {
public:
  common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    return PhysicalOperatorStep::end();
  }
};

TEST(LatestByAllocationFailureTest, CreationClassifiesEveryOwnedAllocationFailure) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    std::unique_ptr<PhysicalOperator> source = std::make_unique<EmptySource>();
    VectorLatestByDefinition definition{.key_column_ordinals = {0U},
                                        .timestamp_column_ordinal = 1U,
                                        .physical_ordering_key_ordinals = {1U},
                                        .row_version_first_column_ordinal = 2U};
    std::optional<common::Result<std::unique_ptr<PhysicalOperator>>> result;
    std::size_t observed = 0U;
    {
      ::chronos::test::ScopedAllocationFailure failure{fail_after};
      result.emplace(LatestByOperator::create(std::move(source), std::move(definition)));
      observed = failure.observed_allocations();
      failure.disable();
    }
    EXPECT_GT(observed, 0U);
    if (result->has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

} // namespace
} // namespace chronos::query
