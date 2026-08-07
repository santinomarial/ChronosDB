#include "chronos/query/head_scan.hpp"
#include "chronos/schema/identity.hpp"
#include "columnar/columnar_test_support.hpp"
#include "query/head_scan_test_fixture.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

template <typename Operation>
[[nodiscard]] auto run_with_head_allocation_failure(const std::size_t fail_after,
                                                    std::size_t& observed, Operation&& operation) {
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

TEST(HeadScanAllocationFailureTest, CreationClassifiesEveryRetainedAllocationFailure) {
  test::HeadFixture fixture{4U};
  fixture.publish({.range = {.first_row = 0U, .row_count = 4U}, .record_sequence = 1U});
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    QueryResourceContext resources =
        QueryResourceContext::create(std::size_t{16U} * 1024U * 1024U).value();
    std::vector<std::uint32_t> ordinals{0U, 1U, 2U, 3U};
    std::size_t observed = 0U;
    auto source = run_with_head_allocation_failure(fail_after, observed, [&] {
      return HeadScanOperator::create(resources, fixture.snapshot(), fixture.schemas(),
                                      columnar::test::id<schema::SchemaId>(test::kInitialSchemaId),
                                      columnar::test::id<schema::TabletId>(test::kTabletId),
                                      std::move(ordinals));
    });
    EXPECT_GT(observed, 0U);
    if (source.has_value()) {
      reached_success = true;
      source->reset();
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      break;
    }
    EXPECT_EQ(source.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(reached_success);
}

TEST(HeadScanAllocationFailureTest, ExactFactoryClassifiesHelperAndWrapperAllocationFailures) {
  test::HeadFixture fixture{4U};
  fixture.publish({.range = {.first_row = 0U, .row_count = 4U}, .record_sequence = 1U});
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    QueryResourceContext resources =
        QueryResourceContext::create(std::size_t{16U} * 1024U * 1024U).value();
    std::vector<std::uint32_t> ordinals{4U};
    std::size_t observed = 0U;
    auto source = run_with_head_allocation_failure(fail_after, observed, [&] {
      return HeadScanOperator::create_event_time_filtered(
          resources, fixture.snapshot(), fixture.schemas(),
          columnar::test::id<schema::SchemaId>(test::kSuccessorSchemaId),
          columnar::test::id<schema::TabletId>(test::kTabletId), std::move(ordinals),
          {.lower = TimestampRangeBound{.value = 10, .inclusive = true},
           .upper = TimestampRangeBound{.value = 10, .inclusive = true}});
    });
    EXPECT_GT(observed, 0U);
    if (source.has_value()) {
      reached_success = true;
      source->reset();
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      break;
    }
    EXPECT_EQ(source.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(reached_success);
}

TEST(HeadScanAllocationFailureTest, PullClassifiesEveryCanonicalOutputAllocationFailure) {
  test::HeadFixture fixture{4U};
  fixture.publish({.range = {.first_row = 0U, .row_count = 4U}, .record_sequence = 1U});
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    QueryResourceContext resources =
        QueryResourceContext::create(std::size_t{16U} * 1024U * 1024U).value();
    HeadScanLimits limits;
    limits.row_version_columns = RowVersionScanMode::kAppend;
    auto source = HeadScanOperator::create(
        resources, fixture.snapshot(), fixture.schemas(),
        columnar::test::id<schema::SchemaId>(test::kSuccessorSchemaId),
        columnar::test::id<schema::TabletId>(test::kTabletId), {4U, 1U, 2U, 3U, 0U}, limits);
    ASSERT_TRUE(source.has_value()) << source.error().to_string();
    const std::size_t source_charge = resources.reserved_memory_bytes();
    std::size_t observed = 0U;
    auto step = run_with_head_allocation_failure(fail_after, observed,
                                                 [&] { return (*source)->next(resources); });
    EXPECT_GT(observed, 0U);
    if (step.has_value()) {
      reached_success = true;
      step = common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "drop owning head step"});
      source->reset();
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      break;
    }
    EXPECT_EQ(step.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(resources.is_cancelled());
    EXPECT_EQ(resources.reserved_memory_bytes(), source_charge);
    source->reset();
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(reached_success);
}

} // namespace
} // namespace chronos::query
