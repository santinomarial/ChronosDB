#include "chronos/cseg/part_codec.hpp"
#include "chronos/query/cseg_scan.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "cseg/cseg_test_fixture.hpp"
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

[[nodiscard]] schema::SchemaLineage valid_lineage() {
  const schema::ColumnId event_time = cseg::test::identifier<schema::ColumnId>(5U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(
      schema::ColumnDefinition::create(
          event_time, "event_time", cseg::test::type(schema::LogicalTypeKind::kTimestampNs), false)
          .value());
  schema::TableSchema table =
      schema::TableSchema::create(
          cseg::test::identifier<schema::TableId>(2U), cseg::test::identifier<schema::SchemaId>(4U),
          schema::SchemaVersion::initial(), std::nullopt, std::move(columns),
          {.event_time_column = event_time,
           .physical_ordering_key = {event_time},
           .partition_columns = {event_time},
           .shard_key = {event_time},
           .deduplication_key = {}})
          .value();
  return schema::SchemaLineage::create(std::move(table)).value();
}

[[nodiscard]] CsegPartPin valid_pin() {
  auto owner = std::make_shared<const cseg::EncodedCsegPart>(
      cseg::test::make_valid_part(cseg::PageCompression::kZstd));
  return CsegPartPin::create(owner, owner->bytes(),
                             owner->retained_buffer_bytes() + sizeof(cseg::EncodedCsegPart) + 64U)
      .value();
}

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

TEST(CsegScanAllocationFailureTest, CreationClassifiesEveryRetainedAllocationFailure) {
  const schema::SchemaLineage lineage = valid_lineage();
  const CsegPartPin part = valid_pin();
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto resources = QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
    std::vector<std::uint32_t> requested{0U};
    std::size_t observed = 0U;
    auto source = run_with_allocation_failure(fail_after, observed, [&] {
      return CsegScanOperator::create(
          resources, part, lineage, cseg::test::identifier<schema::SchemaId>(4U),
          cseg::test::identifier<schema::TabletId>(3U), std::move(requested));
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

TEST(CsegScanAllocationFailureTest, PullClassifiesEveryOutputAllocationFailureAndReleasesCredit) {
  const schema::SchemaLineage lineage = valid_lineage();
  const CsegPartPin part = valid_pin();
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto resources = QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
    auto source = CsegScanOperator::create(
        resources, part, lineage, cseg::test::identifier<schema::SchemaId>(4U),
        cseg::test::identifier<schema::TabletId>(3U), std::vector<std::uint32_t>{0U});
    ASSERT_TRUE(source.has_value());
    const std::size_t source_charge = resources.reserved_memory_bytes();
    std::size_t observed = 0U;
    auto step = run_with_allocation_failure(fail_after, observed,
                                            [&] { return (*source)->next(resources); });
    EXPECT_GT(observed, 0U);
    if (step.has_value()) {
      reached_success = true;
      step = common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "replace owning step"});
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
