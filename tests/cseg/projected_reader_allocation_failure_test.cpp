#include "chronos/cseg/projected_reader.hpp"
#include "chronos/cseg/temporal_format.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "cseg/cseg_test_fixture.hpp"
#include "support/failing_allocator.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::cseg {
namespace {

[[nodiscard]] schema::SchemaLineage valid_lineage() {
  const schema::ColumnId event_time = test::identifier<schema::ColumnId>(5U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(
      schema::ColumnDefinition::create(event_time, "event_time",
                                       test::type(schema::LogicalTypeKind::kTimestampNs), false)
          .value());
  schema::TableSchema table = schema::TableSchema::create(test::identifier<schema::TableId>(2U),
                                                          test::identifier<schema::SchemaId>(4U),
                                                          schema::SchemaVersion::initial(),
                                                          std::nullopt, std::move(columns),
                                                          {.event_time_column = event_time,
                                                           .physical_ordering_key = {event_time},
                                                           .partition_columns = {event_time},
                                                           .shard_key = {event_time},
                                                           .deduplication_key = {}})
                                  .value();
  return schema::SchemaLineage::create(std::move(table)).value();
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

TEST(CsegProjectedReaderAllocationFailureTest, ValidPlanningPerformsNoHeapAllocation) {
  const EncodedCsegPart encoded = test::make_valid_part();
  const schema::SchemaLineage lineage = valid_lineage();
  const auto reader = open_cseg_v1_projected_reader_exact(encoded.bytes(), lineage,
                                                          test::identifier<schema::SchemaId>(4U),
                                                          test::identifier<schema::TabletId>(3U));
  ASSERT_TRUE(reader.has_value());
  const std::array<std::uint32_t, 1> requested{0U};

  std::size_t observed = 99U;
  const auto plan = run_with_allocation_failure(
      0U, observed, [&] { return reader->plan_granule(0U, requested); });
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ(observed, 0U);
}

TEST(CsegProjectedReaderAllocationFailureTest, ExecutionClassifiesEveryOutputAllocationFailure) {
  const EncodedCsegPart encoded = test::make_valid_part();
  const schema::SchemaLineage lineage = valid_lineage();
  const auto reader = open_cseg_v1_projected_reader_exact(encoded.bytes(), lineage,
                                                          test::identifier<schema::SchemaId>(4U),
                                                          test::identifier<schema::TabletId>(3U));
  ASSERT_TRUE(reader.has_value());
  const std::array<std::uint32_t, 1> requested{0U};
  const auto plan = reader->plan_granule(0U, requested);
  ASSERT_TRUE(plan.has_value());

  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::size_t observed = 0U;
    auto granule = run_with_allocation_failure(fail_after, observed,
                                               [&] { return reader->read_granule(*plan); });
    EXPECT_GT(observed, 0U);
    if (granule.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(granule.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(CsegProjectedReaderAllocationFailureTest, OpenClassifiesEveryAllocationFailure) {
  const EncodedCsegPart encoded = test::make_valid_part();
  const schema::SchemaLineage lineage = valid_lineage();

  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::size_t observed = 0U;
    auto reader = run_with_allocation_failure(fail_after, observed, [&] {
      return open_cseg_v1_projected_reader_exact(encoded.bytes(), lineage,
                                                 test::identifier<schema::SchemaId>(4U),
                                                 test::identifier<schema::TabletId>(3U));
    });
    EXPECT_GT(observed, 0U);
    if (reader.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(reader.error().kind(), CsegProjectedReaderOpenErrorKind::kResourceLimit);
    EXPECT_EQ(reader.error().status().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(CsegProjectedReaderAllocationFailureTest, TemporalOpenClassifiesEveryAllocationFailure) {
  const EncodedCsegPart encoded = test::make_valid_temporal_part();
  const schema::SchemaLineage lineage = valid_lineage();

  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::size_t observed = 0U;
    auto reader = run_with_allocation_failure(fail_after, observed, [&] {
      return open_cseg_v2_temporal_projected_reader_exact(encoded.bytes(), lineage,
                                                          test::identifier<schema::SchemaId>(4U),
                                                          test::identifier<schema::TabletId>(3U));
    });
    EXPECT_GT(observed, 0U);
    if (reader.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(reader.error().kind(), CsegProjectedReaderOpenErrorKind::kResourceLimit);
    EXPECT_EQ(reader.error().status().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(CsegProjectedReaderAllocationFailureTest, TemporalPlanningPerformsNoHeapAllocation) {
  const EncodedCsegPart encoded = test::make_valid_temporal_part();
  const schema::SchemaLineage lineage = valid_lineage();
  const auto reader = open_cseg_v2_temporal_projected_reader_exact(
      encoded.bytes(), lineage, test::identifier<schema::SchemaId>(4U),
      test::identifier<schema::TabletId>(3U));
  ASSERT_TRUE(reader.has_value());
  const std::array<std::uint32_t, 1> requested{0U};

  std::size_t observed = 99U;
  const auto plan = run_with_allocation_failure(
      0U, observed, [&] { return reader->plan_granule(0U, requested); });
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ(observed, 0U);
  EXPECT_EQ(plan->decoded_page_count(), 1U + temporal_format::kSystemColumnCount);
}

TEST(CsegProjectedReaderAllocationFailureTest,
     TemporalExecutionClassifiesEveryOutputAllocationFailure) {
  const EncodedCsegPart encoded = test::make_valid_temporal_part();
  const schema::SchemaLineage lineage = valid_lineage();
  const auto reader = open_cseg_v2_temporal_projected_reader_exact(
      encoded.bytes(), lineage, test::identifier<schema::SchemaId>(4U),
      test::identifier<schema::TabletId>(3U));
  ASSERT_TRUE(reader.has_value());
  const std::array<std::uint32_t, 1> requested{0U};
  const auto plan = reader->plan_granule(0U, requested);
  ASSERT_TRUE(plan.has_value());

  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::size_t observed = 0U;
    auto granule = run_with_allocation_failure(fail_after, observed,
                                               [&] { return reader->read_granule(*plan); });
    EXPECT_GT(observed, 0U);
    if (granule.has_value()) {
      reached_success = true;
      EXPECT_EQ(granule->commit_source().row_count(), 2U);
      EXPECT_EQ(granule->logical_identity().row_count(), 2U);
      break;
    }
    EXPECT_EQ(granule.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

} // namespace
} // namespace chronos::cseg
