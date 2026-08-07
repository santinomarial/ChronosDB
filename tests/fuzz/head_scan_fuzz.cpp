#include "chronos/query/head_scan.hpp"
#include "chronos/schema/identity.hpp"
#include "columnar/columnar_test_support.hpp"
#include "query/head_scan_test_fixture.hpp"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <span>
#include <utility>
#include <vector>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const std::span<const std::uint8_t> input{data, size};
  const std::uint32_t rows = input.empty() ? 1U : static_cast<std::uint32_t>(input.front()) + 1U;
  chronos::query::test::HeadFixture fixture{rows};
  fixture.publish({.range = {.first_row = 0U, .row_count = rows}, .record_sequence = 1U});

  std::vector<std::uint32_t> hostile_projection;
  const std::size_t hostile_count = input.size() > 32U ? 32U : input.size();
  hostile_projection.reserve(hostile_count);
  for (std::size_t index = 0U; index < hostile_count; ++index)
    hostile_projection.push_back(input[index] % 8U);
  auto hostile_resources =
      chronos::query::QueryResourceContext::create(std::size_t{16U} * 1024U * 1024U).value();
  chronos::query::HeadScanLimits hostile_limits;
  hostile_limits.chunk.maximum_rows =
      input.size() < 2U ? 1U : static_cast<std::uint32_t>(input[1]) + 1U;
  hostile_limits.chunk.maximum_columns = input.size() < 3U ? 1U : input[2];
  hostile_limits.chunk.maximum_buffer_bytes =
      input.size() < 4U ? 1U : static_cast<std::size_t>(input[3]) * 256U + 1U;
  hostile_limits.chunk.maximum_retained_buffer_bytes =
      input.size() < 5U ? 1U : static_cast<std::size_t>(input[4]) * 512U + 1U;
  hostile_limits.row_version_columns = input.size() >= 10U && (input[9] & 1U) != 0U
                                           ? chronos::query::RowVersionScanMode::kAppend
                                           : chronos::query::RowVersionScanMode::kOmit;
  const chronos::schema::SchemaId hostile_schema =
      chronos::columnar::test::id<chronos::schema::SchemaId>(
          (input.size() >= 6U && (input[5] & 1U) != 0U) ? chronos::query::test::kSuccessorSchemaId
                                                        : chronos::query::test::kInitialSchemaId);
  const chronos::schema::TabletId hostile_tablet =
      chronos::columnar::test::id<chronos::schema::TabletId>(
          input.size() >= 7U && (input[6] & 1U) != 0U ? 999U : chronos::query::test::kTabletId);
  auto hostile = input.size() >= 9U && (input[8] & 1U) != 0U
                     ? chronos::query::HeadScanOperator::create_event_time_filtered(
                           hostile_resources, fixture.snapshot(), fixture.schemas(), hostile_schema,
                           hostile_tablet, std::move(hostile_projection),
                           {.lower =
                                chronos::query::TimestampRangeBound{
                                    .value = static_cast<std::int8_t>(input.front()) * 10,
                                    .inclusive = (input.front() & 2U) != 0U},
                            .upper =
                                chronos::query::TimestampRangeBound{
                                    .value = static_cast<std::int8_t>(input.back()) * 10,
                                    .inclusive = (input.back() & 2U) != 0U}},
                           hostile_limits)
                     : chronos::query::HeadScanOperator::create(
                           hostile_resources, fixture.snapshot(), fixture.schemas(), hostile_schema,
                           hostile_tablet, std::move(hostile_projection), hostile_limits);
  if (hostile.has_value()) {
    if (input.size() >= 8U && (input[7] & 1U) != 0U)
      static_cast<void>(hostile_resources.request_cancel());
    for (std::size_t pull = 0U; pull < 4U; ++pull) {
      auto step = (*hostile)->next(hostile_resources);
      if (!step.has_value() || step->kind() == chronos::query::PhysicalOperatorStepKind::kEnd)
        break;
      if (step->chunk() != nullptr && step->chunk()->chunk().selected_row_count() != 0U &&
          step->chunk()->chunk().column_count() != 0U) {
        static_cast<void>(step->chunk()->chunk().cell({.column_ordinal = 0U, .selected_row = 0U}));
      }
    }
  }

  auto resources =
      chronos::query::QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  chronos::query::HeadScanLimits limits;
  limits.chunk.maximum_rows = input.size() < 2U ? 1U : static_cast<std::uint32_t>(input[1]) + 1U;
  limits.row_version_columns = !input.empty() && (input.back() & 16U) != 0U
                                   ? chronos::query::RowVersionScanMode::kAppend
                                   : chronos::query::RowVersionScanMode::kOmit;
  auto source = chronos::query::HeadScanOperator::create(
      resources, fixture.snapshot(), fixture.schemas(),
      chronos::columnar::test::id<chronos::schema::SchemaId>(
          chronos::query::test::kSuccessorSchemaId),
      chronos::columnar::test::id<chronos::schema::TabletId>(chronos::query::test::kTabletId),
      {4U, 1U, 2U, 3U, 0U}, limits);
  if (!source.has_value())
    return 0;
  std::size_t observed_rows = 0U;
  for (;;) {
    auto step = (*source)->next(resources);
    if (!step.has_value())
      return 0;
    if (step->kind() == chronos::query::PhysicalOperatorStepKind::kEnd)
      break;
    observed_rows += step->chunk()->chunk().selected_row_count();
  }
  if (observed_rows != rows)
    std::abort();

  auto exact_resources =
      chronos::query::QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  const std::uint32_t selected_row = input.empty() ? 0U : input.back() % rows;
  const std::int64_t selected_time = static_cast<std::int64_t>(selected_row) * 10;
  const bool lower_inclusive = input.empty() || (input.front() & 2U) != 0U;
  const bool upper_inclusive = input.empty() || (input.back() & 2U) != 0U;
  auto exact = chronos::query::HeadScanOperator::create_event_time_filtered(
      exact_resources, fixture.snapshot(), fixture.schemas(),
      chronos::columnar::test::id<chronos::schema::SchemaId>(
          chronos::query::test::kSuccessorSchemaId),
      chronos::columnar::test::id<chronos::schema::TabletId>(chronos::query::test::kTabletId), {4U},
      {.lower = chronos::query::TimestampRangeBound{.value = selected_time,
                                                    .inclusive = lower_inclusive},
       .upper = chronos::query::TimestampRangeBound{.value = selected_time,
                                                    .inclusive = upper_inclusive}},
      limits);
  if (!exact.has_value())
    return 0;
  std::size_t exact_rows = 0U;
  for (;;) {
    auto step = (*exact)->next(exact_resources);
    if (!step.has_value())
      return 0;
    if (step->kind() == chronos::query::PhysicalOperatorStepKind::kEnd)
      break;
    const std::size_t expected_columns =
        1U + (limits.row_version_columns == chronos::query::RowVersionScanMode::kAppend
                  ? chronos::query::kVectorRowVersionColumnCount
                  : 0U);
    if (step->chunk()->chunk().column_count() != expected_columns)
      std::abort();
    exact_rows += step->chunk()->chunk().selected_row_count();
  }
  if (exact_rows != (lower_inclusive && upper_inclusive ? 1U : 0U))
    std::abort();
  return 0;
}
