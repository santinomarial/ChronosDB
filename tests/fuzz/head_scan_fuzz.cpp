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
  auto hostile = chronos::query::HeadScanOperator::create(
      hostile_resources, fixture.snapshot(), fixture.schemas(),
      chronos::columnar::test::id<chronos::schema::SchemaId>(
          (input.size() >= 6U && (input[5] & 1U) != 0U) ? chronos::query::test::kSuccessorSchemaId
                                                        : chronos::query::test::kInitialSchemaId),
      chronos::columnar::test::id<chronos::schema::TabletId>(
          input.size() >= 7U && (input[6] & 1U) != 0U ? 999U : chronos::query::test::kTabletId),
      std::move(hostile_projection), hostile_limits);
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
  return 0;
}
