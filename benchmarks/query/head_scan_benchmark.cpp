#include "chronos/query/head_scan.hpp"
#include "chronos/schema/identity.hpp"
#include "columnar/columnar_test_support.hpp"
#include "query/head_scan_test_fixture.hpp"
#include "support/counting_allocator.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace chronos::query {
namespace {

struct HeadScanBenchmarkFixture {
  explicit HeadScanBenchmarkFixture(const std::uint32_t rows)
      : head(rows),
        resources(QueryResourceContext::create(std::size_t{1U} * 1024U * 1024U * 1024U).value()) {
    head.publish({.range = {.first_row = 0U, .row_count = rows}, .record_sequence = 1U});
  }

  [[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
  source(const std::uint32_t rows, const RowVersionScanMode row_version_columns) const {
    HeadScanLimits limits;
    limits.chunk.maximum_rows = rows;
    limits.chunk.maximum_buffer_bytes = std::size_t{256U} * 1024U * 1024U;
    limits.chunk.maximum_retained_buffer_bytes = std::size_t{256U} * 1024U * 1024U;
    limits.row_version_columns = row_version_columns;
    return HeadScanOperator::create(resources, head.snapshot(), head.schemas(),
                                    columnar::test::id<schema::SchemaId>(test::kInitialSchemaId),
                                    columnar::test::id<schema::TabletId>(test::kTabletId),
                                    {0U, 1U, 2U, 3U}, limits);
  }

  // Output capacity and exact event time are independent benchmark fixture controls.
  // NOLINTBEGIN(bugprone-easily-swappable-parameters)
  [[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
  exact_source(const std::uint32_t rows, const std::int64_t event_time) const {
    HeadScanLimits limits;
    limits.chunk.maximum_rows = rows;
    limits.chunk.maximum_buffer_bytes = std::size_t{256U} * 1024U * 1024U;
    limits.chunk.maximum_retained_buffer_bytes = std::size_t{256U} * 1024U * 1024U;
    return HeadScanOperator::create_event_time_filtered(
        resources, head.snapshot(), head.schemas(),
        columnar::test::id<schema::SchemaId>(test::kInitialSchemaId),
        columnar::test::id<schema::TabletId>(test::kTabletId), {1U},
        {.lower = TimestampRangeBound{.value = event_time, .inclusive = true},
         .upper = TimestampRangeBound{.value = event_time, .inclusive = true}},
        limits);
  }
  // NOLINTEND(bugprone-easily-swappable-parameters)

  test::HeadFixture head;
  QueryResourceContext resources;
};

void materialize_one_head_chunk(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const RowVersionScanMode row_version_columns =
      state.range(1) == 0 ? RowVersionScanMode::kOmit : RowVersionScanMode::kAppend;
  const HeadScanBenchmarkFixture fixture{rows};
  std::size_t measured_allocations = 0U;
  std::size_t measured_bytes = 0U;
  {
    auto source = fixture.source(rows, row_version_columns);
    if (!source.has_value()) {
      const std::string message = source.error().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark_support::ScopedAllocationCounting counting;
    auto step = (*source)->next(fixture.resources);
    measured_allocations = counting.stop().allocations;
    if (!step.has_value() || step->chunk() == nullptr) {
      const std::string message =
          step.has_value() ? "head scan returned no benchmark chunk" : step.error().to_string();
      state.SkipWithError(message);
      return;
    }
    measured_bytes = step->chunk()->chunk().buffer_bytes();
  }

  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto source = fixture.source(rows, row_version_columns);
    if (!source.has_value()) {
      const std::string message = source.error().to_string();
      state.SkipWithError(message);
      return;
    }
    state.ResumeTiming();
    auto step = (*source)->next(fixture.resources);
    benchmark::DoNotOptimize(step);
    state.PauseTiming();
    if (!step.has_value() || step->chunk() == nullptr) {
      const std::string message =
          step.has_value() ? "head scan returned no benchmark chunk" : step.error().to_string();
      state.SkipWithError(message);
      return;
    }
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * rows);
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(measured_bytes));
  state.counters["pull_allocations"] = static_cast<double>(measured_allocations);
  state.counters["physical_rows"] = static_cast<double>(rows);
  state.SetLabel(
      row_version_columns == RowVersionScanMode::kAppend
          ? "four user columns plus materialized row-version suffix; source open excluded"
          : "four user columns; race-safe head storage canonicalized; source open excluded");
}

// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(materialize_one_head_chunk)->ArgsProduct({{64, 1'024, 65'536}, {0, 1}});

void materialize_and_exact_filter_one_head_chunk(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const HeadScanBenchmarkFixture fixture{rows};
  const std::int64_t selected_event_time = static_cast<std::int64_t>(rows / 2U) * 10;
  std::size_t measured_allocations = 0U;
  std::size_t measured_bytes = 0U;
  {
    auto source = fixture.exact_source(rows, selected_event_time);
    if (!source.has_value()) {
      const std::string message = source.error().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark_support::ScopedAllocationCounting counting;
    auto step = (*source)->next(fixture.resources);
    measured_allocations = counting.stop().allocations;
    if (!step.has_value() || step->chunk() == nullptr ||
        step->chunk()->chunk().selected_row_count() != 1U ||
        step->chunk()->chunk().column_count() != 1U) {
      const std::string message = step.has_value()
                                      ? "exact head scan returned the wrong output shape"
                                      : step.error().to_string();
      state.SkipWithError(message);
      return;
    }
    measured_bytes = step->chunk()->chunk().buffer_bytes();
  }

  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto source = fixture.exact_source(rows, selected_event_time);
    if (!source.has_value()) {
      const std::string message = source.error().to_string();
      state.SkipWithError(message);
      return;
    }
    state.ResumeTiming();
    auto step = (*source)->next(fixture.resources);
    benchmark::DoNotOptimize(step);
    state.PauseTiming();
    if (!step.has_value() || step->chunk() == nullptr ||
        step->chunk()->chunk().selected_row_count() != 1U ||
        step->chunk()->chunk().column_count() != 1U) {
      const std::string message = step.has_value()
                                      ? "exact head scan returned the wrong output shape"
                                      : step.error().to_string();
      state.SkipWithError(message);
      return;
    }
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * rows);
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(measured_bytes));
  state.counters["matching_rows"] = 1.0;
  state.counters["physical_rows"] = static_cast<double>(rows);
  state.counters["pull_allocations"] = static_cast<double>(measured_allocations);
  state.SetLabel("label plus hidden event time materialized; exact point filter; helper removed; "
                 "open excluded");
}

// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(materialize_and_exact_filter_one_head_chunk)->Arg(64)->Arg(1'024)->Arg(65'536);

} // namespace
} // namespace chronos::query
