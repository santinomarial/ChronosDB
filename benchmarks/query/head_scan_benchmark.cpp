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
  source(const std::uint32_t rows) const {
    HeadScanLimits limits;
    limits.chunk.maximum_rows = rows;
    limits.chunk.maximum_buffer_bytes = std::size_t{256U} * 1024U * 1024U;
    limits.chunk.maximum_retained_buffer_bytes = std::size_t{256U} * 1024U * 1024U;
    return HeadScanOperator::create(resources, head.snapshot(), head.schemas(),
                                    columnar::test::id<schema::SchemaId>(test::kInitialSchemaId),
                                    columnar::test::id<schema::TabletId>(test::kTabletId),
                                    {0U, 1U, 2U, 3U}, limits);
  }

  test::HeadFixture head;
  QueryResourceContext resources;
};

void materialize_one_head_chunk(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const HeadScanBenchmarkFixture fixture{rows};
  std::size_t measured_allocations = 0U;
  std::size_t measured_bytes = 0U;
  {
    auto source = fixture.source(rows);
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
    auto source = fixture.source(rows);
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
  state.SetLabel("four user columns; race-safe head storage canonicalized; source open excluded");
}

// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(materialize_one_head_chunk)->Arg(64)->Arg(1'024)->Arg(65'536);

} // namespace
} // namespace chronos::query
