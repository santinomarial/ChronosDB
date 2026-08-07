#include "chronos/query/sort.hpp"
#include "support/counting_allocator.hpp"

#include <benchmark/benchmark.h>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

inline constexpr std::size_t kMemoryLimit = std::size_t{256U} * 1024U * 1024U;

class OneChunkSource final : public PhysicalOperator {
public:
  explicit OneChunkSource(AccountedVectorChunk chunk) : chunk_(std::move(chunk)) {}

  common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    if (!chunk_.has_value())
      return PhysicalOperatorStep::end();
    AccountedVectorChunk output = std::move(*chunk_);
    chunk_.reset();
    return PhysicalOperatorStep::chunk(std::move(output));
  }

private:
  std::optional<AccountedVectorChunk> chunk_;
};

// Rows and distinct-key cardinality are the benchmark's independent axes.
// NOLINTBEGIN(bugprone-easily-swappable-parameters)
[[nodiscard]] AccountedVectorChunk input(const QueryResourceContext& resources,
                                         const std::uint32_t rows,
                                         const std::uint32_t distinct_keys) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(static_cast<std::size_t>(rows) * sizeof(std::int64_t));
  std::uint64_t state = 0x534f52545f42454eULL;
  for (std::uint32_t row = 0U; row < rows; ++row) {
    state = state * 6'364'136'223'846'793'005ULL + 1'442'695'040'888'963'407ULL;
    const std::uint64_t bits = state % distinct_keys;
    for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
      buffers.values[static_cast<std::size_t>(row) * sizeof(bits) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
    }
  }
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(
      columnar::OwnedPhysicalColumn::create(
          {.type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
           .nullable = false,
           .row_count = rows,
           .null_count = 0U},
          std::move(buffers))
          .value());
  VectorChunk chunk = VectorChunk::create(std::move(columns), VectorSelection::all(rows).value(),
                                          {.maximum_rows = rows,
                                           .maximum_columns = 1U,
                                           .maximum_buffer_bytes = kMemoryLimit,
                                           .maximum_retained_buffer_bytes = kMemoryLimit})
                          .value();
  const std::size_t charge = chunk.retained_buffer_bytes() + 1'024U;
  return AccountedVectorChunk::create(std::move(chunk), resources.reserve(charge).value(),
                                      resources)
      .value();
}

[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
pipeline(const QueryResourceContext& resources, const std::uint32_t rows,
         const std::uint32_t distinct_keys) {
  return SortOperator::create(
      std::make_unique<OneChunkSource>(input(resources, rows, distinct_keys)),
      std::vector<VectorSortKey>{{.column_ordinal = 0U}},
      {.maximum_rows = rows,
       .maximum_keys = 1U,
       .maximum_state_bytes = kMemoryLimit,
       .output_limits = {.maximum_rows = rows,
                         .maximum_columns = 1U,
                         .maximum_buffer_bytes = kMemoryLimit,
                         .maximum_retained_buffer_bytes = kMemoryLimit}});
}
// NOLINTEND(bugprone-easily-swappable-parameters)

void benchmark_sort(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto keys = static_cast<std::uint32_t>(state.range(1));
  QueryResourceContext resources = QueryResourceContext::create(kMemoryLimit).value();
  std::size_t measured_allocations = 0U;
  std::size_t measured_allocated_bytes = 0U;
  {
    std::unique_ptr<PhysicalOperator> sorted = pipeline(resources, rows, keys).value();
    chronos::benchmark_support::ScopedAllocationCounting allocations;
    auto step = sorted->next(resources);
    const chronos::benchmark_support::AllocationCounts counts = allocations.stop();
    measured_allocations = counts.allocations;
    measured_allocated_bytes = counts.allocated_bytes;
    if (!step.has_value() || step->kind() != PhysicalOperatorStepKind::kChunk) {
      state.SkipWithError("sort benchmark preflight failed");
      return;
    }
  }
  std::size_t observed_rows = 0U;
  for (auto _ : state) {
    static_cast<void>(_);
    state.PauseTiming();
    std::unique_ptr<PhysicalOperator> sorted = pipeline(resources, rows, keys).value();
    state.ResumeTiming();
    auto step = sorted->next(resources);
    benchmark::DoNotOptimize(step);
    state.PauseTiming();
    if (!step.has_value() || step->kind() != PhysicalOperatorStepKind::kChunk) {
      state.SkipWithError("sort benchmark execution failed");
      state.ResumeTiming();
      break;
    }
    observed_rows += step->chunk()->chunk().selected_row_count();
    state.ResumeTiming();
  }
  benchmark::DoNotOptimize(observed_rows);
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(rows));
  state.counters["pull_allocated_bytes"] = static_cast<double>(measured_allocated_bytes);
  state.counters["pull_allocations"] = static_cast<double>(measured_allocations);
  state.counters["rows"] = rows;
  state.counters["distinct_keys"] = keys;
  state.SetLabel("one INT64 key; source construction excluded");
}

BENCHMARK(benchmark_sort)->Args({64, 64})->Args({1024, 1024})->Args({2048, 16});

} // namespace
} // namespace chronos::query
