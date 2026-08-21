#include "chronos/query/spill_sort.hpp"
#include "support/counting_allocator.hpp"

#include <algorithm>
#include <benchmark/benchmark.h>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

inline constexpr std::size_t kMemoryLimit = std::size_t{128U} * 1024U * 1024U;

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-spill-benchmark-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

class ChunkSource final : public PhysicalOperator {
public:
  explicit ChunkSource(std::vector<AccountedVectorChunk> chunks) : chunks_(std::move(chunks)) {}
  common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    if (cursor_ == chunks_.size())
      return PhysicalOperatorStep::end();
    return PhysicalOperatorStep::chunk(std::move(chunks_[cursor_++]));
  }

private:
  std::vector<AccountedVectorChunk> chunks_;
  std::size_t cursor_{};
};

// Chunk offset, row count, and key cardinality are independent benchmark fixture controls.
// NOLINTBEGIN(bugprone-easily-swappable-parameters)
[[nodiscard]] AccountedVectorChunk make_chunk(const QueryResourceContext& resources,
                                              const std::uint32_t first, const std::uint32_t rows,
                                              const std::uint32_t distinct_keys) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(static_cast<std::size_t>(rows) * sizeof(std::int64_t));
  std::uint64_t random = 0x5350494c4c42454eULL + first;
  for (std::uint32_t row = 0U; row < rows; ++row) {
    random = random * 6'364'136'223'846'793'005ULL + 1'442'695'040'888'963'407ULL;
    const std::uint64_t value = random % distinct_keys;
    for (std::size_t byte = 0U; byte < sizeof(value); ++byte) {
      buffers.values[static_cast<std::size_t>(row) * sizeof(value) + byte] =
          static_cast<std::byte>((value >> (byte * 8U)) & 0xffU);
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
  VectorChunk vector =
      VectorChunk::create(std::move(columns), VectorSelection::all(rows).value()).value();
  const std::size_t charge = vector.retained_buffer_bytes() + 512U;
  return AccountedVectorChunk::create(std::move(vector), resources.reserve(charge).value(),
                                      resources)
      .value();
}
// NOLINTEND(bugprone-easily-swappable-parameters)

[[nodiscard]] std::unique_ptr<PhysicalOperator> pipeline(const QueryResourceContext& resources,
                                                         const std::filesystem::path& path,
                                                         const std::uint32_t rows,
                                                         const std::uint32_t run_rows,
                                                         const std::uint32_t distinct_keys) {
  std::vector<AccountedVectorChunk> chunks;
  for (std::uint32_t first = 0U; first < rows; first += run_rows) {
    chunks.push_back(make_chunk(resources, first, std::min(run_rows, rows - first), distinct_keys));
  }
  SpillSortLimits limits;
  limits.maximum_rows = rows;
  limits.maximum_runs = chunks.size();
  limits.maximum_spill_bytes = 64U << 20U;
  limits.maximum_serialized_record_bytes = 256U;
  limits.maximum_configuration_bytes = 4U << 20U;
  limits.run_sort_limits.maximum_rows = run_rows;
  limits.run_sort_limits.maximum_state_bytes = 4U << 20U;
  limits.run_sort_limits.output_limits.maximum_rows = run_rows;
  limits.merge_output_limits.maximum_rows = run_rows;
  limits.merge_output_limits.maximum_state_bytes = 4U << 20U;
  limits.merge_output_limits.output_limits.maximum_rows = run_rows;
  return SpillSortOperator::create(std::make_unique<ChunkSource>(std::move(chunks)),
                                   std::vector<VectorSortKey>{{.column_ordinal = 0U}},
                                   io::PosixDirectory::open(path.string()).value(), "benchmark",
                                   limits)
      .value();
}

void benchmark_spill_sort(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto run_rows = static_cast<std::uint32_t>(state.range(1));
  TemporaryDirectory temporary;
  QueryResourceContext resources = QueryResourceContext::create(kMemoryLimit).value();
  std::size_t measured_allocations = 0U;
  std::size_t measured_allocated_bytes = 0U;
  SpillSortMetrics measured_metrics;
  {
    std::unique_ptr<PhysicalOperator> sorted =
        pipeline(resources, temporary.path(), rows, run_rows, 17U);
    auto* spill = dynamic_cast<SpillSortOperator*>(sorted.get());
    chronos::benchmark_support::ScopedAllocationCounting allocations;
    for (;;) {
      auto step = sorted->next(resources);
      if (!step.has_value() || step->kind() == PhysicalOperatorStepKind::kEnd)
        break;
    }
    const chronos::benchmark_support::AllocationCounts counts = allocations.stop();
    measured_allocations = counts.allocations;
    measured_allocated_bytes = counts.allocated_bytes;
    measured_metrics = spill->metrics();
  }
  std::size_t observed_rows = 0U;
  for (auto _ : state) {
    static_cast<void>(_);
    state.PauseTiming();
    std::unique_ptr<PhysicalOperator> sorted =
        pipeline(resources, temporary.path(), rows, run_rows, 17U);
    state.ResumeTiming();
    for (;;) {
      auto step = sorted->next(resources);
      if (!step.has_value()) {
        state.SkipWithError("spill sort execution failed");
        break;
      }
      if (step->kind() == PhysicalOperatorStepKind::kEnd)
        break;
      observed_rows += step->chunk()->chunk().selected_row_count();
    }
  }
  benchmark::DoNotOptimize(observed_rows);
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(rows));
  state.counters["pull_allocated_bytes"] = static_cast<double>(measured_allocated_bytes);
  state.counters["pull_allocations"] = static_cast<double>(measured_allocations);
  state.counters["runs"] = static_cast<double>(measured_metrics.runs_written);
  state.counters["spill_bytes_written"] = static_cast<double>(measured_metrics.spill_bytes_written);
  state.counters["spill_bytes_read"] = static_cast<double>(measured_metrics.spill_bytes_read);
  state.SetLabel("one INT64 key; source construction excluded; ephemeral POSIX run I/O included");
}

BENCHMARK(benchmark_spill_sort)->Args({256, 64})->Args({1024, 128})->Args({2048, 256});

} // namespace
} // namespace chronos::query
