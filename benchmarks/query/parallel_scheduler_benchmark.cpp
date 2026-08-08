#include "chronos/query/parallel_scheduler.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

class ChunkSource final : public PhysicalOperator {
public:
  explicit ChunkSource(std::vector<AccountedVectorChunk> chunks) : chunks_(std::move(chunks)) {}

  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    if (next_ == chunks_.size())
      return PhysicalOperatorStep::end();
    return PhysicalOperatorStep::chunk(std::move(chunks_[next_++]));
  }

private:
  std::vector<AccountedVectorChunk> chunks_;
  std::size_t next_{};
};

[[nodiscard]] AccountedVectorChunk empty_chunk(const QueryResourceContext& resources,
                                               const std::uint32_t rows) {
  VectorChunk chunk = VectorChunk::create({}, VectorSelection::all(rows).value(),
                                          {.maximum_rows = rows,
                                           .maximum_columns = 1U,
                                           .maximum_buffer_bytes = 1U << 20U,
                                           .maximum_retained_buffer_bytes = 1U << 20U})
                          .value();
  const std::size_t charge = chunk.retained_buffer_bytes() + 1U;
  return AccountedVectorChunk::create(std::move(chunk), resources.reserve(charge).value(),
                                      resources)
      .value();
}

void merge_independent_chunks(benchmark::State& state) {
  const auto task_count = static_cast<std::size_t>(state.range(0));
  const auto worker_count = static_cast<std::size_t>(state.range(1));
  constexpr std::size_t chunks_per_task = 8U;
  std::size_t published = 0U;
  for (auto _ : state) {
    static_cast<void>(_);
    state.PauseTiming();
    QueryResourceContext resources = QueryResourceContext::create(8U << 20U).value();
    std::vector<std::unique_ptr<PhysicalOperator>> tasks;
    tasks.reserve(task_count);
    for (std::size_t task = 0U; task < task_count; ++task) {
      std::vector<AccountedVectorChunk> chunks;
      chunks.reserve(chunks_per_task);
      for (std::size_t chunk = 0U; chunk < chunks_per_task; ++chunk)
        chunks.push_back(empty_chunk(resources, 1'024U));
      tasks.push_back(std::make_unique<ChunkSource>(std::move(chunks)));
    }
    state.ResumeTiming();

    auto scheduler =
        ParallelMergeOperator::create(resources, std::move(tasks),
                                      {.maximum_tasks = task_count,
                                       .maximum_workers = worker_count,
                                       .maximum_ready_chunks = worker_count,
                                       .maximum_retained_configuration_bytes = 1U << 20U})
            .value();
    for (;;) {
      common::Result<PhysicalOperatorStep> step = scheduler->next(resources);
      if (step->kind() == PhysicalOperatorStepKind::kEnd)
        break;
      AccountedVectorChunk chunk = std::move(*step).take_chunk().value();
      benchmark::DoNotOptimize(chunk.chunk().selected_row_count());
    }
    published += scheduler->metrics().chunks_published;
    benchmark::DoNotOptimize(scheduler);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(published));
  state.counters["tasks"] = static_cast<double>(task_count);
  state.counters["workers"] = static_cast<double>(worker_count);
  state.counters["chunks_per_task"] = static_cast<double>(chunks_per_task);
}

BENCHMARK(merge_independent_chunks)->Args({1, 1})->Args({4, 1})->Args({4, 2})->Args({4, 4});

} // namespace
} // namespace chronos::query
