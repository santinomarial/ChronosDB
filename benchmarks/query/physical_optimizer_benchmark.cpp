#include "chronos/query/physical_optimizer.hpp"
#include "chronos/schema/logical_type.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] PhysicalColumnShape shape() {
  return {.type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
          .nullable = false};
}

[[nodiscard]] PhysicalPipelinePlan repeated_sort_plan(const std::size_t count) {
  std::vector<PhysicalPipelineStage> stages;
  stages.reserve(count);
  for (std::size_t index = 0U; index < count; ++index) {
    stages.emplace_back(SortStage{.keys = {{.column_ordinal = 0U}}, .limits = {}});
  }
  return PhysicalPipelinePlan::create({shape()}, std::move(stages)).value();
}

[[nodiscard]] PhysicalExecutionStatistics repeated_statistics(const std::size_t count) {
  PhysicalExecutionStatistics statistics{.source_task_count = 1U,
                                         .maximum_source_rows = 1'024U,
                                         .estimated_source_work_units = 1U << 20U,
                                         .sort_stages = {}};
  statistics.sort_stages.reserve(count);
  for (std::size_t index = 0U; index < count; ++index) {
    statistics.sort_stages.push_back({.stage_index = index,
                                      .maximum_rows = 1'024U,
                                      .maximum_input_chunk_rows = 1'024U,
                                      .maximum_output_logical_bytes = 8'192U,
                                      .maximum_output_retained_bytes = 32'768U,
                                      .maximum_spill_bytes = 64'000U,
                                      .maximum_serialized_record_bytes = 64U});
  }
  return statistics;
}

void build_and_select_physical_strategies(benchmark::State& state) {
  const auto stages = static_cast<std::size_t>(state.range(0));
  for (auto _ : state) {
    static_cast<void>(_);
    auto optimized = OptimizedPhysicalPipelinePlan::create(repeated_sort_plan(stages),
                                                           repeated_statistics(stages));
    benchmark::DoNotOptimize(optimized);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(stages));
  state.counters["sort_stages"] = static_cast<double>(stages);
}

class EmptySource final : public PhysicalOperator {
public:
  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    return PhysicalOperatorStep::end();
  }
};

[[nodiscard]] std::vector<std::unique_ptr<PhysicalOperator>>
empty_sources(const std::size_t count) {
  std::vector<std::unique_ptr<PhysicalOperator>> result;
  result.reserve(count);
  for (std::size_t index = 0U; index < count; ++index)
    result.push_back(std::make_unique<EmptySource>());
  return result;
}

void instantiate_selected_source_merge(benchmark::State& state) {
  const auto tasks = static_cast<std::size_t>(state.range(0));
  PhysicalExecutionCapabilities capabilities;
  capabilities.available_parallel_workers = 4U;
  capabilities.parallel_limits = {.maximum_tasks = 8U,
                                  .maximum_workers = 4U,
                                  .maximum_ready_chunks = 4U,
                                  .maximum_retained_configuration_bytes = 1U << 20U};
  PhysicalOptimizerPolicy policy;
  policy.minimum_parallel_rows = 1U;
  policy.minimum_parallel_work_units = 1U;
  policy.parallel_worker_overhead_units = 1U;
  OptimizedPhysicalPipelinePlan optimized =
      OptimizedPhysicalPipelinePlan::create(
          PhysicalPipelinePlan::create({shape()}, {}).value(),
          {.source_task_count = tasks,
           .maximum_source_rows = 1U << 20U,
           .estimated_source_work_units = 1U << 24U,
           .source_merge_requirement = PhysicalSourceMergeRequirement::kOrderIndependent,
           .sort_stages = {}},
          std::move(capabilities), policy)
          .value();
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  for (auto _ : state) {
    static_cast<void>(_);
    state.PauseTiming();
    auto sources = empty_sources(tasks);
    state.ResumeTiming();
    auto pipeline = optimized.instantiate(resources, std::move(sources)).value();
    benchmark::DoNotOptimize(pipeline->next(resources).value());
    pipeline.reset();
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(tasks));
  state.counters["source_tasks"] = static_cast<double>(tasks);
  state.counters["selected_workers"] = static_cast<double>(optimized.selected_parallel_workers());
}

BENCHMARK(build_and_select_physical_strategies)->Arg(1)->Arg(8)->Arg(64);
BENCHMARK(instantiate_selected_source_merge)->Arg(1)->Arg(4);

} // namespace
} // namespace chronos::query
