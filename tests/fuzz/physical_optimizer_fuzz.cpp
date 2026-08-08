#include "chronos/query/physical_optimizer.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <span>
#include <utility>
#include <vector>

namespace {

[[noreturn]] void fail() {
  std::abort();
}

[[nodiscard]] chronos::query::PhysicalPipelinePlan plan(const std::uint32_t rows) {
  chronos::query::SortLimits limits;
  limits.maximum_rows = rows;
  limits.output_limits.maximum_rows = rows;
  std::vector<chronos::query::PhysicalPipelineStage> stages;
  stages.emplace_back(
      chronos::query::SortStage{.keys = {{.column_ordinal = 0U}}, .limits = limits});
  return chronos::query::PhysicalPipelinePlan::create(
             {{.type =
                   chronos::schema::LogicalType::create(chronos::schema::LogicalTypeKind::kInt64)
                       .value(),
               .nullable = false}},
             std::move(stages))
      .value();
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  if (size < 8U)
    return 0;
  const std::uint64_t rows = data[0];
  const std::uint32_t memory_rows = std::max<std::uint32_t>(data[1], 1U);
  const std::uint32_t input_chunk_rows = std::max<std::uint32_t>(data[2], 1U);
  const std::size_t tasks = static_cast<std::size_t>((data[3] % 8U) + 1U);

  chronos::query::PhysicalExecutionStatistics statistics{
      .source_task_count = tasks,
      .maximum_source_rows = rows,
      .estimated_source_work_units = static_cast<std::uint64_t>(data[4]) << 16U,
      .source_merge_requirement =
          (data[5] & 1U) == 0U ? chronos::query::PhysicalSourceMergeRequirement::kPreserveTaskOrder
                               : chronos::query::PhysicalSourceMergeRequirement::kOrderIndependent,
      .sort_stages = {
          {.stage_index = (data[5] & 2U) == 0U ? 0U : 1U,
           .maximum_rows = rows,
           .maximum_input_chunk_rows = input_chunk_rows,
           .maximum_output_logical_bytes = rows * 8U,
           .maximum_output_retained_bytes = static_cast<std::size_t>(rows * 32U + 1U),
           .maximum_spill_bytes = rows * 64U + 1U,
           .maximum_serialized_record_bytes = static_cast<std::size_t>((data[6] % 128U) + 1U)}}};
  chronos::query::PhysicalExecutionCapabilities capabilities;
  capabilities.available_parallel_workers = static_cast<std::size_t>((data[6] % 8U) + 1U);
  capabilities.parallel_limits.maximum_tasks = 8U;
  capabilities.parallel_limits.maximum_workers = capabilities.available_parallel_workers;
  capabilities.parallel_limits.maximum_ready_chunks = 4U;
  capabilities.parallel_limits.maximum_retained_configuration_bytes = 1U << 20U;
  if ((data[7] & 1U) != 0U) {
    chronos::query::SpillSortLimits spill;
    spill.maximum_rows = static_cast<std::uint64_t>(data[7]) + 1U;
    spill.maximum_runs = 256U;
    spill.maximum_spill_bytes = 1U << 20U;
    spill.maximum_serialized_record_bytes = 128U;
    spill.maximum_configuration_bytes = 1U << 20U;
    spill.run_sort_limits.maximum_rows = input_chunk_rows;
    spill.run_sort_limits.output_limits.maximum_rows = input_chunk_rows;
    spill.merge_output_limits.maximum_rows = input_chunk_rows;
    spill.merge_output_limits.output_limits.maximum_rows = input_chunk_rows;
    capabilities.spill_sorts.push_back({.stage_index = 0U, .limits = spill});
  }
  chronos::query::PhysicalOptimizerPolicy policy;
  policy.maximum_in_memory_sort_rows = memory_rows;
  policy.maximum_in_memory_sort_retained_bytes = 1U << 20U;
  policy.minimum_parallel_rows = data[2];
  policy.minimum_parallel_work_units = static_cast<std::uint64_t>(data[3]) << 12U;
  policy.parallel_worker_overhead_units = data[4];

  auto optimized = chronos::query::OptimizedPhysicalPipelinePlan::create(
      plan(memory_rows), std::move(statistics), std::move(capabilities), policy);
  if (!optimized.has_value())
    return 0;
  if (optimized->source_task_count() != tasks || optimized->sort_decisions().size() != 1U)
    fail();
  const auto& decision = optimized->sort_decisions().front();
  if (decision.stage_index != 0U)
    fail();
  if (decision.strategy == chronos::query::PhysicalSortStrategy::kInMemory && rows > memory_rows)
    fail();
  if (optimized->source_merge_strategy() ==
          chronos::query::PhysicalSourceMergeStrategy::kParallel &&
      optimized->selected_parallel_workers() < 2U) {
    fail();
  }
  return 0;
}
