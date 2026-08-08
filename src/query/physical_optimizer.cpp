#include "chronos/query/physical_optimizer.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] constexpr std::uint64_t saturating_add(const std::uint64_t left,
                                                     const std::uint64_t right) noexcept {
  return right > std::numeric_limits<std::uint64_t>::max() - left
             ? std::numeric_limits<std::uint64_t>::max()
             : left + right;
}

[[nodiscard]] constexpr std::uint64_t saturating_multiply(const std::uint64_t left,
                                                          const std::uint64_t right) noexcept {
  return left != 0U && right > std::numeric_limits<std::uint64_t>::max() / left
             ? std::numeric_limits<std::uint64_t>::max()
             : left * right;
}

[[nodiscard]] constexpr std::uint64_t ceiling_divide(const std::uint64_t value,
                                                     const std::uint64_t divisor) noexcept {
  return value == 0U ? 0U : 1U + ((value - 1U) / divisor);
}

struct SortCostInput {
  std::uint64_t rows;
  std::uint64_t run_rows;
  std::size_t keys;
};

[[nodiscard]] constexpr std::uint64_t comparison_units(const SortCostInput input) noexcept {
  if (input.rows < 2U || input.keys == 0U)
    return 0U;
  const std::uint64_t width = std::min(input.rows, input.run_rows);
  const std::uint64_t levels = static_cast<std::uint64_t>(std::bit_width(width - 1U));
  return saturating_multiply(saturating_multiply(input.rows, levels), input.keys);
}

[[nodiscard]] constexpr bool is_in_memory_eligible(const PhysicalSortStageEstimate& estimate,
                                                   const SortStage& stage,
                                                   const PhysicalOptimizerPolicy& policy) noexcept {
  return estimate.maximum_rows <= policy.maximum_in_memory_sort_rows &&
         estimate.maximum_rows <= stage.limits.maximum_rows &&
         estimate.maximum_rows <= stage.limits.output_limits.maximum_rows &&
         estimate.maximum_output_logical_bytes <= stage.limits.output_limits.maximum_buffer_bytes &&
         estimate.maximum_output_retained_bytes <= policy.maximum_in_memory_sort_retained_bytes &&
         estimate.maximum_output_retained_bytes <=
             stage.limits.output_limits.maximum_retained_buffer_bytes;
}

[[nodiscard]] constexpr bool is_external_eligible(const PhysicalSortStageEstimate& estimate,
                                                  const SortStage& stage,
                                                  const SpillSortLimits& limits) noexcept {
  return estimate.maximum_rows <= limits.maximum_rows &&
         estimate.maximum_input_chunk_rows <= limits.run_sort_limits.maximum_rows &&
         estimate.maximum_spill_bytes <= limits.maximum_spill_bytes &&
         estimate.maximum_serialized_record_bytes <= limits.maximum_serialized_record_bytes &&
         stage.keys.size() <= limits.run_sort_limits.maximum_keys &&
         stage.keys.size() <= limits.merge_output_limits.maximum_keys;
}

[[nodiscard]] common::Result<std::size_t>
strategy_retained_bytes(const PhysicalPipelinePlan& plan,
                        const std::vector<PhysicalSortStrategyDecision>& decisions) {
  const std::optional<std::size_t> decision_bytes =
      common::checked_multiply(decisions.capacity(), sizeof(PhysicalSortStrategyDecision));
  if (!decision_bytes.has_value())
    return common::make_unexpected(exhausted("physical optimizer configuration overflowed"));
  std::optional<std::size_t> total =
      common::checked_add(sizeof(OptimizedPhysicalPipelinePlan), *decision_bytes);
  if (total.has_value())
    total = common::checked_add(*total, plan.retained_configuration_bytes());
  if (!total.has_value())
    return common::make_unexpected(exhausted("physical optimizer configuration overflowed"));
  return *total;
}

class SerialSourceMerge final : public PhysicalOperator {
public:
  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(const QueryResourceContext& resources,
         std::vector<std::unique_ptr<PhysicalOperator>> sources,
         const std::size_t maximum_configuration_bytes) {
    if (sources.empty())
      return common::make_unexpected(invalid("serial source merge requires a source"));
    for (const std::unique_ptr<PhysicalOperator>& source : sources) {
      if (source == nullptr)
        return common::make_unexpected(invalid("serial source merge contains a null source"));
    }
    const std::optional<std::size_t> source_bytes =
        common::checked_multiply(sources.capacity(), sizeof(std::unique_ptr<PhysicalOperator>));
    std::optional<std::size_t> charge =
        source_bytes.has_value()
            ? common::checked_add(*source_bytes, sizeof(SerialSourceMerge) + std::size_t{256U})
            : std::nullopt;
    if (!charge.has_value() || *charge > maximum_configuration_bytes) {
      return common::make_unexpected(
          exhausted("serial source merge exceeds its configuration limit"));
    }
    common::Result<QueryMemoryReservation> reservation = resources.reserve(*charge);
    if (!reservation.has_value())
      return common::make_unexpected(reservation.error());
    try {
      return std::unique_ptr<PhysicalOperator>{
          new SerialSourceMerge{std::move(sources), std::move(*reservation)}};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("serial source merge allocation failed"));
    }
  }

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override {
    if (ended_)
      return PhysicalOperatorStep::end();
    if (!resources.owns(reservation_)) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(invalid("serial source merge belongs to another query"));
    }
    const common::Result<void> active = resources.check_cancelled();
    if (!active.has_value())
      return common::make_unexpected(active.error());
    while (next_source_ < sources_.size()) {
      common::Result<PhysicalOperatorStep> step = sources_[next_source_]->next(resources);
      if (!step.has_value()) {
        static_cast<void>(resources.request_cancel());
        sources_.clear();
        reservation_.release();
        ended_ = true;
        return common::make_unexpected(step.error());
      }
      if (step->kind() == PhysicalOperatorStepKind::kEnd) {
        sources_[next_source_].reset();
        ++next_source_;
        continue;
      }
      return step;
    }
    std::vector<std::unique_ptr<PhysicalOperator>>{}.swap(sources_);
    reservation_.release();
    ended_ = true;
    return PhysicalOperatorStep::end();
  }

private:
  SerialSourceMerge(std::vector<std::unique_ptr<PhysicalOperator>> sources,
                    QueryMemoryReservation reservation) noexcept
      : sources_(std::move(sources)), reservation_(std::move(reservation)) {}

  std::vector<std::unique_ptr<PhysicalOperator>> sources_;
  QueryMemoryReservation reservation_;
  std::size_t next_source_{};
  bool ended_{};
};

} // namespace

OptimizedPhysicalPipelinePlan::OptimizedPhysicalPipelinePlan(
    PhysicalPipelinePlan plan, const SourceStrategyConfiguration source,
    std::vector<PhysicalSortStrategyDecision> sort_decisions,
    const PhysicalExecutionStrategyCost estimated_cost,
    const std::size_t retained_configuration_bytes) noexcept
    : pipeline_(std::move(plan)), source_merge_strategy_(source.strategy),
      source_task_count_(source.task_count), selected_parallel_workers_(source.parallel_workers),
      parallel_limits_(source.parallel_limits),
      maximum_serial_merge_configuration_bytes_(source.maximum_serial_merge_configuration_bytes),
      sort_decisions_(std::move(sort_decisions)), estimated_cost_(estimated_cost),
      retained_configuration_bytes_(retained_configuration_bytes) {}

common::Result<OptimizedPhysicalPipelinePlan> OptimizedPhysicalPipelinePlan::create(
    PhysicalPipelinePlan plan, PhysicalExecutionStatistics statistics,
    PhysicalExecutionCapabilities capabilities, const PhysicalOptimizerPolicy policy) {
  if (statistics.source_task_count == 0U ||
      statistics.source_task_count > policy.maximum_source_tasks) {
    return common::make_unexpected(
        invalid("physical optimizer source task count is outside its finite policy"));
  }
  if (policy.maximum_sort_stages == 0U || policy.maximum_retained_configuration_bytes == 0U ||
      policy.maximum_serial_merge_configuration_bytes == 0U ||
      policy.maximum_in_memory_sort_rows == 0U ||
      policy.maximum_in_memory_sort_retained_bytes == 0U || policy.minimum_parallel_tasks == 0U ||
      capabilities.available_parallel_workers == 0U) {
    return common::make_unexpected(invalid("physical optimizer policy limits must be nonzero"));
  }

  try {
    std::vector<std::size_t> sort_stage_indices;
    sort_stage_indices.reserve(plan.stages().size());
    for (std::size_t index = 0U; index < plan.stages().size(); ++index) {
      if (std::get_if<SortStage>(&plan.stages()[index]) != nullptr)
        sort_stage_indices.push_back(index);
    }
    if (sort_stage_indices.size() > policy.maximum_sort_stages ||
        statistics.sort_stages.size() != sort_stage_indices.size() ||
        capabilities.spill_sorts.size() > policy.maximum_sort_stages) {
      return common::make_unexpected(
          invalid("physical optimizer sort statistics do not exactly cover the plan"));
    }
    for (std::size_t index = 0U; index < statistics.sort_stages.size(); ++index) {
      if (statistics.sort_stages[index].stage_index != sort_stage_indices[index]) {
        return common::make_unexpected(
            invalid("physical optimizer sort estimates must follow exact stage order"));
      }
    }
    for (std::size_t index = 0U; index < capabilities.spill_sorts.size(); ++index) {
      const PhysicalSpillSortCapability& capability = capabilities.spill_sorts[index];
      if (capability.stage_index >= plan.stages().size() ||
          std::get_if<SortStage>(&plan.stages()[capability.stage_index]) == nullptr ||
          (index != 0U &&
           capabilities.spill_sorts[index - 1U].stage_index >= capability.stage_index)) {
        return common::make_unexpected(
            invalid("physical optimizer spill capabilities must name unique ordered sort stages"));
      }
      common::Result<std::size_t> valid =
          spill_sort_configuration_reservation_bytes(capability.limits);
      if (!valid.has_value())
        return common::make_unexpected(valid.error());
    }

    std::vector<PhysicalSortStrategyDecision> decisions;
    decisions.reserve(sort_stage_indices.size());
    PhysicalExecutionStrategyCost cost{
        .source_work_units = statistics.estimated_source_work_units,
        .selected_source_work_units = statistics.estimated_source_work_units,
        .sort_comparison_work_units = 0U,
        .sort_io_bytes = 0U,
    };
    for (const PhysicalSortStageEstimate& estimate : statistics.sort_stages) {
      const auto& stage = std::get<SortStage>(plan.stages()[estimate.stage_index]);
      const bool memory_eligible = is_in_memory_eligible(estimate, stage, policy);
      const auto capability = std::ranges::find_if(
          capabilities.spill_sorts, [&](const PhysicalSpillSortCapability& candidate) {
            return candidate.stage_index == estimate.stage_index;
          });
      const bool external_eligible = capability != capabilities.spill_sorts.end() &&
                                     is_external_eligible(estimate, stage, capability->limits);
      if (!memory_eligible && !external_eligible) {
        return common::make_unexpected(exhausted(
            "physical optimizer has no bounded sort strategy for the supplied upper bounds"));
      }

      PhysicalSortStrategyDecision decision{
          .stage_index = estimate.stage_index,
          .strategy =
              memory_eligible ? PhysicalSortStrategy::kInMemory : PhysicalSortStrategy::kExternal,
          .estimated_comparison_work_units = 0U,
          .estimated_io_bytes = 0U,
          .external_limits = {},
      };
      if (decision.strategy == PhysicalSortStrategy::kInMemory) {
        decision.estimated_comparison_work_units =
            comparison_units({.rows = estimate.maximum_rows,
                              .run_rows = estimate.maximum_rows,
                              .keys = stage.keys.size()});
      } else {
        decision.external_limits = capability->limits;
        const std::uint64_t run_rows = capability->limits.run_sort_limits.maximum_rows;
        const std::uint64_t runs = ceiling_divide(estimate.maximum_rows, run_rows);
        const std::uint64_t run_work = comparison_units(
            {.rows = estimate.maximum_rows, .run_rows = run_rows, .keys = stage.keys.size()});
        const std::uint64_t merge_work = saturating_multiply(
            saturating_multiply(estimate.maximum_rows, runs), stage.keys.size());
        decision.estimated_comparison_work_units = saturating_add(run_work, merge_work);
        decision.estimated_io_bytes = saturating_multiply(estimate.maximum_spill_bytes, 2U);
      }
      cost.sort_comparison_work_units =
          saturating_add(cost.sort_comparison_work_units, decision.estimated_comparison_work_units);
      cost.sort_io_bytes = saturating_add(cost.sort_io_bytes, decision.estimated_io_bytes);
      decisions.push_back(decision);
    }

    const std::size_t worker_count =
        std::min({statistics.source_task_count, capabilities.available_parallel_workers,
                  capabilities.parallel_limits.maximum_workers});
    PhysicalSourceMergeStrategy merge = PhysicalSourceMergeStrategy::kSerial;
    std::size_t selected_workers = 1U;
    if (statistics.source_merge_requirement == PhysicalSourceMergeRequirement::kOrderIndependent &&
        statistics.source_task_count >= policy.minimum_parallel_tasks &&
        statistics.maximum_source_rows >= policy.minimum_parallel_rows &&
        statistics.estimated_source_work_units >= policy.minimum_parallel_work_units &&
        statistics.source_task_count <= capabilities.parallel_limits.maximum_tasks &&
        worker_count >= 2U) {
      const std::uint64_t divided_work =
          ceiling_divide(statistics.estimated_source_work_units, worker_count);
      const std::uint64_t overhead = saturating_multiply(policy.parallel_worker_overhead_units,
                                                         static_cast<std::uint64_t>(worker_count));
      const std::uint64_t parallel_work = saturating_add(divided_work, overhead);
      if (parallel_work < statistics.estimated_source_work_units) {
        merge = PhysicalSourceMergeStrategy::kParallel;
        selected_workers = worker_count;
        cost.selected_source_work_units = parallel_work;
      }
    }

    common::Result<std::size_t> retained = strategy_retained_bytes(plan, decisions);
    if (!retained.has_value())
      return common::make_unexpected(retained.error());
    if (*retained > policy.maximum_retained_configuration_bytes) {
      return common::make_unexpected(
          exhausted("physical optimizer retained configuration exceeds its limit"));
    }
    ParallelSchedulerLimits selected_parallel_limits = capabilities.parallel_limits;
    selected_parallel_limits.maximum_workers = selected_workers;
    return OptimizedPhysicalPipelinePlan{std::move(plan),
                                         {.strategy = merge,
                                          .task_count = statistics.source_task_count,
                                          .parallel_workers = selected_workers,
                                          .parallel_limits = selected_parallel_limits,
                                          .maximum_serial_merge_configuration_bytes =
                                              policy.maximum_serial_merge_configuration_bytes},
                                         std::move(decisions),
                                         cost,
                                         *retained};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("physical optimizer allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("physical optimizer exceeds container limits"));
  }
}

const PhysicalPipelinePlan& OptimizedPhysicalPipelinePlan::pipeline() const noexcept {
  return pipeline_;
}

PhysicalSourceMergeStrategy OptimizedPhysicalPipelinePlan::source_merge_strategy() const noexcept {
  return source_merge_strategy_;
}

std::size_t OptimizedPhysicalPipelinePlan::source_task_count() const noexcept {
  return source_task_count_;
}

std::size_t OptimizedPhysicalPipelinePlan::selected_parallel_workers() const noexcept {
  return selected_parallel_workers_;
}

std::span<const PhysicalSortStrategyDecision>
OptimizedPhysicalPipelinePlan::sort_decisions() const noexcept {
  return sort_decisions_;
}

PhysicalExecutionStrategyCost OptimizedPhysicalPipelinePlan::estimated_cost() const noexcept {
  return estimated_cost_;
}

std::size_t OptimizedPhysicalPipelinePlan::retained_configuration_bytes() const noexcept {
  return retained_configuration_bytes_;
}

common::Result<std::unique_ptr<PhysicalOperator>> OptimizedPhysicalPipelinePlan::instantiate(
    const QueryResourceContext& resources, std::vector<std::unique_ptr<PhysicalOperator>> sources,
    std::vector<ExternalSortExecutionTarget> external_sort_targets) const {
  if (sources.size() != source_task_count_)
    return common::make_unexpected(invalid("optimized pipeline source task count mismatch"));
  for (const std::unique_ptr<PhysicalOperator>& source : sources) {
    if (source == nullptr)
      return common::make_unexpected(invalid("optimized pipeline contains a null source"));
  }

  std::size_t external_count = 0U;
  for (const PhysicalSortStrategyDecision& decision : sort_decisions_) {
    if (decision.strategy == PhysicalSortStrategy::kExternal)
      ++external_count;
  }
  if (external_sort_targets.size() != external_count) {
    return common::make_unexpected(
        invalid("optimized pipeline external sort targets do not match its decisions"));
  }
  std::size_t target_index = 0U;
  for (const PhysicalSortStrategyDecision& decision : sort_decisions_) {
    if (decision.strategy != PhysicalSortStrategy::kExternal)
      continue;
    if (external_sort_targets[target_index].stage_index != decision.stage_index ||
        !external_sort_targets[target_index].spill_directory.is_open()) {
      return common::make_unexpected(invalid("optimized pipeline external sort target is invalid"));
    }
    ++target_index;
  }

  try {
    std::vector<ExternalSortStageRuntime> runtimes;
    runtimes.reserve(external_count);
    target_index = 0U;
    for (const PhysicalSortStrategyDecision& decision : sort_decisions_) {
      if (decision.strategy != PhysicalSortStrategy::kExternal)
        continue;
      ExternalSortExecutionTarget& target = external_sort_targets[target_index];
      runtimes.push_back({.stage_index = decision.stage_index,
                          .spill_directory = std::move(target.spill_directory),
                          .file_prefix = std::move(target.file_prefix),
                          .limits = decision.external_limits});
      ++target_index;
    }

    std::unique_ptr<PhysicalOperator> merged;
    if (sources.size() == 1U) {
      merged = std::move(sources.front());
    } else if (source_merge_strategy_ == PhysicalSourceMergeStrategy::kParallel) {
      common::Result<std::unique_ptr<ParallelMergeOperator>> parallel =
          ParallelMergeOperator::create(resources, std::move(sources), parallel_limits_);
      if (!parallel.has_value())
        return common::make_unexpected(parallel.error());
      merged = std::move(*parallel);
    } else {
      common::Result<std::unique_ptr<PhysicalOperator>> serial = SerialSourceMerge::create(
          resources, std::move(sources), maximum_serial_merge_configuration_bytes_);
      if (!serial.has_value())
        return common::make_unexpected(serial.error());
      merged = std::move(*serial);
    }
    return pipeline_.instantiate(std::move(merged), std::move(runtimes));
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("optimized pipeline instantiation allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("optimized pipeline instantiation exceeds container limits"));
  }
}

} // namespace chronos::query
