#ifndef CHRONOS_QUERY_PHYSICAL_OPTIMIZER_HPP_
#define CHRONOS_QUERY_PHYSICAL_OPTIMIZER_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/parallel_scheduler.hpp"
#include "chronos/query/physical_plan.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace chronos::query {

inline constexpr std::size_t kDefaultPhysicalOptimizerSortLimit = 256U;
inline constexpr std::size_t kDefaultPhysicalOptimizerConfigurationByteLimit =
    std::size_t{8U} * 1024U * 1024U;
inline constexpr std::uint64_t kDefaultParallelSelectionRowThreshold = 4'096U;
inline constexpr std::uint64_t kDefaultParallelSelectionWorkThreshold = std::uint64_t{1U} << 20U;
inline constexpr std::uint64_t kDefaultParallelWorkerOverheadUnits = 4'096U;

enum class PhysicalSourceMergeRequirement : std::uint8_t {
  kPreserveTaskOrder,
  kOrderIndependent,
};

enum class PhysicalSourceMergeStrategy : std::uint8_t { kSerial, kParallel };
enum class PhysicalSortStrategy : std::uint8_t { kInMemory, kExternal };

// Authoritative finite upper bounds at one exact SortStage input/output boundary. The optimizer
// does not derive statistics or silently treat estimates as admission proofs.
struct PhysicalSortStageEstimate {
  std::size_t stage_index;
  std::uint64_t maximum_rows;
  std::uint32_t maximum_input_chunk_rows;
  std::uint64_t maximum_output_logical_bytes;
  std::size_t maximum_output_retained_bytes;
  std::uint64_t maximum_spill_bytes;
  std::size_t maximum_serialized_record_bytes;
};

struct PhysicalExecutionStatistics {
  std::size_t source_task_count;
  std::uint64_t maximum_source_rows;
  std::uint64_t estimated_source_work_units;
  PhysicalSourceMergeRequirement source_merge_requirement{
      PhysicalSourceMergeRequirement::kPreserveTaskOrder};
  std::vector<PhysicalSortStageEstimate> sort_stages;
};

struct PhysicalSpillSortCapability {
  std::size_t stage_index;
  SpillSortLimits limits{};
};

struct PhysicalExecutionCapabilities {
  std::size_t available_parallel_workers{1U};
  ParallelSchedulerLimits parallel_limits{};
  std::vector<PhysicalSpillSortCapability> spill_sorts;
};

struct PhysicalOptimizerPolicy {
  std::size_t maximum_source_tasks{kDefaultParallelSchedulerTaskLimit};
  std::size_t maximum_sort_stages{kDefaultPhysicalOptimizerSortLimit};
  std::size_t maximum_retained_configuration_bytes{kDefaultPhysicalOptimizerConfigurationByteLimit};
  std::size_t maximum_serial_merge_configuration_bytes{
      kDefaultParallelSchedulerConfigurationByteLimit};
  std::uint64_t maximum_in_memory_sort_rows{kDefaultSortRowLimit};
  std::size_t maximum_in_memory_sort_retained_bytes{kDefaultSortStateByteLimit};
  std::size_t minimum_parallel_tasks{2U};
  std::uint64_t minimum_parallel_rows{kDefaultParallelSelectionRowThreshold};
  std::uint64_t minimum_parallel_work_units{kDefaultParallelSelectionWorkThreshold};
  std::uint64_t parallel_worker_overhead_units{kDefaultParallelWorkerOverheadUnits};
};

struct PhysicalSortStrategyDecision {
  std::size_t stage_index;
  PhysicalSortStrategy strategy{PhysicalSortStrategy::kInMemory};
  std::uint64_t estimated_comparison_work_units;
  std::uint64_t estimated_io_bytes;
  SpillSortLimits external_limits{};
};

struct PhysicalExecutionStrategyCost {
  std::uint64_t source_work_units;
  std::uint64_t selected_source_work_units;
  std::uint64_t sort_comparison_work_units;
  std::uint64_t sort_io_bytes;
};

struct ExternalSortExecutionTarget {
  std::size_t stage_index;
  io::PosixDirectory spill_directory;
  std::string file_prefix;
};

// Owns the exact checked pipeline it optimized, so decisions cannot be applied to a different
// physical plan. Runtime instantiation consumes the declared number of same-shape sources and one
// target for every selected external sort.
class OptimizedPhysicalPipelinePlan {
public:
  OptimizedPhysicalPipelinePlan() = delete;
  OptimizedPhysicalPipelinePlan(const OptimizedPhysicalPipelinePlan&) = delete;
  OptimizedPhysicalPipelinePlan& operator=(const OptimizedPhysicalPipelinePlan&) = delete;
  OptimizedPhysicalPipelinePlan(OptimizedPhysicalPipelinePlan&&) noexcept = default;
  OptimizedPhysicalPipelinePlan& operator=(OptimizedPhysicalPipelinePlan&&) noexcept = default;

  [[nodiscard]] static common::Result<OptimizedPhysicalPipelinePlan>
  create(PhysicalPipelinePlan plan, PhysicalExecutionStatistics statistics,
         PhysicalExecutionCapabilities capabilities = {}, PhysicalOptimizerPolicy policy = {});

  [[nodiscard]] const PhysicalPipelinePlan& pipeline() const noexcept;
  [[nodiscard]] PhysicalSourceMergeStrategy source_merge_strategy() const noexcept;
  [[nodiscard]] std::size_t source_task_count() const noexcept;
  [[nodiscard]] std::size_t selected_parallel_workers() const noexcept;
  [[nodiscard]] std::span<const PhysicalSortStrategyDecision> sort_decisions() const noexcept;
  [[nodiscard]] PhysicalExecutionStrategyCost estimated_cost() const noexcept;
  [[nodiscard]] std::size_t retained_configuration_bytes() const noexcept;

  [[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
  instantiate(const QueryResourceContext& resources,
              std::vector<std::unique_ptr<PhysicalOperator>> sources,
              std::vector<ExternalSortExecutionTarget> external_sort_targets = {}) const;

private:
  struct SourceStrategyConfiguration {
    PhysicalSourceMergeStrategy strategy;
    std::size_t task_count;
    std::size_t parallel_workers;
    ParallelSchedulerLimits parallel_limits;
    std::size_t maximum_serial_merge_configuration_bytes;
  };

  OptimizedPhysicalPipelinePlan(PhysicalPipelinePlan plan, SourceStrategyConfiguration source,
                                std::vector<PhysicalSortStrategyDecision> sort_decisions,
                                PhysicalExecutionStrategyCost estimated_cost,
                                std::size_t retained_configuration_bytes) noexcept;

  PhysicalPipelinePlan pipeline_;
  PhysicalSourceMergeStrategy source_merge_strategy_;
  std::size_t source_task_count_;
  std::size_t selected_parallel_workers_;
  ParallelSchedulerLimits parallel_limits_;
  std::size_t maximum_serial_merge_configuration_bytes_;
  std::vector<PhysicalSortStrategyDecision> sort_decisions_;
  PhysicalExecutionStrategyCost estimated_cost_;
  std::size_t retained_configuration_bytes_;
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_PHYSICAL_OPTIMIZER_HPP_
