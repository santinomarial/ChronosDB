#ifndef CHRONOS_QUERY_PARALLEL_SCHEDULER_HPP_
#define CHRONOS_QUERY_PARALLEL_SCHEDULER_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/resource_context.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

namespace chronos::query {

inline constexpr std::size_t kDefaultParallelSchedulerTaskLimit = 256U;
inline constexpr std::size_t kDefaultParallelSchedulerWorkerLimit = 16U;
inline constexpr std::size_t kDefaultParallelSchedulerReadyChunkLimit = 64U;
inline constexpr std::size_t kDefaultParallelSchedulerConfigurationByteLimit =
    std::size_t{8U} * 1024U * 1024U;

struct ParallelSchedulerLimits {
  std::size_t maximum_tasks{kDefaultParallelSchedulerTaskLimit};
  std::size_t maximum_workers{kDefaultParallelSchedulerWorkerLimit};
  std::size_t maximum_ready_chunks{kDefaultParallelSchedulerReadyChunkLimit};
  std::size_t maximum_retained_configuration_bytes{kDefaultParallelSchedulerConfigurationByteLimit};
};

struct ParallelSchedulerMetrics {
  std::uint64_t tasks_started{};
  std::uint64_t tasks_completed{};
  std::uint64_t chunks_published{};
  std::size_t peak_ready_chunks{};

  friend bool operator==(const ParallelSchedulerMetrics&,
                         const ParallelSchedulerMetrics&) = default;
};

// Bounded parallel merge for independent unordered physical pipelines. Each task remains
// thread-affine to one worker for its entire pull lifetime. A fixed-capacity queue
// release-publishes complete accounted chunks under one mutex; consumers acquire the same mutex
// before adoption. This operator provides no SQL row order and must never be selected below an
// order-sensitive consumer unless that consumer establishes the complete required order itself.
class ParallelMergeOperator final : public PhysicalOperator {
public:
  ~ParallelMergeOperator() override;

  [[nodiscard]] static common::Result<std::unique_ptr<ParallelMergeOperator>>
  create(const QueryResourceContext& resources,
         std::vector<std::unique_ptr<PhysicalOperator>> tasks, ParallelSchedulerLimits limits = {});

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override;

  [[nodiscard]] ParallelSchedulerMetrics metrics() const;
  [[nodiscard]] std::size_t worker_count() const noexcept;
  [[nodiscard]] std::size_t retained_configuration_bytes() const noexcept;

private:
  class SharedState;

  ParallelMergeOperator(std::shared_ptr<SharedState> state, std::size_t worker_count,
                        std::size_t retained_configuration_bytes);

  void stop_and_join(bool cancel) noexcept;

  std::shared_ptr<SharedState> state_;
  std::vector<std::thread> workers_;
  std::size_t worker_count_{};
  std::size_t retained_configuration_bytes_{};
  bool ended_{};
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_PARALLEL_SCHEDULER_HPP_
