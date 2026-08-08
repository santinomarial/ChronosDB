#include "chronos/common/status.hpp"
#include "chronos/query/parallel_scheduler.hpp"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <new>
#include <span>
#include <utility>
#include <vector>

namespace {

class FuzzSource final : public chronos::query::PhysicalOperator {
public:
  explicit FuzzSource(const std::uint8_t mode) : mode_(mode) {}

  [[nodiscard]] chronos::common::Result<chronos::query::PhysicalOperatorStep>
  next(const chronos::query::QueryResourceContext& resources) override {
    if (returned_)
      return chronos::query::PhysicalOperatorStep::end();
    returned_ = true;
    switch (mode_ % 4U) {
    case 0U:
      return chronos::query::PhysicalOperatorStep::end();
    case 1U:
      return chronos::common::make_unexpected(
          chronos::common::Status{chronos::common::StatusCode::kInvalidArgument, "fuzz failure"});
    case 2U:
      return chronos::common::make_unexpected(
          resources.check_cancelled().has_value()
              ? chronos::common::Status{chronos::common::StatusCode::kInternal, "fuzz task failure"}
              : resources.check_cancelled().error());
    default:
      throw std::bad_alloc{};
    }
  }

private:
  std::uint8_t mode_;
  bool returned_{};
};

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const std::span<const std::uint8_t> input{data, size};
  if (input.size() < 8U)
    return 0;

  const std::size_t task_count = input[0] % 5U;
  std::vector<std::unique_ptr<chronos::query::PhysicalOperator>> tasks;
  tasks.reserve(task_count);
  for (std::size_t task = 0U; task < task_count; ++task) {
    if ((input[7] & static_cast<std::uint8_t>(1U << task)) != 0U) {
      tasks.push_back(nullptr);
    } else {
      tasks.push_back(std::make_unique<FuzzSource>(input[(task + 1U) % input.size()]));
    }
  }

  const std::size_t memory_limit = static_cast<std::size_t>(input[6]) * 1'024U + 1U;
  chronos::query::QueryResourceContext resources =
      chronos::query::QueryResourceContext::create(memory_limit).value();
  const chronos::query::ParallelSchedulerLimits limits{
      .maximum_tasks = input[1] % 6U,
      .maximum_workers = input[2] % 6U,
      .maximum_ready_chunks = input[3] % 6U,
      .maximum_retained_configuration_bytes =
          (static_cast<std::size_t>(input[4]) | (static_cast<std::size_t>(input[5]) << 8U)) * 64U};
  auto scheduler =
      chronos::query::ParallelMergeOperator::create(resources, std::move(tasks), limits);
  if (scheduler.has_value()) {
    for (;;) {
      auto step = (*scheduler)->next(resources);
      if (!step.has_value() || step->kind() == chronos::query::PhysicalOperatorStepKind::kEnd)
        break;
      auto chunk = std::move(*step).take_chunk();
      if (!chunk.has_value())
        break;
    }
    scheduler->reset();
  }
  if (resources.reserved_memory_bytes() != 0U)
    std::abort();
  return 0;
}
