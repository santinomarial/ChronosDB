#include "chronos/query/parallel_scheduler.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"

#include <algorithm>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

inline constexpr std::size_t kConservativeAllocationOverheadBytes = 64U;

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status unavailable(std::string message) {
  return common::Status{common::StatusCode::kUnavailable, std::move(message)};
}

} // namespace

class ParallelMergeOperator::SharedState {
public:
  SharedState(QueryResourceContext resources, std::vector<std::unique_ptr<PhysicalOperator>> tasks,
              const std::size_t queue_capacity, QuerySharedMemoryReservation reservation)
      : resources_(std::move(resources)), tasks_(std::move(tasks)), ready_(queue_capacity),
        reservation_(std::move(reservation)) {}

  [[nodiscard]] static common::Result<std::size_t>
  configuration_bytes(const std::vector<std::unique_ptr<PhysicalOperator>>& tasks,
                      std::size_t workers, ParallelSchedulerLimits limits);

  void register_worker() {
    const std::scoped_lock lock{mutex_};
    ++active_workers_;
    ++registered_workers_;
  }

  void unregister_unstarted_worker() noexcept {
    const std::scoped_lock lock{mutex_};
    --active_workers_;
    --registered_workers_;
    startup_condition_.notify_all();
    ready_condition_.notify_all();
  }

  void run_worker(const std::size_t worker, const runtime::ThreadPlacement placement) noexcept {
    common::Status placement_status = runtime::apply_current_thread_placement(placement);
    const bool placement_succeeded = placement_status.is_ok();
    {
      const std::scoped_lock lock{mutex_};
      ++started_workers_;
      if (!placement_succeeded &&
          (!startup_failure_.has_value() || worker < startup_failure_->first)) {
        startup_failure_.emplace(worker, std::move(placement_status));
        stopping_ = true;
      }
    }
    startup_condition_.notify_all();
    ready_condition_.notify_all();
    space_condition_.notify_all();
    if (!placement_succeeded) {
      finish_worker();
      return;
    }
    {
      std::unique_lock lock{mutex_};
      startup_condition_.wait(lock, [&] {
        return stopping_ || (registration_complete_ && started_workers_ == registered_workers_);
      });
      if (stopping_) {
        lock.unlock();
        finish_worker();
        return;
      }
    }
    while (true) {
      std::optional<std::size_t> task = claim_task();
      if (!task.has_value())
        break;
      bool completed = false;
      try {
        completed = run_task(*task);
      } catch (const std::bad_alloc&) {
        record_error(*task, exhausted("parallel scheduler worker allocation failed"));
      } catch (const std::length_error&) {
        record_error(*task, exhausted("parallel scheduler worker exceeded container limits"));
      } catch (...) {
        record_error(*task, common::Status{common::StatusCode::kInternal,
                                           "parallel scheduler worker threw an exception"});
      }
      if (completed)
        complete_task();
      if (should_stop())
        break;
    }
    finish_worker();
  }

  void complete_worker_registration() noexcept {
    {
      const std::scoped_lock lock{mutex_};
      registration_complete_ = true;
    }
    startup_condition_.notify_all();
  }

  [[nodiscard]] common::Result<void> await_worker_startup() {
    std::unique_lock lock{mutex_};
    startup_condition_.wait(
        lock, [&] { return registration_complete_ && started_workers_ == registered_workers_; });
    if (startup_failure_.has_value())
      return common::make_unexpected(std::move(startup_failure_->second));
    return {};
  }

  [[nodiscard]] bool run_task(const std::size_t task) {
    PhysicalOperator& pipeline = *tasks_[task];
    while (true) {
      common::Result<PhysicalOperatorStep> step = pipeline.next(resources_);
      if (!step.has_value()) {
        record_error(task, std::move(step).error());
        return false;
      }
      if (step->kind() == PhysicalOperatorStepKind::kEnd)
        return true;
      common::Result<AccountedVectorChunk> chunk = std::move(*step).take_chunk();
      if (!chunk.has_value()) {
        record_error(task, std::move(chunk).error());
        return false;
      }
      if (!chunk->belongs_to(resources_)) {
        record_error(task, invalid("parallel scheduler received a foreign query chunk"));
        return false;
      }
      if (!publish(std::move(*chunk)))
        return false;
    }
  }

  [[nodiscard]] common::Result<std::optional<AccountedVectorChunk>>
  take_next(const QueryResourceContext& resources) {
    if (!resources.owns(reservation_)) {
      static_cast<void>(resources_.request_cancel());
      request_stop(true);
      return common::make_unexpected(
          invalid("parallel scheduler belongs to another query resource context"));
    }

    std::unique_lock lock{mutex_};
    ready_condition_.wait(lock, [&] {
      return ready_count_ != 0U || failure_.has_value() || active_workers_ == 0U ||
             resources_.is_cancelled();
    });
    if (failure_.has_value()) {
      ready_condition_.wait(lock, [&] { return active_workers_ == 0U; });
      common::Status error = std::move(failure_->second);
      failure_.reset();
      return common::make_unexpected(std::move(error));
    }
    if (resources_.is_cancelled()) {
      stopping_ = true;
      clear_ready_locked();
      lock.unlock();
      space_condition_.notify_all();
      lock.lock();
      ready_condition_.wait(lock, [&] { return active_workers_ == 0U; });
      return common::make_unexpected(resources_.check_cancelled().error());
    }
    if (ready_count_ == 0U)
      return std::optional<AccountedVectorChunk>{};

    std::optional<AccountedVectorChunk>& slot = ready_[ready_head_];
    if (!slot.has_value())
      std::terminate();
    AccountedVectorChunk result = std::move(slot.value());
    slot.reset();
    ready_head_ = (ready_head_ + 1U) % ready_.size();
    --ready_count_;
    lock.unlock();
    space_condition_.notify_one();
    return std::optional<AccountedVectorChunk>{std::move(result)};
  }

  void request_stop(const bool cancel) noexcept {
    {
      const std::scoped_lock lock{mutex_};
      stopping_ = true;
      clear_ready_locked();
    }
    if (cancel)
      static_cast<void>(resources_.request_cancel());
    startup_condition_.notify_all();
    ready_condition_.notify_all();
    space_condition_.notify_all();
  }

  [[nodiscard]] ParallelSchedulerMetrics metrics() const {
    const std::scoped_lock lock{mutex_};
    return metrics_;
  }

private:
  [[nodiscard]] std::optional<std::size_t> claim_task() noexcept {
    const std::scoped_lock lock{mutex_};
    if (stopping_ || next_task_ == tasks_.size())
      return std::nullopt;
    const std::size_t result = next_task_++;
    ++metrics_.tasks_started;
    return result;
  }

  [[nodiscard]] bool publish(AccountedVectorChunk chunk) noexcept {
    std::unique_lock lock{mutex_};
    space_condition_.wait(lock, [&] {
      return stopping_ || resources_.is_cancelled() || ready_count_ < ready_.size();
    });
    if (stopping_ || resources_.is_cancelled())
      return false;
    ready_[ready_tail_].emplace(std::move(chunk));
    ready_tail_ = (ready_tail_ + 1U) % ready_.size();
    ++ready_count_;
    ++metrics_.chunks_published;
    metrics_.peak_ready_chunks = std::max(metrics_.peak_ready_chunks, ready_count_);
    lock.unlock();
    ready_condition_.notify_one();
    return true;
  }

  void record_error(const std::size_t task, common::Status error) noexcept {
    {
      const std::scoped_lock lock{mutex_};
      const bool incoming_cancelled = error.code() == common::StatusCode::kCancelled;
      const bool retained_cancelled =
          failure_.has_value() && failure_->second.code() == common::StatusCode::kCancelled;
      const bool replace = !failure_.has_value() || (retained_cancelled && !incoming_cancelled) ||
                           (retained_cancelled == incoming_cancelled && task < failure_->first);
      if (replace) {
        if (failure_.has_value()) {
          failure_->first = task;
          failure_->second = std::move(error);
        } else {
          failure_.emplace(task, std::move(error));
        }
      }
      stopping_ = true;
      clear_ready_locked();
    }
    static_cast<void>(resources_.request_cancel());
    ready_condition_.notify_all();
    space_condition_.notify_all();
  }

  void complete_task() noexcept {
    const std::scoped_lock lock{mutex_};
    ++metrics_.tasks_completed;
  }

  [[nodiscard]] bool should_stop() const noexcept {
    const std::scoped_lock lock{mutex_};
    return stopping_;
  }

  void finish_worker() noexcept {
    {
      const std::scoped_lock lock{mutex_};
      --active_workers_;
    }
    ready_condition_.notify_all();
  }

  void clear_ready_locked() noexcept {
    for (std::optional<AccountedVectorChunk>& chunk : ready_)
      chunk.reset();
    ready_head_ = 0U;
    ready_tail_ = 0U;
    ready_count_ = 0U;
  }

  QueryResourceContext resources_;
  std::vector<std::unique_ptr<PhysicalOperator>> tasks_;
  std::vector<std::optional<AccountedVectorChunk>> ready_;
  QuerySharedMemoryReservation reservation_;
  mutable std::mutex mutex_;
  std::condition_variable ready_condition_;
  std::condition_variable space_condition_;
  std::condition_variable startup_condition_;
  std::size_t next_task_{};
  std::size_t active_workers_{};
  std::size_t registered_workers_{};
  std::size_t started_workers_{};
  std::size_t ready_head_{};
  std::size_t ready_tail_{};
  std::size_t ready_count_{};
  bool stopping_{};
  bool registration_complete_{};
  std::optional<std::pair<std::size_t, common::Status>> startup_failure_;
  std::optional<std::pair<std::size_t, common::Status>> failure_;
  ParallelSchedulerMetrics metrics_;
};

common::Result<std::size_t> ParallelMergeOperator::SharedState::configuration_bytes(
    const std::vector<std::unique_ptr<PhysicalOperator>>& tasks, const std::size_t workers,
    const ParallelSchedulerLimits limits) {
  const std::optional<std::size_t> task_bytes =
      common::checked_multiply(tasks.capacity(), sizeof(std::unique_ptr<PhysicalOperator>));
  const std::optional<std::size_t> queue_bytes = common::checked_multiply(
      limits.maximum_ready_chunks, sizeof(std::optional<AccountedVectorChunk>));
  const std::optional<std::size_t> worker_bytes =
      common::checked_multiply(workers, sizeof(std::thread));
  if (!task_bytes.has_value() || !queue_bytes.has_value() || !worker_bytes.has_value()) {
    return common::make_unexpected(
        exhausted("parallel scheduler configuration accounting overflowed"));
  }
  std::optional<std::size_t> total =
      common::checked_add(sizeof(SharedState), sizeof(ParallelMergeOperator));
  total = total.has_value() ? common::checked_add(*total, *task_bytes) : std::nullopt;
  total = total.has_value() ? common::checked_add(*total, *queue_bytes) : std::nullopt;
  total = total.has_value() ? common::checked_add(*total, *worker_bytes) : std::nullopt;
  constexpr std::size_t allocation_count = 5U;
  total = total.has_value()
              ? common::checked_add(*total, allocation_count * kConservativeAllocationOverheadBytes)
              : std::nullopt;
  if (!total.has_value()) {
    return common::make_unexpected(
        exhausted("parallel scheduler configuration accounting overflowed"));
  }
  return *total;
}

// The adjacent counts describe separate, explicitly named scheduler construction properties.
// NOLINTBEGIN(bugprone-easily-swappable-parameters)
ParallelMergeOperator::ParallelMergeOperator(std::shared_ptr<SharedState> state,
                                             const std::size_t worker_count,
                                             const std::size_t retained_configuration_bytes)
    : state_(std::move(state)), worker_count_(worker_count),
      retained_configuration_bytes_(retained_configuration_bytes) {
  workers_.reserve(worker_count);
}
// NOLINTEND(bugprone-easily-swappable-parameters)

ParallelMergeOperator::~ParallelMergeOperator() {
  stop_and_join(!ended_);
}

common::Result<std::unique_ptr<ParallelMergeOperator>>
ParallelMergeOperator::create(const QueryResourceContext& resources,
                              std::vector<std::unique_ptr<PhysicalOperator>> tasks,
                              const ParallelSchedulerLimits limits,
                              const std::span<const runtime::ThreadPlacement> worker_placements) {
  if (limits.maximum_tasks == 0U || limits.maximum_workers == 0U ||
      limits.maximum_ready_chunks == 0U || limits.maximum_retained_configuration_bytes == 0U) {
    return common::make_unexpected(invalid("parallel scheduler limits must be nonzero"));
  }
  if (tasks.empty())
    return common::make_unexpected(invalid("parallel scheduler requires at least one task"));
  if (tasks.size() > limits.maximum_tasks || tasks.capacity() > limits.maximum_tasks) {
    return common::make_unexpected(
        exhausted("parallel scheduler task count exceeds its configured limit"));
  }
  for (const std::unique_ptr<PhysicalOperator>& task : tasks) {
    if (task == nullptr)
      return common::make_unexpected(invalid("parallel scheduler tasks must be non-null"));
  }
  const std::size_t worker_count = std::min(tasks.size(), limits.maximum_workers);
  if (!worker_placements.empty() && worker_placements.size() != worker_count) {
    return common::make_unexpected(
        invalid("parallel scheduler placement count must equal its worker count"));
  }
  common::Result<std::size_t> retained =
      SharedState::configuration_bytes(tasks, worker_count, limits);
  if (!retained.has_value())
    return common::make_unexpected(retained.error());
  if (*retained > limits.maximum_retained_configuration_bytes) {
    return common::make_unexpected(
        exhausted("parallel scheduler retained configuration exceeds its limit"));
  }
  common::Result<QuerySharedMemoryReservation> reservation = resources.reserve_shared(*retained);
  if (!reservation.has_value())
    return common::make_unexpected(reservation.error());

  try {
    auto state = std::make_shared<SharedState>(
        resources, std::move(tasks), limits.maximum_ready_chunks, std::move(*reservation));
    auto result = std::unique_ptr<ParallelMergeOperator>{
        new ParallelMergeOperator{state, worker_count, *retained}};
    for (std::size_t worker = 0U; worker < worker_count; ++worker) {
      state->register_worker();
      try {
        const runtime::ThreadPlacement placement =
            worker_placements.empty() ? runtime::ThreadPlacement{} : worker_placements[worker];
        result->workers_.emplace_back(
            [state, worker, placement] { state->run_worker(worker, placement); });
      } catch (...) {
        state->unregister_unstarted_worker();
        throw;
      }
    }
    state->complete_worker_registration();
    common::Result<void> startup = state->await_worker_startup();
    if (!startup.has_value()) {
      result->stop_and_join(false);
      result->ended_ = true;
      return common::make_unexpected(std::move(startup).error());
    }
    return result;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("parallel scheduler allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("parallel scheduler exceeds container limits"));
  } catch (const std::system_error&) {
    return common::make_unexpected(unavailable("parallel scheduler could not start a worker"));
  }
}

common::Result<PhysicalOperatorStep>
ParallelMergeOperator::next(const QueryResourceContext& resources) {
  if (ended_)
    return PhysicalOperatorStep::end();
  common::Result<std::optional<AccountedVectorChunk>> next = state_->take_next(resources);
  if (!next.has_value()) {
    ended_ = true;
    stop_and_join(false);
    return common::make_unexpected(next.error());
  }
  if (!next->has_value()) {
    ended_ = true;
    stop_and_join(false);
    return PhysicalOperatorStep::end();
  }
  std::optional<AccountedVectorChunk> output = std::move(next).value();
  if (!output.has_value())
    std::terminate();
  return PhysicalOperatorStep::chunk(std::move(output.value()));
}

ParallelSchedulerMetrics ParallelMergeOperator::metrics() const {
  return state_ == nullptr ? ParallelSchedulerMetrics{} : state_->metrics();
}

std::size_t ParallelMergeOperator::worker_count() const noexcept {
  return worker_count_;
}

std::size_t ParallelMergeOperator::retained_configuration_bytes() const noexcept {
  return retained_configuration_bytes_;
}

void ParallelMergeOperator::stop_and_join(const bool cancel) noexcept {
  if (state_ != nullptr)
    state_->request_stop(cancel);
  for (std::thread& worker : workers_) {
    if (worker.joinable()) {
      try {
        worker.join();
      } catch (...) {
        std::terminate();
      }
    }
  }
  workers_.clear();
}

} // namespace chronos::query
