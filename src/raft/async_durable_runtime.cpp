#include "chronos/raft/async_durable_runtime.hpp"

#include "durable_batch_admission.hpp"
#include "io/posix_syscalls.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <condition_variable>
#include <deque>
#include <exception>
#include <fcntl.h>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

using BatchResult = common::Result<std::vector<DurableRaftResult>>;
using ReclamationResult = common::Result<RaftPersistentLogReclamation>;

void saturating_increment(std::uint64_t& value) noexcept {
  if (value != std::numeric_limits<std::uint64_t>::max())
    ++value;
}

[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status io_error(const char* operation, const int error = errno) {
  return common::Status{common::StatusCode::kIoError,
                        std::string(operation) + ": " +
                            std::error_code(error, std::generic_category()).message()};
}

[[nodiscard]] common::Result<std::array<int, 2>> create_completion_pipe() {
  std::array<int, 2> descriptors{-1, -1};
  if (::pipe(descriptors.data()) != 0)
    return common::make_unexpected(io_error("creating durable Raft completion pipe"));
  for (const int descriptor : descriptors) {
    const int status_flags = ::fcntl(descriptor, F_GETFL, 0);
    const int descriptor_flags = ::fcntl(descriptor, F_GETFD, 0);
    if (status_flags < 0 || descriptor_flags < 0 ||
        ::fcntl(descriptor, F_SETFL, status_flags | O_NONBLOCK) != 0 ||
        ::fcntl(descriptor, F_SETFD, descriptor_flags | FD_CLOEXEC) != 0) {
      const common::Status failure = io_error("configuring durable Raft completion pipe");
      ::close(descriptors[0]);
      ::close(descriptors[1]);
      return common::make_unexpected(failure);
    }
  }
  return descriptors;
}

} // namespace

namespace detail {

class AsyncDurableRaftCompletionState {
public:
  void complete(BatchResult result) {
    {
      const std::lock_guard lock{mutex_};
      result_.emplace(std::move(result));
    }
    condition_.notify_all();
  }

  [[nodiscard]] bool is_ready() const {
    const std::lock_guard lock{mutex_};
    return result_.has_value() && !consumed_;
  }

  [[nodiscard]] BatchResult wait() {
    std::unique_lock lock{mutex_};
    condition_.wait(lock, [this] { return result_.has_value() || consumed_; });
    if (consumed_) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInvalidArgument,
                         "asynchronous durable Raft completion was already consumed"});
    }
    if (!result_.has_value()) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal,
                         "asynchronous durable Raft completion has no published result"});
    }
    consumed_ = true;
    BatchResult result = std::move(result_).value();
    result_.reset();
    return result;
  }

private:
  mutable std::mutex mutex_;
  mutable std::condition_variable condition_;
  std::optional<BatchResult> result_;
  bool consumed_{};
};

class AsyncRaftLogReclamationCompletionState {
public:
  void complete(ReclamationResult result) {
    {
      const std::lock_guard lock{mutex_};
      result_.emplace(std::move(result));
    }
    condition_.notify_all();
  }

  [[nodiscard]] bool is_ready() const {
    const std::lock_guard lock{mutex_};
    return result_.has_value() && !consumed_;
  }

  [[nodiscard]] ReclamationResult wait() {
    std::unique_lock lock{mutex_};
    condition_.wait(lock, [this] { return result_.has_value() || consumed_; });
    if (consumed_) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInvalidArgument,
                         "asynchronous Raft log-reclamation completion was already consumed"});
    }
    if (!result_.has_value()) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal,
                         "asynchronous Raft log-reclamation completion has no result"});
    }
    consumed_ = true;
    ReclamationResult result = std::move(result_).value();
    result_.reset();
    return result;
  }

private:
  mutable std::mutex mutex_;
  mutable std::condition_variable condition_;
  std::optional<ReclamationResult> result_;
  bool consumed_{};
};

} // namespace detail

class AsyncDurableMultiRaftRuntime::Impl {
public:
  struct Task {
    enum class Kind : std::uint8_t { kBatch, kCheckpointAndReclaim };

    Kind kind{Kind::kBatch};
    std::vector<DurableRaftRequest> requests;
    std::shared_ptr<detail::AsyncDurableRaftCompletionState> completion;
    std::shared_ptr<detail::AsyncRaftLogReclamationCompletionState> reclamation_completion;
    std::size_t operation_count{};
  };

  Impl(DurableMultiRaftRuntime runtime, const AsyncDurableMultiRaftLimits configured,
       const std::array<int, 2> completion_pipe,
       std::shared_ptr<AsyncDurableRaftWorkerExtension> extension) noexcept
      : runtime_(std::move(runtime)), limits_(configured), completion_pipe_(completion_pipe) {
    extension_ = std::move(extension);
  }

  ~Impl() noexcept {
    try {
      static_cast<void>(shutdown());
    } catch (...) {
      std::terminate();
    }
    for (const int descriptor : completion_pipe_)
      if (descriptor >= 0)
        ::close(descriptor);
  }

  [[nodiscard]] common::Status start() {
    try {
      worker_ = std::thread{[this] { run(); }};
    } catch (const std::system_error& error) {
      metrics_.accepting = false;
      return common::Status{common::StatusCode::kResourceExhausted,
                            std::string{"cannot start durable Multi-Raft worker: "} + error.what()};
    }
    std::unique_lock lock{initialization_mutex_};
    initialization_condition_.wait(lock, [this] { return initialization_complete_; });
    return initialization_status_;
  }

  [[nodiscard]] common::Result<AsyncDurableRaftCompletion>
  try_submit(std::vector<DurableRaftRequest> requests) {
    if (requests.empty())
      return reject(invalid("asynchronous durable Multi-Raft batch cannot be empty"));
    if (requests.size() > limits_.durable.maximum_batch_operations ||
        requests.size() > limits_.maximum_pending_operations) {
      return reject(common::Status{common::StatusCode::kResourceExhausted,
                                   "asynchronous durable Multi-Raft batch exceeds limits"});
    }
    if (const common::Status admitted =
            detail::admit_durable_batch_outbound(requests, limits_.durable);
        !admitted.is_ok()) {
      return reject(admitted);
    }

    try {
      auto completion = std::make_shared<detail::AsyncDurableRaftCompletionState>();
      const std::size_t operation_count = requests.size();
      auto task = std::make_unique<Task>(
          Task{Task::Kind::kBatch, std::move(requests), completion, nullptr, operation_count});
      std::unique_lock lock{mutex_};
      if (!metrics_.accepting) {
        saturating_increment(metrics_.rejected_batches);
        return common::make_unexpected(
            terminal_status_.is_ok()
                ? common::Status{common::StatusCode::kUnavailable,
                                 "asynchronous durable Multi-Raft admission is closed"}
                : terminal_status_);
      }
      if (metrics_.pending_batches >= limits_.maximum_pending_batches ||
          operation_count > limits_.maximum_pending_operations - metrics_.pending_operations) {
        saturating_increment(metrics_.rejected_batches);
        return common::make_unexpected(
            common::Status{common::StatusCode::kResourceExhausted,
                           "asynchronous durable Multi-Raft admission capacity is full"});
      }
      if (next_submission_sequence_ == std::numeric_limits<std::uint64_t>::max()) {
        saturating_increment(metrics_.rejected_batches);
        return common::make_unexpected(
            common::Status{common::StatusCode::kResourceExhausted,
                           "asynchronous durable Multi-Raft submission sequence is exhausted"});
      }
      queue_.push_back(std::move(task));
      const std::uint64_t submission_sequence = next_submission_sequence_++;
      ++metrics_.pending_batches;
      metrics_.pending_operations += operation_count;
      metrics_.high_water_pending_batches =
          std::max(metrics_.high_water_pending_batches, metrics_.pending_batches);
      metrics_.high_water_pending_operations =
          std::max(metrics_.high_water_pending_operations, metrics_.pending_operations);
      saturating_increment(metrics_.admitted_batches);
      lock.unlock();
      condition_.notify_one();
      return AsyncDurableRaftCompletion{std::move(completion), submission_sequence};
    } catch (const std::bad_alloc&) {
      return reject(common::Status{common::StatusCode::kResourceExhausted,
                                   "cannot allocate asynchronous durable Multi-Raft request"});
    }
  }

  [[nodiscard]] common::Result<AsyncRaftLogReclamationCompletion> try_checkpoint_and_reclaim() {
    try {
      auto completion = std::make_shared<detail::AsyncRaftLogReclamationCompletionState>();
      auto task = std::make_unique<Task>(
          Task{Task::Kind::kCheckpointAndReclaim, {}, nullptr, completion, std::size_t{1U}});
      std::unique_lock lock{mutex_};
      if (!metrics_.accepting) {
        saturating_increment(metrics_.rejected_reclamations);
        return common::make_unexpected(
            terminal_status_.is_ok()
                ? common::Status{common::StatusCode::kUnavailable,
                                 "asynchronous durable Multi-Raft admission is closed"}
                : terminal_status_);
      }
      if (metrics_.pending_batches >= limits_.maximum_pending_batches ||
          metrics_.pending_operations >= limits_.maximum_pending_operations) {
        saturating_increment(metrics_.rejected_reclamations);
        return common::make_unexpected(
            common::Status{common::StatusCode::kResourceExhausted,
                           "asynchronous durable Multi-Raft admission capacity is full"});
      }
      if (next_submission_sequence_ == std::numeric_limits<std::uint64_t>::max()) {
        saturating_increment(metrics_.rejected_reclamations);
        return common::make_unexpected(
            common::Status{common::StatusCode::kResourceExhausted,
                           "asynchronous durable Multi-Raft submission sequence is exhausted"});
      }
      queue_.push_back(std::move(task));
      const std::uint64_t submission_sequence = next_submission_sequence_++;
      ++metrics_.pending_batches;
      ++metrics_.pending_operations;
      metrics_.high_water_pending_batches =
          std::max(metrics_.high_water_pending_batches, metrics_.pending_batches);
      metrics_.high_water_pending_operations =
          std::max(metrics_.high_water_pending_operations, metrics_.pending_operations);
      saturating_increment(metrics_.admitted_reclamations);
      lock.unlock();
      condition_.notify_one();
      return AsyncRaftLogReclamationCompletion{std::move(completion), submission_sequence};
    } catch (const std::bad_alloc&) {
      const std::lock_guard lock{mutex_};
      saturating_increment(metrics_.rejected_reclamations);
      return common::make_unexpected(
          common::Status{common::StatusCode::kResourceExhausted,
                         "cannot allocate asynchronous Raft log-reclamation request"});
    }
  }

  [[nodiscard]] common::Status shutdown() {
    const std::lock_guard shutdown_lock{shutdown_mutex_};
    {
      const std::lock_guard lock{mutex_};
      shutdown_requested_ = true;
      metrics_.accepting = false;
    }
    condition_.notify_all();
    if (worker_.joinable())
      worker_.join();
    const std::lock_guard lock{mutex_};
    return terminal_status_;
  }

  [[nodiscard]] common::Status signal_completion() {
    const std::uint8_t signal = 1U;
    while (true) {
      const ssize_t written = ::write(completion_pipe_[1], &signal, sizeof(signal));
      if (written == static_cast<ssize_t>(sizeof(signal))) {
        const std::lock_guard lock{mutex_};
        saturating_increment(metrics_.written_completion_notifications);
        return common::Status::ok();
      }
      if (written < 0 && errno == EINTR)
        continue;
      if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        const std::lock_guard lock{mutex_};
        saturating_increment(metrics_.coalesced_completion_notifications);
        return common::Status::ok();
      }
      return io_error("signaling durable Raft completion");
    }
  }

  [[nodiscard]] common::Status drain_completion_notifications() {
    std::array<std::uint8_t, 256> signals{};
    while (true) {
      const ssize_t read = ::read(completion_pipe_[0], signals.data(), signals.size());
      if (read > 0)
        continue;
      if (read < 0 && errno == EINTR)
        continue;
      if (read < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        return common::Status::ok();
      return io_error("draining durable Raft completion notifications", read == 0 ? EPIPE : errno);
    }
  }

  [[nodiscard]] int completion_descriptor() const noexcept {
    return completion_pipe_[0];
  }

  [[nodiscard]] AsyncDurableMultiRaftMetrics metrics() const {
    const std::lock_guard lock{mutex_};
    return metrics_;
  }

  [[nodiscard]] bool is_accepting() const {
    const std::lock_guard lock{mutex_};
    return metrics_.accepting;
  }

  [[nodiscard]] bool
  owns_worker_extension(const AsyncDurableRaftWorkerExtension& extension) const noexcept {
    return extension_ != nullptr && extension_->contains_worker_extension(extension);
  }

  [[nodiscard]] common::Status terminal_status() const {
    const std::lock_guard lock{mutex_};
    return terminal_status_;
  }

private:
  [[nodiscard]] common::Result<AsyncDurableRaftCompletion> reject(common::Status status) {
    const std::lock_guard lock{mutex_};
    saturating_increment(metrics_.rejected_batches);
    return common::make_unexpected(std::move(status));
  }

  void release_admission(const Task& task, const bool succeeded) {
    if (metrics_.pending_batches == 0U || metrics_.pending_operations < task.operation_count) {
      if (terminal_status_.is_ok()) {
        terminal_status_ = common::Status{common::StatusCode::kInternal,
                                          "asynchronous Multi-Raft accounting underflow"};
      }
      metrics_.terminal_failure = true;
      metrics_.accepting = false;
      return;
    }
    --metrics_.pending_batches;
    metrics_.pending_operations -= task.operation_count;
    if (task.kind == Task::Kind::kBatch) {
      if (succeeded)
        saturating_increment(metrics_.completed_batches);
      else
        saturating_increment(metrics_.failed_batches);
    } else if (succeeded) {
      saturating_increment(metrics_.completed_reclamations);
    } else {
      saturating_increment(metrics_.failed_reclamations);
    }
  }

  void fail_pending(const common::Status& status) {
    std::deque<std::unique_ptr<Task>> failed;
    {
      const std::lock_guard lock{mutex_};
      if (terminal_status_.is_ok())
        terminal_status_ = status;
      metrics_.terminal_failure = true;
      metrics_.accepting = false;
      failed.swap(queue_);
      for (const auto& task : failed)
        release_admission(*task, false);
    }
    for (auto& task : failed) {
      if (task->kind == Task::Kind::kBatch) {
        task->completion->complete(common::make_unexpected(terminal_status()));
      } else {
        task->reclamation_completion->complete(common::make_unexpected(terminal_status()));
      }
    }
    if (!failed.empty())
      static_cast<void>(signal_completion());
  }

  [[nodiscard]] BatchResult execute(Task& task) {
    try {
      std::unique_ptr<AsyncDurableRaftWorkerBatchContext> context;
      if (extension_ != nullptr) {
        auto prepared = extension_->prepare_batch(runtime_, task.requests);
        if (!prepared.has_value())
          return common::make_unexpected(prepared.error());
        if (*prepared == nullptr) {
          return common::make_unexpected(
              common::Status{common::StatusCode::kInternal,
                             "durable Raft worker extension returned a missing batch context"});
        }
        context = std::move(*prepared);
      }
      BatchResult result = runtime_.execute_batch(std::move(task.requests));
      if (!result.has_value() || extension_ == nullptr)
        return result;
      const common::Status completed =
          extension_->complete_batch(runtime_, std::move(context), *result);
      if (!completed.is_ok())
        return common::make_unexpected(completed);
      return result;
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kResourceExhausted,
          "durable Multi-Raft worker exhausted memory while executing an admitted batch"});
    } catch (const std::exception& error) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal,
                         std::string{"durable Multi-Raft worker caught an unexpected exception: "} +
                             error.what()});
    } catch (...) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "durable Multi-Raft worker caught an unknown exception"});
    }
  }

  [[nodiscard]] ReclamationResult checkpoint_and_reclaim() {
    try {
      return runtime_.checkpoint_and_reclaim();
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kResourceExhausted,
          "durable Multi-Raft worker exhausted memory while reclaiming its physical log"});
    } catch (const std::exception& error) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal,
          std::string{"durable Raft log reclamation caught an unexpected exception: "} +
              error.what()});
    } catch (...) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal,
                         "durable Raft log reclamation caught an unknown exception"});
    }
  }

  void close_runtime() {
    if (extension_ != nullptr) {
      common::Status extension_closed = common::Status::ok();
      try {
        extension_closed = extension_->shutdown(runtime_);
      } catch (const std::bad_alloc&) {
        extension_closed =
            common::Status{common::StatusCode::kResourceExhausted,
                           "durable Raft worker extension shutdown exhausted memory"};
      } catch (const std::exception& error) {
        extension_closed = common::Status{
            common::StatusCode::kInternal,
            std::string{"durable Raft worker extension shutdown threw: "} + error.what()};
      } catch (...) {
        extension_closed = common::Status{common::StatusCode::kInternal,
                                          "durable Raft worker extension shutdown threw"};
      }
      if (!extension_closed.is_ok()) {
        const std::lock_guard lock{mutex_};
        if (terminal_status_.is_ok())
          terminal_status_ = extension_closed;
        metrics_.terminal_failure = true;
        metrics_.accepting = false;
      }
    }
    const common::Status closed = runtime_.close();
    if (!closed.is_ok()) {
      const std::lock_guard lock{mutex_};
      if (terminal_status_.is_ok())
        terminal_status_ = closed;
      metrics_.terminal_failure = true;
      metrics_.accepting = false;
    }
  }

  void run() {
    common::Status initialized = common::Status::ok();
    try {
      if (extension_ != nullptr)
        initialized = extension_->initialize(runtime_);
    } catch (const std::bad_alloc&) {
      initialized = common::Status{common::StatusCode::kResourceExhausted,
                                   "durable Raft worker extension initialization exhausted memory"};
    } catch (const std::exception& error) {
      initialized = common::Status{
          common::StatusCode::kInternal,
          std::string{"durable Raft worker extension initialization threw: "} + error.what()};
    } catch (...) {
      initialized = common::Status{common::StatusCode::kInternal,
                                   "durable Raft worker extension initialization threw"};
    }
    {
      const std::lock_guard lock{mutex_};
      if (initialized.is_ok()) {
        metrics_.accepting = true;
      } else {
        terminal_status_ = initialized;
        metrics_.terminal_failure = true;
        metrics_.accepting = false;
      }
    }
    {
      const std::lock_guard lock{initialization_mutex_};
      initialization_status_ = initialized;
      initialization_complete_ = true;
    }
    initialization_condition_.notify_all();
    if (!initialized.is_ok()) {
      close_runtime();
      return;
    }
    while (true) {
      std::unique_ptr<Task> task;
      {
        std::unique_lock lock{mutex_};
        condition_.wait(lock, [this] { return shutdown_requested_ || !queue_.empty(); });
        if (queue_.empty()) {
          if (shutdown_requested_)
            break;
          continue;
        }
        task = std::move(queue_.front());
        queue_.pop_front();
      }

      BatchResult batch_result = common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "Raft batch task was not executed"});
      ReclamationResult reclamation_result = common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "Raft log-reclamation task was not executed"});
      if (task->kind == Task::Kind::kBatch)
        batch_result = execute(*task);
      else
        reclamation_result = checkpoint_and_reclaim();
      const bool succeeded = task->kind == Task::Kind::kBatch ? batch_result.has_value()
                                                              : reclamation_result.has_value();
      const common::Status failure =
          succeeded ? common::Status::ok()
                    : (task->kind == Task::Kind::kBatch ? batch_result.error()
                                                        : reclamation_result.error());
      {
        const std::lock_guard lock{mutex_};
        release_admission(*task, succeeded);
      }
      if (!succeeded) {
        fail_pending(failure);
        if (task->kind == Task::Kind::kBatch)
          task->completion->complete(std::move(batch_result));
        else
          task->reclamation_completion->complete(std::move(reclamation_result));
        static_cast<void>(signal_completion());
        break;
      }
      if (task->kind == Task::Kind::kBatch)
        task->completion->complete(std::move(batch_result));
      else
        task->reclamation_completion->complete(std::move(reclamation_result));
      const common::Status signaled = signal_completion();
      if (!signaled.is_ok()) {
        fail_pending(signaled);
        break;
      }
    }
    close_runtime();
  }

  DurableMultiRaftRuntime runtime_;
  AsyncDurableMultiRaftLimits limits_;
  std::shared_ptr<AsyncDurableRaftWorkerExtension> extension_;
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::deque<std::unique_ptr<Task>> queue_;
  AsyncDurableMultiRaftMetrics metrics_;
  common::Status terminal_status_;
  bool shutdown_requested_{};
  std::thread worker_;
  std::mutex shutdown_mutex_;
  std::mutex initialization_mutex_;
  std::condition_variable initialization_condition_;
  common::Status initialization_status_;
  bool initialization_complete_{};
  std::array<int, 2> completion_pipe_{-1, -1};
  std::uint64_t next_submission_sequence_{1U};
};

AsyncDurableRaftCompletion::AsyncDurableRaftCompletion() noexcept = default;
AsyncDurableRaftCompletion::~AsyncDurableRaftCompletion() = default;
AsyncDurableRaftCompletion::AsyncDurableRaftCompletion(AsyncDurableRaftCompletion&&) noexcept =
    default;
AsyncDurableRaftCompletion&
AsyncDurableRaftCompletion::operator=(AsyncDurableRaftCompletion&&) noexcept = default;
AsyncDurableRaftCompletion::AsyncDurableRaftCompletion(
    std::shared_ptr<detail::AsyncDurableRaftCompletionState> state,
    const std::uint64_t submission_sequence) noexcept
    : state_(std::move(state)), submission_sequence_(submission_sequence) {}

bool AsyncDurableRaftCompletion::is_valid() const noexcept {
  return state_ != nullptr;
}

std::uint64_t AsyncDurableRaftCompletion::submission_sequence() const noexcept {
  return state_ == nullptr ? 0U : submission_sequence_;
}

bool AsyncDurableRaftCompletion::is_ready() const {
  return state_ != nullptr && state_->is_ready();
}

BatchResult AsyncDurableRaftCompletion::wait() {
  if (state_ == nullptr) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInvalidArgument, "asynchronous durable Raft completion is invalid"});
  }
  return state_->wait();
}

AsyncRaftLogReclamationCompletion::AsyncRaftLogReclamationCompletion() noexcept = default;
AsyncRaftLogReclamationCompletion::~AsyncRaftLogReclamationCompletion() = default;
AsyncRaftLogReclamationCompletion::AsyncRaftLogReclamationCompletion(
    AsyncRaftLogReclamationCompletion&&) noexcept = default;
AsyncRaftLogReclamationCompletion& AsyncRaftLogReclamationCompletion::operator=(
    AsyncRaftLogReclamationCompletion&&) noexcept = default;
AsyncRaftLogReclamationCompletion::AsyncRaftLogReclamationCompletion(
    std::shared_ptr<detail::AsyncRaftLogReclamationCompletionState> state,
    const std::uint64_t submission_sequence) noexcept
    : state_(std::move(state)), submission_sequence_(submission_sequence) {}

bool AsyncRaftLogReclamationCompletion::is_valid() const noexcept {
  return state_ != nullptr;
}

std::uint64_t AsyncRaftLogReclamationCompletion::submission_sequence() const noexcept {
  return state_ == nullptr ? 0U : submission_sequence_;
}

bool AsyncRaftLogReclamationCompletion::is_ready() const {
  return state_ != nullptr && state_->is_ready();
}

ReclamationResult AsyncRaftLogReclamationCompletion::wait() {
  if (state_ == nullptr) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument,
                       "asynchronous Raft log-reclamation completion is invalid"});
  }
  return state_->wait();
}

AsyncDurableMultiRaftRuntime::AsyncDurableMultiRaftRuntime() noexcept = default;
AsyncDurableMultiRaftRuntime::~AsyncDurableMultiRaftRuntime() = default;
AsyncDurableMultiRaftRuntime::AsyncDurableMultiRaftRuntime(
    AsyncDurableMultiRaftRuntime&&) noexcept = default;
AsyncDurableMultiRaftRuntime&
AsyncDurableMultiRaftRuntime::operator=(AsyncDurableMultiRaftRuntime&&) noexcept = default;
AsyncDurableMultiRaftRuntime::AsyncDurableMultiRaftRuntime(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

common::Result<AsyncDurableMultiRaftRuntime> AsyncDurableMultiRaftRuntime::create_new(
    const NodeId local_node_id, const RaftPersistentLogConfig& log_config,
    std::vector<RaftGroupConfiguration> groups, const AsyncDurableMultiRaftLimits limits,
    std::shared_ptr<AsyncDurableRaftWorkerExtension> extension) {
  return create_new_with(local_node_id, log_config, std::move(groups), limits, std::move(extension),
                         io::detail::system_posix_syscalls());
}

common::Result<AsyncDurableMultiRaftRuntime> AsyncDurableMultiRaftRuntime::create_new_with(
    const NodeId local_node_id, const RaftPersistentLogConfig& log_config,
    std::vector<RaftGroupConfiguration> groups, const AsyncDurableMultiRaftLimits limits,
    std::shared_ptr<AsyncDurableRaftWorkerExtension> extension,
    io::detail::PosixSyscalls& syscalls) {
  if (limits.maximum_pending_batches == 0U || limits.maximum_pending_operations == 0U) {
    return common::make_unexpected(invalid("asynchronous durable Multi-Raft limits are invalid"));
  }
  auto runtime = DurableMultiRaftRuntime::create_new_with(
      local_node_id, log_config, std::move(groups), limits.durable, syscalls);
  if (!runtime.has_value())
    return common::make_unexpected(runtime.error());
  auto completion_pipe = create_completion_pipe();
  if (!completion_pipe.has_value())
    return common::make_unexpected(completion_pipe.error());
  std::unique_ptr<Impl> impl;
  try {
    impl =
        std::make_unique<Impl>(std::move(*runtime), limits, *completion_pipe, std::move(extension));
  } catch (const std::bad_alloc&) {
    ::close((*completion_pipe)[0]);
    ::close((*completion_pipe)[1]);
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "cannot allocate asynchronous durable Multi-Raft runtime owner"});
  }
  const common::Status started = impl->start();
  if (!started.is_ok()) {
    static_cast<void>(impl->shutdown());
    return common::make_unexpected(started);
  }
  return AsyncDurableMultiRaftRuntime{std::move(impl)};
}

common::Result<AsyncDurableMultiRaftRuntime> AsyncDurableMultiRaftRuntime::open_existing(
    const NodeId local_node_id, const RaftPersistentLogConfig& log_config,
    const RaftPersistentLogOpenOptions& open_options, std::vector<RaftGroupConfiguration> groups,
    const AsyncDurableMultiRaftLimits limits,
    std::shared_ptr<AsyncDurableRaftWorkerExtension> extension) {
  if (limits.maximum_pending_batches == 0U || limits.maximum_pending_operations == 0U) {
    return common::make_unexpected(invalid("asynchronous durable Multi-Raft limits are invalid"));
  }
  auto runtime = DurableMultiRaftRuntime::open_existing(local_node_id, log_config, open_options,
                                                        std::move(groups), limits.durable);
  if (!runtime.has_value())
    return common::make_unexpected(runtime.error());
  auto completion_pipe = create_completion_pipe();
  if (!completion_pipe.has_value())
    return common::make_unexpected(completion_pipe.error());
  std::unique_ptr<Impl> impl;
  try {
    impl =
        std::make_unique<Impl>(std::move(*runtime), limits, *completion_pipe, std::move(extension));
  } catch (const std::bad_alloc&) {
    ::close((*completion_pipe)[0]);
    ::close((*completion_pipe)[1]);
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "cannot allocate asynchronous durable Multi-Raft runtime owner"});
  }
  const common::Status started = impl->start();
  if (!started.is_ok()) {
    static_cast<void>(impl->shutdown());
    return common::make_unexpected(started);
  }
  return AsyncDurableMultiRaftRuntime{std::move(impl)};
}

common::Result<AsyncDurableRaftCompletion>
AsyncDurableMultiRaftRuntime::try_submit(std::vector<DurableRaftRequest> requests) {
  if (impl_ == nullptr) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kUnavailable, "asynchronous durable Multi-Raft runtime is not open"});
  }
  return impl_->try_submit(std::move(requests));
}

common::Result<AsyncDurableRaftCompletion>
AsyncDurableMultiRaftRuntime::try_observe_group(const GroupId& group_id) {
  return try_submit({DurableRaftRequest{group_id, ObserveGroupOperation{}}});
}

common::Result<AsyncRaftLogReclamationCompletion>
AsyncDurableMultiRaftRuntime::try_checkpoint_and_reclaim() {
  if (impl_ == nullptr) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kUnavailable, "asynchronous durable Multi-Raft runtime is not open"});
  }
  return impl_->try_checkpoint_and_reclaim();
}

common::Status AsyncDurableMultiRaftRuntime::shutdown() {
  return impl_ == nullptr ? common::Status::ok() : impl_->shutdown();
}

int AsyncDurableMultiRaftRuntime::completion_descriptor() const noexcept {
  return impl_ == nullptr ? -1 : impl_->completion_descriptor();
}

common::Status AsyncDurableMultiRaftRuntime::drain_completion_notifications() {
  return impl_ == nullptr ? invalid("asynchronous durable Multi-Raft runtime is not open")
                          : impl_->drain_completion_notifications();
}

AsyncDurableMultiRaftMetrics AsyncDurableMultiRaftRuntime::metrics() const {
  return impl_ == nullptr ? AsyncDurableMultiRaftMetrics{} : impl_->metrics();
}

bool AsyncDurableMultiRaftRuntime::is_accepting() const {
  return impl_ != nullptr && impl_->is_accepting();
}

bool AsyncDurableMultiRaftRuntime::owns_worker_extension(
    const AsyncDurableRaftWorkerExtension& extension) const noexcept {
  return impl_ != nullptr && impl_->owns_worker_extension(extension);
}

common::Status AsyncDurableMultiRaftRuntime::terminal_status() const {
  return impl_ == nullptr ? common::Status::ok() : impl_->terminal_status();
}

} // namespace chronos::raft
