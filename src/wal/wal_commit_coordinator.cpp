#include "chronos/wal/wal_commit_coordinator.hpp"

#include "chronos/wal/codec.hpp"

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <limits>
#include <mutex>
#include <new>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace chronos::wal {
namespace {

[[nodiscard]] common::Status invalid_completion() {
  return common::Status{common::StatusCode::kInvalidArgument,
                        "WAL commit completion is not valid"};
}

[[nodiscard]] common::Status invalid_coordinator(std::string operation) {
  operation.append(" requires a valid WAL commit coordinator");
  return common::Status{common::StatusCode::kInvalidArgument, std::move(operation)};
}

[[nodiscard]] common::Status validate_config(const WalCommitCoordinatorConfig& config) {
  if (config.maximum_pending_requests == 0U) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "maximum pending WAL requests must be nonzero"};
  }
  if (config.maximum_pending_encoded_bytes < kMinimumRecordLength) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "maximum pending WAL bytes cannot admit one minimum record"};
  }
  if (config.maximum_sync_batch_requests == 0U) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "maximum WAL sync batch requests must be nonzero"};
  }
  if (config.maximum_sync_batch_encoded_bytes < kMinimumRecordLength) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "maximum WAL sync batch bytes cannot hold one minimum record"};
  }
  if (config.maximum_sync_batch_delay.count() < 0) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "maximum WAL sync batch delay must not be negative"};
  }
  return common::Status::ok();
}

void saturating_add(std::uint64_t& destination, const std::uint64_t increment) noexcept {
  if (destination > std::numeric_limits<std::uint64_t>::max() - increment) {
    destination = std::numeric_limits<std::uint64_t>::max();
    return;
  }
  destination += increment;
}

void saturating_increment(std::uint64_t& destination) noexcept {
  saturating_add(destination, 1U);
}

} // namespace

namespace detail {

class WalCommitCompletionState {
public:
  void complete(common::Result<WalCommitResult> result) {
    {
      const std::lock_guard lock{mutex_};
      if (result_.has_value()) {
        return;
      }
      result_.emplace(std::move(result));
    }
    condition_.notify_all();
  }

  [[nodiscard]] bool is_ready() const {
    const std::lock_guard lock{mutex_};
    return result_.has_value();
  }

  [[nodiscard]] common::Result<WalCommitResult> wait() const {
    std::unique_lock lock{mutex_};
    condition_.wait(lock, [this] { return result_.has_value(); });
    return *result_;
  }

private:
  mutable std::mutex mutex_;
  mutable std::condition_variable condition_;
  std::optional<common::Result<WalCommitResult>> result_;
};

} // namespace detail

class WalCommitCoordinator::Impl {
public:
  struct Request {
    std::uint64_t admission_sequence{};
    WalDurabilityMode durability{WalDurabilityMode::kAsync};
    std::size_t encoded_bytes{};
    std::vector<std::byte> payload;
    std::shared_ptr<detail::WalCommitCompletionState> completion;
  };

  struct PendingLocalSync {
    std::unique_ptr<Request> request;
    WalAppendResult append;
  };

  struct SyncWindow {
    bool active{false};
    std::size_t request_count{};
    std::size_t encoded_bytes{};
    std::chrono::steady_clock::time_point deadline{};

    void reset() noexcept {
      active = false;
      request_count = 0U;
      encoded_bytes = 0U;
      deadline = {};
    }
  };

  Impl(WalWriter writer, WalCommitCoordinatorConfig config, void (*worker_start_hook)(void*),
       void* worker_start_context)
      : writer_(std::move(writer)), config_(config), worker_start_hook_(worker_start_hook),
        worker_start_context_(worker_start_context) {
    pending_local_sync_.reserve(config_.maximum_pending_requests);
    metrics_.accepting = true;
  }

  ~Impl() {
    static_cast<void>(shutdown());
  }

  [[nodiscard]] common::Status start_worker() {
    try {
      worker_ = std::thread{[this] { run(); }};
    } catch (const std::system_error& error) {
      return common::Status{common::StatusCode::kResourceExhausted,
                            std::string{"cannot start WAL commit worker: "} + error.what()};
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Result<WalCommitCompletion>
  try_submit(const common::ByteView application_payload, const WalDurabilityMode durability) {
    if (durability != WalDurabilityMode::kAsync &&
        durability != WalDurabilityMode::kLocalSync) {
      reject_request();
      return common::make_unexpected(common::Status{
          common::StatusCode::kInvalidArgument, "unknown WAL durability mode"});
    }
    const common::Result<RecordLayout> layout = calculate_record_layout(application_payload.size());
    if (!layout.has_value()) {
      reject_request();
      return common::make_unexpected(layout.error());
    }
    const auto encoded_bytes = static_cast<std::size_t>(layout->total_length);
    if (durability == WalDurabilityMode::kLocalSync &&
        encoded_bytes > config_.maximum_sync_batch_encoded_bytes) {
      reject_request();
      return common::make_unexpected(common::Status{
          common::StatusCode::kOutOfRange,
          "LOCAL_SYNC WAL record exceeds the configured synchronization batch byte limit"});
    }

    std::unique_lock lock{mutex_};
    if (!metrics_.accepting) {
      saturating_increment(metrics_.rejected_requests);
      return common::make_unexpected(terminal_status_.is_ok()
                                         ? common::Status{common::StatusCode::kUnavailable,
                                                          "WAL commit admission is closed"}
                                         : terminal_status_);
    }
    if (admission_sequence_exhausted_) {
      saturating_increment(metrics_.rejected_requests);
      return common::make_unexpected(common::Status{
          common::StatusCode::kResourceExhausted, "WAL admission sequence UINT64_MAX exhausted"});
    }
    if (metrics_.pending_requests >= config_.maximum_pending_requests ||
        encoded_bytes > config_.maximum_pending_encoded_bytes - metrics_.pending_encoded_bytes) {
      saturating_increment(metrics_.rejected_requests);
      return common::make_unexpected(common::Status{
          common::StatusCode::kResourceExhausted, "WAL commit admission capacity is full"});
    }

    try {
      auto completion = std::make_shared<detail::WalCommitCompletionState>();
      auto request = std::make_unique<Request>();
      request->admission_sequence = next_admission_sequence_;
      request->durability = durability;
      request->encoded_bytes = encoded_bytes;
      request->payload.assign(application_payload.begin(), application_payload.end());
      request->completion = completion;
      queue_.push_back(std::move(request));

      if (next_admission_sequence_ == std::numeric_limits<std::uint64_t>::max()) {
        admission_sequence_exhausted_ = true;
      } else {
        ++next_admission_sequence_;
      }
      ++metrics_.pending_requests;
      metrics_.pending_encoded_bytes += encoded_bytes;
      saturating_increment(metrics_.admitted_requests);
      saturating_add(metrics_.admitted_encoded_bytes, layout->total_length);
      metrics_.high_water_pending_requests =
          std::max(metrics_.high_water_pending_requests, metrics_.pending_requests);
      metrics_.high_water_pending_encoded_bytes =
          std::max(metrics_.high_water_pending_encoded_bytes, metrics_.pending_encoded_bytes);
      lock.unlock();
      condition_.notify_one();
      return WalCommitCompletion{std::move(completion)};
    } catch (const std::bad_alloc&) {
      saturating_increment(metrics_.rejected_requests);
      return common::make_unexpected(common::Status{
          common::StatusCode::kResourceExhausted, "cannot allocate bounded WAL commit request"});
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
    if (worker_.joinable()) {
      worker_.join();
    }
    const std::lock_guard lock{mutex_};
    return terminal_status_;
  }

  [[nodiscard]] WalCommitMetrics metrics() const {
    const std::lock_guard lock{mutex_};
    return metrics_;
  }

  [[nodiscard]] bool is_accepting() const noexcept {
    const std::lock_guard lock{mutex_};
    return metrics_.accepting;
  }

  [[nodiscard]] common::Status terminal_status() const {
    const std::lock_guard lock{mutex_};
    return terminal_status_;
  }

private:
  void reject_request() {
    const std::lock_guard lock{mutex_};
    saturating_increment(metrics_.rejected_requests);
  }

  [[nodiscard]] bool can_add_to_window(const Request& request,
                                       const SyncWindow& window) const noexcept {
    if (window.request_count >= config_.maximum_sync_batch_requests) {
      return false;
    }
    return request.encoded_bytes <=
           config_.maximum_sync_batch_encoded_bytes - window.encoded_bytes;
  }

  void release_admission(const Request& request) noexcept {
    if (metrics_.pending_requests == 0U || metrics_.pending_encoded_bytes < request.encoded_bytes) {
      terminal_status_ = common::Status{common::StatusCode::kInternal,
                                        "WAL commit admission accounting underflow"};
      metrics_.terminal_failure = true;
      metrics_.accepting = false;
      return;
    }
    --metrics_.pending_requests;
    metrics_.pending_encoded_bytes -= request.encoded_bytes;
  }

  void finish_failure(std::unique_ptr<Request> request, const common::Status& status) {
    request->completion->complete(common::make_unexpected(status));
    saturating_increment(metrics_.failed_requests);
    release_admission(*request);
  }

  void finish_success(std::unique_ptr<Request> request, const WalAppendResult& append,
                      const std::optional<PhysicalWalPosition>& synchronization_position) {
    const WalDurabilityMode durability = request->durability;
    request->completion->complete(WalCommitResult{
        .admission_sequence = request->admission_sequence,
        .requested_durability = durability,
        .effective_durability = durability,
        .append = append,
        .synchronization_position = synchronization_position,
    });
    if (durability == WalDurabilityMode::kAsync) {
      saturating_increment(metrics_.acknowledged_async_requests);
    } else {
      saturating_increment(metrics_.acknowledged_local_sync_requests);
    }
    release_admission(*request);
  }

  void release_durable_local(const std::uint64_t durable_sequence,
                             const PhysicalWalPosition& durable_position) {
    std::size_t released_requests = 0U;
    std::size_t released_bytes = 0U;
    std::size_t released_prefix = 0U;
    while (released_prefix < pending_local_sync_.size() &&
           pending_local_sync_[released_prefix].append.record_sequence <= durable_sequence) {
      PendingLocalSync pending = std::move(pending_local_sync_[released_prefix]);
      ++released_requests;
      released_bytes += pending.request->encoded_bytes;
      finish_success(std::move(pending.request), pending.append, durable_position);
      ++released_prefix;
    }
    if (released_prefix != 0U) {
      pending_local_sync_.erase(
          pending_local_sync_.begin(),
          pending_local_sync_.begin() + static_cast<std::ptrdiff_t>(released_prefix));
    }
    if (released_requests != 0U) {
      saturating_increment(metrics_.local_sync_batches);
      saturating_add(metrics_.local_sync_requests_in_batches,
                     static_cast<std::uint64_t>(released_requests));
      metrics_.maximum_observed_local_sync_batch_requests =
          std::max(metrics_.maximum_observed_local_sync_batch_requests, released_requests);
      metrics_.maximum_observed_local_sync_batch_encoded_bytes =
          std::max(metrics_.maximum_observed_local_sync_batch_encoded_bytes, released_bytes);
    }
  }

  void fail_terminal(std::unique_ptr<Request> current, const common::Status& failure) {
    {
      const std::lock_guard lock{mutex_};
      if (terminal_status_.is_ok()) {
        terminal_status_ = failure;
      }
      metrics_.terminal_failure = true;
      metrics_.accepting = false;
      if (current != nullptr) {
        finish_failure(std::move(current), terminal_status_);
      }
      for (PendingLocalSync& pending : pending_local_sync_) {
        finish_failure(std::move(pending.request), terminal_status_);
      }
      pending_local_sync_.clear();
      while (!queue_.empty()) {
        std::unique_ptr<Request> queued = std::move(queue_.front());
        queue_.pop_front();
        finish_failure(std::move(queued), terminal_status_);
      }
    }
    condition_.notify_all();
  }

  [[nodiscard]] bool perform_sync(SyncWindow& window) {
    {
      const std::lock_guard lock{mutex_};
      saturating_increment(metrics_.synchronization_attempts);
    }
    const common::Result<PhysicalWalPosition> synchronized = writer_.synchronize();
    if (!synchronized.has_value()) {
      {
        const std::lock_guard lock{mutex_};
        saturating_increment(metrics_.failed_synchronizations);
        release_durable_local(writer_.durable_record_sequence(), writer_.durable_position());
      }
      fail_terminal(nullptr, synchronized.error());
      window.reset();
      return false;
    }
    {
      const std::lock_guard lock{mutex_};
      saturating_increment(metrics_.successful_synchronizations);
      release_durable_local(writer_.durable_record_sequence(), *synchronized);
    }
    window.reset();
    return true;
  }

  void append_request(std::unique_ptr<Request> request, SyncWindow& window) {
    const std::uint64_t segment_before = writer_.active_segment().header.segment_number;
    common::Result<WalAppendResult> appended = writer_.append_application_entry(request->payload);
    if (!appended.has_value()) {
      {
        const std::lock_guard lock{mutex_};
        release_durable_local(writer_.durable_record_sequence(), writer_.durable_position());
      }
      if (writer_.is_failed()) {
        fail_terminal(std::move(request), appended.error());
        window.reset();
        return;
      }
      const std::lock_guard lock{mutex_};
      finish_failure(std::move(request), appended.error());
      return;
    }

    const bool rotated = appended->record_start.segment_number != segment_before;
    {
      const std::lock_guard lock{mutex_};
      saturating_increment(metrics_.appended_requests);
      saturating_add(metrics_.appended_encoded_bytes,
                     static_cast<std::uint64_t>(request->encoded_bytes));
      if (rotated) {
        release_durable_local(writer_.durable_record_sequence(), writer_.durable_position());
        window.reset();
      }

      if (window.active) {
        ++window.request_count;
        window.encoded_bytes += request->encoded_bytes;
      } else if (request->durability == WalDurabilityMode::kLocalSync) {
        window.active = true;
        window.request_count = 1U;
        window.encoded_bytes = request->encoded_bytes;
        window.deadline = std::chrono::steady_clock::now() + config_.maximum_sync_batch_delay;
      }

      std::vector<std::byte>{}.swap(request->payload);
      if (request->durability == WalDurabilityMode::kAsync) {
        finish_success(std::move(request), *appended, std::nullopt);
      } else {
        pending_local_sync_.push_back(
            PendingLocalSync{.request = std::move(request), .append = *appended});
      }
    }
  }

  void close_writer() {
    const common::Status close_status = writer_.close();
    if (!close_status.is_ok()) {
      const std::lock_guard lock{mutex_};
      if (terminal_status_.is_ok()) {
        terminal_status_ = close_status;
      }
      metrics_.terminal_failure = true;
      metrics_.accepting = false;
    }
  }

  void run() {
    if (worker_start_hook_ != nullptr) {
      worker_start_hook_(worker_start_context_);
    }

    SyncWindow window;
    while (true) {
      std::unique_ptr<Request> request;
      bool synchronize = false;
      bool stop = false;
      {
        std::unique_lock lock{mutex_};
        while (request == nullptr && !synchronize && !stop) {
          if (metrics_.terminal_failure) {
            stop = true;
            break;
          }
          if (window.active) {
            const auto now = std::chrono::steady_clock::now();
            if (shutdown_requested_ ||
                window.request_count >= config_.maximum_sync_batch_requests ||
                window.encoded_bytes >= config_.maximum_sync_batch_encoded_bytes ||
                now >= window.deadline) {
              synchronize = true;
              break;
            }
            if (!queue_.empty()) {
              if (can_add_to_window(*queue_.front(), window)) {
                request = std::move(queue_.front());
                queue_.pop_front();
              } else {
                synchronize = true;
              }
              break;
            }
            condition_.wait_until(lock, window.deadline, [this] {
              return shutdown_requested_ || !queue_.empty() || metrics_.terminal_failure;
            });
            continue;
          }
          if (!queue_.empty()) {
            request = std::move(queue_.front());
            queue_.pop_front();
            break;
          }
          if (shutdown_requested_) {
            stop = true;
            break;
          }
          condition_.wait(lock, [this] {
            return shutdown_requested_ || !queue_.empty() || metrics_.terminal_failure;
          });
        }
      }

      if (stop) {
        close_writer();
        return;
      }
      if (synchronize) {
        if (!perform_sync(window)) {
          close_writer();
          return;
        }
        continue;
      }
      append_request(std::move(request), window);
      if (writer_.is_failed()) {
        close_writer();
        return;
      }
    }
  }

  WalWriter writer_;
  WalCommitCoordinatorConfig config_;
  void (*worker_start_hook_)(void*);
  void* worker_start_context_;
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::deque<std::unique_ptr<Request>> queue_;
  std::vector<PendingLocalSync> pending_local_sync_;
  WalCommitMetrics metrics_;
  common::Status terminal_status_;
  std::uint64_t next_admission_sequence_{1U};
  bool admission_sequence_exhausted_{false};
  bool shutdown_requested_{false};
  std::thread worker_;
  std::mutex shutdown_mutex_;
};

WalCommitCompletion::WalCommitCompletion() noexcept = default;
WalCommitCompletion::~WalCommitCompletion() = default;
WalCommitCompletion::WalCommitCompletion(WalCommitCompletion&&) noexcept = default;
WalCommitCompletion& WalCommitCompletion::operator=(WalCommitCompletion&&) noexcept = default;

WalCommitCompletion::WalCommitCompletion(
    std::shared_ptr<detail::WalCommitCompletionState> state) noexcept
    : state_(std::move(state)) {}

bool WalCommitCompletion::is_valid() const noexcept {
  return state_ != nullptr;
}

bool WalCommitCompletion::is_ready() const {
  return state_ != nullptr && state_->is_ready();
}

common::Result<WalCommitResult> WalCommitCompletion::wait() const {
  if (state_ == nullptr) {
    return common::make_unexpected(invalid_completion());
  }
  return state_->wait();
}

WalCommitCoordinator::WalCommitCoordinator() noexcept = default;
WalCommitCoordinator::~WalCommitCoordinator() = default;
WalCommitCoordinator::WalCommitCoordinator(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
WalCommitCoordinator::WalCommitCoordinator(WalCommitCoordinator&&) noexcept = default;
WalCommitCoordinator& WalCommitCoordinator::operator=(WalCommitCoordinator&&) noexcept = default;

common::Result<WalCommitCoordinator>
WalCommitCoordinator::start(WalWriter writer, const WalCommitCoordinatorConfig& config) {
  return start_with_worker_hook(std::move(writer), config, nullptr, nullptr);
}

common::Result<WalCommitCoordinator> WalCommitCoordinator::start_with_worker_hook(
    WalWriter writer, const WalCommitCoordinatorConfig& config,
    void (*worker_start_hook)(void*), void* const worker_start_context) {
  const common::Status config_status = validate_config(config);
  if (!config_status.is_ok()) {
    return common::make_unexpected(config_status);
  }
  if (!writer.is_open()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInvalidArgument, "WAL commit coordinator requires an open writer"});
  }
  if (writer.is_failed()) {
    return common::make_unexpected(writer.failure_status());
  }
  try {
    auto implementation = std::make_unique<Impl>(std::move(writer), config, worker_start_hook,
                                                 worker_start_context);
    const common::Status worker_status = implementation->start_worker();
    if (!worker_status.is_ok()) {
      return common::make_unexpected(worker_status);
    }
    return WalCommitCoordinator{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kResourceExhausted, "cannot allocate WAL commit coordinator state"});
  }
}

common::Result<WalCommitCompletion>
WalCommitCoordinator::try_submit(const common::ByteView application_payload,
                                 const WalDurabilityMode durability) {
  if (implementation_ == nullptr) {
    return common::make_unexpected(invalid_coordinator("try_submit"));
  }
  return implementation_->try_submit(application_payload, durability);
}

common::Status WalCommitCoordinator::shutdown() {
  return implementation_ == nullptr ? common::Status::ok() : implementation_->shutdown();
}

WalCommitMetrics WalCommitCoordinator::metrics() const {
  return implementation_ == nullptr ? WalCommitMetrics{} : implementation_->metrics();
}

bool WalCommitCoordinator::is_accepting() const noexcept {
  return implementation_ != nullptr && implementation_->is_accepting();
}

common::Status WalCommitCoordinator::terminal_status() const {
  return implementation_ == nullptr ? invalid_coordinator("terminal_status")
                                    : implementation_->terminal_status();
}

} // namespace chronos::wal
