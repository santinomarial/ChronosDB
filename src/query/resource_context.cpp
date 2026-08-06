#include "chronos/query/resource_context.hpp"

#include "chronos/common/status.hpp"

#include <atomic>
#include <cstddef>
#include <memory>
#include <new>
#include <utility>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid_limit() {
  return common::Status{common::StatusCode::kInvalidArgument,
                        "query maximum memory bytes must be nonzero"};
}

[[nodiscard]] common::Status invalid_reservation() {
  return common::Status{common::StatusCode::kInvalidArgument,
                        "query memory reservation bytes must be nonzero"};
}

[[nodiscard]] common::Status exhausted() {
  return common::Status{common::StatusCode::kResourceExhausted,
                        "query memory reservation exceeds the available budget"};
}

[[nodiscard]] common::Status cancelled() {
  return common::Status{common::StatusCode::kCancelled, "query execution was cancelled"};
}

} // namespace

namespace detail {

// These atomics are control counters, not publication primitives. No query data or owner lifetime
// becomes visible through them, so relaxed ordering is sufficient. Scheduler queues must provide
// the release/acquire edge for task and chunk handoff.
class QueryResourceState {
public:
  explicit QueryResourceState(const std::size_t maximum_memory_bytes) noexcept
      : maximum_memory_bytes_(maximum_memory_bytes) {}

  [[nodiscard]] common::Result<void> reserve(const std::size_t bytes) {
    if (cancelled_.load(std::memory_order_relaxed))
      return common::make_unexpected(cancelled());

    std::size_t current = reserved_memory_bytes_.load(std::memory_order_relaxed);
    for (;;) {
      if (bytes > maximum_memory_bytes_ - current)
        return common::make_unexpected(exhausted());
      const std::size_t next = current + bytes;
      if (reserved_memory_bytes_.compare_exchange_weak(current, next, std::memory_order_relaxed,
                                                       std::memory_order_relaxed)) {
        update_peak(next);
        return {};
      }
      if (cancelled_.load(std::memory_order_relaxed))
        return common::make_unexpected(cancelled());
    }
  }

  void release(const std::size_t bytes) noexcept {
    static_cast<void>(reserved_memory_bytes_.fetch_sub(bytes, std::memory_order_relaxed));
  }

  [[nodiscard]] bool request_cancel() noexcept {
    return !cancelled_.exchange(true, std::memory_order_relaxed);
  }

  [[nodiscard]] bool is_cancelled() const noexcept {
    return cancelled_.load(std::memory_order_relaxed);
  }

  [[nodiscard]] std::size_t maximum_memory_bytes() const noexcept {
    return maximum_memory_bytes_;
  }

  [[nodiscard]] std::size_t reserved_memory_bytes() const noexcept {
    return reserved_memory_bytes_.load(std::memory_order_relaxed);
  }

  [[nodiscard]] std::size_t peak_reserved_memory_bytes() const noexcept {
    return peak_reserved_memory_bytes_.load(std::memory_order_relaxed);
  }

private:
  void update_peak(const std::size_t candidate) noexcept {
    std::size_t peak = peak_reserved_memory_bytes_.load(std::memory_order_relaxed);
    while (peak < candidate &&
           !peak_reserved_memory_bytes_.compare_exchange_weak(
               peak, candidate, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
  }

  const std::size_t maximum_memory_bytes_;
  std::atomic<std::size_t> reserved_memory_bytes_;
  std::atomic<std::size_t> peak_reserved_memory_bytes_;
  std::atomic<bool> cancelled_;
};

} // namespace detail

QueryMemoryReservation::QueryMemoryReservation(std::shared_ptr<detail::QueryResourceState> state,
                                               const std::size_t bytes) noexcept
    : state_(std::move(state)), bytes_(bytes) {}

QueryMemoryReservation::QueryMemoryReservation(QueryMemoryReservation&& other) noexcept
    : state_(std::move(other.state_)), bytes_(std::exchange(other.bytes_, 0U)) {}

QueryMemoryReservation& QueryMemoryReservation::operator=(QueryMemoryReservation&& other) noexcept {
  if (this != &other) {
    release();
    state_ = std::move(other.state_);
    bytes_ = std::exchange(other.bytes_, 0U);
  }
  return *this;
}

QueryMemoryReservation::~QueryMemoryReservation() {
  release();
}

bool QueryMemoryReservation::is_valid() const noexcept {
  return state_ != nullptr;
}

std::size_t QueryMemoryReservation::bytes() const noexcept {
  return bytes_;
}

void QueryMemoryReservation::release() noexcept {
  if (state_ != nullptr) {
    state_->release(bytes_);
    state_.reset();
    bytes_ = 0U;
  }
}

QueryResourceContext::QueryResourceContext(
    std::shared_ptr<detail::QueryResourceState> state) noexcept
    : state_(std::move(state)) {}

common::Result<QueryResourceContext>
QueryResourceContext::create(const std::size_t maximum_memory_bytes) {
  if (maximum_memory_bytes == 0U)
    return common::make_unexpected(invalid_limit());
  try {
    return QueryResourceContext{std::make_shared<detail::QueryResourceState>(maximum_memory_bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "query resource state allocation failed"});
  }
}

common::Result<QueryMemoryReservation>
QueryResourceContext::reserve(const std::size_t bytes) const {
  if (bytes == 0U)
    return common::make_unexpected(invalid_reservation());
  const common::Result<void> reserved = state_->reserve(bytes);
  if (!reserved.has_value())
    return common::make_unexpected(reserved.error());
  return QueryMemoryReservation{state_, bytes};
}

bool QueryResourceContext::owns(const QueryMemoryReservation& reservation) const noexcept {
  return reservation.state_ == state_;
}

bool QueryResourceContext::request_cancel() const noexcept {
  return state_->request_cancel();
}

bool QueryResourceContext::is_cancelled() const noexcept {
  return state_->is_cancelled();
}

common::Result<void> QueryResourceContext::check_cancelled() const {
  if (is_cancelled())
    return common::make_unexpected(cancelled());
  return {};
}

std::size_t QueryResourceContext::maximum_memory_bytes() const noexcept {
  return state_->maximum_memory_bytes();
}

std::size_t QueryResourceContext::reserved_memory_bytes() const noexcept {
  return state_->reserved_memory_bytes();
}

std::size_t QueryResourceContext::available_memory_bytes() const noexcept {
  return maximum_memory_bytes() - reserved_memory_bytes();
}

std::size_t QueryResourceContext::peak_reserved_memory_bytes() const noexcept {
  return state_->peak_reserved_memory_bytes();
}

} // namespace chronos::query
