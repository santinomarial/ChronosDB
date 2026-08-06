#include "chronos/ingest/sealed_head_flush_queue.hpp"

#include "chronos/ingest/tablet_state.hpp"
#include "sealed_head_flush_queue_internal.hpp"

#include <algorithm>
#include <chrono>
#include <exception>
#include <limits>
#include <mutex>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status unavailable(std::string message) {
  return common::Status{common::StatusCode::kUnavailable, std::move(message)};
}

[[nodiscard]] std::chrono::steady_clock::time_point steady_now(void*) noexcept {
  return std::chrono::steady_clock::now();
}

enum class SlotPhase : std::uint8_t {
  kFree,
  kReserved,
  kReady,
  kInFlight,
};

struct QueueSlot {
  SlotPhase phase{SlotPhase::kFree};
  std::uint64_t sequence{};
  std::optional<head::HeadSnapshot> snapshot;
  std::chrono::steady_clock::time_point enqueued_at;
};

} // namespace

namespace detail {

class SealedHeadFlushQueueState : public std::enable_shared_from_this<SealedHeadFlushQueueState> {
public:
  using Clock = std::chrono::steady_clock::time_point (*)(void*) noexcept;

  SealedHeadFlushQueueState(const std::size_t capacity, Clock clock, void* clock_context)
      : slots_(capacity), clock_(clock), clock_context_(clock_context) {}

  [[nodiscard]] common::Result<SealedHeadFlushReservation> reserve() {
    std::scoped_lock lock{mutex_};
    if (occupied_ == slots_.size()) {
      ++capacity_rejections_;
      return common::make_unexpected(exhausted("sealed-head flush queue reached its capacity"));
    }
    if (next_sequence_ == std::numeric_limits<std::uint64_t>::max()) {
      return common::make_unexpected(
          exhausted("sealed-head flush queue exhausted its sequence space"));
    }
    const auto free = std::ranges::find(slots_, SlotPhase::kFree, &QueueSlot::phase);
    if (free == slots_.end()) {
      return common::make_unexpected(
          unavailable("sealed-head flush queue accounting has no free reservation slot"));
    }
    const std::size_t slot = static_cast<std::size_t>(std::distance(slots_.begin(), free));
    const std::uint64_t sequence = next_sequence_++;
    free->phase = SlotPhase::kReserved;
    free->sequence = sequence;
    ++occupied_;
    ++reserved_;
    return SealedHeadFlushReservation{shared_from_this(), SealedHeadFlushReservation::Position{
                                                              .slot = slot, .sequence = sequence}};
  }

  void cancel(const std::size_t slot, const std::uint64_t sequence) noexcept {
    std::scoped_lock lock{mutex_};
    QueueSlot& target = checked_slot(slot, sequence, SlotPhase::kReserved);
    target.snapshot.reset();
    target.phase = SlotPhase::kFree;
    --occupied_;
    --reserved_;
  }

  void stage(const std::size_t slot, const std::uint64_t sequence,
             head::HeadSnapshot snapshot) noexcept {
    std::scoped_lock lock{mutex_};
    QueueSlot& target = checked_slot(slot, sequence, SlotPhase::kReserved);
    target.snapshot.emplace(std::move(snapshot));
  }

  void publish(const std::size_t slot, const std::uint64_t sequence) noexcept {
    std::scoped_lock lock{mutex_};
    QueueSlot& target = checked_slot(slot, sequence, SlotPhase::kReserved);
    if (!target.snapshot.has_value()) {
      std::terminate();
    }
    target.enqueued_at = clock_(clock_context_);
    target.phase = SlotPhase::kReady;
    --reserved_;
    ++ready_;
    ++accepted_;
  }

  [[nodiscard]] common::Result<std::optional<SealedHeadFlushWork>> try_acquire() {
    std::scoped_lock lock{mutex_};
    if (in_flight_ != 0U) {
      return std::optional<SealedHeadFlushWork>{};
    }
    QueueSlot* oldest = nullptr;
    for (QueueSlot& slot : slots_) {
      if (slot.phase != SlotPhase::kFree &&
          (oldest == nullptr || slot.sequence < oldest->sequence)) {
        oldest = &slot;
      }
    }
    if (oldest == nullptr || oldest->phase == SlotPhase::kReserved) {
      return std::optional<SealedHeadFlushWork>{};
    }
    if (oldest->phase != SlotPhase::kReady || !oldest->snapshot.has_value()) {
      return common::make_unexpected(
          unavailable("sealed-head flush queue has an inconsistent consumer boundary"));
    }
    oldest->phase = SlotPhase::kInFlight;
    --ready_;
    ++in_flight_;
    SealedHeadFlushWork work{shared_from_this(), *oldest->snapshot, oldest->sequence};
    return std::optional<SealedHeadFlushWork>{std::move(work)};
  }

  [[nodiscard]] common::Status complete(const std::uint64_t sequence) {
    std::scoped_lock lock{mutex_};
    QueueSlot* target = find_sequence(sequence);
    if (target == nullptr || target->phase != SlotPhase::kInFlight) {
      return invalid("sealed-head flush work no longer owns the in-flight queue item");
    }
    target->snapshot.reset();
    target->phase = SlotPhase::kFree;
    --occupied_;
    --in_flight_;
    ++completed_;
    return common::Status::ok();
  }

  [[nodiscard]] common::Status retry(const std::uint64_t sequence) noexcept {
    std::scoped_lock lock{mutex_};
    QueueSlot* target = find_sequence(sequence);
    if (target == nullptr || target->phase != SlotPhase::kInFlight) {
      return invalid("sealed-head flush work no longer owns the in-flight queue item");
    }
    target->phase = SlotPhase::kReady;
    --in_flight_;
    ++ready_;
    ++retries_;
    return common::Status::ok();
  }

  [[nodiscard]] SealedHeadFlushQueueMetrics metrics() const noexcept {
    std::scoped_lock lock{mutex_};
    std::optional<std::chrono::steady_clock::time_point> oldest;
    for (const QueueSlot& slot : slots_) {
      if ((slot.phase == SlotPhase::kReady || slot.phase == SlotPhase::kInFlight) &&
          (!oldest.has_value() || slot.enqueued_at < *oldest)) {
        oldest = slot.enqueued_at;
      }
    }
    std::chrono::nanoseconds age{};
    if (oldest.has_value()) {
      const std::chrono::steady_clock::time_point now = clock_(clock_context_);
      if (now > *oldest) {
        age = std::chrono::duration_cast<std::chrono::nanoseconds>(now - *oldest);
      }
    }
    return SealedHeadFlushQueueMetrics{.capacity = slots_.size(),
                                       .occupied = occupied_,
                                       .reserved = reserved_,
                                       .ready = ready_,
                                       .in_flight = in_flight_,
                                       .accepted = accepted_,
                                       .completed = completed_,
                                       .retries = retries_,
                                       .capacity_rejections = capacity_rejections_,
                                       .oldest_age = age};
  }

private:
  [[nodiscard]] QueueSlot& checked_slot(const std::size_t slot, const std::uint64_t sequence,
                                        const SlotPhase phase) noexcept {
    if (slot >= slots_.size() || slots_[slot].sequence != sequence || slots_[slot].phase != phase) {
      std::terminate();
    }
    return slots_[slot];
  }

  [[nodiscard]] QueueSlot* find_sequence(const std::uint64_t sequence) noexcept {
    const auto found = std::ranges::find(slots_, sequence, &QueueSlot::sequence);
    return found == slots_.end() ? nullptr : &*found;
  }

  mutable std::mutex mutex_;
  std::vector<QueueSlot> slots_;
  Clock clock_;
  void* clock_context_;
  std::uint64_t next_sequence_{1U};
  std::size_t occupied_{};
  std::size_t reserved_{};
  std::size_t ready_{};
  std::size_t in_flight_{};
  std::uint64_t accepted_{};
  std::uint64_t completed_{};
  std::uint64_t retries_{};
  std::uint64_t capacity_rejections_{};
};

SealedHeadFlushReservation::SealedHeadFlushReservation(
    std::shared_ptr<SealedHeadFlushQueueState> state, const Position position) noexcept
    : state_(std::move(state)), slot_(position.slot), sequence_(position.sequence) {}

SealedHeadFlushReservation::~SealedHeadFlushReservation() {
  cancel();
}

SealedHeadFlushReservation::SealedHeadFlushReservation(SealedHeadFlushReservation&& other) noexcept
    : state_(std::move(other.state_)), slot_(other.slot_), sequence_(other.sequence_) {}

SealedHeadFlushReservation&
SealedHeadFlushReservation::operator=(SealedHeadFlushReservation&& other) noexcept {
  if (this != &other) {
    cancel();
    state_ = std::move(other.state_);
    slot_ = other.slot_;
    sequence_ = other.sequence_;
  }
  return *this;
}

bool SealedHeadFlushReservation::is_valid() const noexcept {
  return state_ != nullptr;
}

void SealedHeadFlushReservation::stage(head::HeadSnapshot snapshot) noexcept {
  if (state_ == nullptr) {
    std::terminate();
  }
  state_->stage(slot_, sequence_, std::move(snapshot));
}

void SealedHeadFlushReservation::publish() noexcept {
  if (state_ == nullptr) {
    std::terminate();
  }
  state_->publish(slot_, sequence_);
  state_.reset();
}

void SealedHeadFlushReservation::cancel() noexcept {
  if (state_ != nullptr) {
    state_->cancel(slot_, sequence_);
    state_.reset();
  }
}

} // namespace detail

SealedHeadFlushWork::SealedHeadFlushWork(std::shared_ptr<detail::SealedHeadFlushQueueState> state,
                                         head::HeadSnapshot snapshot,
                                         const std::uint64_t sequence) noexcept
    : state_(std::move(state)), snapshot_(std::move(snapshot)), sequence_(sequence) {}

SealedHeadFlushWork::~SealedHeadFlushWork() {
  release_for_retry_noexcept();
}

SealedHeadFlushWork::SealedHeadFlushWork(SealedHeadFlushWork&& other) noexcept
    : state_(std::move(other.state_)), snapshot_(std::move(other.snapshot_)),
      sequence_(other.sequence_) {}

SealedHeadFlushWork& SealedHeadFlushWork::operator=(SealedHeadFlushWork&& other) noexcept {
  if (this != &other) {
    release_for_retry_noexcept();
    state_ = std::move(other.state_);
    snapshot_ = std::move(other.snapshot_);
    sequence_ = other.sequence_;
  }
  return *this;
}

bool SealedHeadFlushWork::is_valid() const noexcept {
  return state_ != nullptr && snapshot_.has_value();
}

std::uint64_t SealedHeadFlushWork::sequence() const noexcept {
  return sequence_;
}

const head::HeadSnapshot* SealedHeadFlushWork::snapshot() const noexcept {
  return snapshot_.has_value() ? &*snapshot_ : nullptr;
}

common::Status SealedHeadFlushWork::complete(const SealedGenerationRetirementReceipt& receipt) {
  if (!is_valid()) {
    return invalid("sealed-head flush work is invalid");
  }
  const head::HeadSnapshot* const queued_snapshot = snapshot();
  if (queued_snapshot == nullptr) {
    return invalid("sealed-head flush work lost its queued generation");
  }
  const head::HeadSnapshot& sealed = *queued_snapshot;
  if (sealed.table_id() != receipt.table_id() || sealed.tablet_id() != receipt.tablet_id() ||
      sealed.schema_ptr()->schema_id() != receipt.schema_id() ||
      sealed.schema_ptr()->version() != receipt.schema_version() ||
      sealed.generation() != receipt.head_generation() ||
      sealed.row_count() != receipt.row_count() || !sealed.is_sealed()) {
    return invalid("sealed-head flush completion receipt disagrees with the queued generation");
  }
  std::uint64_t minimum_sequence = std::numeric_limits<std::uint64_t>::max();
  std::uint64_t maximum_sequence = 0U;
  for (std::uint32_t row = 0U; row < sealed.row_count(); ++row) {
    const common::Result<head::HeadRowMetadata> metadata = sealed.row_metadata(row);
    if (!metadata.has_value()) {
      return metadata.error();
    }
    if (metadata->commit_position.wal_id != receipt.wal_id()) {
      return invalid("sealed-head flush completion receipt belongs to a different WAL history");
    }
    minimum_sequence = std::min(minimum_sequence, metadata->commit_position.record_sequence);
    maximum_sequence = std::max(maximum_sequence, metadata->commit_position.record_sequence);
  }
  if (minimum_sequence != receipt.minimum_record_sequence() ||
      maximum_sequence != receipt.maximum_record_sequence()) {
    return invalid("sealed-head flush completion receipt record bounds disagree with the queue");
  }
  common::Status status = state_->complete(sequence_);
  if (status.is_ok()) {
    state_.reset();
    snapshot_.reset();
  }
  return status;
}

common::Status SealedHeadFlushWork::release_for_retry() {
  if (!is_valid()) {
    return invalid("sealed-head flush work is invalid");
  }
  common::Status status = state_->retry(sequence_);
  if (status.is_ok()) {
    state_.reset();
    snapshot_.reset();
  }
  return status;
}

void SealedHeadFlushWork::release_for_retry_noexcept() noexcept {
  if (is_valid()) {
    static_cast<void>(state_->retry(sequence_));
    state_.reset();
    snapshot_.reset();
  }
}

SealedHeadFlushQueue::SealedHeadFlushQueue(
    std::shared_ptr<detail::SealedHeadFlushQueueState> state) noexcept
    : state_(std::move(state)) {}

SealedHeadFlushQueue::~SealedHeadFlushQueue() = default;

common::Result<std::shared_ptr<SealedHeadFlushQueue>>
SealedHeadFlushQueue::create(const SealedHeadFlushQueueConfig config) {
  return create_with_clock(config, &steady_now, nullptr);
}

common::Result<std::shared_ptr<SealedHeadFlushQueue>>
SealedHeadFlushQueue::create_with_clock(const SealedHeadFlushQueueConfig config, const Clock clock,
                                        void* const clock_context) {
  if (config.capacity == 0U) {
    return common::make_unexpected(invalid("sealed-head flush queue capacity must be nonzero"));
  }
  if (clock == nullptr) {
    return common::make_unexpected(invalid("sealed-head flush queue requires a clock"));
  }
  try {
    auto state =
        std::make_shared<detail::SealedHeadFlushQueueState>(config.capacity, clock, clock_context);
    return std::shared_ptr<SealedHeadFlushQueue>{
        new SealedHeadFlushQueue{std::move(state)}}; // NOLINT(cppcoreguidelines-owning-memory)
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("sealed-head flush queue allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("sealed-head flush queue capacity is too large"));
  }
}

common::Result<detail::SealedHeadFlushReservation> SealedHeadFlushQueue::reserve() {
  return state_->reserve();
}

common::Result<std::optional<SealedHeadFlushWork>> SealedHeadFlushQueue::try_acquire() {
  return state_->try_acquire();
}

SealedHeadFlushQueueMetrics SealedHeadFlushQueue::metrics() const noexcept {
  return state_->metrics();
}

} // namespace chronos::ingest
