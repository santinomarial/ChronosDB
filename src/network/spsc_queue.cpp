#include "chronos/network/spsc_queue.hpp"

#include <new>
#include <utility>

namespace chronos::network {

SpscNetworkTaskQueue::SpscNetworkTaskQueue(std::vector<std::optional<NetworkTask>> cells) noexcept
    : cells_(std::move(cells)) {}

SpscNetworkTaskQueue::SpscNetworkTaskQueue(SpscNetworkTaskQueue&& other) noexcept
    : producer_index_(other.producer_index_.load(std::memory_order_relaxed)),
      cells_(std::move(other.cells_)),
      consumer_index_(other.consumer_index_.load(std::memory_order_relaxed)) {}

common::Result<SpscNetworkTaskQueue> SpscNetworkTaskQueue::create(const std::size_t capacity) {
  if (capacity == 0U || capacity > 1'048'576U)
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument, "SPSC queue capacity is invalid"});
  try {
    return SpscNetworkTaskQueue{std::vector<std::optional<NetworkTask>>(capacity + 1U)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted, "SPSC queue allocation failed"});
  }
}

bool SpscNetworkTaskQueue::try_push(NetworkTask task) noexcept {
  const std::size_t producer = producer_index_.load(std::memory_order_relaxed);
  const std::size_t next = (producer + 1U) % cells_.size();
  if (next == consumer_index_.load(std::memory_order_acquire))
    return false;
  cells_[producer].emplace(std::move(task));
  producer_index_.store(next, std::memory_order_release);
  return true;
}

std::optional<NetworkTask> SpscNetworkTaskQueue::try_pop() noexcept {
  const std::size_t consumer = consumer_index_.load(std::memory_order_relaxed);
  if (consumer == producer_index_.load(std::memory_order_acquire))
    return std::nullopt;
  std::optional<NetworkTask> task{std::move(cells_[consumer])};
  cells_[consumer].reset();
  consumer_index_.store((consumer + 1U) % cells_.size(), std::memory_order_release);
  return task;
}

std::size_t SpscNetworkTaskQueue::capacity() const noexcept {
  return cells_.size() - 1U;
}
bool SpscNetworkTaskQueue::empty() const noexcept {
  return consumer_index_.load(std::memory_order_acquire) ==
         producer_index_.load(std::memory_order_acquire);
}

} // namespace chronos::network
