#ifndef CHRONOS_INGEST_SEALED_HEAD_FLUSH_QUEUE_HPP_
#define CHRONOS_INGEST_SEALED_HEAD_FLUSH_QUEUE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/common/time_source.hpp"
#include "chronos/head/mutable_head.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

namespace chronos::ingest {

class SealedGenerationRetirementReceipt;

namespace detail {
class SealedHeadFlushQueueState;
class SealedHeadFlushReservation;
class SealedHeadFlushQueueTestAccess;
class TabletStateCore;
} // namespace detail

struct SealedHeadFlushQueueConfig {
  std::size_t capacity{};
};

struct SealedHeadFlushQueueMetrics {
  std::size_t capacity{};
  std::size_t occupied{};
  std::size_t reserved{};
  std::size_t ready{};
  std::size_t in_flight{};
  std::uint64_t accepted{};
  std::uint64_t completed{};
  std::uint64_t retries{};
  std::uint64_t capacity_rejections{};
  std::chrono::nanoseconds oldest_age{};

  friend bool operator==(const SealedHeadFlushQueueMetrics&,
                         const SealedHeadFlushQueueMetrics&) = default;
};

// Move-only ownership of the queue's single in-flight sealed generation. Destruction or an
// explicit release_for_retry() makes the same item ready again without changing its enqueue age.
// complete() is reserved for a coordinator that has finished durable publication and retirement.
class SealedHeadFlushWork {
public:
  SealedHeadFlushWork() = delete;
  ~SealedHeadFlushWork();

  SealedHeadFlushWork(const SealedHeadFlushWork&) = delete;
  SealedHeadFlushWork& operator=(const SealedHeadFlushWork&) = delete;
  SealedHeadFlushWork(SealedHeadFlushWork&& other) noexcept;
  SealedHeadFlushWork& operator=(SealedHeadFlushWork&& other) noexcept;

  [[nodiscard]] bool is_valid() const noexcept;
  [[nodiscard]] std::uint64_t sequence() const noexcept;
  // Returns null after completion, retry release, or move.
  [[nodiscard]] const head::HeadSnapshot* snapshot() const noexcept;
  // Releases capacity only for the exact receipt issued after durable aggregate publication.
  [[nodiscard]] common::Status complete(const SealedGenerationRetirementReceipt& receipt);
  [[nodiscard]] common::Status release_for_retry();

private:
  SealedHeadFlushWork(std::shared_ptr<detail::SealedHeadFlushQueueState> state,
                      head::HeadSnapshot snapshot, std::uint64_t sequence) noexcept;
  void release_for_retry_noexcept() noexcept;

  std::shared_ptr<detail::SealedHeadFlushQueueState> state_;
  std::optional<head::HeadSnapshot> snapshot_;
  std::uint64_t sequence_{};

  friend class detail::SealedHeadFlushQueueState;
};

// Fixed-capacity multi-producer/single-consumer handoff. Tablet writers reserve capacity before
// changing topology and make a sealed immutable pin ready only after the new tablet epoch is
// release-published. The storage consumer may hold at most one work item at a time.
class SealedHeadFlushQueue {
public:
  SealedHeadFlushQueue() = delete;
  ~SealedHeadFlushQueue();

  SealedHeadFlushQueue(const SealedHeadFlushQueue&) = delete;
  SealedHeadFlushQueue& operator=(const SealedHeadFlushQueue&) = delete;
  SealedHeadFlushQueue(SealedHeadFlushQueue&&) = delete;
  SealedHeadFlushQueue& operator=(SealedHeadFlushQueue&&) = delete;

  [[nodiscard]] static common::Result<std::shared_ptr<SealedHeadFlushQueue>>
  create(SealedHeadFlushQueueConfig config);

  // Returns no work when the queue is empty, its oldest reservation has not published, or the
  // single consumer already owns an in-flight item.
  [[nodiscard]] common::Result<std::optional<SealedHeadFlushWork>> try_acquire();
  [[nodiscard]] SealedHeadFlushQueueMetrics metrics() const noexcept;

private:
  explicit SealedHeadFlushQueue(std::shared_ptr<detail::SealedHeadFlushQueueState> state) noexcept;
  [[nodiscard]] static common::Result<std::shared_ptr<SealedHeadFlushQueue>>
  create_with_time_source(SealedHeadFlushQueueConfig config, const common::TimeSource& time_source);
  [[nodiscard]] common::Result<detail::SealedHeadFlushReservation> reserve();

  std::shared_ptr<detail::SealedHeadFlushQueueState> state_;

  friend class detail::SealedHeadFlushQueueTestAccess;
  friend class detail::TabletStateCore;
};

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_SEALED_HEAD_FLUSH_QUEUE_HPP_
