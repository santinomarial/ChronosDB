#ifndef CHRONOS_INGEST_SEALED_HEAD_FLUSH_QUEUE_INTERNAL_HPP_
#define CHRONOS_INGEST_SEALED_HEAD_FLUSH_QUEUE_INTERNAL_HPP_

#include "chronos/ingest/sealed_head_flush_queue.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

namespace chronos::ingest::detail {

class SealedHeadFlushReservation {
public:
  SealedHeadFlushReservation() noexcept = default;
  ~SealedHeadFlushReservation();

  SealedHeadFlushReservation(const SealedHeadFlushReservation&) = delete;
  SealedHeadFlushReservation& operator=(const SealedHeadFlushReservation&) = delete;
  SealedHeadFlushReservation(SealedHeadFlushReservation&& other) noexcept;
  SealedHeadFlushReservation& operator=(SealedHeadFlushReservation&& other) noexcept;

  [[nodiscard]] bool is_valid() const noexcept;
  void stage(head::HeadSnapshot snapshot) noexcept;
  void publish() noexcept;

private:
  struct Position {
    std::size_t slot;
    std::uint64_t sequence;
  };

  SealedHeadFlushReservation(std::shared_ptr<SealedHeadFlushQueueState> state,
                             Position position) noexcept;
  void cancel() noexcept;

  std::shared_ptr<SealedHeadFlushQueueState> state_;
  std::size_t slot_{};
  std::uint64_t sequence_{};

  friend class SealedHeadFlushQueueState;
};

class SealedHeadFlushQueueTestAccess {
public:
  [[nodiscard]] static common::Result<std::shared_ptr<SealedHeadFlushQueue>>
  create(SealedHeadFlushQueueConfig config, const common::TimeSource& time_source) {
    return SealedHeadFlushQueue::create_with_time_source(config, time_source);
  }

  [[nodiscard]] static common::Result<SealedHeadFlushReservation>
  reserve(SealedHeadFlushQueue& queue) {
    return queue.reserve();
  }
};

} // namespace chronos::ingest::detail

#endif // CHRONOS_INGEST_SEALED_HEAD_FLUSH_QUEUE_INTERNAL_HPP_
