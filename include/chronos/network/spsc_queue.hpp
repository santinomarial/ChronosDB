#ifndef CHRONOS_NETWORK_SPSC_QUEUE_HPP_
#define CHRONOS_NETWORK_SPSC_QUEUE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/protocol.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace chronos::network {

struct NetworkTaskProtocolContext {
  std::uint16_t protocol_major{kProtocolMajor};
  std::uint16_t protocol_minor{kProtocolMinor};
  std::uint64_t feature_bits{};
  std::uint32_t maximum_payload_size{kDefaultMaximumPayloadSize};

  friend bool operator==(const NetworkTaskProtocolContext&,
                         const NetworkTaskProtocolContext&) = default;
};

struct NetworkTask {
  std::uint64_t connection_id{};
  std::uint64_t principal_id{};
  // Immutable negotiated connection facts captured by the reactor when it dispatches the task.
  // Services must use these instead of reconstructing capabilities from frame bytes.
  NetworkTaskProtocolContext protocol{};
  Frame frame{};
};

// Exactly one producer calls try_push and exactly one consumer calls try_pop. Moving the queue is
// permitted only before either thread receives it; destruction requires both threads to be joined.
class SpscNetworkTaskQueue {
public:
  SpscNetworkTaskQueue() = delete;
  ~SpscNetworkTaskQueue() = default;
  SpscNetworkTaskQueue(const SpscNetworkTaskQueue&) = delete;
  SpscNetworkTaskQueue& operator=(const SpscNetworkTaskQueue&) = delete;
  SpscNetworkTaskQueue(SpscNetworkTaskQueue&& other) noexcept;
  SpscNetworkTaskQueue& operator=(SpscNetworkTaskQueue&&) = delete;

  [[nodiscard]] static common::Result<SpscNetworkTaskQueue> create(std::size_t capacity);
  [[nodiscard]] bool try_push(NetworkTask task) noexcept;
  // Checks capacity before moving from task. On false, task remains unchanged so a bounded
  // producer can retain and retry an already-encoded response without copying its payload.
  [[nodiscard]] bool try_push_preserving(NetworkTask& task) noexcept;
  [[nodiscard]] std::optional<NetworkTask> try_pop() noexcept;
  [[nodiscard]] std::size_t capacity() const noexcept;
  [[nodiscard]] bool empty() const noexcept;

private:
  explicit SpscNetworkTaskQueue(std::vector<std::optional<NetworkTask>> cells) noexcept;

  alignas(64) std::atomic<std::size_t> producer_index_{0U};
  std::vector<std::optional<NetworkTask>> cells_;
  alignas(64) std::atomic<std::size_t> consumer_index_{0U};
};

} // namespace chronos::network

#endif // CHRONOS_NETWORK_SPSC_QUEUE_HPP_
