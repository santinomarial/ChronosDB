#ifndef CHRONOS_NETWORK_EPOLL_REACTOR_HPP_
#define CHRONOS_NETWORK_EPOLL_REACTOR_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/connection_buffers.hpp"
#include "chronos/network/connection_state.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/spsc_queue.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace chronos::network {

class Reactor;

struct EpollServerConfig {
  std::array<std::uint8_t, 4> bind_address{127U, 0U, 0U, 1U};
  std::uint16_t port{};
  std::size_t maximum_connections{1024U};
  std::size_t maximum_events_per_poll{128U};
  std::size_t maximum_io_operations_per_event{16U};
  std::size_t read_chunk_bytes{std::size_t{64U} * 1024U};
  int listen_backlog{128};
  std::chrono::milliseconds handshake_timeout{5000};
  std::chrono::milliseconds idle_timeout{60'000};
  ConnectionBufferConfig buffers;
  ConnectionStateConfig state;
  NetworkSecurityConfig security;
};

struct EpollServerMetrics {
  std::uint64_t accepted_connections{};
  std::uint64_t rejected_connections{};
  std::uint64_t closed_connections{};
  std::uint64_t timed_out_connections{};
  std::uint64_t decoded_frames{};
  std::uint64_t dispatched_requests{};
  std::uint64_t queue_overloads{};
  std::uint64_t protocol_errors{};
  std::uint64_t dropped_responses{};
  std::uint64_t authentication_rejections{};
  std::uint64_t response_wakeups{};
  std::uint64_t bytes_read{};
  std::uint64_t bytes_written{};
  std::size_t active_connections{};
};

struct EpollReactorQueues {
  SpscNetworkTaskQueue* requests{};
  SpscNetworkTaskQueue* responses{};
};

class EpollReactor {
public:
  EpollReactor() noexcept;
  ~EpollReactor();
  EpollReactor(const EpollReactor&) = delete;
  EpollReactor& operator=(const EpollReactor&) = delete;
  EpollReactor(EpollReactor&&) noexcept;
  EpollReactor& operator=(EpollReactor&&) noexcept;

  [[nodiscard]] static common::Result<EpollReactor> start(const EpollServerConfig& config,
                                                          const EpollReactorQueues& queues);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  // The single response producer may call this while the owner is inside poll_once. It must be
  // joined before shutdown or destruction.
  [[nodiscard]] common::Status notify_response_ready() noexcept;
  [[nodiscard]] common::Status shutdown() noexcept;
  [[nodiscard]] std::uint16_t bound_port() const noexcept;
  [[nodiscard]] EpollServerMetrics metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit EpollReactor(std::unique_ptr<Impl> implementation) noexcept;
  [[nodiscard]] int native_poll_descriptor() const noexcept;
  std::unique_ptr<Impl> implementation_;

  friend class Reactor;
};

} // namespace chronos::network

#endif // CHRONOS_NETWORK_EPOLL_REACTOR_HPP_
