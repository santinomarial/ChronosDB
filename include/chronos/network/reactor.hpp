#ifndef CHRONOS_NETWORK_REACTOR_HPP_
#define CHRONOS_NETWORK_REACTOR_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/epoll_reactor.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <string_view>

namespace chronos::network {

enum class ReactorBackend : std::uint8_t { kEpoll = 1, kIoUring = 2 };

[[nodiscard]] std::string_view reactor_backend_name(ReactorBackend backend) noexcept;
[[nodiscard]] bool reactor_backend_compiled(ReactorBackend backend) noexcept;

// Explicit portable backend owner. Epoll remains the reference; the optional Linux backend owns
// accept, receive, send, and response-wakeup operations through liburing. Both preserve the same
// portable protocol, bounded queue, connection-state, partial-I/O, cancellation, and shutdown
// contracts. Backend availability is explicit and makes no relative performance claim.
class Reactor {
public:
  Reactor() = delete;
  ~Reactor();
  Reactor(const Reactor&) = delete;
  Reactor& operator=(const Reactor&) = delete;
  Reactor(Reactor&&) noexcept;
  Reactor& operator=(Reactor&&) noexcept;

  [[nodiscard]] static common::Result<Reactor>
  start(ReactorBackend backend, const EpollServerConfig& config, const EpollReactorQueues& queues);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status reload_tls_security(NetworkSecurityConfig&& replacement);
  [[nodiscard]] common::Status notify_response_ready() noexcept;
  [[nodiscard]] common::Status shutdown() noexcept;
  [[nodiscard]] std::uint16_t bound_port() const noexcept;
  [[nodiscard]] EpollServerMetrics metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;
  [[nodiscard]] ReactorBackend backend() const noexcept;

private:
  class Impl;
  explicit Reactor(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::network

#endif // CHRONOS_NETWORK_REACTOR_HPP_
