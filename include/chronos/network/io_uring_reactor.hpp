#ifndef CHRONOS_NETWORK_IO_URING_REACTOR_HPP_
#define CHRONOS_NETWORK_IO_URING_REACTOR_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/epoll_reactor.hpp"

#include <chrono>
#include <cstdint>
#include <memory>

namespace chronos::network {

// Linux-only liburing socket reactor. Portable protocol and connection types remain independent of
// liburing; the implementation PIMPL owns every SQE/CQE lifetime and native descriptor. At most one
// receive or send operation is outstanding per connection, so connection buffers cannot be
// reclaimed or advanced before their completion is observed.
class IoUringReactor {
public:
  IoUringReactor() noexcept;
  ~IoUringReactor();
  IoUringReactor(const IoUringReactor&) = delete;
  IoUringReactor& operator=(const IoUringReactor&) = delete;
  IoUringReactor(IoUringReactor&&) noexcept;
  IoUringReactor& operator=(IoUringReactor&&) noexcept;

  [[nodiscard]] static common::Result<IoUringReactor> start(const EpollServerConfig& config,
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
  explicit IoUringReactor(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::network

#endif // CHRONOS_NETWORK_IO_URING_REACTOR_HPP_
