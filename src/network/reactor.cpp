#include "chronos/network/reactor.hpp"

#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>

#if defined(CHRONOS_HAS_LIBURING)
#include <cerrno>
#include <liburing.h>
#include <poll.h>
#endif

namespace chronos::network {

class Reactor::Impl {
public:
  Impl(const ReactorBackend selected, EpollReactor reactor) noexcept
      : selected_backend(selected), epoll(std::move(reactor)) {}
  ~Impl() {
#if defined(CHRONOS_HAS_LIBURING)
    if (ring_initialized)
      io_uring_queue_exit(&ring);
#endif
  }
  ReactorBackend selected_backend;
  EpollReactor epoll;
#if defined(CHRONOS_HAS_LIBURING)
  io_uring ring{};
  bool ring_initialized{};
  std::uint64_t next_operation{1U};
#endif
};

std::string_view reactor_backend_name(const ReactorBackend backend) noexcept {
  switch (backend) {
  case ReactorBackend::kEpoll:
    return "epoll";
  case ReactorBackend::kIoUring:
    return "io_uring";
  }
  return "unknown";
}

bool reactor_backend_compiled(const ReactorBackend backend) noexcept {
  if (backend == ReactorBackend::kEpoll)
    return true;
#if defined(CHRONOS_HAS_LIBURING)
  return backend == ReactorBackend::kIoUring;
#else
  return false;
#endif
}

Reactor::Reactor(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}
Reactor::~Reactor() = default;
Reactor::Reactor(Reactor&&) noexcept = default;
Reactor& Reactor::operator=(Reactor&&) noexcept = default;

common::Result<Reactor> Reactor::start(const ReactorBackend backend,
                                       const EpollServerConfig& config,
                                       const EpollReactorQueues& queues) {
  if (backend != ReactorBackend::kEpoll && backend != ReactorBackend::kIoUring) {
    return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                  "reactor backend selection is invalid"});
  }
  if (!reactor_backend_compiled(backend)) {
    return common::make_unexpected(common::Status{common::StatusCode::kNotSupported,
                                                  "requested reactor backend is not compiled"});
  }
  auto epoll = EpollReactor::start(config, queues);
  if (!epoll.has_value())
    return common::make_unexpected(epoll.error());
  auto impl = std::make_unique<Impl>(backend, std::move(*epoll));
#if defined(CHRONOS_HAS_LIBURING)
  if (backend == ReactorBackend::kIoUring) {
    const int result = io_uring_queue_init(8U, &impl->ring, 0U);
    if (result < 0) {
      static_cast<void>(impl->epoll.shutdown());
      return common::make_unexpected(common::Status{result == -ENOSYS || result == -EINVAL
                                                        ? common::StatusCode::kNotSupported
                                                        : common::StatusCode::kIoError,
                                                    "io_uring initialization failed"});
    }
    impl->ring_initialized = true;
  }
#endif
  return Reactor{std::move(impl)};
}

common::Status Reactor::poll_once(const std::chrono::milliseconds maximum_wait) {
  if (!impl_ || maximum_wait.count() < 0 ||
      maximum_wait.count() > std::numeric_limits<std::int32_t>::max()) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "reactor is not running or wait is invalid"};
  }
  if (impl_->selected_backend == ReactorBackend::kEpoll) {
    return impl_->epoll.poll_once(maximum_wait);
  }
#if defined(CHRONOS_HAS_LIBURING)
  io_uring_sqe* poll = io_uring_get_sqe(&impl_->ring);
  if (poll == nullptr) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "io_uring submission queue is full"};
  }
  if (impl_->next_operation >= (std::uint64_t{1U} << 63U)) {
    return common::Status{common::StatusCode::kOutOfRange,
                          "io_uring operation identity is exhausted"};
  }
  const std::uint64_t operation = impl_->next_operation++;
  io_uring_prep_poll_add(poll, impl_->epoll.native_poll_descriptor(), POLLIN);
  io_uring_sqe_set_data64(poll, operation);
  const int submitted = io_uring_submit(&impl_->ring);
  if (submitted != 1) {
    return common::Status{common::StatusCode::kIoError, "io_uring poll submission failed"};
  }
  __kernel_timespec timeout{maximum_wait.count() / 1000, (maximum_wait.count() % 1000) * 1'000'000};
  io_uring_cqe* completion = nullptr;
  const int waited = io_uring_wait_cqe_timeout(&impl_->ring, &completion, &timeout);
  if (waited == -ETIME) {
    io_uring_sqe* cancel = io_uring_get_sqe(&impl_->ring);
    if (cancel == nullptr) {
      return common::Status{common::StatusCode::kResourceExhausted,
                            "io_uring cancellation queue is full"};
    }
    io_uring_prep_cancel64(cancel, operation, 0U);
    io_uring_sqe_set_data64(cancel, operation + (std::uint64_t{1U} << 63U));
    if (io_uring_submit(&impl_->ring) < 0) {
      return common::Status{common::StatusCode::kIoError, "io_uring cancellation failed"};
    }
    for (std::size_t pending = 0U; pending < 2U; ++pending) {
      io_uring_cqe* cancelled = nullptr;
      if (io_uring_wait_cqe(&impl_->ring, &cancelled) != 0 || cancelled == nullptr) {
        return common::Status{common::StatusCode::kIoError,
                              "io_uring cancellation completion failed"};
      }
      io_uring_cqe_seen(&impl_->ring, cancelled);
    }
    return impl_->epoll.poll_once(std::chrono::milliseconds{0});
  }
  if (waited < 0 || completion == nullptr) {
    return common::Status{common::StatusCode::kIoError, "io_uring readiness wait failed"};
  }
  const int result = completion->res;
  io_uring_cqe_seen(&impl_->ring, completion);
  if (result < 0) {
    return common::Status{common::StatusCode::kIoError, "io_uring readiness operation failed"};
  }
  return impl_->epoll.poll_once(std::chrono::milliseconds{0});
#else
  return common::Status{common::StatusCode::kNotSupported, "io_uring backend is unavailable"};
#endif
}

common::Status Reactor::notify_response_ready() noexcept {
  return impl_ ? impl_->epoll.notify_response_ready()
               : common::Status{common::StatusCode::kInvalidArgument, "reactor is not running"};
}
common::Status Reactor::shutdown() noexcept {
  return impl_ ? impl_->epoll.shutdown() : common::Status::ok();
}
std::uint16_t Reactor::bound_port() const noexcept {
  return impl_ ? impl_->epoll.bound_port() : 0U;
}
EpollServerMetrics Reactor::metrics() const noexcept {
  return impl_ ? impl_->epoll.metrics() : EpollServerMetrics{};
}
bool Reactor::is_running() const noexcept {
  return impl_ && impl_->epoll.is_running();
}
ReactorBackend Reactor::backend() const noexcept {
  return impl_->selected_backend;
}

} // namespace chronos::network
