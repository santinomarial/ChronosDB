#include "chronos/network/reactor.hpp"

#include "chronos/network/io_uring_reactor.hpp"

#include <chrono>
#include <memory>
#include <optional>
#include <utility>

namespace chronos::network {

class Reactor::Impl {
public:
  explicit Impl(EpollReactor reactor) noexcept
      : selected_backend(ReactorBackend::kEpoll), epoll(std::move(reactor)) {}
  explicit Impl(IoUringReactor reactor) noexcept
      : selected_backend(ReactorBackend::kIoUring), io_uring(std::move(reactor)) {}

  ReactorBackend selected_backend;
  std::optional<EpollReactor> epoll;
  std::optional<IoUringReactor> io_uring;
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
  if (backend == ReactorBackend::kEpoll) {
    auto epoll = EpollReactor::start(config, queues);
    if (!epoll.has_value())
      return common::make_unexpected(epoll.error());
    return Reactor{std::make_unique<Impl>(std::move(*epoll))};
  }
  auto io_uring = IoUringReactor::start(config, queues);
  if (!io_uring.has_value())
    return common::make_unexpected(io_uring.error());
  return Reactor{std::make_unique<Impl>(std::move(*io_uring))};
}

common::Status Reactor::poll_once(const std::chrono::milliseconds maximum_wait) {
  if (!impl_) {
    return common::Status{common::StatusCode::kInvalidArgument, "reactor is not running"};
  }
  return impl_->selected_backend == ReactorBackend::kEpoll
             ? impl_->epoll->poll_once(maximum_wait)
             : impl_->io_uring->poll_once(maximum_wait);
}

common::Status Reactor::notify_response_ready() noexcept {
  if (!impl_)
    return common::Status{common::StatusCode::kInvalidArgument, "reactor is not running"};
  return impl_->selected_backend == ReactorBackend::kEpoll
             ? impl_->epoll->notify_response_ready()
             : impl_->io_uring->notify_response_ready();
}
common::Status Reactor::shutdown() noexcept {
  if (!impl_)
    return common::Status::ok();
  return impl_->selected_backend == ReactorBackend::kEpoll ? impl_->epoll->shutdown()
                                                           : impl_->io_uring->shutdown();
}
std::uint16_t Reactor::bound_port() const noexcept {
  if (!impl_)
    return 0U;
  return impl_->selected_backend == ReactorBackend::kEpoll ? impl_->epoll->bound_port()
                                                           : impl_->io_uring->bound_port();
}
EpollServerMetrics Reactor::metrics() const noexcept {
  if (!impl_)
    return {};
  return impl_->selected_backend == ReactorBackend::kEpoll ? impl_->epoll->metrics()
                                                           : impl_->io_uring->metrics();
}
bool Reactor::is_running() const noexcept {
  if (!impl_)
    return false;
  return impl_->selected_backend == ReactorBackend::kEpoll ? impl_->epoll->is_running()
                                                           : impl_->io_uring->is_running();
}
ReactorBackend Reactor::backend() const noexcept {
  return impl_ ? impl_->selected_backend : ReactorBackend::kEpoll;
}

} // namespace chronos::network
