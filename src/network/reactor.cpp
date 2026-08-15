#include "chronos/network/reactor.hpp"

#include "chronos/network/io_uring_reactor.hpp"

#include <chrono>
#include <exception>
#include <memory>
#include <utility>
#include <variant>

namespace chronos::network {

class Reactor::Impl {
public:
  explicit Impl(EpollReactor reactor) noexcept : backend(std::move(reactor)) {}
  explicit Impl(IoUringReactor reactor) noexcept : backend(std::move(reactor)) {}

  [[nodiscard]] common::Status poll_once(const std::chrono::milliseconds maximum_wait) {
    if (auto* reactor = std::get_if<EpollReactor>(&backend); reactor != nullptr)
      return reactor->poll_once(maximum_wait);
    if (auto* reactor = std::get_if<IoUringReactor>(&backend); reactor != nullptr)
      return reactor->poll_once(maximum_wait);
    std::terminate();
  }

  [[nodiscard]] common::Status notify_response_ready() noexcept {
    if (auto* reactor = std::get_if<EpollReactor>(&backend); reactor != nullptr)
      return reactor->notify_response_ready();
    if (auto* reactor = std::get_if<IoUringReactor>(&backend); reactor != nullptr)
      return reactor->notify_response_ready();
    std::terminate();
  }

  [[nodiscard]] common::Status shutdown() noexcept {
    if (auto* reactor = std::get_if<EpollReactor>(&backend); reactor != nullptr)
      return reactor->shutdown();
    if (auto* reactor = std::get_if<IoUringReactor>(&backend); reactor != nullptr)
      return reactor->shutdown();
    std::terminate();
  }

  [[nodiscard]] std::uint16_t bound_port() const noexcept {
    if (const auto* reactor = std::get_if<EpollReactor>(&backend); reactor != nullptr)
      return reactor->bound_port();
    if (const auto* reactor = std::get_if<IoUringReactor>(&backend); reactor != nullptr)
      return reactor->bound_port();
    std::terminate();
  }

  [[nodiscard]] EpollServerMetrics metrics() const noexcept {
    if (const auto* reactor = std::get_if<EpollReactor>(&backend); reactor != nullptr)
      return reactor->metrics();
    if (const auto* reactor = std::get_if<IoUringReactor>(&backend); reactor != nullptr)
      return reactor->metrics();
    std::terminate();
  }

  [[nodiscard]] bool is_running() const noexcept {
    if (const auto* reactor = std::get_if<EpollReactor>(&backend); reactor != nullptr)
      return reactor->is_running();
    if (const auto* reactor = std::get_if<IoUringReactor>(&backend); reactor != nullptr)
      return reactor->is_running();
    std::terminate();
  }

  [[nodiscard]] ReactorBackend selected_backend() const noexcept {
    return std::holds_alternative<EpollReactor>(backend) ? ReactorBackend::kEpoll
                                                         : ReactorBackend::kIoUring;
  }

  std::variant<EpollReactor, IoUringReactor> backend;
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
  return impl_->poll_once(maximum_wait);
}

common::Status Reactor::notify_response_ready() noexcept {
  if (!impl_)
    return common::Status{common::StatusCode::kInvalidArgument, "reactor is not running"};
  return impl_->notify_response_ready();
}
common::Status Reactor::shutdown() noexcept {
  if (!impl_)
    return common::Status::ok();
  return impl_->shutdown();
}
std::uint16_t Reactor::bound_port() const noexcept {
  if (!impl_)
    return 0U;
  return impl_->bound_port();
}
EpollServerMetrics Reactor::metrics() const noexcept {
  if (!impl_)
    return {};
  return impl_->metrics();
}
bool Reactor::is_running() const noexcept {
  if (!impl_)
    return false;
  return impl_->is_running();
}
ReactorBackend Reactor::backend() const noexcept {
  return impl_ ? impl_->selected_backend() : ReactorBackend::kEpoll;
}

} // namespace chronos::network
