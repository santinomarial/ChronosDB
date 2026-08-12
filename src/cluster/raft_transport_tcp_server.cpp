#include "chronos/cluster/raft_transport_tcp_server.hpp"

#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <memory>
#include <new>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {
[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}
[[nodiscard]] common::Status poll_error(const int error = errno) {
  return {common::StatusCode::kIoError,
          std::string("polling Raft TCP server: ") +
              std::error_code(error, std::generic_category()).message()};
}
[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      RaftTransportTlsServer::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}
} // namespace

class RaftTransportTcpServer::Impl {
public:
  struct Connection {
    Connection(network::TcpSocket owned_socket, RaftTransportTlsServer owned_carrier) noexcept
        : socket(std::move(owned_socket)), carrier(std::move(owned_carrier)) {}
    network::TcpSocket socket;
    RaftTransportTlsServer carrier;
  };
  Impl(RaftTransportTcpServerConfig configured, network::TcpListener listener_owner,
       network::TlsServerContext context,
       std::vector<std::unique_ptr<Connection>> connection_storage,
       std::vector<pollfd> poll_storage) noexcept
      : config(std::move(configured)), listener(std::move(listener_owner)),
        tls_context(std::move(context)), connections(std::move(connection_storage)),
        poll_descriptors(std::move(poll_storage)) {}
  void remove(const std::size_t index) {
    connections.erase(connections.begin() + static_cast<std::ptrdiff_t>(index));
    ++metrics.failed_connections;
    metrics.active_connections = connections.size();
  }
  void accept_ready(const std::chrono::steady_clock::time_point now) {
    for (std::size_t admitted = 0U; admitted < config.maximum_accepts_per_poll; ++admitted) {
      auto next = listener.accept_one();
      if (!next.has_value()) {
        ++metrics.accept_errors;
        return;
      }
      if (!next->has_value())
        return;
      if (connections.size() == config.maximum_connections) {
        ++metrics.rejected_connections;
        continue;
      }
      network::TcpSocket socket = std::move(**next);
      auto peer = socket.peer_endpoint();
      if (!peer.has_value()) {
        ++metrics.rejected_connections;
        continue;
      }
      auto tls = network::TlsSocket::accept(tls_context, socket.descriptor());
      if (!tls.has_value()) {
        ++metrics.rejected_connections;
        continue;
      }
      auto carrier = RaftTransportTlsServer::create(std::move(*tls),
                                                    {.authenticator = config.authenticator,
                                                     .receiver = config.receiver,
                                                     .peer_ipv4_address = peer->address,
                                                     .limits = config.carrier_limits,
                                                     .codec_limits = config.codec_limits},
                                                    now);
      if (!carrier.has_value()) {
        ++metrics.rejected_connections;
        continue;
      }
      try {
        connections.emplace_back(
            std::make_unique<Connection>(std::move(socket), std::move(*carrier)));
      } catch (const std::bad_alloc&) {
        ++metrics.rejected_connections;
        continue;
      }
      ++metrics.accepted_connections;
      metrics.active_connections = connections.size();
    }
  }
  RaftTransportTcpServerConfig config;
  network::TcpListener listener;
  network::TlsServerContext tls_context;
  std::vector<std::unique_ptr<Connection>> connections;
  std::vector<pollfd> poll_descriptors;
  RaftTransportTcpServerMetrics metrics;
  bool running{true};
};

RaftTransportTcpServer::RaftTransportTcpServer() noexcept = default;
RaftTransportTcpServer::RaftTransportTcpServer(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftTransportTcpServer::~RaftTransportTcpServer() = default;
RaftTransportTcpServer::RaftTransportTcpServer(RaftTransportTcpServer&&) noexcept = default;
RaftTransportTcpServer&
RaftTransportTcpServer::operator=(RaftTransportTcpServer&&) noexcept = default;

common::Result<RaftTransportTcpServer>
RaftTransportTcpServer::start(RaftTransportTcpServerConfig config) {
  if (config.authenticator == nullptr || config.receiver == nullptr ||
      config.maximum_connections == 0U || config.maximum_connections > 65'536U ||
      config.maximum_accepts_per_poll == 0U || config.maximum_accepts_per_poll > 1024U ||
      !valid_timeout(config.carrier_limits.handshake_timeout) ||
      !valid_timeout(config.carrier_limits.frame_read_timeout))
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "Raft TCP server configuration is invalid"));
  auto reader = raft::RaftTransportFrameReader::create(config.codec_limits);
  if (!reader.has_value())
    return common::make_unexpected(reader.error());
  auto context = network::TlsServerContext::create(config.tls);
  if (!context.has_value())
    return common::make_unexpected(context.error());
  auto listener = network::TcpListener::bind(config.listener);
  if (!listener.has_value())
    return common::make_unexpected(listener.error());
  try {
    std::vector<std::unique_ptr<Impl::Connection>> connections;
    connections.reserve(config.maximum_connections);
    std::vector<pollfd> descriptors(config.maximum_connections + 1U);
    return RaftTransportTcpServer{
        std::make_unique<Impl>(std::move(config), std::move(*listener), std::move(*context),
                               std::move(connections), std::move(descriptors))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted, "Raft TCP server allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft TCP server limits exceed container limits"));
  }
}

common::Status RaftTransportTcpServer::poll_once(const std::chrono::milliseconds maximum_wait) {
  if (!implementation_ || !implementation_->running)
    return status(common::StatusCode::kInvalidArgument, "Raft TCP server is not running");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return status(common::StatusCode::kInvalidArgument, "Raft TCP poll timeout is invalid");
  Impl& impl = *implementation_;
  impl.poll_descriptors[0] = {.fd = impl.listener.descriptor(), .events = POLLIN};
  for (std::size_t index = 0U; index < impl.connections.size(); ++index) {
    const auto interest = impl.connections[index]->carrier.interest();
    short events{};
    if (interest.want_read)
      events |= POLLIN;
    if (interest.want_write)
      events |= POLLOUT;
    impl.poll_descriptors[index + 1U] = {.fd = impl.connections[index]->socket.descriptor(),
                                         .events = events};
  }
  const nfds_t count = static_cast<nfds_t>(impl.connections.size() + 1U);
  const int ready =
      ::poll(impl.poll_descriptors.data(), count, static_cast<int>(maximum_wait.count()));
  if (ready < 0 && errno != EINTR)
    return poll_error();
  const auto now = std::chrono::steady_clock::now();
  for (std::size_t remaining = impl.connections.size(); remaining > 0U; --remaining) {
    const std::size_t index = remaining - 1U;
    const short events = impl.poll_descriptors[index + 1U].revents;
    const bool readable = (events & POLLIN) != 0;
    const bool writable = (events & POLLOUT) != 0;
    if ((events & (POLLERR | POLLHUP | POLLNVAL)) != 0 && !readable && !writable) {
      impl.remove(index);
      continue;
    }
    const common::Status progress =
        impl.connections[index]->carrier.on_ready(readable, writable, now);
    if (!progress.is_ok() ||
        impl.connections[index]->carrier.state() == RaftTransportTlsServerState::kFailed)
      impl.remove(index);
  }
  if (ready > 0 && (impl.poll_descriptors[0].revents & POLLIN) != 0)
    impl.accept_ready(now);
  return common::Status::ok();
}

common::Result<std::optional<RaftTransportCompletedReceive>>
RaftTransportTcpServer::take_completed() {
  if (!implementation_ || !implementation_->running)
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "Raft TCP server is not running"));
  for (std::unique_ptr<Impl::Connection>& connection : implementation_->connections) {
    if (connection->carrier.state() != RaftTransportTlsServerState::kResultReady)
      continue;
    auto completed = connection->carrier.take_completed(std::chrono::steady_clock::now());
    if (!completed.has_value())
      return common::make_unexpected(completed.error());
    ++implementation_->metrics.completed_results;
    return std::optional<RaftTransportCompletedReceive>{std::move(*completed)};
  }
  return std::optional<RaftTransportCompletedReceive>{};
}

std::optional<std::chrono::steady_clock::time_point>
RaftTransportTcpServer::next_deadline() const noexcept {
  std::optional<std::chrono::steady_clock::time_point> next;
  if (!implementation_)
    return next;
  for (const std::unique_ptr<Impl::Connection>& connection : implementation_->connections) {
    const auto deadline = connection->carrier.next_deadline();
    if (deadline.has_value() && (!next.has_value() || *deadline < *next))
      next = deadline;
  }
  return next;
}

common::Status RaftTransportTcpServer::shutdown() {
  if (!implementation_ || !implementation_->running)
    return common::Status::ok();
  implementation_->connections.clear();
  implementation_->metrics.active_connections = 0U;
  const common::Status closed = implementation_->listener.close();
  implementation_->running = false;
  return closed;
}
network::Ipv4Endpoint RaftTransportTcpServer::bound_endpoint() const noexcept {
  return implementation_ ? implementation_->listener.bound_endpoint() : network::Ipv4Endpoint{};
}
RaftTransportTcpServerMetrics RaftTransportTcpServer::metrics() const noexcept {
  return implementation_ ? implementation_->metrics : RaftTransportTcpServerMetrics{};
}
bool RaftTransportTcpServer::is_running() const noexcept {
  return implementation_ && implementation_->running;
}

} // namespace chronos::cluster
