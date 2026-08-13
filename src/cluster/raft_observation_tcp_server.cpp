#include "chronos/cluster/raft_observation_tcp_server.hpp"

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

[[nodiscard]] common::Status status(common::StatusCode code, const char* message) {
  return {code, message};
}

[[nodiscard]] common::Status poll_error(const int error = errno) {
  return {common::StatusCode::kIoError,
          std::string("polling Raft observation TCP server: ") +
              std::error_code(error, std::generic_category()).message()};
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      RaftObservationTlsServer::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

} // namespace

class RaftObservationTcpServer::Impl {
public:
  struct Connection {
    Connection(network::TcpSocket owned_socket, RaftObservationTlsServer owned_session) noexcept
        : socket(std::move(owned_socket)), session(std::move(owned_session)) {}

    // Reverse declaration order destroys TLS before its borrowed descriptor.
    network::TcpSocket socket;
    RaftObservationTlsServer session;
  };

  Impl(RaftObservationTcpServerConfig configured, network::TcpListener listener_owner,
       network::TlsServerContext context,
       std::vector<std::unique_ptr<Connection>> connection_storage,
       std::vector<pollfd> poll_storage) noexcept
      : config(std::move(configured)), listener(std::move(listener_owner)),
        tls_context(std::move(context)), connections(std::move(connection_storage)),
        poll_descriptors(std::move(poll_storage)) {}

  void remove_connection(const std::size_t index, const bool complete) {
    connections.erase(connections.begin() + static_cast<std::ptrdiff_t>(index));
    if (complete)
      ++server_metrics.completed_connections;
    else
      ++server_metrics.failed_connections;
    server_metrics.active_connections = connections.size();
  }

  void accept_ready(const std::chrono::steady_clock::time_point now) {
    for (std::size_t accepted = 0U; accepted < config.maximum_accepts_per_poll; ++accepted) {
      auto next = listener.accept_one();
      if (!next.has_value()) {
        ++server_metrics.accept_errors;
        return;
      }
      if (!next->has_value())
        return;
      if (connections.size() == config.maximum_connections) {
        ++server_metrics.rejected_connections;
        continue;
      }
      network::TcpSocket socket = std::move(**next);
      auto peer = socket.peer_endpoint();
      if (!peer.has_value()) {
        ++server_metrics.rejected_connections;
        continue;
      }
      auto tls = network::TlsSocket::accept(tls_context, socket.descriptor());
      if (!tls.has_value()) {
        ++server_metrics.rejected_connections;
        continue;
      }
      auto session = RaftObservationTlsServer::create(std::move(*tls),
                                                      {.authenticator = config.authenticator,
                                                       .receiver = config.receiver,
                                                       .peer_ipv4_address = peer->address,
                                                       .limits = config.session_limits},
                                                      now);
      if (!session.has_value()) {
        ++server_metrics.rejected_connections;
        continue;
      }
      try {
        connections.emplace_back(
            std::make_unique<Connection>(std::move(socket), std::move(*session)));
      } catch (const std::bad_alloc&) {
        ++server_metrics.rejected_connections;
        continue;
      }
      ++server_metrics.accepted_connections;
      server_metrics.active_connections = connections.size();
    }
  }

  RaftObservationTcpServerConfig config;
  network::TcpListener listener;
  network::TlsServerContext tls_context;
  std::vector<std::unique_ptr<Connection>> connections;
  std::vector<pollfd> poll_descriptors;
  RaftObservationTcpServerMetrics server_metrics;
  bool running{true};
};

RaftObservationTcpServer::RaftObservationTcpServer() noexcept = default;
RaftObservationTcpServer::RaftObservationTcpServer(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftObservationTcpServer::~RaftObservationTcpServer() = default;
RaftObservationTcpServer::RaftObservationTcpServer(RaftObservationTcpServer&&) noexcept = default;
RaftObservationTcpServer&
RaftObservationTcpServer::operator=(RaftObservationTcpServer&&) noexcept = default;

common::Result<RaftObservationTcpServer>
RaftObservationTcpServer::start(RaftObservationTcpServerConfig config) {
  if (config.authenticator == nullptr || config.receiver == nullptr ||
      config.maximum_connections == 0U || config.maximum_connections > 65536U ||
      config.maximum_accepts_per_poll == 0U || config.maximum_accepts_per_poll > 1024U ||
      !valid_timeout(config.session_limits.handshake_timeout) ||
      !valid_timeout(config.session_limits.exchange_timeout)) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft observation TCP server configuration is invalid"));
  }
  auto limits = RaftObservationResponseReader::create(config.session_limits.transport);
  if (!limits.has_value())
    return common::make_unexpected(limits.error());
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
    return RaftObservationTcpServer{
        std::make_unique<Impl>(std::move(config), std::move(*listener), std::move(*context),
                               std::move(connections), std::move(descriptors))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft observation TCP server allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft observation TCP server limits are too large"));
  }
}

common::Status RaftObservationTcpServer::poll_once(const std::chrono::milliseconds maximum_wait) {
  if (!implementation_ || !implementation_->running)
    return status(common::StatusCode::kInvalidArgument,
                  "Raft observation TCP server is not running");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return status(common::StatusCode::kInvalidArgument,
                  "Raft observation TCP poll timeout is invalid");
  Impl& impl = *implementation_;
  impl.poll_descriptors[0] = {.fd = impl.listener.descriptor(), .events = POLLIN};
  for (std::size_t index = 0U; index < impl.connections.size(); ++index) {
    const auto interest = impl.connections[index]->session.interest();
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
      impl.remove_connection(index, false);
      continue;
    }
    const common::Status progress =
        impl.connections[index]->session.on_ready(readable, writable, now);
    const auto state = impl.connections[index]->session.state();
    if (!progress.is_ok() || state == RaftObservationTlsServerState::kFailed)
      impl.remove_connection(index, false);
    else if (state == RaftObservationTlsServerState::kComplete)
      impl.remove_connection(index, true);
  }
  if (ready > 0 && (impl.poll_descriptors[0].revents & POLLIN) != 0)
    impl.accept_ready(now);
  return common::Status::ok();
}

common::Status RaftObservationTcpServer::shutdown() {
  if (!implementation_ || !implementation_->running)
    return common::Status::ok();
  Impl& impl = *implementation_;
  impl.connections.clear();
  impl.server_metrics.active_connections = 0U;
  const common::Status closed = impl.listener.close();
  impl.running = false;
  return closed;
}

network::Ipv4Endpoint RaftObservationTcpServer::bound_endpoint() const noexcept {
  return implementation_ ? implementation_->listener.bound_endpoint() : network::Ipv4Endpoint{};
}

RaftObservationTcpServerMetrics RaftObservationTcpServer::metrics() const noexcept {
  return implementation_ ? implementation_->server_metrics : RaftObservationTcpServerMetrics{};
}

bool RaftObservationTcpServer::is_running() const noexcept {
  return implementation_ && implementation_->running;
}

} // namespace chronos::cluster
