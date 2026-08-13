#include "chronos/cluster/distributed_grouped_query_tcp_server.hpp"

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

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status poll_error(const int error = errno) {
  return {common::StatusCode::kIoError,
          std::string("polling grouped query TCP server: ") +
              std::error_code(error, std::generic_category()).message()};
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      DistributedGroupedQueryTlsServer::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

} // namespace

class DistributedGroupedQueryTcpServer::Impl {
public:
  struct Connection {
    Connection(network::TcpSocket owned_socket,
               DistributedGroupedQueryTlsServer owned_carrier) noexcept
        : socket(std::move(owned_socket)), carrier(std::move(owned_carrier)) {}

    // Destruction is reverse declaration order: TLS carrier before its borrowed descriptor.
    network::TcpSocket socket;
    DistributedGroupedQueryTlsServer carrier;
  };

  Impl(DistributedGroupedQueryTcpServerConfig configured, network::TcpListener listener_owner,
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
    for (std::size_t admitted = 0U; admitted < config.maximum_accepts_per_poll; ++admitted) {
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
      const auto peer = socket.peer_endpoint();
      if (!peer.has_value()) {
        ++server_metrics.rejected_connections;
        continue;
      }
      auto tls = network::TlsSocket::accept(tls_context, socket.descriptor());
      if (!tls.has_value()) {
        ++server_metrics.rejected_connections;
        continue;
      }
      auto carrier =
          DistributedGroupedQueryTlsServer::create(std::move(*tls),
                                                   {.authenticator = config.authenticator,
                                                    .receiver = config.receiver,
                                                    .peer_ipv4_address = peer->address,
                                                    .limits = config.carrier_limits},
                                                   now);
      if (!carrier.has_value()) {
        ++server_metrics.rejected_connections;
        continue;
      }
      try {
        connections.emplace_back(
            std::make_unique<Connection>(std::move(socket), std::move(*carrier)));
      } catch (const std::bad_alloc&) {
        ++server_metrics.rejected_connections;
        continue;
      }
      ++server_metrics.accepted_connections;
      server_metrics.active_connections = connections.size();
    }
  }

  DistributedGroupedQueryTcpServerConfig config;
  network::TcpListener listener;
  network::TlsServerContext tls_context;
  std::vector<std::unique_ptr<Connection>> connections;
  std::vector<pollfd> poll_descriptors;
  DistributedGroupedQueryTcpServerMetrics server_metrics;
  bool running{true};
};

DistributedGroupedQueryTcpServer::DistributedGroupedQueryTcpServer() noexcept = default;
DistributedGroupedQueryTcpServer::DistributedGroupedQueryTcpServer(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedGroupedQueryTcpServer::~DistributedGroupedQueryTcpServer() = default;
DistributedGroupedQueryTcpServer::DistributedGroupedQueryTcpServer(
    DistributedGroupedQueryTcpServer&&) noexcept = default;
DistributedGroupedQueryTcpServer&
DistributedGroupedQueryTcpServer::operator=(DistributedGroupedQueryTcpServer&&) noexcept = default;

common::Result<DistributedGroupedQueryTcpServer>
DistributedGroupedQueryTcpServer::start(DistributedGroupedQueryTcpServerConfig config) {
  if (config.authenticator == nullptr || config.receiver == nullptr ||
      config.maximum_connections == 0U || config.maximum_connections > 65536U ||
      config.maximum_accepts_per_poll == 0U || config.maximum_accepts_per_poll > 1024U ||
      !valid_timeout(config.carrier_limits.handshake_timeout) ||
      !valid_timeout(config.carrier_limits.exchange_timeout) ||
      config.carrier_limits.maximum_response_frames == 0U ||
      config.carrier_limits.maximum_response_frames >
          query::kMaximumDistributedCoordinatorMessages) {
    return common::make_unexpected(invalid("grouped query TCP server configuration is invalid"));
  }
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
    return DistributedGroupedQueryTcpServer{
        std::make_unique<Impl>(std::move(config), std::move(*listener), std::move(*context),
                               std::move(connections), std::move(descriptors))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped query TCP server allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped query TCP server limits are too large"));
  }
}

common::Status
DistributedGroupedQueryTcpServer::poll_once(const std::chrono::milliseconds maximum_wait) {
  if (!implementation_ || !implementation_->running)
    return invalid("grouped query TCP server is not running");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return invalid("grouped query TCP poll timeout is invalid");
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
      impl.remove_connection(index, false);
      continue;
    }
    const common::Status status =
        impl.connections[index]->carrier.on_ready(readable, writable, now);
    const auto state = impl.connections[index]->carrier.state();
    if (!status.is_ok() || state == DistributedGroupedQueryTlsState::kFailed)
      impl.remove_connection(index, false);
    else if (state == DistributedGroupedQueryTlsState::kComplete)
      impl.remove_connection(index, true);
  }
  if (ready > 0 && (impl.poll_descriptors[0].revents & POLLIN) != 0)
    impl.accept_ready(now);
  return common::Status::ok();
}

common::Status DistributedGroupedQueryTcpServer::shutdown() {
  if (!implementation_ || !implementation_->running)
    return common::Status::ok();
  Impl& impl = *implementation_;
  impl.connections.clear();
  impl.server_metrics.active_connections = 0U;
  const common::Status closed = impl.listener.close();
  impl.running = false;
  return closed;
}

network::Ipv4Endpoint DistributedGroupedQueryTcpServer::bound_endpoint() const noexcept {
  return implementation_ ? implementation_->listener.bound_endpoint() : network::Ipv4Endpoint{};
}

DistributedGroupedQueryTcpServerMetrics DistributedGroupedQueryTcpServer::metrics() const noexcept {
  return implementation_ ? implementation_->server_metrics
                         : DistributedGroupedQueryTcpServerMetrics{};
}

bool DistributedGroupedQueryTcpServer::is_running() const noexcept {
  return implementation_ && implementation_->running;
}

} // namespace chronos::cluster
