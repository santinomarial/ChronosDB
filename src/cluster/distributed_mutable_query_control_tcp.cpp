#include "chronos/cluster/distributed_mutable_query_control_tcp.hpp"

#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <limits>
#include <memory>
#include <new>
#include <poll.h>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}

void increment_saturated(std::uint64_t& value) noexcept {
  if (value != std::numeric_limits<std::uint64_t>::max())
    ++value;
}

} // namespace

class DistributedMutableQueryControlTcpServer::Impl {
public:
  struct Connection {
    Connection(network::TcpSocket owned_socket,
               DistributedMutableQueryControlTlsServer owned_carrier)
        : socket(std::move(owned_socket)), carrier(std::move(owned_carrier)) {}
    network::TcpSocket socket;
    DistributedMutableQueryControlTlsServer carrier;
  };

  Impl(DistributedMutableQueryControlTcpServerConfig configured,
       network::TcpListener owned_listener, network::TlsServerContext owned_context,
       std::vector<std::unique_ptr<Connection>> owned_connections,
       std::vector<pollfd> descriptors) noexcept
      : config(std::move(configured)), listener(std::move(owned_listener)),
        tls_context(std::move(owned_context)), connections(std::move(owned_connections)),
        poll_descriptors(std::move(descriptors)) {}

  void remove_connection(const std::size_t index, const bool completed) noexcept {
    if (completed) {
      const auto protocol = connections[index]->carrier.protocol();
      if (protocol == DistributedMutableQueryControlProtocol::kMutableVectorQuery)
        increment_saturated(server_metrics.completed_mutable_queries);
      else if (protocol ==
               DistributedMutableQueryControlProtocol::kMutableVectorGroupedAggregateQuery)
        increment_saturated(server_metrics.completed_mutable_grouped_queries);
      else if (protocol == DistributedMutableQueryControlProtocol::kRaftReadAuthority)
        increment_saturated(server_metrics.completed_read_authorities);
      else if (protocol == DistributedMutableQueryControlProtocol::kGroupedShuffleJobControl)
        increment_saturated(server_metrics.completed_grouped_shuffle_job_controls);
      else
        increment_saturated(server_metrics.failed_connections);
    } else {
      increment_saturated(server_metrics.failed_connections);
    }
    if (index + 1U != connections.size())
      connections[index] = std::move(connections.back());
    connections.pop_back();
    server_metrics.active_connections = connections.size();
  }

  void reject_connection() noexcept {
    increment_saturated(server_metrics.rejected_connections);
  }

  void accept_ready(const std::chrono::steady_clock::time_point now) {
    for (std::size_t admitted = 0U; admitted < config.maximum_accepts_per_poll; ++admitted) {
      auto next = listener.accept_one();
      if (!next.has_value()) {
        increment_saturated(server_metrics.accept_errors);
        return;
      }
      if (!next->has_value())
        return;
      if (connections.size() == config.maximum_connections) {
        reject_connection();
        continue;
      }
      network::TcpSocket socket =
          std::move(next->value()); // NOLINT(bugprone-unchecked-optional-access)
      const auto peer = socket.peer_endpoint();
      if (!peer.has_value()) {
        reject_connection();
        continue;
      }
      auto tls = network::TlsSocket::accept(tls_context, socket.descriptor());
      if (!tls.has_value()) {
        reject_connection();
        continue;
      }
      auto carrier = DistributedMutableQueryControlTlsServer::create(
          std::move(*tls),
          {.authenticator = config.authenticator,
           .mutable_receiver = config.mutable_receiver,
           .mutable_grouped_receiver = config.mutable_grouped_receiver,
           .read_authority_receiver = config.read_authority_receiver,
           .grouped_shuffle_job_service = config.grouped_shuffle_job_service,
           .peer_ipv4_address = peer->address,
           .limits = config.carrier_limits},
          now);
      if (!carrier.has_value()) {
        reject_connection();
        continue;
      }
      try {
        connections.emplace_back(
            std::make_unique<Connection>(std::move(socket), std::move(*carrier)));
      } catch (const std::bad_alloc&) {
        reject_connection();
        continue;
      }
      increment_saturated(server_metrics.accepted_connections);
      server_metrics.active_connections = connections.size();
    }
  }

  DistributedMutableQueryControlTcpServerConfig config;
  network::TcpListener listener;
  network::TlsServerContext tls_context;
  std::vector<std::unique_ptr<Connection>> connections;
  std::vector<pollfd> poll_descriptors;
  DistributedMutableQueryControlTcpServerMetrics server_metrics;
  bool running{true};
};

DistributedMutableQueryControlTcpServer::DistributedMutableQueryControlTcpServer() noexcept =
    default;
DistributedMutableQueryControlTcpServer::DistributedMutableQueryControlTcpServer(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedMutableQueryControlTcpServer::~DistributedMutableQueryControlTcpServer() = default;
DistributedMutableQueryControlTcpServer::DistributedMutableQueryControlTcpServer(
    DistributedMutableQueryControlTcpServer&&) noexcept = default;
DistributedMutableQueryControlTcpServer& DistributedMutableQueryControlTcpServer::operator=(
    DistributedMutableQueryControlTcpServer&&) noexcept = default;

common::Result<DistributedMutableQueryControlTcpServer>
DistributedMutableQueryControlTcpServer::start(
    DistributedMutableQueryControlTcpServerConfig config) {
  if (config.authenticator == nullptr || config.mutable_receiver == nullptr ||
      config.mutable_grouped_receiver == nullptr || config.read_authority_receiver == nullptr ||
      config.maximum_connections == 0U || config.maximum_connections > 65536U ||
      config.maximum_accepts_per_poll == 0U || config.maximum_accepts_per_poll > 1024U) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "query-control TCP server configuration is invalid"));
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
    return DistributedMutableQueryControlTcpServer{
        std::make_unique<Impl>(std::move(config), std::move(*listener), std::move(*context),
                               std::move(connections), std::move(descriptors))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "query-control TCP server allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "query-control TCP server limits are too large"));
  }
}

common::Status
DistributedMutableQueryControlTcpServer::poll_once(const std::chrono::milliseconds maximum_wait) {
  if (!implementation_ || !implementation_->running)
    return status(common::StatusCode::kInvalidArgument, "query-control TCP server is not running");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return status(common::StatusCode::kInvalidArgument,
                  "query-control TCP poll timeout is invalid");
  Impl& impl = *implementation_;
  impl.poll_descriptors[0] = {.fd = impl.listener.descriptor(), .events = POLLIN, .revents = 0};
  for (std::size_t index = 0U; index < impl.connections.size(); ++index) {
    const auto interest = impl.connections[index]->carrier.interest();
    impl.poll_descriptors[index + 1U] = {
        .fd = impl.connections[index]->socket.descriptor(),
        .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                     (interest.want_write ? POLLOUT : 0)),
        .revents = 0};
  }
  const nfds_t count = static_cast<nfds_t>(impl.connections.size() + 1U);
  const int ready =
      ::poll(impl.poll_descriptors.data(), count, static_cast<int>(maximum_wait.count()));
  if (ready < 0 && errno != EINTR)
    return status(common::StatusCode::kIoError, "polling query-control TCP server failed");
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
        impl.connections[index]->carrier.on_ready(readable, writable, now);
    const auto state = impl.connections[index]->carrier.state();
    if (!progress.is_ok() || state == DistributedMutableQueryControlTlsServerState::kFailed)
      impl.remove_connection(index, false);
    else if (state == DistributedMutableQueryControlTlsServerState::kComplete)
      impl.remove_connection(index, true);
  }
  if (ready > 0 && (impl.poll_descriptors[0].revents & POLLIN) != 0)
    impl.accept_ready(now);
  return impl.config.grouped_shuffle_job_service == nullptr
             ? common::Status::ok()
             : impl.config.grouped_shuffle_job_service->poll_once(std::chrono::milliseconds{0},
                                                                  now);
}

common::Status DistributedMutableQueryControlTcpServer::shutdown() {
  if (!implementation_ || !implementation_->running)
    return common::Status::ok();
  Impl& impl = *implementation_;
  impl.connections.clear();
  impl.server_metrics.active_connections = 0U;
  const common::Status closed = impl.listener.close();
  impl.running = false;
  return closed;
}

network::Ipv4Endpoint DistributedMutableQueryControlTcpServer::bound_endpoint() const noexcept {
  return implementation_ ? implementation_->listener.bound_endpoint() : network::Ipv4Endpoint{};
}

DistributedMutableQueryControlTcpServerMetrics
DistributedMutableQueryControlTcpServer::metrics() const noexcept {
  return implementation_ ? implementation_->server_metrics
                         : DistributedMutableQueryControlTcpServerMetrics{};
}

bool DistributedMutableQueryControlTcpServer::is_running() const noexcept {
  return implementation_ && implementation_->running;
}

} // namespace chronos::cluster
