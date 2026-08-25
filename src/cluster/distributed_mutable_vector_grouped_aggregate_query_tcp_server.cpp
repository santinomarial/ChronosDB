#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tcp_server.hpp"

#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <limits>
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
          std::string("polling mutable vector grouped aggregate query TCP server: ") +
              std::error_code(error, std::generic_category()).message()};
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      DistributedMutableVectorGroupedAggregateQueryTlsServer::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] bool
valid_limits(const DistributedMutableVectorGroupedAggregateQueryTlsLimits& limits) noexcept {
  constexpr std::size_t kMinimumResponseBytes =
      kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize +
      kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize;
  return valid_timeout(limits.handshake_timeout) && valid_timeout(limits.exchange_timeout) &&
         limits.maximum_response_frames > 0U &&
         limits.maximum_response_frames <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups &&
         limits.maximum_response_frames <= limits.payload.maximum_groups &&
         limits.maximum_response_bytes >= kMinimumResponseBytes &&
         limits.maximum_response_bytes <=
             kMaximumDistributedVectorGroupedAggregateQueryV2ResponseBytes &&
         limits.payload.maximum_frame_length >=
             query::distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength &&
         limits.payload.maximum_frame_length <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumFrameLength &&
         limits.payload.maximum_key_payload_bytes > 0U &&
         limits.payload.maximum_key_payload_bytes <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumKeyPayloadBytes &&
         limits.payload.maximum_groups > 0U &&
         limits.payload.maximum_groups <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups &&
         limits.payload.maximum_group_keys > 0U &&
         limits.payload.maximum_group_keys <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroupKeys &&
         limits.payload.maximum_aggregates <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumAggregates &&
         limits.payload.state.maximum_frame_length >=
             query::distributed_vector_aggregate_state_format::kMinimumFrameLength &&
         limits.payload.state.maximum_frame_length <=
             query::distributed_vector_aggregate_state_format::kMaximumFrameLength &&
         limits.payload.state.maximum_variable_extremum_bytes > 0U &&
         limits.payload.state.maximum_variable_extremum_bytes <=
             query::distributed_vector_aggregate_state_format::kMaximumExtremumBytes;
}

void increment_saturated(std::uint64_t& value) noexcept {
  if (value != std::numeric_limits<std::uint64_t>::max())
    ++value;
}

} // namespace

class DistributedMutableVectorGroupedAggregateQueryTcpServer::Impl {
public:
  struct Connection {
    Connection(network::TcpSocket owned_socket,
               DistributedMutableVectorGroupedAggregateQueryTlsServer owned_carrier) noexcept
        : socket(std::move(owned_socket)), carrier(std::move(owned_carrier)) {}

    // Destruction is reverse declaration order: TLS carrier before its borrowed descriptor.
    network::TcpSocket socket;
    DistributedMutableVectorGroupedAggregateQueryTlsServer carrier;
  };

  Impl(DistributedMutableVectorGroupedAggregateQueryTcpServerConfig configured,
       network::TcpListener listener_owner, network::TlsServerContext context,
       std::vector<std::unique_ptr<Connection>> connection_storage,
       std::vector<pollfd> poll_storage) noexcept
      : config(std::move(configured)), listener(std::move(listener_owner)),
        tls_context(std::move(context)), connections(std::move(connection_storage)),
        poll_descriptors(std::move(poll_storage)) {}

  void remove_connection(const std::size_t index, const bool complete) {
    connections.erase(connections.begin() + static_cast<std::ptrdiff_t>(index));
    increment_saturated(complete ? server_metrics.completed_connections
                                 : server_metrics.failed_connections);
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
      // The nested optional was checked above and is immediately consumed once.
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
      network::TcpSocket socket = std::move(next->value());
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
      auto carrier = DistributedMutableVectorGroupedAggregateQueryTlsServer::create(
          std::move(*tls),
          {.authenticator = config.authenticator,
           .receiver = config.receiver,
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

  DistributedMutableVectorGroupedAggregateQueryTcpServerConfig config;
  network::TcpListener listener;
  network::TlsServerContext tls_context;
  std::vector<std::unique_ptr<Connection>> connections;
  std::vector<pollfd> poll_descriptors;
  DistributedMutableVectorGroupedAggregateQueryTcpServerMetrics server_metrics;
  bool running{true};
};

DistributedMutableVectorGroupedAggregateQueryTcpServer::
    DistributedMutableVectorGroupedAggregateQueryTcpServer() noexcept = default;
DistributedMutableVectorGroupedAggregateQueryTcpServer::
    DistributedMutableVectorGroupedAggregateQueryTcpServer(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedMutableVectorGroupedAggregateQueryTcpServer::
    ~DistributedMutableVectorGroupedAggregateQueryTcpServer() = default;
DistributedMutableVectorGroupedAggregateQueryTcpServer::
    DistributedMutableVectorGroupedAggregateQueryTcpServer(
        DistributedMutableVectorGroupedAggregateQueryTcpServer&&) noexcept = default;
DistributedMutableVectorGroupedAggregateQueryTcpServer&
DistributedMutableVectorGroupedAggregateQueryTcpServer::operator=(
    DistributedMutableVectorGroupedAggregateQueryTcpServer&&) noexcept = default;

common::Result<DistributedMutableVectorGroupedAggregateQueryTcpServer>
DistributedMutableVectorGroupedAggregateQueryTcpServer::start(
    DistributedMutableVectorGroupedAggregateQueryTcpServerConfig config) {
  if (config.authenticator == nullptr || config.receiver == nullptr ||
      config.maximum_connections == 0U || config.maximum_connections > 65536U ||
      config.maximum_accepts_per_poll == 0U || config.maximum_accepts_per_poll > 1024U ||
      !valid_limits(config.carrier_limits)) {
    return common::make_unexpected(
        invalid("mutable vector grouped aggregate query TCP server configuration is invalid"));
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
    return DistributedMutableVectorGroupedAggregateQueryTcpServer{
        std::make_unique<Impl>(std::move(config), std::move(*listener), std::move(*context),
                               std::move(connections), std::move(descriptors))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("mutable vector grouped aggregate query TCP server allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("mutable vector grouped aggregate query TCP server limits are too large"));
  }
}

common::Status DistributedMutableVectorGroupedAggregateQueryTcpServer::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  if (!implementation_ || !implementation_->running)
    return invalid("mutable vector grouped aggregate query TCP server is not running");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return invalid("mutable vector grouped aggregate query TCP poll timeout is invalid");
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
    if (!status.is_ok() || state == DistributedMutableVectorGroupedAggregateQueryTlsState::kFailed)
      impl.remove_connection(index, false);
    else if (state == DistributedMutableVectorGroupedAggregateQueryTlsState::kComplete)
      impl.remove_connection(index, true);
  }
  if (ready > 0 && (impl.poll_descriptors[0].revents & POLLIN) != 0)
    impl.accept_ready(now);
  return common::Status::ok();
}

common::Status DistributedMutableVectorGroupedAggregateQueryTcpServer::shutdown() {
  if (!implementation_ || !implementation_->running)
    return common::Status::ok();
  Impl& impl = *implementation_;
  impl.connections.clear();
  impl.server_metrics.active_connections = 0U;
  const common::Status closed = impl.listener.close();
  impl.running = false;
  return closed;
}

network::Ipv4Endpoint
DistributedMutableVectorGroupedAggregateQueryTcpServer::bound_endpoint() const noexcept {
  return implementation_ ? implementation_->listener.bound_endpoint() : network::Ipv4Endpoint{};
}

DistributedMutableVectorGroupedAggregateQueryTcpServerMetrics
DistributedMutableVectorGroupedAggregateQueryTcpServer::metrics() const noexcept {
  return implementation_ ? implementation_->server_metrics
                         : DistributedMutableVectorGroupedAggregateQueryTcpServerMetrics{};
}

bool DistributedMutableVectorGroupedAggregateQueryTcpServer::is_running() const noexcept {
  return implementation_ && implementation_->running;
}

} // namespace chronos::cluster
