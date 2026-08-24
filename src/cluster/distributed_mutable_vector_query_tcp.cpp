#include "chronos/cluster/distributed_mutable_vector_query_tcp.hpp"

#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <limits>
#include <new>
#include <optional>
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

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status poll_error(const int error = errno) {
  return {common::StatusCode::kIoError,
          std::string("polling mutable vector query TCP server: ") +
              std::error_code(error, std::generic_category()).message()};
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      DistributedMutableVectorQueryTcpClient::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] bool valid_limits(const DistributedMutableVectorQueryTlsLimits& limits) noexcept {
  constexpr std::size_t kMinimumResponseBytes =
      kDistributedVectorQueryResponseV2HeaderSize + kDistributedVectorQueryResponseV2TrailerSize;
  return valid_timeout(limits.handshake_timeout) && valid_timeout(limits.exchange_timeout) &&
         limits.maximum_response_frames > 0U &&
         limits.maximum_response_frames <= query::kMaximumDistributedCoordinatorMessages &&
         limits.maximum_response_bytes >= kMinimumResponseBytes &&
         limits.maximum_response_bytes <= kMaximumDistributedVectorQueryV2ResponseBytes;
}

[[nodiscard]] DistributedMutableVectorQueryTcpClient::TimePoint
deadline_after(const DistributedMutableVectorQueryTcpClient::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration =
      std::chrono::duration_cast<DistributedMutableVectorQueryTcpClient::TimePoint::duration>(
          timeout);
  if (now > DistributedMutableVectorQueryTcpClient::TimePoint::max() - duration)
    return DistributedMutableVectorQueryTcpClient::TimePoint::max();
  return now + duration;
}

void increment_saturated(std::uint64_t& value) noexcept {
  if (value != std::numeric_limits<std::uint64_t>::max())
    ++value;
}

} // namespace

class DistributedMutableVectorQueryTcpClient::Impl {
public:
  Impl(network::TcpSocket owned_socket, DistributedMutableVectorQueryAttempt owned_attempt,
       const DistributedMutableVectorQueryTcpClientConfig configured, const TimePoint now)
      : socket(std::move(owned_socket)), attempt(std::move(owned_attempt)), config(configured),
        connect_deadline(deadline_after(now, config.connect_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (client_state != DistributedMutableVectorQueryTcpClientState::kFailed) {
      carrier.reset();
      static_cast<void>(socket.close());
      client_failure = std::move(status);
      client_state = DistributedMutableVectorQueryTcpClientState::kFailed;
    }
    return client_failure;
  }

  // Socket precedes carrier so reverse destruction releases TLS before its borrowed descriptor.
  network::TcpSocket socket;
  DistributedMutableVectorQueryAttempt attempt;
  DistributedMutableVectorQueryTcpClientConfig config;
  TimePoint connect_deadline;
  std::optional<DistributedMutableVectorQueryTlsClient> carrier;
  DistributedMutableVectorQueryTcpClientState client_state{
      DistributedMutableVectorQueryTcpClientState::kConnecting};
  common::Status client_failure{common::StatusCode::kInternal,
                                "mutable vector query TCP client has not failed"};
};

DistributedMutableVectorQueryTcpClient::DistributedMutableVectorQueryTcpClient(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedMutableVectorQueryTcpClient::~DistributedMutableVectorQueryTcpClient() = default;
DistributedMutableVectorQueryTcpClient::DistributedMutableVectorQueryTcpClient(
    DistributedMutableVectorQueryTcpClient&&) noexcept = default;
DistributedMutableVectorQueryTcpClient& DistributedMutableVectorQueryTcpClient::operator=(
    DistributedMutableVectorQueryTcpClient&&) noexcept = default;

common::Result<DistributedMutableVectorQueryTcpClient>
DistributedMutableVectorQueryTcpClient::begin(
    DistributedMutableVectorQueryAttempt attempt,
    const DistributedMutableVectorQueryTcpClientConfig config, const TimePoint now) {
  if (config.tls_context == nullptr || config.carrier.authenticator == nullptr ||
      config.carrier.node_authorizer == nullptr || !valid_timeout(config.connect_timeout) ||
      !valid_limits(config.carrier.limits) ||
      config.carrier.peer_ipv4_address != config.remote_endpoint.address ||
      attempt.attempt_number == 0U || attempt.target_node_id == 0U) {
    return common::make_unexpected(
        invalid("mutable vector query TCP client configuration is invalid"));
  }
  try {
    auto decoded = decode_distributed_mutable_vector_query_request_exact(attempt.request_bytes);
    if (!decoded.has_value())
      return common::make_unexpected(decoded.error());
    if (decoded->target_node_id != attempt.target_node_id) {
      return common::make_unexpected(
          invalid("mutable vector query TCP attempt target is inconsistent"));
    }
    auto socket = network::TcpSocket::begin_connect(config.remote_endpoint);
    if (!socket.has_value())
      return common::make_unexpected(socket.error());
    return DistributedMutableVectorQueryTcpClient{
        std::make_unique<Impl>(std::move(*socket), std::move(attempt), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable vector query TCP client allocation failed"));
  }
}

common::Status DistributedMutableVectorQueryTcpClient::on_ready(const bool readable,
                                                                const bool writable,
                                                                const TimePoint now) {
  if (!implementation_)
    return invalid("mutable vector query TCP client is empty");
  Impl& impl = *implementation_;
  if (impl.client_state == DistributedMutableVectorQueryTcpClientState::kFailed)
    return impl.client_failure;
  if (impl.client_state == DistributedMutableVectorQueryTcpClientState::kComplete)
    return common::Status::ok();
  if (impl.client_state == DistributedMutableVectorQueryTcpClientState::kConnecting) {
    if (now >= impl.connect_deadline)
      return impl.fail(unavailable("mutable vector query TCP connect timed out"));
    if (!writable)
      return common::Status::ok();
    auto connected = impl.socket.finish_connect();
    if (!connected.has_value())
      return impl.fail(connected.error());
    if (*connected == network::TcpConnectState::kInProgress)
      return common::Status::ok();
    auto tls = network::TlsSocket::connect(*impl.config.tls_context, impl.socket.descriptor());
    if (!tls.has_value())
      return impl.fail(tls.error());
    auto carrier = DistributedMutableVectorQueryTlsClient::create(
        std::move(*tls), std::move(impl.attempt), impl.config.carrier, now);
    if (!carrier.has_value())
      return impl.fail(carrier.error());
    impl.carrier.emplace(std::move(*carrier));
    impl.client_state = DistributedMutableVectorQueryTcpClientState::kExchanging;
    return common::Status::ok();
  }
  const common::Status status = impl.carrier->on_ready(readable, writable, now);
  if (!status.is_ok() || impl.carrier->state() == DistributedMutableVectorQueryTlsState::kFailed)
    return impl.fail(status.is_ok() ? impl.carrier->failure() : status);
  if (impl.carrier->state() == DistributedMutableVectorQueryTlsState::kComplete)
    impl.client_state = DistributedMutableVectorQueryTcpClientState::kComplete;
  return common::Status::ok();
}

DistributedMutableVectorQueryTcpClientState
DistributedMutableVectorQueryTcpClient::state() const noexcept {
  return implementation_ ? implementation_->client_state
                         : DistributedMutableVectorQueryTcpClientState::kFailed;
}

DistributedMutableVectorQueryTlsInterest
DistributedMutableVectorQueryTcpClient::interest() const noexcept {
  if (!implementation_ ||
      implementation_->client_state == DistributedMutableVectorQueryTcpClientState::kFailed ||
      implementation_->client_state == DistributedMutableVectorQueryTcpClientState::kComplete) {
    return {};
  }
  if (implementation_->client_state == DistributedMutableVectorQueryTcpClientState::kConnecting)
    return {.want_write = true};
  if (!implementation_->carrier.has_value())
    return {};
  return implementation_->carrier.value().interest(); // NOLINT(bugprone-unchecked-optional-access)
}

int DistributedMutableVectorQueryTcpClient::descriptor() const noexcept {
  return implementation_ ? implementation_->socket.descriptor() : -1;
}

common::Result<std::span<const DistributedVectorQueryResponseV2>>
DistributedMutableVectorQueryTcpClient::responses() const {
  if (!implementation_ ||
      implementation_->client_state != DistributedMutableVectorQueryTcpClientState::kComplete) {
    return common::make_unexpected(
        invalid("mutable vector query TCP responses are unavailable before completion"));
  }
  if (!implementation_->carrier.has_value())
    return common::make_unexpected(invalid("mutable vector query TCP carrier is unavailable"));
  return implementation_->carrier.value().responses(); // NOLINT(bugprone-unchecked-optional-access)
}

const common::Status& DistributedMutableVectorQueryTcpClient::failure() const noexcept {
  static const common::Status empty_failure{common::StatusCode::kInvalidArgument,
                                            "mutable vector query TCP client is empty"};
  return implementation_ ? implementation_->client_failure : empty_failure;
}

class DistributedMutableVectorQueryTcpServer::Impl {
public:
  struct Connection {
    Connection(network::TcpSocket owned_socket,
               DistributedMutableVectorQueryTlsServer owned_carrier) noexcept
        : socket(std::move(owned_socket)), carrier(std::move(owned_carrier)) {}

    network::TcpSocket socket;
    DistributedMutableVectorQueryTlsServer carrier;
  };

  Impl(DistributedMutableVectorQueryTcpServerConfig configured, network::TcpListener listener_owner,
       network::TlsServerContext context,
       std::vector<std::unique_ptr<Connection>> connection_storage,
       std::vector<pollfd> poll_storage)
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
      auto carrier =
          DistributedMutableVectorQueryTlsServer::create(std::move(*tls),
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

  DistributedMutableVectorQueryTcpServerConfig config;
  network::TcpListener listener;
  network::TlsServerContext tls_context;
  std::vector<std::unique_ptr<Connection>> connections;
  std::vector<pollfd> poll_descriptors;
  DistributedMutableVectorQueryTcpServerMetrics server_metrics;
  bool running{true};
};

DistributedMutableVectorQueryTcpServer::DistributedMutableVectorQueryTcpServer() noexcept = default;
DistributedMutableVectorQueryTcpServer::DistributedMutableVectorQueryTcpServer(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedMutableVectorQueryTcpServer::~DistributedMutableVectorQueryTcpServer() = default;
DistributedMutableVectorQueryTcpServer::DistributedMutableVectorQueryTcpServer(
    DistributedMutableVectorQueryTcpServer&&) noexcept = default;
DistributedMutableVectorQueryTcpServer& DistributedMutableVectorQueryTcpServer::operator=(
    DistributedMutableVectorQueryTcpServer&&) noexcept = default;

common::Result<DistributedMutableVectorQueryTcpServer>
DistributedMutableVectorQueryTcpServer::start(DistributedMutableVectorQueryTcpServerConfig config) {
  if (config.authenticator == nullptr || config.receiver == nullptr ||
      config.maximum_connections == 0U || config.maximum_connections > 65536U ||
      config.maximum_accepts_per_poll == 0U || config.maximum_accepts_per_poll > 1024U ||
      !valid_limits(config.carrier_limits)) {
    return common::make_unexpected(
        invalid("mutable vector query TCP server configuration is invalid"));
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
    return DistributedMutableVectorQueryTcpServer{
        std::make_unique<Impl>(std::move(config), std::move(*listener), std::move(*context),
                               std::move(connections), std::move(descriptors))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable vector query TCP server allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("mutable vector query TCP server limits are too large"));
  }
}

common::Status
DistributedMutableVectorQueryTcpServer::poll_once(const std::chrono::milliseconds maximum_wait) {
  if (!implementation_ || !implementation_->running)
    return invalid("mutable vector query TCP server is not running");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return invalid("mutable vector query TCP poll timeout is invalid");
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
    if (!status.is_ok() || state == DistributedMutableVectorQueryTlsState::kFailed)
      impl.remove_connection(index, false);
    else if (state == DistributedMutableVectorQueryTlsState::kComplete)
      impl.remove_connection(index, true);
  }
  if (ready > 0 && (impl.poll_descriptors[0].revents & POLLIN) != 0)
    impl.accept_ready(now);
  return common::Status::ok();
}

common::Status DistributedMutableVectorQueryTcpServer::shutdown() {
  if (!implementation_ || !implementation_->running)
    return common::Status::ok();
  Impl& impl = *implementation_;
  impl.connections.clear();
  impl.server_metrics.active_connections = 0U;
  const common::Status closed = impl.listener.close();
  impl.running = false;
  return closed;
}

network::Ipv4Endpoint DistributedMutableVectorQueryTcpServer::bound_endpoint() const noexcept {
  return implementation_ ? implementation_->listener.bound_endpoint() : network::Ipv4Endpoint{};
}

DistributedMutableVectorQueryTcpServerMetrics
DistributedMutableVectorQueryTcpServer::metrics() const noexcept {
  return implementation_ ? implementation_->server_metrics
                         : DistributedMutableVectorQueryTcpServerMetrics{};
}

bool DistributedMutableVectorQueryTcpServer::is_running() const noexcept {
  return implementation_ && implementation_->running;
}

} // namespace chronos::cluster
