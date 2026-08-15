#include "chronos/cluster/distributed_grouped_query_tcp_client.hpp"

#include <chrono>
#include <new>
#include <optional>
#include <utility>

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

[[nodiscard]] common::Status internal(const char* message) {
  return {common::StatusCode::kInternal, message};
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      DistributedGroupedQueryTcpClient::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] DistributedGroupedQueryTcpClient::TimePoint
deadline_after(const DistributedGroupedQueryTcpClient::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration =
      std::chrono::duration_cast<DistributedGroupedQueryTcpClient::TimePoint::duration>(timeout);
  if (now > DistributedGroupedQueryTcpClient::TimePoint::max() - duration)
    return DistributedGroupedQueryTcpClient::TimePoint::max();
  return now + duration;
}

} // namespace

class DistributedGroupedQueryTcpClient::Impl {
public:
  Impl(network::TcpSocket owned_socket, DistributedGroupedQueryAttempt owned_attempt,
       DistributedGroupedQueryTcpClientConfig configured, const TimePoint now)
      : socket(std::move(owned_socket)), attempt(std::move(owned_attempt)), config(configured),
        connect_deadline(deadline_after(now, config.connect_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (client_state != DistributedGroupedQueryTcpClientState::kFailed) {
      carrier.reset();
      static_cast<void>(socket.close());
      client_failure = std::move(status);
      client_state = DistributedGroupedQueryTcpClientState::kFailed;
    }
    return client_failure;
  }

  // Socket is declared before carrier so destruction is carrier first, descriptor second.
  network::TcpSocket socket;
  DistributedGroupedQueryAttempt attempt;
  DistributedGroupedQueryTcpClientConfig config;
  TimePoint connect_deadline;
  std::optional<DistributedGroupedQueryTlsClient> carrier;
  DistributedGroupedQueryTcpClientState client_state{
      DistributedGroupedQueryTcpClientState::kConnecting};
  common::Status client_failure{common::StatusCode::kInternal,
                                "grouped query TCP client has not failed"};
};

DistributedGroupedQueryTcpClient::DistributedGroupedQueryTcpClient(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedGroupedQueryTcpClient::~DistributedGroupedQueryTcpClient() = default;
DistributedGroupedQueryTcpClient::DistributedGroupedQueryTcpClient(
    DistributedGroupedQueryTcpClient&&) noexcept = default;
DistributedGroupedQueryTcpClient&
DistributedGroupedQueryTcpClient::operator=(DistributedGroupedQueryTcpClient&&) noexcept = default;

common::Result<DistributedGroupedQueryTcpClient>
DistributedGroupedQueryTcpClient::begin(DistributedGroupedQueryAttempt attempt,
                                        const DistributedGroupedQueryTcpClientConfig config,
                                        const TimePoint now) {
  if (config.tls_context == nullptr || config.carrier.authenticator == nullptr ||
      config.carrier.node_authorizer == nullptr || !valid_timeout(config.connect_timeout) ||
      !valid_timeout(config.carrier.limits.handshake_timeout) ||
      !valid_timeout(config.carrier.limits.exchange_timeout) ||
      config.carrier.limits.maximum_response_frames == 0U ||
      config.carrier.limits.maximum_response_frames >
          query::kMaximumDistributedCoordinatorMessages ||
      config.carrier.peer_ipv4_address != config.remote_endpoint.address ||
      attempt.attempt_number == 0U || attempt.target_node_id == 0U) {
    return common::make_unexpected(invalid("grouped query TCP client configuration is invalid"));
  }
  auto decoded = decode_distributed_grouped_query_request_v1(attempt.request_bytes);
  if (!decoded.has_value())
    return common::make_unexpected(decoded.error());
  if (decoded->target_node_id != attempt.target_node_id) {
    return common::make_unexpected(invalid("grouped query TCP attempt target is inconsistent"));
  }
  auto socket = network::TcpSocket::begin_connect(config.remote_endpoint);
  if (!socket.has_value())
    return common::make_unexpected(socket.error());
  try {
    return DistributedGroupedQueryTcpClient{
        std::make_unique<Impl>(std::move(*socket), std::move(attempt), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped query TCP client allocation failed"));
  }
}

common::Status DistributedGroupedQueryTcpClient::on_ready(const bool readable, const bool writable,
                                                          const TimePoint now) {
  if (!implementation_)
    return invalid("grouped query TCP client is empty");
  Impl& impl = *implementation_;
  if (impl.client_state == DistributedGroupedQueryTcpClientState::kFailed)
    return impl.client_failure;
  if (impl.client_state == DistributedGroupedQueryTcpClientState::kComplete)
    return common::Status::ok();
  if (impl.client_state == DistributedGroupedQueryTcpClientState::kConnecting) {
    if (now >= impl.connect_deadline)
      return impl.fail(unavailable("grouped query TCP connect timed out"));
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
    auto carrier = DistributedGroupedQueryTlsClient::create(
        std::move(*tls), std::move(impl.attempt), impl.config.carrier, now);
    if (!carrier.has_value())
      return impl.fail(carrier.error());
    impl.carrier.emplace(std::move(*carrier));
    impl.client_state = DistributedGroupedQueryTcpClientState::kExchanging;
    return common::Status::ok();
  }
  const common::Status status = impl.carrier->on_ready(readable, writable, now);
  if (!status.is_ok() || impl.carrier->state() == DistributedGroupedQueryTlsState::kFailed)
    return impl.fail(status.is_ok() ? impl.carrier->failure() : status);
  if (impl.carrier->state() == DistributedGroupedQueryTlsState::kComplete)
    impl.client_state = DistributedGroupedQueryTcpClientState::kComplete;
  return common::Status::ok();
}

DistributedGroupedQueryTcpClientState DistributedGroupedQueryTcpClient::state() const noexcept {
  return implementation_ ? implementation_->client_state
                         : DistributedGroupedQueryTcpClientState::kFailed;
}

DistributedGroupedQueryTlsInterest DistributedGroupedQueryTcpClient::interest() const noexcept {
  if (!implementation_ ||
      implementation_->client_state == DistributedGroupedQueryTcpClientState::kFailed ||
      implementation_->client_state == DistributedGroupedQueryTcpClientState::kComplete) {
    return {};
  }
  if (implementation_->client_state == DistributedGroupedQueryTcpClientState::kConnecting)
    return {.want_write = true};
  return implementation_->carrier
      .transform([](const DistributedGroupedQueryTlsClient& carrier) { return carrier.interest(); })
      .value_or(DistributedGroupedQueryTlsInterest{});
}

int DistributedGroupedQueryTcpClient::descriptor() const noexcept {
  return implementation_ ? implementation_->socket.descriptor() : -1;
}

common::Result<std::span<const DistributedGroupedQueryResponse>>
DistributedGroupedQueryTcpClient::responses() const {
  if (!implementation_ ||
      implementation_->client_state != DistributedGroupedQueryTcpClientState::kComplete) {
    return common::make_unexpected(
        invalid("grouped query TCP responses are unavailable before completion"));
  }
  return implementation_->carrier
      .transform(
          [](const DistributedGroupedQueryTlsClient& carrier) { return carrier.responses(); })
      .value_or(common::make_unexpected(
          internal("completed grouped query TCP client has no TLS carrier")));
}

const common::Status& DistributedGroupedQueryTcpClient::failure() const noexcept {
  static const common::Status empty_failure{common::StatusCode::kInvalidArgument,
                                            "grouped query TCP client is empty"};
  return implementation_ ? implementation_->client_failure : empty_failure;
}

} // namespace chronos::cluster
