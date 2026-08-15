#include "chronos/cluster/distributed_query_tcp_client.hpp"

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
      DistributedQueryTcpClient::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] DistributedQueryTcpClient::TimePoint
deadline_after(const DistributedQueryTcpClient::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration =
      std::chrono::duration_cast<DistributedQueryTcpClient::TimePoint::duration>(timeout);
  if (now > DistributedQueryTcpClient::TimePoint::max() - duration)
    return DistributedQueryTcpClient::TimePoint::max();
  return now + duration;
}

} // namespace

class DistributedQueryTcpClient::Impl {
public:
  Impl(network::TcpSocket owned_socket, DistributedQueryAttempt owned_attempt,
       DistributedQueryTcpClientConfig configured, const TimePoint now)
      : socket(std::move(owned_socket)), attempt(std::move(owned_attempt)), config(configured),
        connect_deadline(deadline_after(now, config.connect_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (client_state != DistributedQueryTcpClientState::kFailed) {
      carrier.reset();
      static_cast<void>(socket.close());
      client_failure = std::move(status);
      client_state = DistributedQueryTcpClientState::kFailed;
    }
    return client_failure;
  }

  // Socket is declared before carrier so destruction is carrier first, descriptor second.
  network::TcpSocket socket;
  DistributedQueryAttempt attempt;
  DistributedQueryTcpClientConfig config;
  TimePoint connect_deadline;
  std::optional<DistributedQueryTlsClient> carrier;
  DistributedQueryTcpClientState client_state{DistributedQueryTcpClientState::kConnecting};
  common::Status client_failure{common::StatusCode::kInternal,
                                "distributed query TCP client has not failed"};
};

DistributedQueryTcpClient::DistributedQueryTcpClient(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedQueryTcpClient::~DistributedQueryTcpClient() = default;
DistributedQueryTcpClient::DistributedQueryTcpClient(DistributedQueryTcpClient&&) noexcept =
    default;
DistributedQueryTcpClient&
DistributedQueryTcpClient::operator=(DistributedQueryTcpClient&&) noexcept = default;

common::Result<DistributedQueryTcpClient>
DistributedQueryTcpClient::begin(DistributedQueryAttempt attempt,
                                 const DistributedQueryTcpClientConfig config,
                                 const TimePoint now) {
  if (config.tls_context == nullptr || config.carrier.authenticator == nullptr ||
      config.carrier.node_authorizer == nullptr || !valid_timeout(config.connect_timeout) ||
      !valid_timeout(config.carrier.limits.handshake_timeout) ||
      !valid_timeout(config.carrier.limits.exchange_timeout) ||
      config.carrier.peer_ipv4_address != config.remote_endpoint.address ||
      attempt.attempt_number == 0U || attempt.target_node_id == 0U) {
    return common::make_unexpected(
        invalid("distributed query TCP client configuration is invalid"));
  }
  auto decoded = decode_distributed_query_request_v1(attempt.request_bytes);
  if (!decoded.has_value())
    return common::make_unexpected(decoded.error());
  if (decoded->target_node_id != attempt.target_node_id)
    return common::make_unexpected(invalid("distributed query TCP attempt target is inconsistent"));
  auto socket = network::TcpSocket::begin_connect(config.remote_endpoint);
  if (!socket.has_value())
    return common::make_unexpected(socket.error());
  try {
    return DistributedQueryTcpClient{
        std::make_unique<Impl>(std::move(*socket), std::move(attempt), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed query TCP client allocation failed"));
  }
}

common::Status DistributedQueryTcpClient::on_ready(const bool readable, const bool writable,
                                                   const TimePoint now) {
  if (!implementation_)
    return invalid("distributed query TCP client is empty");
  Impl& impl = *implementation_;
  if (impl.client_state == DistributedQueryTcpClientState::kFailed)
    return impl.client_failure;
  if (impl.client_state == DistributedQueryTcpClientState::kComplete)
    return common::Status::ok();
  if (impl.client_state == DistributedQueryTcpClientState::kConnecting) {
    if (now >= impl.connect_deadline)
      return impl.fail(unavailable("distributed query TCP connect timed out"));
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
    auto carrier = DistributedQueryTlsClient::create(std::move(*tls), std::move(impl.attempt),
                                                     impl.config.carrier, now);
    if (!carrier.has_value())
      return impl.fail(carrier.error());
    impl.carrier.emplace(std::move(*carrier));
    impl.client_state = DistributedQueryTcpClientState::kExchanging;
    return common::Status::ok();
  }
  const common::Status status = impl.carrier->on_ready(readable, writable, now);
  if (!status.is_ok() || impl.carrier->state() == DistributedQueryTlsClientState::kFailed)
    return impl.fail(status.is_ok() ? impl.carrier->failure() : status);
  if (impl.carrier->state() == DistributedQueryTlsClientState::kComplete)
    impl.client_state = DistributedQueryTcpClientState::kComplete;
  return common::Status::ok();
}

DistributedQueryTcpClientState DistributedQueryTcpClient::state() const noexcept {
  return implementation_ ? implementation_->client_state : DistributedQueryTcpClientState::kFailed;
}

DistributedQueryTlsInterest DistributedQueryTcpClient::interest() const noexcept {
  if (!implementation_ ||
      implementation_->client_state == DistributedQueryTcpClientState::kFailed ||
      implementation_->client_state == DistributedQueryTcpClientState::kComplete) {
    return {};
  }
  if (implementation_->client_state == DistributedQueryTcpClientState::kConnecting)
    return {.want_write = true};
  return implementation_->carrier
      .transform([](const DistributedQueryTlsClient& carrier) { return carrier.interest(); })
      .value_or(DistributedQueryTlsInterest{});
}

int DistributedQueryTcpClient::descriptor() const noexcept {
  return implementation_ ? implementation_->socket.descriptor() : -1;
}

common::Result<common::ByteView> DistributedQueryTcpClient::response_bytes() const {
  if (!implementation_ ||
      implementation_->client_state != DistributedQueryTcpClientState::kComplete)
    return common::make_unexpected(
        invalid("distributed query TCP response is unavailable before completion"));
  if (!implementation_->carrier.has_value())
    return common::make_unexpected(
        internal("completed distributed query TCP client has no TLS carrier"));
  // Guarded by the carrier-presence check above.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  return implementation_->carrier->response_bytes();
}

const common::Status& DistributedQueryTcpClient::failure() const noexcept {
  static const common::Status empty_failure{common::StatusCode::kInvalidArgument,
                                            "distributed query TCP client is empty"};
  return implementation_ ? implementation_->client_failure : empty_failure;
}

} // namespace chronos::cluster
