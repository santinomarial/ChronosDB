#include "chronos/cluster/distributed_vector_query_tcp_client_v2.hpp"

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

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      DistributedVectorQueryTcpClientV2::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] bool valid_limits(const DistributedVectorQueryTlsLimitsV2& limits) noexcept {
  constexpr std::size_t kMinimumResponseBytes =
      kDistributedVectorQueryResponseV2HeaderSize + kDistributedVectorQueryResponseV2TrailerSize;
  return valid_timeout(limits.handshake_timeout) && valid_timeout(limits.exchange_timeout) &&
         limits.maximum_response_frames > 0U &&
         limits.maximum_response_frames <= query::kMaximumDistributedCoordinatorMessages &&
         limits.maximum_response_bytes >= kMinimumResponseBytes &&
         limits.maximum_response_bytes <= kMaximumDistributedVectorQueryV2ResponseBytes;
}

[[nodiscard]] DistributedVectorQueryTcpClientV2::TimePoint
deadline_after(const DistributedVectorQueryTcpClientV2::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration =
      std::chrono::duration_cast<DistributedVectorQueryTcpClientV2::TimePoint::duration>(timeout);
  if (now > DistributedVectorQueryTcpClientV2::TimePoint::max() - duration)
    return DistributedVectorQueryTcpClientV2::TimePoint::max();
  return now + duration;
}

} // namespace

class DistributedVectorQueryTcpClientV2::Impl {
public:
  Impl(network::TcpSocket owned_socket, DistributedVectorQueryAttemptV2 owned_attempt,
       DistributedVectorQueryTcpClientConfigV2 configured, const TimePoint now) noexcept
      : socket(std::move(owned_socket)), attempt(std::move(owned_attempt)), config(configured),
        connect_deadline(deadline_after(now, config.connect_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (client_state != DistributedVectorQueryTcpClientStateV2::kFailed) {
      carrier.reset();
      static_cast<void>(socket.close());
      client_failure = std::move(status);
      client_state = DistributedVectorQueryTcpClientStateV2::kFailed;
    }
    return client_failure;
  }

  // Socket is declared before carrier so destruction is carrier first, descriptor second.
  network::TcpSocket socket;
  DistributedVectorQueryAttemptV2 attempt;
  DistributedVectorQueryTcpClientConfigV2 config;
  TimePoint connect_deadline;
  std::optional<DistributedVectorQueryTlsClientV2> carrier;
  DistributedVectorQueryTcpClientStateV2 client_state{
      DistributedVectorQueryTcpClientStateV2::kConnecting};
  common::Status client_failure{common::StatusCode::kInternal,
                                "vector query v2 TCP client has not failed"};
};

DistributedVectorQueryTcpClientV2::DistributedVectorQueryTcpClientV2(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorQueryTcpClientV2::~DistributedVectorQueryTcpClientV2() = default;
DistributedVectorQueryTcpClientV2::DistributedVectorQueryTcpClientV2(
    DistributedVectorQueryTcpClientV2&&) noexcept = default;
DistributedVectorQueryTcpClientV2& DistributedVectorQueryTcpClientV2::operator=(
    DistributedVectorQueryTcpClientV2&&) noexcept = default;

common::Result<DistributedVectorQueryTcpClientV2>
DistributedVectorQueryTcpClientV2::begin(DistributedVectorQueryAttemptV2 attempt,
                                         const DistributedVectorQueryTcpClientConfigV2 config,
                                         const TimePoint now) {
  if (config.tls_context == nullptr || config.carrier.authenticator == nullptr ||
      config.carrier.node_authorizer == nullptr || !valid_timeout(config.connect_timeout) ||
      !valid_limits(config.carrier.limits) ||
      config.carrier.peer_ipv4_address != config.remote_endpoint.address ||
      attempt.attempt_number == 0U || attempt.target_node_id == 0U) {
    return common::make_unexpected(invalid("vector query v2 TCP client configuration is invalid"));
  }
  auto decoded = decode_distributed_vector_query_request_v2_exact(attempt.request_bytes);
  if (!decoded.has_value())
    return common::make_unexpected(decoded.error());
  if (decoded->target_node_id != attempt.target_node_id) {
    return common::make_unexpected(invalid("vector query v2 TCP attempt target is inconsistent"));
  }
  auto socket = network::TcpSocket::begin_connect(config.remote_endpoint);
  if (!socket.has_value())
    return common::make_unexpected(socket.error());
  try {
    return DistributedVectorQueryTcpClientV2{
        std::make_unique<Impl>(std::move(*socket), std::move(attempt), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("vector query v2 TCP client allocation failed"));
  }
}

common::Status DistributedVectorQueryTcpClientV2::on_ready(const bool readable, const bool writable,
                                                           const TimePoint now) {
  if (!implementation_)
    return invalid("vector query v2 TCP client is empty");
  Impl& impl = *implementation_;
  if (impl.client_state == DistributedVectorQueryTcpClientStateV2::kFailed)
    return impl.client_failure;
  if (impl.client_state == DistributedVectorQueryTcpClientStateV2::kComplete)
    return common::Status::ok();
  if (impl.client_state == DistributedVectorQueryTcpClientStateV2::kConnecting) {
    if (now >= impl.connect_deadline)
      return impl.fail(unavailable("vector query v2 TCP connect timed out"));
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
    auto carrier = DistributedVectorQueryTlsClientV2::create(
        std::move(*tls), std::move(impl.attempt), impl.config.carrier, now);
    if (!carrier.has_value())
      return impl.fail(carrier.error());
    impl.carrier.emplace(std::move(*carrier));
    impl.client_state = DistributedVectorQueryTcpClientStateV2::kExchanging;
    return common::Status::ok();
  }
  const common::Status status = impl.carrier->on_ready(readable, writable, now);
  if (!status.is_ok() || impl.carrier->state() == DistributedVectorQueryTlsStateV2::kFailed)
    return impl.fail(status.is_ok() ? impl.carrier->failure() : status);
  if (impl.carrier->state() == DistributedVectorQueryTlsStateV2::kComplete)
    impl.client_state = DistributedVectorQueryTcpClientStateV2::kComplete;
  return common::Status::ok();
}

DistributedVectorQueryTcpClientStateV2 DistributedVectorQueryTcpClientV2::state() const noexcept {
  return implementation_ ? implementation_->client_state
                         : DistributedVectorQueryTcpClientStateV2::kFailed;
}

DistributedVectorQueryTlsInterestV2 DistributedVectorQueryTcpClientV2::interest() const noexcept {
  if (!implementation_ ||
      implementation_->client_state == DistributedVectorQueryTcpClientStateV2::kFailed ||
      implementation_->client_state == DistributedVectorQueryTcpClientStateV2::kComplete) {
    return {};
  }
  if (implementation_->client_state == DistributedVectorQueryTcpClientStateV2::kConnecting)
    return {.want_write = true};
  if (!implementation_->carrier.has_value())
    return {};
  // Guarded above; clang-tidy does not preserve the state/optional relationship here.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  return implementation_->carrier.value().interest();
}

int DistributedVectorQueryTcpClientV2::descriptor() const noexcept {
  return implementation_ ? implementation_->socket.descriptor() : -1;
}

common::Result<std::span<const DistributedVectorQueryResponseV2>>
DistributedVectorQueryTcpClientV2::responses() const {
  if (!implementation_ ||
      implementation_->client_state != DistributedVectorQueryTcpClientStateV2::kComplete) {
    return common::make_unexpected(
        invalid("vector query v2 TCP responses are unavailable before completion"));
  }
  if (!implementation_->carrier.has_value())
    return common::make_unexpected(invalid("vector query v2 TCP carrier is unavailable"));
  // Guarded above; clang-tidy does not preserve the state/optional relationship here.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  return implementation_->carrier.value().responses();
}

const common::Status& DistributedVectorQueryTcpClientV2::failure() const noexcept {
  static const common::Status empty_failure{common::StatusCode::kInvalidArgument,
                                            "vector query v2 TCP client is empty"};
  return implementation_ ? implementation_->client_failure : empty_failure;
}

} // namespace chronos::cluster
