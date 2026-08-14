#include "chronos/cluster/distributed_vector_aggregate_query_tcp_client_v2.hpp"

#include <chrono>
#include <cstddef>
#include <new>
#include <optional>
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

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      DistributedVectorAggregateQueryTcpClientV2::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] bool valid_limits(const DistributedVectorAggregateQueryTlsLimitsV2& limits) noexcept {
  constexpr std::size_t kMinimumResponseBytes =
      kDistributedVectorAggregateQueryResponseV2HeaderSize +
      kDistributedVectorAggregateQueryResponseV2TrailerSize;
  return valid_timeout(limits.handshake_timeout) && valid_timeout(limits.exchange_timeout) &&
         limits.maximum_response_frames > 0U &&
         limits.maximum_response_frames <= query::kMaximumUngroupedAggregateWidth &&
         limits.maximum_response_bytes >= kMinimumResponseBytes &&
         limits.maximum_response_bytes <= kMaximumDistributedVectorAggregateQueryV2ResponseBytes &&
         limits.payload.maximum_frame_length >=
             query::distributed_vector_aggregate_exchange_format::kMinimumFrameLength &&
         limits.payload.maximum_frame_length <=
             query::distributed_vector_aggregate_exchange_format::kMaximumFrameLength &&
         limits.payload.maximum_aggregates > 0U &&
         limits.payload.maximum_aggregates <=
             query::distributed_vector_aggregate_exchange_format::kMaximumAggregates &&
         limits.payload.state.maximum_frame_length >=
             query::distributed_vector_aggregate_state_format::kMinimumFrameLength &&
         limits.payload.state.maximum_frame_length <=
             query::distributed_vector_aggregate_state_format::kMaximumFrameLength &&
         limits.payload.state.maximum_variable_extremum_bytes > 0U &&
         limits.payload.state.maximum_variable_extremum_bytes <=
             query::distributed_vector_aggregate_state_format::kMaximumExtremumBytes;
}

[[nodiscard]] DistributedVectorAggregateQueryTcpClientV2::TimePoint
deadline_after(const DistributedVectorAggregateQueryTcpClientV2::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration =
      std::chrono::duration_cast<DistributedVectorAggregateQueryTcpClientV2::TimePoint::duration>(
          timeout);
  return now > DistributedVectorAggregateQueryTcpClientV2::TimePoint::max() - duration
             ? DistributedVectorAggregateQueryTcpClientV2::TimePoint::max()
             : now + duration;
}

} // namespace

class DistributedVectorAggregateQueryTcpClientV2::Impl {
public:
  Impl(network::TcpSocket owned_socket, DistributedVectorAggregateQueryAttemptV2 owned_attempt,
       std::vector<query::VectorAggregateDefinition>&& owned_definitions,
       query::QueryResourceContext owned_resources,
       const DistributedVectorAggregateQueryTcpClientConfigV2 configured, const TimePoint now)
      : socket(std::move(owned_socket)), attempt(std::move(owned_attempt)),
        definitions(std::move(owned_definitions)), resources(std::move(owned_resources)),
        config(configured), connect_deadline(deadline_after(now, config.connect_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (client_state != DistributedVectorAggregateQueryTcpClientStateV2::kFailed) {
      carrier.reset();
      static_cast<void>(socket.close());
      client_failure = std::move(status);
      client_state = DistributedVectorAggregateQueryTcpClientStateV2::kFailed;
    }
    return client_failure;
  }

  // Socket is declared before carrier so destruction is carrier first, descriptor second.
  network::TcpSocket socket;
  DistributedVectorAggregateQueryAttemptV2 attempt;
  std::vector<query::VectorAggregateDefinition> definitions;
  query::QueryResourceContext resources;
  DistributedVectorAggregateQueryTcpClientConfigV2 config;
  TimePoint connect_deadline;
  std::optional<DistributedVectorAggregateQueryTlsClientV2> carrier;
  DistributedVectorAggregateQueryTcpClientStateV2 client_state{
      DistributedVectorAggregateQueryTcpClientStateV2::kConnecting};
  common::Status client_failure{common::StatusCode::kInternal,
                                "vector aggregate query v2 TCP client has not failed"};
};

DistributedVectorAggregateQueryTcpClientV2::DistributedVectorAggregateQueryTcpClientV2(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorAggregateQueryTcpClientV2::~DistributedVectorAggregateQueryTcpClientV2() = default;
DistributedVectorAggregateQueryTcpClientV2::DistributedVectorAggregateQueryTcpClientV2(
    DistributedVectorAggregateQueryTcpClientV2&&) noexcept = default;
DistributedVectorAggregateQueryTcpClientV2& DistributedVectorAggregateQueryTcpClientV2::operator=(
    DistributedVectorAggregateQueryTcpClientV2&&) noexcept = default;

common::Result<DistributedVectorAggregateQueryTcpClientV2>
DistributedVectorAggregateQueryTcpClientV2::begin(
    DistributedVectorAggregateQueryAttemptV2 attempt,
    std::vector<query::VectorAggregateDefinition>&& definitions,
    query::QueryResourceContext resources,
    const DistributedVectorAggregateQueryTcpClientConfigV2 config, const TimePoint now) {
  if (config.tls_context == nullptr || config.carrier.authenticator == nullptr ||
      config.carrier.node_authorizer == nullptr || !valid_timeout(config.connect_timeout) ||
      !valid_limits(config.carrier.limits) ||
      config.carrier.peer_ipv4_address != config.remote_endpoint.address ||
      attempt.attempt_number == 0U || attempt.target_node_id == 0U ||
      definitions.size() > config.carrier.limits.maximum_response_frames ||
      definitions.size() > config.carrier.limits.payload.maximum_aggregates) {
    return common::make_unexpected(
        invalid("vector aggregate query v2 TCP client configuration is invalid"));
  }
  auto decoded = decode_distributed_vector_query_request_v2_exact(attempt.request_bytes);
  if (!decoded.has_value())
    return common::make_unexpected(decoded.error());
  if (decoded->target_node_id != attempt.target_node_id) {
    return common::make_unexpected(
        invalid("vector aggregate query v2 TCP attempt target is inconsistent"));
  }
  const common::Status definitions_status =
      validate_distributed_vector_aggregate_query_definitions_v2(decoded->dispatch, definitions);
  if (!definitions_status.is_ok())
    return common::make_unexpected(definitions_status);
  auto socket = network::TcpSocket::begin_connect(config.remote_endpoint);
  if (!socket.has_value())
    return common::make_unexpected(socket.error());
  try {
    return DistributedVectorAggregateQueryTcpClientV2{
        std::make_unique<Impl>(std::move(*socket), std::move(attempt), std::move(definitions),
                               std::move(resources), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("vector aggregate query v2 TCP client allocation failed"));
  }
}

common::Status DistributedVectorAggregateQueryTcpClientV2::on_ready(const bool readable,
                                                                    const bool writable,
                                                                    const TimePoint now) {
  if (!implementation_)
    return invalid("vector aggregate query v2 TCP client is empty");
  Impl& impl = *implementation_;
  if (impl.client_state == DistributedVectorAggregateQueryTcpClientStateV2::kFailed)
    return impl.client_failure;
  if (impl.client_state == DistributedVectorAggregateQueryTcpClientStateV2::kComplete)
    return common::Status::ok();
  if (impl.client_state == DistributedVectorAggregateQueryTcpClientStateV2::kConnecting) {
    if (now >= impl.connect_deadline)
      return impl.fail(unavailable("vector aggregate query v2 TCP connect timed out"));
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
    auto carrier = DistributedVectorAggregateQueryTlsClientV2::create(
        std::move(*tls), std::move(impl.attempt), std::move(impl.definitions),
        std::move(impl.resources), impl.config.carrier, now);
    if (!carrier.has_value())
      return impl.fail(carrier.error());
    impl.carrier.emplace(std::move(*carrier));
    impl.client_state = DistributedVectorAggregateQueryTcpClientStateV2::kExchanging;
    return common::Status::ok();
  }
  const common::Status status = impl.carrier->on_ready(readable, writable, now);
  if (!status.is_ok() ||
      impl.carrier->state() == DistributedVectorAggregateQueryTlsStateV2::kFailed) {
    return impl.fail(status.is_ok() ? impl.carrier->failure() : status);
  }
  if (impl.carrier->state() == DistributedVectorAggregateQueryTlsStateV2::kComplete)
    impl.client_state = DistributedVectorAggregateQueryTcpClientStateV2::kComplete;
  return common::Status::ok();
}

DistributedVectorAggregateQueryTcpClientStateV2
DistributedVectorAggregateQueryTcpClientV2::state() const noexcept {
  return implementation_ ? implementation_->client_state
                         : DistributedVectorAggregateQueryTcpClientStateV2::kFailed;
}

DistributedVectorAggregateQueryTlsInterestV2
DistributedVectorAggregateQueryTcpClientV2::interest() const noexcept {
  if (!implementation_ ||
      implementation_->client_state == DistributedVectorAggregateQueryTcpClientStateV2::kFailed ||
      implementation_->client_state == DistributedVectorAggregateQueryTcpClientStateV2::kComplete) {
    return {};
  }
  if (implementation_->client_state ==
      DistributedVectorAggregateQueryTcpClientStateV2::kConnecting) {
    return {.want_write = true};
  }
  if (!implementation_->carrier.has_value())
    return {};
  // Guarded above; clang-tidy does not preserve the state/optional relationship here.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  return implementation_->carrier.value().interest();
}

int DistributedVectorAggregateQueryTcpClientV2::descriptor() const noexcept {
  return implementation_ ? implementation_->socket.descriptor() : -1;
}

common::Result<std::span<const DistributedVectorAggregateQueryResponseV2>>
DistributedVectorAggregateQueryTcpClientV2::responses() const {
  if (!implementation_ ||
      implementation_->client_state != DistributedVectorAggregateQueryTcpClientStateV2::kComplete) {
    return common::make_unexpected(
        invalid("vector aggregate query v2 TCP responses are unavailable before completion"));
  }
  if (!implementation_->carrier.has_value()) {
    return common::make_unexpected(invalid("vector aggregate query v2 TCP carrier is unavailable"));
  }
  // Guarded above; clang-tidy does not preserve the state/optional relationship here.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  return implementation_->carrier.value().responses();
}

const common::Status& DistributedVectorAggregateQueryTcpClientV2::failure() const noexcept {
  static const common::Status empty_failure{common::StatusCode::kInvalidArgument,
                                            "vector aggregate query v2 TCP client is empty"};
  return implementation_ ? implementation_->client_failure : empty_failure;
}

} // namespace chronos::cluster
