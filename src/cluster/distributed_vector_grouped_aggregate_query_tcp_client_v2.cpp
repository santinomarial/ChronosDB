#include "chronos/cluster/distributed_vector_grouped_aggregate_query_tcp_client_v2.hpp"

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
      DistributedVectorGroupedAggregateQueryTcpClientV2::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] bool
valid_limits(const DistributedVectorGroupedAggregateQueryTlsLimitsV2& limits) noexcept {
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

[[nodiscard]] DistributedVectorGroupedAggregateQueryTcpClientV2::TimePoint
deadline_after(const DistributedVectorGroupedAggregateQueryTcpClientV2::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration = std::chrono::duration_cast<
      DistributedVectorGroupedAggregateQueryTcpClientV2::TimePoint::duration>(timeout);
  return now > DistributedVectorGroupedAggregateQueryTcpClientV2::TimePoint::max() - duration
             ? DistributedVectorGroupedAggregateQueryTcpClientV2::TimePoint::max()
             : now + duration;
}

} // namespace

class DistributedVectorGroupedAggregateQueryTcpClientV2::Impl {
public:
  Impl(network::TcpSocket owned_socket,
       DistributedVectorGroupedAggregateQueryAttemptV2 owned_attempt,
       std::vector<query::VectorGroupKeyDefinition>&& owned_keys,
       std::vector<query::VectorAggregateDefinition>&& owned_aggregates,
       query::QueryResourceContext owned_resources,
       const DistributedVectorGroupedAggregateQueryTcpClientConfigV2 configured,
       const TimePoint now)
      : socket(std::move(owned_socket)), attempt(std::move(owned_attempt)),
        keys(std::move(owned_keys)), aggregates(std::move(owned_aggregates)),
        resources(std::move(owned_resources)), config(configured),
        connect_deadline(deadline_after(now, config.connect_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (client_state != DistributedVectorGroupedAggregateQueryTcpClientStateV2::kFailed) {
      carrier.reset();
      static_cast<void>(socket.close());
      client_failure = std::move(status);
      client_state = DistributedVectorGroupedAggregateQueryTcpClientStateV2::kFailed;
    }
    return client_failure;
  }

  // Socket is declared before carrier so destruction is carrier first, descriptor second.
  network::TcpSocket socket;
  DistributedVectorGroupedAggregateQueryAttemptV2 attempt;
  std::vector<query::VectorGroupKeyDefinition> keys;
  std::vector<query::VectorAggregateDefinition> aggregates;
  query::QueryResourceContext resources;
  DistributedVectorGroupedAggregateQueryTcpClientConfigV2 config;
  TimePoint connect_deadline;
  std::optional<DistributedVectorGroupedAggregateQueryTlsClientV2> carrier;
  DistributedVectorGroupedAggregateQueryTcpClientStateV2 client_state{
      DistributedVectorGroupedAggregateQueryTcpClientStateV2::kConnecting};
  common::Status client_failure{common::StatusCode::kInternal,
                                "vector grouped aggregate query v2 TCP client has not failed"};
};

DistributedVectorGroupedAggregateQueryTcpClientV2::
    DistributedVectorGroupedAggregateQueryTcpClientV2(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateQueryTcpClientV2::
    ~DistributedVectorGroupedAggregateQueryTcpClientV2() = default;
DistributedVectorGroupedAggregateQueryTcpClientV2::
    DistributedVectorGroupedAggregateQueryTcpClientV2(
        DistributedVectorGroupedAggregateQueryTcpClientV2&&) noexcept = default;
DistributedVectorGroupedAggregateQueryTcpClientV2&
DistributedVectorGroupedAggregateQueryTcpClientV2::operator=(
    DistributedVectorGroupedAggregateQueryTcpClientV2&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateQueryTcpClientV2>
DistributedVectorGroupedAggregateQueryTcpClientV2::begin(
    DistributedVectorGroupedAggregateQueryAttemptV2 attempt,
    std::vector<query::VectorGroupKeyDefinition>&& keys,
    std::vector<query::VectorAggregateDefinition>&& aggregates,
    query::QueryResourceContext resources,
    const DistributedVectorGroupedAggregateQueryTcpClientConfigV2 config, const TimePoint now) {
  if (config.tls_context == nullptr || config.carrier.authenticator == nullptr ||
      config.carrier.node_authorizer == nullptr || !valid_timeout(config.connect_timeout) ||
      !valid_limits(config.carrier.limits) ||
      config.carrier.peer_ipv4_address != config.remote_endpoint.address ||
      attempt.attempt_number == 0U || attempt.target_node_id == 0U ||
      keys.size() > config.carrier.limits.payload.maximum_group_keys ||
      aggregates.size() > config.carrier.limits.payload.maximum_aggregates) {
    return common::make_unexpected(
        invalid("vector grouped aggregate query v2 TCP client configuration is invalid"));
  }
  auto decoded = decode_distributed_vector_query_request_v2_exact(attempt.request_bytes);
  if (!decoded.has_value())
    return common::make_unexpected(decoded.error());
  if (decoded->target_node_id != attempt.target_node_id) {
    return common::make_unexpected(
        invalid("vector grouped aggregate query v2 TCP attempt target is inconsistent"));
  }
  const common::Status authority_status =
      validate_distributed_vector_grouped_aggregate_query_authority_v2(decoded->dispatch, keys,
                                                                       aggregates);
  if (!authority_status.is_ok())
    return common::make_unexpected(authority_status);
  auto socket = network::TcpSocket::begin_connect(config.remote_endpoint);
  if (!socket.has_value())
    return common::make_unexpected(socket.error());
  try {
    return DistributedVectorGroupedAggregateQueryTcpClientV2{
        std::make_unique<Impl>(std::move(*socket), std::move(attempt), std::move(keys),
                               std::move(aggregates), std::move(resources), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("vector grouped aggregate query v2 TCP client allocation failed"));
  }
}

common::Status DistributedVectorGroupedAggregateQueryTcpClientV2::on_ready(const bool readable,
                                                                           const bool writable,
                                                                           const TimePoint now) {
  if (!implementation_)
    return invalid("vector grouped aggregate query v2 TCP client is empty");
  Impl& impl = *implementation_;
  if (impl.client_state == DistributedVectorGroupedAggregateQueryTcpClientStateV2::kFailed)
    return impl.client_failure;
  if (impl.client_state == DistributedVectorGroupedAggregateQueryTcpClientStateV2::kComplete)
    return common::Status::ok();
  if (impl.client_state == DistributedVectorGroupedAggregateQueryTcpClientStateV2::kConnecting) {
    if (now >= impl.connect_deadline)
      return impl.fail(unavailable("vector grouped aggregate query v2 TCP connect timed out"));
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
    auto carrier = DistributedVectorGroupedAggregateQueryTlsClientV2::create(
        std::move(*tls), std::move(impl.attempt), std::move(impl.keys), std::move(impl.aggregates),
        std::move(impl.resources), impl.config.carrier, now);
    if (!carrier.has_value())
      return impl.fail(carrier.error());
    impl.carrier.emplace(std::move(*carrier));
    impl.client_state = DistributedVectorGroupedAggregateQueryTcpClientStateV2::kExchanging;
    return common::Status::ok();
  }
  const common::Status status = impl.carrier->on_ready(readable, writable, now);
  if (!status.is_ok() ||
      impl.carrier->state() == DistributedVectorGroupedAggregateQueryTlsStateV2::kFailed) {
    return impl.fail(status.is_ok() ? impl.carrier->failure() : status);
  }
  if (impl.carrier->state() == DistributedVectorGroupedAggregateQueryTlsStateV2::kComplete)
    impl.client_state = DistributedVectorGroupedAggregateQueryTcpClientStateV2::kComplete;
  return common::Status::ok();
}

DistributedVectorGroupedAggregateQueryTcpClientStateV2
DistributedVectorGroupedAggregateQueryTcpClientV2::state() const noexcept {
  return implementation_ ? implementation_->client_state
                         : DistributedVectorGroupedAggregateQueryTcpClientStateV2::kFailed;
}

DistributedVectorGroupedAggregateQueryTlsInterestV2
DistributedVectorGroupedAggregateQueryTcpClientV2::interest() const noexcept {
  if (!implementation_ ||
      implementation_->client_state ==
          DistributedVectorGroupedAggregateQueryTcpClientStateV2::kFailed ||
      implementation_->client_state ==
          DistributedVectorGroupedAggregateQueryTcpClientStateV2::kComplete) {
    return {};
  }
  if (implementation_->client_state ==
      DistributedVectorGroupedAggregateQueryTcpClientStateV2::kConnecting) {
    return {.want_write = true};
  }
  if (!implementation_->carrier.has_value())
    return {};
  // Guarded above; clang-tidy does not preserve the state/optional relationship here.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  return implementation_->carrier.value().interest();
}

int DistributedVectorGroupedAggregateQueryTcpClientV2::descriptor() const noexcept {
  return implementation_ ? implementation_->socket.descriptor() : -1;
}

common::Result<std::span<const DistributedVectorGroupedAggregateQueryResponseV2>>
DistributedVectorGroupedAggregateQueryTcpClientV2::responses() const {
  if (!implementation_ || implementation_->client_state !=
                              DistributedVectorGroupedAggregateQueryTcpClientStateV2::kComplete) {
    return common::make_unexpected(invalid(
        "vector grouped aggregate query v2 TCP responses are unavailable before completion"));
  }
  if (!implementation_->carrier.has_value()) {
    return common::make_unexpected(
        invalid("vector grouped aggregate query v2 TCP carrier is unavailable"));
  }
  // Guarded above; clang-tidy does not preserve the state/optional relationship here.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  return implementation_->carrier.value().responses();
}

const common::Status& DistributedVectorGroupedAggregateQueryTcpClientV2::failure() const noexcept {
  static const common::Status empty_failure{
      common::StatusCode::kInvalidArgument,
      "vector grouped aggregate query v2 TCP client is empty"};
  return implementation_ ? implementation_->client_failure : empty_failure;
}

} // namespace chronos::cluster
