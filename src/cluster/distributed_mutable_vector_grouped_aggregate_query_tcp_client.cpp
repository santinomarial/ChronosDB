#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tcp_client.hpp"

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
      DistributedMutableVectorGroupedAggregateQueryTcpClient::TimePoint::duration::max());
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

[[nodiscard]] DistributedMutableVectorGroupedAggregateQueryTcpClient::TimePoint
deadline_after(const DistributedMutableVectorGroupedAggregateQueryTcpClient::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration = std::chrono::duration_cast<
      DistributedMutableVectorGroupedAggregateQueryTcpClient::TimePoint::duration>(timeout);
  return now > DistributedMutableVectorGroupedAggregateQueryTcpClient::TimePoint::max() - duration
             ? DistributedMutableVectorGroupedAggregateQueryTcpClient::TimePoint::max()
             : now + duration;
}

} // namespace

class DistributedMutableVectorGroupedAggregateQueryTcpClient::Impl {
public:
  Impl(network::TcpSocket owned_socket,
       DistributedMutableVectorGroupedAggregateQueryAttempt owned_attempt,
       std::vector<query::VectorGroupKeyDefinition>&& owned_keys,
       std::vector<query::VectorAggregateDefinition>&& owned_aggregates,
       query::QueryResourceContext owned_resources,
       const DistributedMutableVectorGroupedAggregateQueryTcpClientConfig configured,
       const TimePoint now)
      : socket(std::move(owned_socket)), attempt(std::move(owned_attempt)),
        keys(std::move(owned_keys)), aggregates(std::move(owned_aggregates)),
        resources(std::move(owned_resources)), config(configured),
        connect_deadline(deadline_after(now, config.connect_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (client_state != DistributedMutableVectorGroupedAggregateQueryTcpClientState::kFailed) {
      carrier.reset();
      static_cast<void>(socket.close());
      client_failure = std::move(status);
      client_state = DistributedMutableVectorGroupedAggregateQueryTcpClientState::kFailed;
    }
    return client_failure;
  }

  // Socket is declared before carrier so destruction is carrier first, descriptor second.
  network::TcpSocket socket;
  DistributedMutableVectorGroupedAggregateQueryAttempt attempt;
  std::vector<query::VectorGroupKeyDefinition> keys;
  std::vector<query::VectorAggregateDefinition> aggregates;
  query::QueryResourceContext resources;
  DistributedMutableVectorGroupedAggregateQueryTcpClientConfig config;
  TimePoint connect_deadline;
  std::optional<DistributedMutableVectorGroupedAggregateQueryTlsClient> carrier;
  DistributedMutableVectorGroupedAggregateQueryTcpClientState client_state{
      DistributedMutableVectorGroupedAggregateQueryTcpClientState::kConnecting};
  common::Status client_failure{common::StatusCode::kInternal,
                                "mutable vector grouped aggregate query TCP client has not failed"};
};

DistributedMutableVectorGroupedAggregateQueryTcpClient::
    DistributedMutableVectorGroupedAggregateQueryTcpClient(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedMutableVectorGroupedAggregateQueryTcpClient::
    ~DistributedMutableVectorGroupedAggregateQueryTcpClient() = default;
DistributedMutableVectorGroupedAggregateQueryTcpClient::
    DistributedMutableVectorGroupedAggregateQueryTcpClient(
        DistributedMutableVectorGroupedAggregateQueryTcpClient&&) noexcept = default;
DistributedMutableVectorGroupedAggregateQueryTcpClient&
DistributedMutableVectorGroupedAggregateQueryTcpClient::operator=(
    DistributedMutableVectorGroupedAggregateQueryTcpClient&&) noexcept = default;

common::Result<DistributedMutableVectorGroupedAggregateQueryTcpClient>
DistributedMutableVectorGroupedAggregateQueryTcpClient::begin(
    DistributedMutableVectorGroupedAggregateQueryAttempt attempt,
    std::vector<query::VectorGroupKeyDefinition>&& keys,
    std::vector<query::VectorAggregateDefinition>&& aggregates,
    query::QueryResourceContext resources,
    const DistributedMutableVectorGroupedAggregateQueryTcpClientConfig config,
    const TimePoint now) {
  if (config.tls_context == nullptr || config.carrier.authenticator == nullptr ||
      config.carrier.node_authorizer == nullptr || !valid_timeout(config.connect_timeout) ||
      !valid_limits(config.carrier.limits) ||
      config.carrier.peer_ipv4_address != config.remote_endpoint.address ||
      attempt.attempt_number == 0U || attempt.target_node_id == 0U ||
      keys.size() > config.carrier.limits.payload.maximum_group_keys ||
      aggregates.size() > config.carrier.limits.payload.maximum_aggregates) {
    return common::make_unexpected(
        invalid("mutable vector grouped aggregate query TCP client configuration is invalid"));
  }
  auto decoded = decode_distributed_mutable_vector_query_request_exact(attempt.request_bytes);
  if (!decoded.has_value())
    return common::make_unexpected(decoded.error());
  if (decoded->target_node_id != attempt.target_node_id) {
    return common::make_unexpected(
        invalid("mutable vector grouped aggregate query TCP attempt target is inconsistent"));
  }
  const common::Status authority_status =
      validate_distributed_mutable_vector_grouped_aggregate_query_authority(decoded->fragment, keys,
                                                                            aggregates);
  if (!authority_status.is_ok())
    return common::make_unexpected(authority_status);
  auto socket = network::TcpSocket::begin_connect(config.remote_endpoint);
  if (!socket.has_value())
    return common::make_unexpected(socket.error());
  try {
    return DistributedMutableVectorGroupedAggregateQueryTcpClient{
        std::make_unique<Impl>(std::move(*socket), std::move(attempt), std::move(keys),
                               std::move(aggregates), std::move(resources), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("mutable vector grouped aggregate query TCP client allocation failed"));
  }
}

common::Status DistributedMutableVectorGroupedAggregateQueryTcpClient::on_ready(
    const bool readable, const bool writable, const TimePoint now) {
  if (!implementation_)
    return invalid("mutable vector grouped aggregate query TCP client is empty");
  Impl& impl = *implementation_;
  if (impl.client_state == DistributedMutableVectorGroupedAggregateQueryTcpClientState::kFailed)
    return impl.client_failure;
  if (impl.client_state == DistributedMutableVectorGroupedAggregateQueryTcpClientState::kComplete)
    return common::Status::ok();
  if (impl.client_state ==
      DistributedMutableVectorGroupedAggregateQueryTcpClientState::kConnecting) {
    if (now >= impl.connect_deadline)
      return impl.fail(unavailable("mutable vector grouped aggregate query TCP connect timed out"));
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
    auto carrier = DistributedMutableVectorGroupedAggregateQueryTlsClient::create(
        std::move(*tls), std::move(impl.attempt), std::move(impl.keys), std::move(impl.aggregates),
        std::move(impl.resources), impl.config.carrier, now);
    if (!carrier.has_value())
      return impl.fail(carrier.error());
    impl.carrier.emplace(std::move(*carrier));
    impl.client_state = DistributedMutableVectorGroupedAggregateQueryTcpClientState::kExchanging;
    return common::Status::ok();
  }
  const common::Status status = impl.carrier->on_ready(readable, writable, now);
  if (!status.is_ok() ||
      impl.carrier->state() == DistributedMutableVectorGroupedAggregateQueryTlsState::kFailed) {
    return impl.fail(status.is_ok() ? impl.carrier->failure() : status);
  }
  if (impl.carrier->state() == DistributedMutableVectorGroupedAggregateQueryTlsState::kComplete)
    impl.client_state = DistributedMutableVectorGroupedAggregateQueryTcpClientState::kComplete;
  return common::Status::ok();
}

DistributedMutableVectorGroupedAggregateQueryTcpClientState
DistributedMutableVectorGroupedAggregateQueryTcpClient::state() const noexcept {
  return implementation_ ? implementation_->client_state
                         : DistributedMutableVectorGroupedAggregateQueryTcpClientState::kFailed;
}

DistributedMutableVectorGroupedAggregateQueryTlsInterest
DistributedMutableVectorGroupedAggregateQueryTcpClient::interest() const noexcept {
  if (!implementation_ ||
      implementation_->client_state ==
          DistributedMutableVectorGroupedAggregateQueryTcpClientState::kFailed ||
      implementation_->client_state ==
          DistributedMutableVectorGroupedAggregateQueryTcpClientState::kComplete) {
    return {};
  }
  if (implementation_->client_state ==
      DistributedMutableVectorGroupedAggregateQueryTcpClientState::kConnecting) {
    return {.want_write = true};
  }
  if (!implementation_->carrier.has_value())
    return {};
  // Guarded above; clang-tidy does not preserve the state/optional relationship here.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  return implementation_->carrier.value().interest();
}

int DistributedMutableVectorGroupedAggregateQueryTcpClient::descriptor() const noexcept {
  return implementation_ ? implementation_->socket.descriptor() : -1;
}

common::Result<std::span<const DistributedVectorGroupedAggregateQueryResponseV2>>
DistributedMutableVectorGroupedAggregateQueryTcpClient::responses() const {
  if (!implementation_ ||
      implementation_->client_state !=
          DistributedMutableVectorGroupedAggregateQueryTcpClientState::kComplete) {
    return common::make_unexpected(invalid(
        "mutable vector grouped aggregate query TCP responses are unavailable before completion"));
  }
  if (!implementation_->carrier.has_value()) {
    return common::make_unexpected(
        invalid("mutable vector grouped aggregate query TCP carrier is unavailable"));
  }
  // Guarded above; clang-tidy does not preserve the state/optional relationship here.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  return implementation_->carrier.value().responses();
}

const common::Status&
DistributedMutableVectorGroupedAggregateQueryTcpClient::failure() const noexcept {
  static const common::Status empty_failure{
      common::StatusCode::kInvalidArgument,
      "mutable vector grouped aggregate query TCP client is empty"};
  return implementation_ ? implementation_->client_failure : empty_failure;
}

} // namespace chronos::cluster
