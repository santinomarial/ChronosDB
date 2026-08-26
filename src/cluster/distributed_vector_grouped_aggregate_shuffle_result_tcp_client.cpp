#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tcp_client.hpp"

#include <chrono>
#include <functional>
#include <memory>
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
      DistributedVectorGroupedAggregateShuffleResultTcpClient::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] bool
valid_limits(const DistributedVectorGroupedAggregateShuffleResultTlsLimits& limits) noexcept {
  return valid_timeout(limits.handshake_timeout) && valid_timeout(limits.exchange_timeout) &&
         validate_distributed_vector_grouped_aggregate_shuffle_result_stream_limits(limits.stream);
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleResultTcpClient::TimePoint
deadline_after(const DistributedVectorGroupedAggregateShuffleResultTcpClient::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration = std::chrono::duration_cast<
      DistributedVectorGroupedAggregateShuffleResultTcpClient::TimePoint::duration>(timeout);
  return now > DistributedVectorGroupedAggregateShuffleResultTcpClient::TimePoint::max() - duration
             ? DistributedVectorGroupedAggregateShuffleResultTcpClient::TimePoint::max()
             : now + duration;
}

} // namespace

class DistributedVectorGroupedAggregateShuffleResultTcpClient::Impl {
public:
  Impl(network::TcpSocket socket, DistributedVectorGroupedAggregateShuffleResultAttempt attempt,
       const DistributedVectorGroupedAggregateShuffleAuthority& authority,
       const query::DistributedVectorResultSchema& result_schema,
       const DistributedVectorGroupedAggregateShuffleResultTcpClientConfig config,
       const TimePoint now)
      : socket_(std::move(socket)), attempt_(std::move(attempt)), authority_(authority),
        result_schema_(result_schema), config_(config),
        connect_deadline_(deadline_after(now, config.connect_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ != DistributedVectorGroupedAggregateShuffleResultTcpClientState::kFailed) {
      carrier_.reset();
      static_cast<void>(socket_.close());
      failure_ = std::move(status);
      state_ = DistributedVectorGroupedAggregateShuffleResultTcpClientState::kFailed;
    }
    return failure_;
  }

  // Destruction is reverse declaration order: TLS carrier before its borrowed descriptor.
  network::TcpSocket socket_;
  DistributedVectorGroupedAggregateShuffleResultAttempt attempt_;
  std::reference_wrapper<const DistributedVectorGroupedAggregateShuffleAuthority> authority_;
  std::reference_wrapper<const query::DistributedVectorResultSchema> result_schema_;
  DistributedVectorGroupedAggregateShuffleResultTcpClientConfig config_;
  TimePoint connect_deadline_;
  std::optional<DistributedVectorGroupedAggregateShuffleResultTlsClient> carrier_;
  DistributedVectorGroupedAggregateShuffleResultTcpClientState state_{
      DistributedVectorGroupedAggregateShuffleResultTcpClientState::kConnecting};
  common::Status failure_{common::StatusCode::kInternal,
                          "grouped shuffle result TCP client has not failed"};
};

DistributedVectorGroupedAggregateShuffleResultTcpClient::
    DistributedVectorGroupedAggregateShuffleResultTcpClient(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateShuffleResultTcpClient::
    ~DistributedVectorGroupedAggregateShuffleResultTcpClient() = default;
DistributedVectorGroupedAggregateShuffleResultTcpClient::
    DistributedVectorGroupedAggregateShuffleResultTcpClient(
        DistributedVectorGroupedAggregateShuffleResultTcpClient&&) noexcept = default;
DistributedVectorGroupedAggregateShuffleResultTcpClient&
DistributedVectorGroupedAggregateShuffleResultTcpClient::operator=(
    DistributedVectorGroupedAggregateShuffleResultTcpClient&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateShuffleResultTcpClient>
DistributedVectorGroupedAggregateShuffleResultTcpClient::begin(
    DistributedVectorGroupedAggregateShuffleResultAttempt attempt,
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema,
    const DistributedVectorGroupedAggregateShuffleResultTcpClientConfig config,
    const TimePoint now) {
  const auto source_node = authority.destination_node(attempt.stream.partition_id());
  if (config.tls_context == nullptr || config.carrier.authenticator == nullptr ||
      config.carrier.node_authorizer == nullptr || !valid_timeout(config.connect_timeout) ||
      !valid_limits(config.carrier.limits) ||
      config.carrier.peer_ipv4_address != config.remote_endpoint.address ||
      attempt.attempt_number == 0U || attempt.target_node_id == 0U || !source_node.has_value() ||
      *source_node != attempt.stream.source_node_id() || *source_node == attempt.target_node_id ||
      attempt.stream.coordinator_node_id() != attempt.target_node_id ||
      !query::validate_distributed_vector_result_schema_value(result_schema).is_ok()) {
    return common::make_unexpected(
        invalid("grouped shuffle result TCP client configuration is invalid"));
  }
  auto socket = network::TcpSocket::begin_connect(config.remote_endpoint);
  if (!socket.has_value())
    return common::make_unexpected(socket.error());
  try {
    return DistributedVectorGroupedAggregateShuffleResultTcpClient{std::make_unique<Impl>(
        std::move(*socket), std::move(attempt), authority, result_schema, config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped shuffle result TCP client allocation failed"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleResultTcpClient::on_ready(
    const bool readable, const bool writable, const TimePoint now) {
  if (!implementation_)
    return invalid("grouped shuffle result TCP client is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleResultTcpClientState::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleResultTcpClientState::kComplete)
    return common::Status::ok();
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleResultTcpClientState::kConnecting) {
    if (now >= impl.connect_deadline_)
      return impl.fail(unavailable("grouped shuffle result TCP connect timed out"));
    if (!writable)
      return common::Status::ok();
    auto connected = impl.socket_.finish_connect();
    if (!connected.has_value())
      return impl.fail(connected.error());
    if (*connected == network::TcpConnectState::kInProgress)
      return common::Status::ok();
    auto tls = network::TlsSocket::connect(*impl.config_.tls_context, impl.socket_.descriptor());
    if (!tls.has_value())
      return impl.fail(tls.error());
    auto carrier = DistributedVectorGroupedAggregateShuffleResultTlsClient::create(
        std::move(*tls), std::move(impl.attempt_.stream), impl.authority_.get(),
        impl.result_schema_.get(), impl.attempt_.target_node_id, impl.config_.carrier, now);
    if (!carrier.has_value())
      return impl.fail(carrier.error());
    impl.carrier_.emplace(std::move(*carrier));
    impl.state_ = DistributedVectorGroupedAggregateShuffleResultTcpClientState::kExchanging;
    return common::Status::ok();
  }
  const common::Status status = impl.carrier_->on_ready(readable, writable, now);
  if (!status.is_ok() ||
      impl.carrier_->state() == DistributedVectorGroupedAggregateShuffleResultTlsState::kFailed) {
    return impl.fail(status.is_ok() ? impl.carrier_->failure() : status);
  }
  if (impl.carrier_->state() == DistributedVectorGroupedAggregateShuffleResultTlsState::kComplete)
    impl.state_ = DistributedVectorGroupedAggregateShuffleResultTcpClientState::kComplete;
  return common::Status::ok();
}

DistributedVectorGroupedAggregateShuffleResultTcpClientState
DistributedVectorGroupedAggregateShuffleResultTcpClient::state() const noexcept {
  return implementation_ ? implementation_->state_
                         : DistributedVectorGroupedAggregateShuffleResultTcpClientState::kFailed;
}

DistributedVectorGroupedAggregateShuffleResultTlsInterest
DistributedVectorGroupedAggregateShuffleResultTcpClient::interest() const noexcept {
  if (!implementation_ ||
      implementation_->state_ ==
          DistributedVectorGroupedAggregateShuffleResultTcpClientState::kFailed ||
      implementation_->state_ ==
          DistributedVectorGroupedAggregateShuffleResultTcpClientState::kComplete) {
    return {};
  }
  if (implementation_->state_ ==
      DistributedVectorGroupedAggregateShuffleResultTcpClientState::kConnecting) {
    return {.want_write = true};
  }
  if (!implementation_->carrier_.has_value())
    return {};
  // Guarded above; clang-tidy does not preserve the state/optional relationship here.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  return implementation_->carrier_.value().interest();
}

DistributedVectorGroupedAggregateShuffleResultTcpClient::TimePoint
DistributedVectorGroupedAggregateShuffleResultTcpClient::deadline() const noexcept {
  if (!implementation_)
    return TimePoint::min();
  if (implementation_->state_ ==
      DistributedVectorGroupedAggregateShuffleResultTcpClientState::kConnecting) {
    return implementation_->connect_deadline_;
  }
  if (implementation_->carrier_.has_value())
    return implementation_->carrier_->deadline();
  return TimePoint::min();
}

int DistributedVectorGroupedAggregateShuffleResultTcpClient::descriptor() const noexcept {
  return implementation_ ? implementation_->socket_.descriptor() : -1;
}

std::size_t
DistributedVectorGroupedAggregateShuffleResultTcpClient::attempt_number() const noexcept {
  return implementation_ ? implementation_->attempt_.attempt_number : 0U;
}

raft::NodeId
DistributedVectorGroupedAggregateShuffleResultTcpClient::target_node_id() const noexcept {
  return implementation_ ? implementation_->attempt_.target_node_id : 0U;
}

const common::Status&
DistributedVectorGroupedAggregateShuffleResultTcpClient::failure() const noexcept {
  static const common::Status empty_failure{common::StatusCode::kInvalidArgument,
                                            "grouped shuffle result TCP client is empty"};
  return implementation_ ? implementation_->failure_ : empty_failure;
}

} // namespace chronos::cluster
