#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_tcp_client.hpp"

#include <chrono>
#include <new>
#include <optional>
#include <utility>
#include <variant>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      DistributedVectorGroupedAggregateShuffleJobControlTcpClient::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleJobControlTcpClient::TimePoint
deadline_after(const DistributedVectorGroupedAggregateShuffleJobControlTcpClient::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration = std::chrono::duration_cast<
      DistributedVectorGroupedAggregateShuffleJobControlTcpClient::TimePoint::duration>(timeout);
  return now > DistributedVectorGroupedAggregateShuffleJobControlTcpClient::TimePoint::max() -
                     duration
             ? DistributedVectorGroupedAggregateShuffleJobControlTcpClient::TimePoint::max()
             : now + duration;
}

[[nodiscard]] common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest>
encode_request(const DistributedVectorGroupedAggregateShuffleJobControlRequest& request) {
  if (const auto* prepare =
          std::get_if<DistributedVectorGroupedAggregateShuffleJobPrepare>(&request)) {
    return encode_distributed_vector_grouped_aggregate_shuffle_job_prepare_v1(*prepare);
  }
  if (const auto* seal = std::get_if<DistributedVectorGroupedAggregateShuffleJobSeal>(&request))
    return encode_distributed_vector_grouped_aggregate_shuffle_job_seal_v1(*seal);
  return encode_distributed_vector_grouped_aggregate_shuffle_job_install_routes_v2(
      std::get<DistributedVectorGroupedAggregateShuffleJobInstallRoutes>(request));
}

} // namespace

class DistributedVectorGroupedAggregateShuffleJobControlTcpClient::Impl {
public:
  Impl(network::TcpSocket owned_socket,
       DistributedVectorGroupedAggregateShuffleJobControlTcpClientConfig configured,
       const TimePoint now)
      : socket(std::move(owned_socket)), config(std::move(configured)),
        connect_deadline(deadline_after(now, config.connect_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (client_state != DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kFailed) {
      carrier.reset();
      static_cast<void>(socket.close());
      client_failure = std::move(failure);
      client_state = DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kFailed;
    }
    return client_failure;
  }

  // Declaration order releases the TLS carrier before its borrowed descriptor.
  network::TcpSocket socket;
  DistributedVectorGroupedAggregateShuffleJobControlTcpClientConfig config;
  TimePoint connect_deadline;
  std::optional<DistributedVectorGroupedAggregateShuffleJobControlTlsClient> carrier;
  DistributedVectorGroupedAggregateShuffleJobControlTcpClientState client_state{
      DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kConnecting};
  common::Status client_failure{common::StatusCode::kInternal,
                                "grouped shuffle reducer-job TCP client has not failed"};
};

DistributedVectorGroupedAggregateShuffleJobControlTcpClient::
    DistributedVectorGroupedAggregateShuffleJobControlTcpClient(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateShuffleJobControlTcpClient::
    ~DistributedVectorGroupedAggregateShuffleJobControlTcpClient() = default;
DistributedVectorGroupedAggregateShuffleJobControlTcpClient::
    DistributedVectorGroupedAggregateShuffleJobControlTcpClient(
        DistributedVectorGroupedAggregateShuffleJobControlTcpClient&&) noexcept = default;
DistributedVectorGroupedAggregateShuffleJobControlTcpClient&
DistributedVectorGroupedAggregateShuffleJobControlTcpClient::operator=(
    DistributedVectorGroupedAggregateShuffleJobControlTcpClient&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateShuffleJobControlTcpClient>
DistributedVectorGroupedAggregateShuffleJobControlTcpClient::begin(
    DistributedVectorGroupedAggregateShuffleJobControlTcpClientConfig config, const TimePoint now) {
  if (config.tls_context == nullptr || config.carrier.authenticator == nullptr ||
      config.carrier.node_authorizer == nullptr || !valid_timeout(config.connect_timeout) ||
      !valid_timeout(config.carrier.limits.handshake_timeout) ||
      !valid_timeout(config.carrier.limits.exchange_timeout) ||
      config.carrier.peer_ipv4_address != config.remote_endpoint.address) {
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument,
               "grouped shuffle reducer-job TCP client configuration is invalid"));
  }
  const common::Status limits =
      validate_distributed_vector_grouped_aggregate_shuffle_job_control_decode_limits(
          config.carrier.limits.request);
  if (!limits.is_ok())
    return common::make_unexpected(limits);
  auto encoded = encode_request(config.carrier.request);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  if (encoded->bytes().size() > config.carrier.limits.request.maximum_frame_length) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted,
               "grouped shuffle reducer-job TCP request exceeds configured frame limit"));
  }
  if (const auto* prepare =
          std::get_if<DistributedVectorGroupedAggregateShuffleJobPrepare>(&config.carrier.request);
      prepare != nullptr &&
      prepare->execution_timeout > config.carrier.limits.request.maximum_execution_timeout) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted,
               "grouped shuffle reducer-job TCP request exceeds configured execution timeout"));
  }
  auto socket = network::TcpSocket::begin_connect(config.remote_endpoint);
  if (!socket.has_value())
    return common::make_unexpected(socket.error());
  try {
    return DistributedVectorGroupedAggregateShuffleJobControlTcpClient{
        std::make_unique<Impl>(std::move(*socket), std::move(config), now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "grouped shuffle reducer-job TCP allocation failed"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleJobControlTcpClient::on_ready(
    const bool readable, const bool writable, const TimePoint now) {
  if (!implementation_) {
    return status(common::StatusCode::kInvalidArgument,
                  "grouped shuffle reducer-job TCP client is empty");
  }
  Impl& impl = *implementation_;
  if (impl.client_state ==
      DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kFailed) {
    return impl.client_failure;
  }
  if (impl.client_state ==
      DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kComplete) {
    return common::Status::ok();
  }
  if (impl.client_state ==
      DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kConnecting) {
    if (now >= impl.connect_deadline) {
      return impl.fail(status(common::StatusCode::kUnavailable,
                              "grouped shuffle reducer-job TCP connect timed out"));
    }
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
    auto carrier = DistributedVectorGroupedAggregateShuffleJobControlTlsClient::create(
        std::move(*tls), std::move(impl.config.carrier), now);
    if (!carrier.has_value())
      return impl.fail(carrier.error());
    impl.carrier.emplace(std::move(*carrier));
    impl.client_state =
        DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kExchanging;
    return common::Status::ok();
  }
  const common::Status progress = impl.carrier->on_ready(readable, writable, now);
  if (!progress.is_ok() ||
      impl.carrier->state() ==
          DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kFailed) {
    return impl.fail(progress.is_ok() ? impl.carrier->failure() : progress);
  }
  if (impl.carrier->state() ==
      DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kComplete) {
    impl.client_state = DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kComplete;
  }
  return common::Status::ok();
}

DistributedVectorGroupedAggregateShuffleJobControlTcpClientState
DistributedVectorGroupedAggregateShuffleJobControlTcpClient::state() const noexcept {
  return implementation_
             ? implementation_->client_state
             : DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kFailed;
}

DistributedVectorGroupedAggregateShuffleJobControlTlsInterest
DistributedVectorGroupedAggregateShuffleJobControlTcpClient::interest() const noexcept {
  if (!implementation_ ||
      implementation_->client_state ==
          DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kFailed ||
      implementation_->client_state ==
          DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kComplete) {
    return {};
  }
  if (implementation_->client_state ==
      DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kConnecting) {
    return {.want_write = true};
  }
  const auto* carrier = implementation_->carrier.transform([](const auto& value) { return &value; })
                            .value_or(nullptr);
  return carrier != nullptr ? carrier->interest()
                            : DistributedVectorGroupedAggregateShuffleJobControlTlsInterest{};
}

int DistributedVectorGroupedAggregateShuffleJobControlTcpClient::descriptor() const noexcept {
  return implementation_ ? implementation_->socket.descriptor() : -1;
}

std::optional<DistributedVectorGroupedAggregateShuffleJobControlTcpClient::TimePoint>
DistributedVectorGroupedAggregateShuffleJobControlTcpClient::deadline() const noexcept {
  if (!implementation_ ||
      implementation_->client_state ==
          DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kFailed ||
      implementation_->client_state ==
          DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kComplete) {
    return std::nullopt;
  }
  if (implementation_->client_state ==
      DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kConnecting) {
    return implementation_->connect_deadline;
  }
  const auto* carrier = implementation_->carrier.transform([](const auto& value) { return &value; })
                            .value_or(nullptr);
  return carrier != nullptr ? std::optional<TimePoint>{carrier->deadline()} : std::nullopt;
}

common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponse>
DistributedVectorGroupedAggregateShuffleJobControlTcpClient::result() const {
  if (!implementation_ ||
      implementation_->client_state !=
          DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kComplete) {
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument,
               "grouped shuffle reducer-job TCP response is unavailable"));
  }
  const auto* carrier = implementation_->carrier.transform([](const auto& value) { return &value; })
                            .value_or(nullptr);
  if (carrier == nullptr) {
    return common::make_unexpected(status(
        common::StatusCode::kInternal, "grouped shuffle reducer-job TCP carrier is unavailable"));
  }
  return carrier->result();
}

const common::Status&
DistributedVectorGroupedAggregateShuffleJobControlTcpClient::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "grouped shuffle reducer-job TCP client is empty"};
  return implementation_ ? implementation_->client_failure : empty;
}

} // namespace chronos::cluster
