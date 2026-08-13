#include "chronos/cluster/raft_observation_tcp_client.hpp"

#include <chrono>
#include <new>
#include <optional>
#include <utility>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status status(common::StatusCode code, const char* message) {
  return {code, message};
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      RaftObservationTcpClient::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] RaftObservationTcpClient::TimePoint
deadline_after(const RaftObservationTcpClient::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration =
      std::chrono::duration_cast<RaftObservationTcpClient::TimePoint::duration>(timeout);
  return now > RaftObservationTcpClient::TimePoint::max() - duration
             ? RaftObservationTcpClient::TimePoint::max()
             : now + duration;
}

} // namespace

class RaftObservationTcpClient::Impl {
public:
  Impl(network::TcpSocket owned_socket, RaftObservationTcpClientConfig configured,
       const TimePoint now) noexcept
      : socket(std::move(owned_socket)), config(configured),
        connect_deadline(deadline_after(now, config.connect_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (client_state != RaftObservationTcpClientState::kFailed) {
      carrier.reset();
      static_cast<void>(socket.close());
      client_failure = std::move(failure);
      client_state = RaftObservationTcpClientState::kFailed;
    }
    return client_failure;
  }

  // Socket precedes carrier so reverse destruction releases TLS before its borrowed descriptor.
  network::TcpSocket socket;
  RaftObservationTcpClientConfig config;
  TimePoint connect_deadline{};
  std::optional<RaftObservationTlsClient> carrier;
  RaftObservationTcpClientState client_state{RaftObservationTcpClientState::kConnecting};
  common::Status client_failure{common::StatusCode::kInternal,
                                "Raft observation TCP client has not failed"};
};

RaftObservationTcpClient::RaftObservationTcpClient(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftObservationTcpClient::~RaftObservationTcpClient() = default;
RaftObservationTcpClient::RaftObservationTcpClient(RaftObservationTcpClient&&) noexcept = default;
RaftObservationTcpClient&
RaftObservationTcpClient::operator=(RaftObservationTcpClient&&) noexcept = default;

common::Result<RaftObservationTcpClient>
RaftObservationTcpClient::begin(const RaftObservationTcpClientConfig config, const TimePoint now) {
  if (config.tls_context == nullptr || config.carrier.authenticator == nullptr ||
      config.carrier.node_authorizer == nullptr || !valid_timeout(config.connect_timeout) ||
      !valid_timeout(config.carrier.limits.handshake_timeout) ||
      !valid_timeout(config.carrier.limits.exchange_timeout) ||
      config.carrier.peer_ipv4_address != config.remote_endpoint.address) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft observation TCP configuration is invalid"));
  }
  auto request = encode_raft_observation_request_v1(config.carrier.request);
  if (!request.has_value())
    return common::make_unexpected(request.error());
  auto limits = RaftObservationResponseReader::create(config.carrier.limits.transport);
  if (!limits.has_value())
    return common::make_unexpected(limits.error());
  auto socket = network::TcpSocket::begin_connect(config.remote_endpoint);
  if (!socket.has_value())
    return common::make_unexpected(socket.error());
  try {
    return RaftObservationTcpClient{std::make_unique<Impl>(std::move(*socket), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted, "Raft observation TCP allocation failed"));
  }
}

common::Status RaftObservationTcpClient::on_ready(const bool readable, const bool writable,
                                                  const TimePoint now) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft observation TCP client is empty");
  Impl& impl = *implementation_;
  if (impl.client_state == RaftObservationTcpClientState::kFailed)
    return impl.client_failure;
  if (impl.client_state == RaftObservationTcpClientState::kComplete)
    return common::Status::ok();
  if (impl.client_state == RaftObservationTcpClientState::kConnecting) {
    if (now >= impl.connect_deadline)
      return impl.fail(
          status(common::StatusCode::kUnavailable, "Raft observation TCP connect timed out"));
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
    auto carrier = RaftObservationTlsClient::create(std::move(*tls), impl.config.carrier, now);
    if (!carrier.has_value())
      return impl.fail(carrier.error());
    impl.carrier.emplace(std::move(*carrier));
    impl.client_state = RaftObservationTcpClientState::kExchanging;
    return common::Status::ok();
  }
  const common::Status progress = impl.carrier->on_ready(readable, writable, now);
  if (!progress.is_ok() || impl.carrier->state() == RaftObservationTlsClientState::kFailed)
    return impl.fail(progress.is_ok() ? impl.carrier->failure() : progress);
  if (impl.carrier->state() == RaftObservationTlsClientState::kComplete)
    impl.client_state = RaftObservationTcpClientState::kComplete;
  return common::Status::ok();
}

RaftObservationTcpClientState RaftObservationTcpClient::state() const noexcept {
  return implementation_ ? implementation_->client_state : RaftObservationTcpClientState::kFailed;
}

RaftObservationTlsInterest RaftObservationTcpClient::interest() const noexcept {
  if (!implementation_ || implementation_->client_state == RaftObservationTcpClientState::kFailed ||
      implementation_->client_state == RaftObservationTcpClientState::kComplete)
    return {};
  if (implementation_->client_state == RaftObservationTcpClientState::kConnecting)
    return {.want_write = true};
  return implementation_->carrier->interest();
}

int RaftObservationTcpClient::descriptor() const noexcept {
  return implementation_ ? implementation_->socket.descriptor() : -1;
}

common::Result<raft::RaftGroupObservation> RaftObservationTcpClient::result() const {
  if (!implementation_ || implementation_->client_state != RaftObservationTcpClientState::kComplete)
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "Raft observation TCP result is unavailable"));
  return implementation_->carrier->result();
}

const common::Status& RaftObservationTcpClient::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "Raft observation TCP client is empty"};
  return implementation_ ? implementation_->client_failure : empty;
}

} // namespace chronos::cluster
