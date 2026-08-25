#include "chronos/cluster/raft_read_authority_tcp_client.hpp"

#include <chrono>
#include <new>
#include <optional>
#include <utility>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      RaftReadAuthorityTcpClient::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] RaftReadAuthorityTcpClient::TimePoint
deadline_after(const RaftReadAuthorityTcpClient::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration =
      std::chrono::duration_cast<RaftReadAuthorityTcpClient::TimePoint::duration>(timeout);
  return now > RaftReadAuthorityTcpClient::TimePoint::max() - duration
             ? RaftReadAuthorityTcpClient::TimePoint::max()
             : now + duration;
}

} // namespace

class RaftReadAuthorityTcpClient::Impl {
public:
  Impl(network::TcpSocket owned_socket, RaftReadAuthorityTcpClientConfig configured,
       const TimePoint now)
      : socket(std::move(owned_socket)), config(configured),
        connect_deadline(deadline_after(now, config.connect_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (client_state != RaftReadAuthorityTcpClientState::kFailed) {
      carrier.reset();
      static_cast<void>(socket.close());
      client_failure = std::move(failure);
      client_state = RaftReadAuthorityTcpClientState::kFailed;
    }
    return client_failure;
  }

  // Declaration order ensures reverse destruction releases TLS before its borrowed descriptor.
  network::TcpSocket socket;
  RaftReadAuthorityTcpClientConfig config;
  TimePoint connect_deadline;
  std::optional<RaftReadAuthorityTlsClient> carrier;
  RaftReadAuthorityTcpClientState client_state{RaftReadAuthorityTcpClientState::kConnecting};
  common::Status client_failure{common::StatusCode::kInternal,
                                "Raft read-authority TCP client has not failed"};
};

RaftReadAuthorityTcpClient::RaftReadAuthorityTcpClient(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftReadAuthorityTcpClient::~RaftReadAuthorityTcpClient() = default;
RaftReadAuthorityTcpClient::RaftReadAuthorityTcpClient(RaftReadAuthorityTcpClient&&) noexcept =
    default;
RaftReadAuthorityTcpClient&
RaftReadAuthorityTcpClient::operator=(RaftReadAuthorityTcpClient&&) noexcept = default;

common::Result<RaftReadAuthorityTcpClient>
RaftReadAuthorityTcpClient::begin(const RaftReadAuthorityTcpClientConfig config,
                                  const TimePoint now) {
  if (config.tls_context == nullptr || config.carrier.authenticator == nullptr ||
      config.carrier.node_authorizer == nullptr || !valid_timeout(config.connect_timeout) ||
      !valid_timeout(config.carrier.limits.handshake_timeout) ||
      !valid_timeout(config.carrier.limits.exchange_timeout) ||
      config.carrier.peer_ipv4_address != config.remote_endpoint.address) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft read-authority TCP configuration is invalid"));
  }
  auto request = encode_raft_read_authority_request_v1(config.carrier.request);
  if (!request.has_value())
    return common::make_unexpected(request.error());
  auto limits = RaftReadAuthorityResponseReader::create(config.carrier.limits.transport);
  if (!limits.has_value())
    return common::make_unexpected(limits.error());
  auto socket = network::TcpSocket::begin_connect(config.remote_endpoint);
  if (!socket.has_value())
    return common::make_unexpected(socket.error());
  try {
    return RaftReadAuthorityTcpClient{std::make_unique<Impl>(std::move(*socket), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft read-authority TCP allocation failed"));
  }
}

common::Status RaftReadAuthorityTcpClient::on_ready(const bool readable, const bool writable,
                                                    const TimePoint now) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft read-authority TCP client is empty");
  Impl& impl = *implementation_;
  if (impl.client_state == RaftReadAuthorityTcpClientState::kFailed)
    return impl.client_failure;
  if (impl.client_state == RaftReadAuthorityTcpClientState::kComplete)
    return common::Status::ok();
  if (impl.client_state == RaftReadAuthorityTcpClientState::kConnecting) {
    if (now >= impl.connect_deadline) {
      return impl.fail(
          status(common::StatusCode::kUnavailable, "Raft read-authority TCP connect timed out"));
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
    auto carrier = RaftReadAuthorityTlsClient::create(std::move(*tls), impl.config.carrier, now);
    if (!carrier.has_value())
      return impl.fail(carrier.error());
    impl.carrier.emplace(std::move(*carrier));
    impl.client_state = RaftReadAuthorityTcpClientState::kExchanging;
    return common::Status::ok();
  }
  const common::Status progress = impl.carrier->on_ready(readable, writable, now);
  if (!progress.is_ok() || impl.carrier->state() == RaftReadAuthorityTlsClientState::kFailed)
    return impl.fail(progress.is_ok() ? impl.carrier->failure() : progress);
  if (impl.carrier->state() == RaftReadAuthorityTlsClientState::kComplete)
    impl.client_state = RaftReadAuthorityTcpClientState::kComplete;
  return common::Status::ok();
}

RaftReadAuthorityTcpClientState RaftReadAuthorityTcpClient::state() const noexcept {
  return implementation_ ? implementation_->client_state : RaftReadAuthorityTcpClientState::kFailed;
}

RaftReadAuthorityTlsInterest RaftReadAuthorityTcpClient::interest() const noexcept {
  if (!implementation_ ||
      implementation_->client_state == RaftReadAuthorityTcpClientState::kFailed ||
      implementation_->client_state == RaftReadAuthorityTcpClientState::kComplete) {
    return {};
  }
  if (implementation_->client_state == RaftReadAuthorityTcpClientState::kConnecting)
    return {.want_write = true};
  const auto* carrier = implementation_->carrier.transform([](const auto& value) { return &value; })
                            .value_or(nullptr);
  return carrier != nullptr ? carrier->interest() : RaftReadAuthorityTlsInterest{};
}

int RaftReadAuthorityTcpClient::descriptor() const noexcept {
  return implementation_ ? implementation_->socket.descriptor() : -1;
}

std::optional<RaftReadAuthorityTcpClient::TimePoint>
RaftReadAuthorityTcpClient::deadline() const noexcept {
  if (!implementation_ ||
      implementation_->client_state == RaftReadAuthorityTcpClientState::kFailed ||
      implementation_->client_state == RaftReadAuthorityTcpClientState::kComplete) {
    return std::nullopt;
  }
  if (implementation_->client_state == RaftReadAuthorityTcpClientState::kConnecting)
    return implementation_->connect_deadline;
  const auto* carrier = implementation_->carrier.transform([](const auto& value) { return &value; })
                            .value_or(nullptr);
  return carrier != nullptr ? std::optional<TimePoint>{carrier->deadline()} : std::nullopt;
}

common::Result<RaftReadAuthority> RaftReadAuthorityTcpClient::result() const {
  if (!implementation_ ||
      implementation_->client_state != RaftReadAuthorityTcpClientState::kComplete) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft read-authority TCP result is unavailable"));
  }
  const auto* carrier = implementation_->carrier.transform([](const auto& value) { return &value; })
                            .value_or(nullptr);
  if (carrier == nullptr) {
    return common::make_unexpected(
        status(common::StatusCode::kInternal, "Raft read-authority TCP carrier is unavailable"));
  }
  return carrier->result();
}

const common::Status& RaftReadAuthorityTcpClient::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "Raft read-authority TCP client is empty"};
  return implementation_ ? implementation_->client_failure : empty;
}

} // namespace chronos::cluster
