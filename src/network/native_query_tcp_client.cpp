#include "chronos/network/native_query_tcp_client.hpp"

#include <algorithm>
#include <chrono>
#include <memory>
#include <new>
#include <utility>

namespace chronos::network {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}

template <typename Value>
[[nodiscard]] Value* optional_pointer(std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      NativeQueryTcpClient::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] NativeQueryTcpClient::TimePoint
deadline_after(const NativeQueryTcpClient::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration =
      std::chrono::duration_cast<NativeQueryTcpClient::TimePoint::duration>(timeout);
  return now > NativeQueryTcpClient::TimePoint::max() - duration
             ? NativeQueryTcpClient::TimePoint::max()
             : now + duration;
}

} // namespace

class NativeQueryTcpClient::Impl {
public:
  Impl(TcpSocket owned_socket, NativeQueryRetry owned_retry,
       std::vector<std::byte> owned_read_buffer, ConnectionAuthenticator& owned_authenticator,
       const NativeNodePrincipalAuthorizer& owned_node_authorizer,
       const NativeQueryTcpClientLimits configured_limits, const TimePoint now) noexcept
      : socket(std::move(owned_socket)), retry(std::move(owned_retry)),
        read_buffer(std::move(owned_read_buffer)), authenticator(owned_authenticator),
        node_authorizer(owned_node_authorizer), limits(configured_limits),
        active_deadline(deadline_after(now, limits.connect_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (client_state != NativeQueryTcpClientState::kFailed) {
      tls.reset();
      static_cast<void>(socket.close());
      client_failure = std::move(failure);
      client_state = NativeQueryTcpClientState::kFailed;
      readiness = {};
    }
    return client_failure;
  }

  void complete() {
    tls.reset();
    static_cast<void>(socket.close());
    client_state = NativeQueryTcpClientState::kComplete;
    readiness = {};
  }

  [[nodiscard]] common::Status authenticate_server(const TimePoint now) {
    TlsSocket* active_tls = optional_pointer(tls);
    if (active_tls == nullptr)
      return fail(status(common::StatusCode::kInternal, "native query TLS session is missing"));
    auto fingerprint = active_tls->peer_certificate_sha256();
    if (!fingerprint.has_value())
      return fail(fingerprint.error());
    const NativeLeaderRoute route = retry.current_route();
    const NetworkSecurityConfig security{.mode = TransportSecurityMode::kTlsRequired,
                                         .authenticator = &authenticator};
    auto authentication = authenticate_peer(security, {.ipv4_address = route.endpoint.address,
                                                       .transport_authenticated = true,
                                                       .peer_certificate_sha256 = *fingerprint});
    if (!authentication.has_value())
      return fail(authentication.error());
    if (!authentication->authorized || authentication->principal_id == 0U)
      return fail(status(common::StatusCode::kUnauthenticated,
                         "native query server principal is not authenticated"));
    auto authorized = node_authorizer.authorize_node(authentication->principal_id, route.node_id);
    if (!authorized.has_value())
      return fail(authorized.error());
    if (!*authorized)
      return fail(status(common::StatusCode::kUnauthenticated,
                         "native query server principal cannot claim the route node"));
    client_state = NativeQueryTcpClientState::kExchanging;
    active_deadline = deadline_after(now, limits.exchange_timeout);
    readiness = {.want_write = true};
    return common::Status::ok();
  }

  [[nodiscard]] common::Status begin_redirect_connect(const TimePoint now) {
    tls.reset();
    static_cast<void>(socket.close());
    auto next = TcpSocket::begin_connect(retry.current_route().endpoint);
    if (!next.has_value())
      return fail(next.error());
    socket = std::move(*next);
    client_state = NativeQueryTcpClientState::kConnecting;
    active_deadline = deadline_after(now, limits.connect_timeout);
    readiness = {.want_write = true};
    return common::Status::ok();
  }

  [[nodiscard]] common::Status advance_connect(const bool writable, const TimePoint now) {
    if (!writable)
      return common::Status::ok();
    auto connected = socket.finish_connect();
    if (!connected.has_value())
      return fail(connected.error());
    if (*connected == TcpConnectState::kInProgress)
      return common::Status::ok();
    const NativeLeaderRoute route = retry.current_route();
    auto connected_tls = TlsSocket::connect(*route.tls_context, socket.descriptor());
    if (!connected_tls.has_value())
      return fail(connected_tls.error());
    tls.emplace(std::move(*connected_tls));
    client_state = NativeQueryTcpClientState::kHandshaking;
    active_deadline = deadline_after(now, limits.handshake_timeout);
    readiness = {.want_write = true};
    return common::Status::ok();
  }

  [[nodiscard]] common::Status advance_handshake(const bool readable, const bool writable,
                                                 const TimePoint now) {
    if ((!readiness.want_read || !readable) && (!readiness.want_write || !writable))
      return common::Status::ok();
    TlsSocket* active_tls = optional_pointer(tls);
    if (active_tls == nullptr)
      return fail(status(common::StatusCode::kInternal, "native query TLS session is missing"));
    auto progress = active_tls->handshake();
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == TlsIoState::kClosed)
      return fail(status(common::StatusCode::kUnavailable, "native query TLS handshake closed"));
    if (progress->state == TlsIoState::kWantRead) {
      readiness = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == TlsIoState::kWantWrite) {
      readiness = {.want_write = true};
      return common::Status::ok();
    }
    return authenticate_server(now);
  }

  [[nodiscard]] common::Status advance_write(const bool readable, const bool writable) {
    if ((!readiness.want_read || !readable) && (!readiness.want_write || !writable))
      return common::Status::ok();
    const common::ByteView pending = retry.pending_write();
    if (pending.empty()) {
      readiness = {.want_read = true};
      return common::Status::ok();
    }
    const common::ByteView chunk = pending.first(std::min(pending.size(), read_buffer.size()));
    TlsSocket* active_tls = optional_pointer(tls);
    if (active_tls == nullptr)
      return fail(status(common::StatusCode::kInternal, "native query TLS session is missing"));
    auto progress = active_tls->write(chunk);
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == TlsIoState::kClosed)
      return fail(status(common::StatusCode::kUnavailable, "native query request socket closed"));
    if (progress->state == TlsIoState::kWantRead) {
      readiness = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == TlsIoState::kWantWrite) {
      readiness = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(
          status(common::StatusCode::kUnavailable, "native query request write made no progress"));
    const common::Status consumed = retry.consume_written(progress->bytes_transferred);
    if (!consumed.is_ok())
      return fail(consumed);
    readiness = retry.pending_write().empty() ? NativeQueryTcpInterest{.want_read = true}
                                              : NativeQueryTcpInterest{.want_write = true};
    return common::Status::ok();
  }

  [[nodiscard]] common::Status advance_read(const bool readable, const bool writable,
                                            const TimePoint now) {
    TlsSocket* active_tls = optional_pointer(tls);
    if (active_tls == nullptr)
      return fail(status(common::StatusCode::kInternal, "native query TLS session is missing"));
    if ((!readiness.want_read || (!readable && active_tls->pending_plaintext_bytes() == 0U)) &&
        (!readiness.want_write || !writable)) {
      return common::Status::ok();
    }
    auto progress = active_tls->read(read_buffer);
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == TlsIoState::kClosed)
      return fail(status(common::StatusCode::kUnavailable, "native query response socket closed"));
    if (progress->state == TlsIoState::kWantRead) {
      readiness = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == TlsIoState::kWantWrite) {
      readiness = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(
          status(common::StatusCode::kUnavailable, "native query response read made no progress"));
    auto received = retry.receive(common::ByteView{read_buffer}.first(progress->bytes_transferred));
    if (!received.has_value())
      return fail(received.error());
    if (retry.state() == NativeQueryRetryState::kComplete) {
      complete();
      return common::Status::ok();
    }
    if (received->reconnect_required)
      return begin_redirect_connect(now);
    readiness = retry.pending_write().empty() ? NativeQueryTcpInterest{.want_read = true}
                                              : NativeQueryTcpInterest{.want_write = true};
    return common::Status::ok();
  }

  // Socket is declared before TLS so reverse destruction releases the borrowed descriptor last.
  TcpSocket socket;
  std::optional<TlsSocket> tls;
  NativeQueryRetry retry;
  std::vector<std::byte> read_buffer;
  ConnectionAuthenticator& authenticator;
  const NativeNodePrincipalAuthorizer& node_authorizer;
  NativeQueryTcpClientLimits limits;
  TimePoint active_deadline;
  NativeQueryTcpClientState client_state{NativeQueryTcpClientState::kConnecting};
  NativeQueryTcpInterest readiness{.want_write = true};
  common::Status client_failure{common::StatusCode::kInternal,
                                "native query TCP client has not failed"};
};

NativeQueryTcpClient::NativeQueryTcpClient(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
NativeQueryTcpClient::~NativeQueryTcpClient() = default;
NativeQueryTcpClient::NativeQueryTcpClient(NativeQueryTcpClient&&) noexcept = default;
NativeQueryTcpClient& NativeQueryTcpClient::operator=(NativeQueryTcpClient&&) noexcept = default;

common::Result<NativeQueryTcpClient> NativeQueryTcpClient::begin(NativeQueryTcpClientConfig config,
                                                                 std::string sql,
                                                                 const TimePoint now) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr ||
      !valid_timeout(config.limits.connect_timeout) ||
      !valid_timeout(config.limits.handshake_timeout) ||
      !valid_timeout(config.limits.exchange_timeout) ||
      config.limits.maximum_io_chunk_bytes == 0U ||
      config.limits.maximum_io_chunk_bytes > config.retry.buffers.maximum_inbound_buffer_bytes) {
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "native query TCP configuration is invalid"));
  }
  auto retry = NativeQueryRetry::create(std::move(config.retry), std::move(sql));
  if (!retry.has_value())
    return common::make_unexpected(retry.error());
  try {
    std::vector<std::byte> read_buffer(config.limits.maximum_io_chunk_bytes);
    auto socket = TcpSocket::begin_connect(retry->current_route().endpoint);
    if (!socket.has_value())
      return common::make_unexpected(socket.error());
    return NativeQueryTcpClient{
        std::make_unique<Impl>(std::move(*socket), std::move(*retry), std::move(read_buffer),
                               *config.authenticator, *config.node_authorizer, config.limits, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted, "native query TCP allocation failed"));
  }
}

common::Status NativeQueryTcpClient::on_ready(const bool readable, const bool writable,
                                              const TimePoint now) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "native query TCP client is empty");
  Impl& impl = *implementation_;
  if (impl.client_state == NativeQueryTcpClientState::kFailed)
    return impl.client_failure;
  if (impl.client_state == NativeQueryTcpClientState::kComplete)
    return common::Status::ok();
  if (now >= impl.active_deadline) {
    const char* message = impl.client_state == NativeQueryTcpClientState::kConnecting
                              ? "native query TCP connect timed out"
                          : impl.client_state == NativeQueryTcpClientState::kHandshaking
                              ? "native query TLS handshake timed out"
                              : "native query exchange timed out";
    return impl.fail(status(common::StatusCode::kUnavailable, message));
  }
  if (impl.client_state == NativeQueryTcpClientState::kConnecting)
    return impl.advance_connect(writable, now);
  if (impl.client_state == NativeQueryTcpClientState::kHandshaking)
    return impl.advance_handshake(readable, writable, now);
  return impl.retry.pending_write().empty() ? impl.advance_read(readable, writable, now)
                                            : impl.advance_write(readable, writable);
}

NativeQueryTcpClientState NativeQueryTcpClient::state() const noexcept {
  return implementation_ ? implementation_->client_state : NativeQueryTcpClientState::kFailed;
}

NativeQueryTcpInterest NativeQueryTcpClient::interest() const noexcept {
  return implementation_ ? implementation_->readiness : NativeQueryTcpInterest{};
}

int NativeQueryTcpClient::descriptor() const noexcept {
  return implementation_ ? implementation_->socket.descriptor() : -1;
}

std::optional<NativeQueryTcpClient::TimePoint> NativeQueryTcpClient::deadline() const noexcept {
  if (!implementation_ || implementation_->client_state == NativeQueryTcpClientState::kFailed ||
      implementation_->client_state == NativeQueryTcpClientState::kComplete) {
    return std::nullopt;
  }
  return implementation_->active_deadline;
}

NativeLeaderRoute NativeQueryTcpClient::current_route() const noexcept {
  return implementation_ ? implementation_->retry.current_route() : NativeLeaderRoute{};
}

std::size_t NativeQueryTcpClient::attempts_started() const noexcept {
  return implementation_ ? implementation_->retry.attempts_started() : 0U;
}

const std::optional<NativeQueryResult>& NativeQueryTcpClient::result() const noexcept {
  static const std::optional<NativeQueryResult> empty;
  if (!implementation_ || implementation_->client_state != NativeQueryTcpClientState::kComplete)
    return empty;
  return implementation_->retry.result();
}

const common::Status& NativeQueryTcpClient::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "native query TCP client is empty"};
  return implementation_ ? implementation_->client_failure : empty;
}

} // namespace chronos::network
