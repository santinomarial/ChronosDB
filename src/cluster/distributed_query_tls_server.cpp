#include "chronos/cluster/distributed_query_tls_server.hpp"

#include <array>
#include <chrono>
#include <cstddef>
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

[[nodiscard]] common::Status unauthenticated(const char* message) {
  return {common::StatusCode::kUnauthenticated, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status internal(const char* message) {
  return {common::StatusCode::kInternal, message};
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      DistributedQueryTlsServer::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] DistributedQueryTlsServer::TimePoint
deadline_after(const DistributedQueryTlsServer::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration =
      std::chrono::duration_cast<DistributedQueryTlsServer::TimePoint::duration>(timeout);
  if (now > DistributedQueryTlsServer::TimePoint::max() - duration)
    return DistributedQueryTlsServer::TimePoint::max();
  return now + duration;
}

} // namespace

class DistributedQueryTlsServer::Impl {
public:
  Impl(network::TlsSocket socket, DistributedQueryTlsServerConfig config, const TimePoint now)
      : socket_(std::move(socket)), config_(config),
        deadline_(deadline_after(now, config.limits.handshake_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ != DistributedQueryTlsServerState::kFailed) {
      failure_ = std::move(status);
      state_ = DistributedQueryTlsServerState::kFailed;
      interest_ = {};
    }
    return failure_;
  }

  [[nodiscard]] common::Status authenticate_client(const TimePoint now) {
    auto fingerprint = socket_.peer_certificate_sha256();
    if (!fingerprint.has_value())
      return fail(fingerprint.error());
    const network::NetworkSecurityConfig security{.mode =
                                                      network::TransportSecurityMode::kTlsRequired,
                                                  .authenticator = config_.authenticator};
    auto authentication =
        network::authenticate_peer(security, {.ipv4_address = config_.peer_ipv4_address,
                                              .transport_authenticated = true,
                                              .peer_certificate_sha256 = *fingerprint});
    if (!authentication.has_value())
      return fail(authentication.error());
    if (!authentication->authorized || authentication->principal_id == 0U)
      return fail(unauthenticated("distributed query client principal is not authenticated"));
    authenticated_peer_ = *authentication;
    state_ = DistributedQueryTlsServerState::kReadingRequest;
    interest_ = {.want_read = true};
    deadline_ = deadline_after(now, config_.limits.exchange_timeout);
    return common::Status::ok();
  }

  [[nodiscard]] common::Status advance_handshake(const bool readable, const bool writable,
                                                 const TimePoint now) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    auto progress = socket_.handshake();
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("distributed query TLS handshake closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    return authenticate_client(now);
  }

  [[nodiscard]] common::Status advance_read(const bool readable, const bool writable) {
    if ((!interest_.want_read || (!readable && socket_.pending_plaintext_bytes() == 0U)) &&
        (!interest_.want_write || !writable)) {
      return common::Status::ok();
    }
    if (request_size_ == request_bytes_.size())
      return fail(corruption("distributed query request exceeds its fixed carrier bound"));
    auto progress = socket_.read(common::MutableByteView{request_bytes_}.subspan(request_size_));
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("distributed query request socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("distributed query request read made no progress"));
    const common::ByteView received =
        common::ByteView{request_bytes_}.subspan(request_size_, progress->bytes_transferred);
    auto step = request_reader_.consume(received);
    if (!step.has_value())
      return fail(step.error());
    request_size_ += progress->bytes_transferred;
    if (step->consumed_bytes != progress->bytes_transferred)
      return fail(corruption("distributed query TLS request has a coalesced suffix"));
    if (!step->request.has_value()) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    auto response = config_.receiver->receive(common::ByteView{request_bytes_}.first(request_size_),
                                              *authenticated_peer_);
    if (!response.has_value())
      return fail(response.error());
    auto writer = DistributedQueryFrameWriteCursor::create(std::move(*response));
    if (!writer.has_value())
      return fail(writer.error());
    response_writer_.emplace(std::move(*writer));
    state_ = DistributedQueryTlsServerState::kWritingResponse;
    interest_ = {.want_write = true};
    return common::Status::ok();
  }

  [[nodiscard]] common::Status advance_write(const bool readable, const bool writable) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    DistributedQueryFrameWriteCursor* const response_writer =
        response_writer_.transform([](DistributedQueryFrameWriteCursor& writer) { return &writer; })
            .value_or(nullptr);
    if (response_writer == nullptr)
      return fail(internal("distributed query TLS response writer is missing"));
    auto progress = socket_.write(response_writer->pending_write());
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("distributed query response socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("distributed query response write made no progress"));
    const common::Status consumed = response_writer->consume_written(progress->bytes_transferred);
    if (!consumed.is_ok())
      return fail(consumed);
    if (response_writer->complete()) {
      state_ = DistributedQueryTlsServerState::kComplete;
      interest_ = {};
    } else {
      interest_ = {.want_write = true};
    }
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  DistributedQueryTlsServerConfig config_;
  TimePoint deadline_;
  DistributedQueryTlsServerState state_{DistributedQueryTlsServerState::kHandshaking};
  DistributedQueryTlsServerInterest interest_{.want_read = true};
  std::optional<network::PeerAuthenticationResult> authenticated_peer_;
  DistributedQueryRequestReader request_reader_;
  std::array<std::byte, kMaximumDistributedQueryRequestSize> request_bytes_{};
  std::size_t request_size_{};
  std::optional<DistributedQueryFrameWriteCursor> response_writer_;
  common::Status failure_{common::StatusCode::kInternal,
                          "distributed query TLS server has not failed"};
};

DistributedQueryTlsServer::DistributedQueryTlsServer(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedQueryTlsServer::~DistributedQueryTlsServer() = default;
DistributedQueryTlsServer::DistributedQueryTlsServer(DistributedQueryTlsServer&&) noexcept =
    default;
DistributedQueryTlsServer&
DistributedQueryTlsServer::operator=(DistributedQueryTlsServer&&) noexcept = default;

common::Result<DistributedQueryTlsServer> DistributedQueryTlsServer::create(
    network::TlsSocket socket, const DistributedQueryTlsServerConfig config, const TimePoint now) {
  if (config.authenticator == nullptr || config.receiver == nullptr ||
      !valid_timeout(config.limits.handshake_timeout) ||
      !valid_timeout(config.limits.exchange_timeout)) {
    return common::make_unexpected(
        invalid("distributed query TLS server configuration is invalid"));
  }
  try {
    return DistributedQueryTlsServer{std::make_unique<Impl>(std::move(socket), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed query TLS server allocation failed"));
  }
}

common::Status DistributedQueryTlsServer::on_ready(const bool readable, const bool writable,
                                                   const TimePoint now) {
  if (!implementation_)
    return invalid("distributed query TLS server is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedQueryTlsServerState::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedQueryTlsServerState::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_)
    return impl.fail(unavailable(impl.state_ == DistributedQueryTlsServerState::kHandshaking
                                     ? "distributed query TLS handshake timed out"
                                     : "distributed query TLS exchange timed out"));
  if (impl.state_ == DistributedQueryTlsServerState::kHandshaking)
    return impl.advance_handshake(readable, writable, now);
  if (impl.state_ == DistributedQueryTlsServerState::kReadingRequest)
    return impl.advance_read(readable, writable);
  return impl.advance_write(readable, writable);
}

DistributedQueryTlsServerState DistributedQueryTlsServer::state() const noexcept {
  return implementation_ ? implementation_->state_ : DistributedQueryTlsServerState::kFailed;
}

DistributedQueryTlsServerInterest DistributedQueryTlsServer::interest() const noexcept {
  return implementation_ ? implementation_->interest_ : DistributedQueryTlsServerInterest{};
}

const common::Status& DistributedQueryTlsServer::failure() const noexcept {
  static const common::Status empty_failure{common::StatusCode::kInvalidArgument,
                                            "distributed query TLS server is empty"};
  return implementation_ ? implementation_->failure_ : empty_failure;
}

} // namespace chronos::cluster
