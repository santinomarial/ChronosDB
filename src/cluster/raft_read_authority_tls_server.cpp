#include "chronos/cluster/raft_read_authority_tls_server.hpp"

#include <array>
#include <chrono>
#include <cstddef>
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
      RaftReadAuthorityTlsServer::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] RaftReadAuthorityTlsServer::TimePoint
deadline_after(const RaftReadAuthorityTlsServer::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration =
      std::chrono::duration_cast<RaftReadAuthorityTlsServer::TimePoint::duration>(timeout);
  return now > RaftReadAuthorityTlsServer::TimePoint::max() - duration
             ? RaftReadAuthorityTlsServer::TimePoint::max()
             : now + duration;
}

} // namespace

class RaftReadAuthorityTlsServer::Impl {
public:
  Impl(network::TlsSocket socket, RaftReadAuthorityTlsServerConfig config,
       const TimePoint now) noexcept
      : socket_(std::move(socket)), config_(config),
        deadline_(deadline_after(now, config.limits.handshake_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (state_ != RaftReadAuthorityTlsServerState::kFailed) {
      failure_ = std::move(failure);
      state_ = RaftReadAuthorityTlsServerState::kFailed;
      interest_ = {};
    }
    return failure_;
  }

  [[nodiscard]] common::Status authenticate(const TimePoint now) {
    auto fingerprint = socket_.peer_certificate_sha256();
    if (!fingerprint.has_value())
      return fail(fingerprint.error());
    const network::NetworkSecurityConfig security{.mode =
                                                      network::TransportSecurityMode::kTlsRequired,
                                                  .authenticator = config_.authenticator};
    auto authenticated =
        network::authenticate_peer(security, {.ipv4_address = config_.peer_ipv4_address,
                                              .transport_authenticated = true,
                                              .peer_certificate_sha256 = *fingerprint});
    if (!authenticated.has_value())
      return fail(authenticated.error());
    if (!authenticated->authorized || authenticated->principal_id == 0U) {
      return fail(status(common::StatusCode::kUnauthenticated,
                         "Raft read-authority client principal is not authenticated"));
    }
    authenticated_peer_ = *authenticated;
    state_ = RaftReadAuthorityTlsServerState::kReadingRequest;
    interest_ = {.want_read = true};
    deadline_ = deadline_after(now, config_.limits.exchange_timeout);
    return common::Status::ok();
  }

  [[nodiscard]] common::Status handshake(const bool readable, const bool writable,
                                         const TimePoint now) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    auto progress = socket_.handshake();
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed) {
      return fail(
          status(common::StatusCode::kUnavailable, "Raft read-authority TLS handshake closed"));
    }
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    return authenticate(now);
  }

  [[nodiscard]] common::Status read(const bool readable, const bool writable) {
    if ((!interest_.want_read || (!readable && socket_.pending_plaintext_bytes() == 0U)) &&
        (!interest_.want_write || !writable)) {
      return common::Status::ok();
    }
    if (request_size_ == request_bytes_.size()) {
      return fail(status(common::StatusCode::kCorruption,
                         "Raft read-authority request exceeds its fixed bound"));
    }
    auto progress = socket_.read(common::MutableByteView{request_bytes_}.subspan(request_size_));
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed) {
      return fail(
          status(common::StatusCode::kUnavailable, "Raft read-authority request socket closed"));
    }
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U) {
      return fail(status(common::StatusCode::kUnavailable,
                         "Raft read-authority request read made no progress"));
    }
    const common::ByteView received =
        common::ByteView{request_bytes_}.subspan(request_size_, progress->bytes_transferred);
    auto step = request_reader_.consume(received);
    if (!step.has_value())
      return fail(step.error());
    request_size_ += progress->bytes_transferred;
    if (step->consumed_bytes != progress->bytes_transferred) {
      return fail(status(common::StatusCode::kCorruption,
                         "Raft read-authority request has a coalesced suffix"));
    }
    if (!step->request.has_value()) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    auto response = config_.receiver->receive(common::ByteView{request_bytes_}.first(request_size_),
                                              *authenticated_peer_);
    if (!response.has_value())
      return fail(response.error());
    auto writer =
        RaftReadAuthorityFrameWriteCursor::create(std::move(*response), config_.limits.transport);
    if (!writer.has_value())
      return fail(writer.error());
    response_writer_.emplace(std::move(*writer));
    state_ = RaftReadAuthorityTlsServerState::kWritingResponse;
    interest_ = {.want_write = true};
    return common::Status::ok();
  }

  [[nodiscard]] common::Status write(const bool readable, const bool writable) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    auto* writer = response_writer_.transform([](auto& value) { return &value; }).value_or(nullptr);
    if (writer == nullptr) {
      return fail(status(common::StatusCode::kInternal,
                         "Raft read-authority response writer is unavailable"));
    }
    auto progress = socket_.write(writer->pending_write());
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed) {
      return fail(
          status(common::StatusCode::kUnavailable, "Raft read-authority response socket closed"));
    }
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U) {
      return fail(status(common::StatusCode::kUnavailable,
                         "Raft read-authority response write made no progress"));
    }
    const common::Status consumed = writer->consume_written(progress->bytes_transferred);
    if (!consumed.is_ok())
      return fail(consumed);
    if (writer->complete()) {
      state_ = RaftReadAuthorityTlsServerState::kComplete;
      interest_ = {};
    } else {
      interest_ = {.want_write = true};
    }
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  RaftReadAuthorityTlsServerConfig config_;
  TimePoint deadline_;
  RaftReadAuthorityTlsServerState state_{RaftReadAuthorityTlsServerState::kHandshaking};
  RaftReadAuthorityTlsServerInterest interest_{.want_read = true};
  std::optional<network::PeerAuthenticationResult> authenticated_peer_;
  RaftReadAuthorityRequestReader request_reader_;
  std::array<std::byte, 2U * kRaftReadAuthorityRequestSize> request_bytes_{};
  std::size_t request_size_{};
  std::optional<RaftReadAuthorityFrameWriteCursor> response_writer_;
  common::Status failure_{common::StatusCode::kInternal,
                          "Raft read-authority TLS server has not failed"};
};

RaftReadAuthorityTlsServer::RaftReadAuthorityTlsServer(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftReadAuthorityTlsServer::~RaftReadAuthorityTlsServer() = default;
RaftReadAuthorityTlsServer::RaftReadAuthorityTlsServer(RaftReadAuthorityTlsServer&&) noexcept =
    default;
RaftReadAuthorityTlsServer&
RaftReadAuthorityTlsServer::operator=(RaftReadAuthorityTlsServer&&) noexcept = default;

common::Result<RaftReadAuthorityTlsServer> RaftReadAuthorityTlsServer::create(
    network::TlsSocket socket, const RaftReadAuthorityTlsServerConfig config, const TimePoint now) {
  if (config.authenticator == nullptr || config.receiver == nullptr ||
      !valid_timeout(config.limits.handshake_timeout) ||
      !valid_timeout(config.limits.exchange_timeout)) {
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument,
               "Raft read-authority TLS server configuration is invalid"));
  }
  auto limits = RaftReadAuthorityResponseReader::create(config.limits.transport);
  if (!limits.has_value())
    return common::make_unexpected(limits.error());
  try {
    return RaftReadAuthorityTlsServer{std::make_unique<Impl>(std::move(socket), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft read-authority TLS server allocation failed"));
  }
}

common::Status RaftReadAuthorityTlsServer::on_ready(const bool readable, const bool writable,
                                                    const TimePoint now) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft read-authority TLS server is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == RaftReadAuthorityTlsServerState::kFailed)
    return impl.failure_;
  if (impl.state_ == RaftReadAuthorityTlsServerState::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_) {
    return impl.fail(status(common::StatusCode::kUnavailable,
                            impl.state_ == RaftReadAuthorityTlsServerState::kHandshaking
                                ? "Raft read-authority TLS handshake timed out"
                                : "Raft read-authority TLS exchange timed out"));
  }
  if (impl.state_ == RaftReadAuthorityTlsServerState::kHandshaking)
    return impl.handshake(readable, writable, now);
  if (impl.state_ == RaftReadAuthorityTlsServerState::kReadingRequest)
    return impl.read(readable, writable);
  return impl.write(readable, writable);
}

RaftReadAuthorityTlsServerState RaftReadAuthorityTlsServer::state() const noexcept {
  return implementation_ ? implementation_->state_ : RaftReadAuthorityTlsServerState::kFailed;
}

RaftReadAuthorityTlsServerInterest RaftReadAuthorityTlsServer::interest() const noexcept {
  return implementation_ ? implementation_->interest_ : RaftReadAuthorityTlsServerInterest{};
}

const common::Status& RaftReadAuthorityTlsServer::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "Raft read-authority TLS server is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::cluster
