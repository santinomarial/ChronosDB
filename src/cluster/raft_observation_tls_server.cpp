#include "chronos/cluster/raft_observation_tls_server.hpp"

#include <array>
#include <chrono>
#include <cstddef>
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
      RaftObservationTlsServer::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] RaftObservationTlsServer::TimePoint
deadline_after(const RaftObservationTlsServer::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration =
      std::chrono::duration_cast<RaftObservationTlsServer::TimePoint::duration>(timeout);
  return now > RaftObservationTlsServer::TimePoint::max() - duration
             ? RaftObservationTlsServer::TimePoint::max()
             : now + duration;
}

} // namespace

class RaftObservationTlsServer::Impl {
public:
  Impl(network::TlsSocket socket, RaftObservationTlsServerConfig config,
       const TimePoint now) noexcept
      : socket_(std::move(socket)), config_(config),
        deadline_(deadline_after(now, config.limits.handshake_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (state_ != RaftObservationTlsServerState::kFailed) {
      failure_ = std::move(failure);
      state_ = RaftObservationTlsServerState::kFailed;
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
    if (!authenticated->authorized || authenticated->principal_id == 0U)
      return fail(status(common::StatusCode::kUnauthenticated,
                         "Raft observation client principal is not authenticated"));
    authenticated_peer_ = *authenticated;
    state_ = RaftObservationTlsServerState::kReadingRequest;
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
    if (progress->state == network::TlsIoState::kClosed)
      return fail(
          status(common::StatusCode::kUnavailable, "Raft observation TLS handshake closed"));
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
        (!interest_.want_write || !writable))
      return common::Status::ok();
    if (request_size_ == request_bytes_.size())
      return fail(status(common::StatusCode::kCorruption,
                         "Raft observation request exceeds its fixed bound"));
    auto progress = socket_.read(common::MutableByteView{request_bytes_}.subspan(request_size_));
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(
          status(common::StatusCode::kUnavailable, "Raft observation request socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(status(common::StatusCode::kUnavailable,
                         "Raft observation request read made no progress"));
    const common::ByteView received =
        common::ByteView{request_bytes_}.subspan(request_size_, progress->bytes_transferred);
    auto step = request_reader_.consume(received);
    if (!step.has_value())
      return fail(step.error());
    request_size_ += progress->bytes_transferred;
    if (step->consumed_bytes != progress->bytes_transferred)
      return fail(status(common::StatusCode::kCorruption,
                         "Raft observation request has a coalesced suffix"));
    if (!step->request.has_value()) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    auto response = config_.receiver->receive(common::ByteView{request_bytes_}.first(request_size_),
                                              *authenticated_peer_);
    if (!response.has_value())
      return fail(response.error());
    auto writer =
        RaftObservationFrameWriteCursor::create(std::move(*response), config_.limits.transport);
    if (!writer.has_value())
      return fail(writer.error());
    response_writer_.emplace(std::move(*writer));
    state_ = RaftObservationTlsServerState::kWritingResponse;
    interest_ = {.want_write = true};
    return common::Status::ok();
  }

  [[nodiscard]] common::Status write(const bool readable, const bool writable) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    auto* writer = response_writer_.transform([](auto& value) { return &value; }).value_or(nullptr);
    if (writer == nullptr)
      return fail(
          status(common::StatusCode::kInternal, "Raft observation response writer is unavailable"));
    auto progress = socket_.write(writer->pending_write());
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(
          status(common::StatusCode::kUnavailable, "Raft observation response socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(status(common::StatusCode::kUnavailable,
                         "Raft observation response write made no progress"));
    const common::Status consumed = writer->consume_written(progress->bytes_transferred);
    if (!consumed.is_ok())
      return fail(consumed);
    if (writer->complete()) {
      state_ = RaftObservationTlsServerState::kComplete;
      interest_ = {};
    } else {
      interest_ = {.want_write = true};
    }
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  RaftObservationTlsServerConfig config_;
  TimePoint deadline_;
  RaftObservationTlsServerState state_{RaftObservationTlsServerState::kHandshaking};
  RaftObservationTlsServerInterest interest_{.want_read = true};
  std::optional<network::PeerAuthenticationResult> authenticated_peer_;
  RaftObservationRequestReader request_reader_;
  // One extra frame of scratch permits an immediately coalesced second request to be detected and
  // rejected by exact consumed-prefix accounting; it is never dispatched.
  std::array<std::byte, 2U * kRaftObservationRequestSize> request_bytes_{};
  std::size_t request_size_{};
  std::optional<RaftObservationFrameWriteCursor> response_writer_;
  common::Status failure_{common::StatusCode::kInternal,
                          "Raft observation TLS server has not failed"};
};

RaftObservationTlsServer::RaftObservationTlsServer(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftObservationTlsServer::~RaftObservationTlsServer() = default;
RaftObservationTlsServer::RaftObservationTlsServer(RaftObservationTlsServer&&) noexcept = default;
RaftObservationTlsServer&
RaftObservationTlsServer::operator=(RaftObservationTlsServer&&) noexcept = default;

common::Result<RaftObservationTlsServer>
RaftObservationTlsServer::create(network::TlsSocket socket,
                                 const RaftObservationTlsServerConfig config, const TimePoint now) {
  if (config.authenticator == nullptr || config.receiver == nullptr ||
      !valid_timeout(config.limits.handshake_timeout) ||
      !valid_timeout(config.limits.exchange_timeout)) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft observation TLS server configuration is invalid"));
  }
  auto limits = RaftObservationResponseReader::create(config.limits.transport);
  if (!limits.has_value())
    return common::make_unexpected(limits.error());
  try {
    return RaftObservationTlsServer{std::make_unique<Impl>(std::move(socket), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft observation TLS server allocation failed"));
  }
}

common::Status RaftObservationTlsServer::on_ready(const bool readable, const bool writable,
                                                  const TimePoint now) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft observation TLS server is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == RaftObservationTlsServerState::kFailed)
    return impl.failure_;
  if (impl.state_ == RaftObservationTlsServerState::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_)
    return impl.fail(status(common::StatusCode::kUnavailable,
                            impl.state_ == RaftObservationTlsServerState::kHandshaking
                                ? "Raft observation TLS handshake timed out"
                                : "Raft observation TLS exchange timed out"));
  if (impl.state_ == RaftObservationTlsServerState::kHandshaking)
    return impl.handshake(readable, writable, now);
  if (impl.state_ == RaftObservationTlsServerState::kReadingRequest)
    return impl.read(readable, writable);
  return impl.write(readable, writable);
}

RaftObservationTlsServerState RaftObservationTlsServer::state() const noexcept {
  return implementation_ ? implementation_->state_ : RaftObservationTlsServerState::kFailed;
}

RaftObservationTlsServerInterest RaftObservationTlsServer::interest() const noexcept {
  return implementation_ ? implementation_->interest_ : RaftObservationTlsServerInterest{};
}

const common::Status& RaftObservationTlsServer::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "Raft observation TLS server is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::cluster
