#include "chronos/cluster/raft_transport_tls_server.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <new>
#include <optional>
#include <utility>

namespace chronos::cluster {
namespace {

inline constexpr std::size_t kReadScratchBytes = 16U * 1024U;

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

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      RaftTransportTlsServer::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] RaftTransportTlsServer::TimePoint
deadline_after(const RaftTransportTlsServer::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration =
      std::chrono::duration_cast<RaftTransportTlsServer::TimePoint::duration>(timeout);
  if (now > RaftTransportTlsServer::TimePoint::max() - duration)
    return RaftTransportTlsServer::TimePoint::max();
  return now + duration;
}

} // namespace

class RaftTransportTlsServer::Impl {
public:
  Impl(network::TlsSocket socket, RaftTransportTlsServerConfig config,
       raft::RaftTransportFrameReader reader, const TimePoint now)
      : socket_(std::move(socket)), config_(config), reader_(std::move(reader)),
        deadline_(deadline_after(now, config.limits.handshake_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) noexcept {
    if (state_ != RaftTransportTlsServerState::kFailed) {
      failure_ = std::move(status);
      state_ = RaftTransportTlsServerState::kFailed;
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
      return fail(unauthenticated("Raft TLS peer principal is not authenticated"));
    authenticated_peer_ = *authentication;
    state_ = RaftTransportTlsServerState::kReadingFrame;
    interest_ = {.want_read = true};
    deadline_ = deadline_after(now, config_.limits.frame_read_timeout);
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
      return fail(unavailable("Raft TLS handshake closed"));
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

  [[nodiscard]] std::size_t next_read_limit() const noexcept {
    const std::size_t buffered = reader_.buffered_bytes();
    const std::size_t remaining = reader_.expected_frame_bytes().has_value()
                                      ? *reader_.expected_frame_bytes() - buffered
                                      : raft::kRaftTransportHeaderSize - buffered;
    return std::min(remaining, read_scratch_.size());
  }

  [[nodiscard]] common::Status advance_read(const bool readable, const bool writable) {
    if ((!interest_.want_read || (!readable && socket_.pending_plaintext_bytes() == 0U)) &&
        (!interest_.want_write || !writable))
      return common::Status::ok();
    const std::size_t limit = next_read_limit();
    if (limit == 0U)
      return fail(corruption("Raft TLS reader has no remaining bounded input"));
    auto progress = socket_.read(common::MutableByteView{read_scratch_}.first(limit));
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("Raft TLS input socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("Raft TLS input read made no progress"));
    auto step = reader_.consume(common::ByteView{read_scratch_}.first(progress->bytes_transferred));
    if (!step.has_value())
      return fail(step.error());
    if (step->consumed_bytes != progress->bytes_transferred)
      return fail(corruption("Raft TLS input crossed its exact frame boundary"));
    if (!step->envelope.has_value()) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    auto admission =
        config_.receiver->try_receive_decoded(std::move(*step->envelope), *authenticated_peer_);
    if (!admission.has_value())
      return fail(admission.error());
    admission_.emplace(std::move(*admission));
    state_ = RaftTransportTlsServerState::kAwaitingDurableResult;
    interest_ = {};
    return common::Status::ok();
  }

  [[nodiscard]] common::Status advance_completion() {
    if (!admission_->completion.is_ready())
      return common::Status::ok();
    auto result = admission_->completion.wait();
    if (!result.has_value())
      return fail(result.error());
    if (result->size() != 2U)
      return fail(corruption("Raft TLS receive completion has an invalid result count"));
    raft::DurableRaftResult& received = (*result)[0];
    raft::DurableRaftResult& observed = (*result)[1];
    if (observed.transition.has_value() ||
        (observed.status.is_ok() != observed.observation.has_value()) ||
        (observed.observation.has_value() &&
         observed.observation->group_id != admission_->group_id) ||
        (!observed.status.is_ok() && received.status.is_ok()))
      return fail(corruption("Raft TLS receive completion has an invalid group observation"));
    completed_.emplace(
        RaftTransportCompletedReceive{admission_->group_id, admission_->source_node_id,
                                      std::move(received), std::move(observed.observation)});
    admission_.reset();
    state_ = RaftTransportTlsServerState::kResultReady;
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  RaftTransportTlsServerConfig config_;
  raft::RaftTransportFrameReader reader_;
  std::array<std::byte, kReadScratchBytes> read_scratch_{};
  TimePoint deadline_{};
  RaftTransportTlsServerState state_{RaftTransportTlsServerState::kHandshaking};
  RaftTransportTlsServerInterest interest_{.want_read = true};
  std::optional<network::PeerAuthenticationResult> authenticated_peer_;
  std::optional<RaftTransportAdmission> admission_;
  std::optional<RaftTransportCompletedReceive> completed_;
  common::Status failure_{common::StatusCode::kInternal, "Raft TLS server has not failed"};
};

RaftTransportTlsServer::RaftTransportTlsServer(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftTransportTlsServer::~RaftTransportTlsServer() = default;
RaftTransportTlsServer::RaftTransportTlsServer(RaftTransportTlsServer&&) noexcept = default;
RaftTransportTlsServer&
RaftTransportTlsServer::operator=(RaftTransportTlsServer&&) noexcept = default;

common::Result<RaftTransportTlsServer>
RaftTransportTlsServer::create(network::TlsSocket socket, const RaftTransportTlsServerConfig config,
                               const TimePoint now) {
  if (config.authenticator == nullptr || config.receiver == nullptr ||
      !valid_timeout(config.limits.handshake_timeout) ||
      !valid_timeout(config.limits.frame_read_timeout))
    return common::make_unexpected(invalid("Raft TLS server configuration is invalid"));
  auto reader = raft::RaftTransportFrameReader::create(config.codec_limits);
  if (!reader.has_value())
    return common::make_unexpected(reader.error());
  try {
    return RaftTransportTlsServer{
        std::make_unique<Impl>(std::move(socket), config, std::move(*reader), now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft TLS server allocation failed"));
  }
}

common::Status RaftTransportTlsServer::on_ready(const bool readable, const bool writable,
                                                const TimePoint now) {
  if (!implementation_)
    return invalid("Raft TLS server is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == RaftTransportTlsServerState::kFailed)
    return impl.failure_;
  if (impl.state_ == RaftTransportTlsServerState::kResultReady)
    return common::Status::ok();
  if (impl.state_ == RaftTransportTlsServerState::kAwaitingDurableResult)
    return impl.advance_completion();
  if (now >= impl.deadline_)
    return impl.fail(unavailable(impl.state_ == RaftTransportTlsServerState::kHandshaking
                                     ? "Raft TLS handshake timed out"
                                     : "Raft TLS frame read timed out"));
  if (impl.state_ == RaftTransportTlsServerState::kHandshaking)
    return impl.advance_handshake(readable, writable, now);
  return impl.advance_read(readable, writable);
}

common::Result<RaftTransportCompletedReceive>
RaftTransportTlsServer::take_completed(const TimePoint now) {
  if (!implementation_)
    return common::make_unexpected(invalid("Raft TLS server is empty"));
  Impl& impl = *implementation_;
  if (impl.state_ == RaftTransportTlsServerState::kFailed)
    return common::make_unexpected(impl.failure_);
  if (impl.state_ != RaftTransportTlsServerState::kResultReady || !impl.completed_.has_value())
    return common::make_unexpected(unavailable("Raft TLS durable result is not ready"));
  RaftTransportCompletedReceive completed = std::move(*impl.completed_);
  impl.completed_.reset();
  impl.state_ = RaftTransportTlsServerState::kReadingFrame;
  impl.interest_ = {.want_read = true};
  impl.deadline_ = deadline_after(now, impl.config_.limits.frame_read_timeout);
  return completed;
}

RaftTransportTlsServerState RaftTransportTlsServer::state() const noexcept {
  return implementation_ ? implementation_->state_ : RaftTransportTlsServerState::kFailed;
}

RaftTransportTlsServerInterest RaftTransportTlsServer::interest() const noexcept {
  return implementation_ ? implementation_->interest_ : RaftTransportTlsServerInterest{};
}

const common::Status& RaftTransportTlsServer::failure() const noexcept {
  static const common::Status empty_failure{common::StatusCode::kInvalidArgument,
                                            "Raft TLS server is empty"};
  return implementation_ ? implementation_->failure_ : empty_failure;
}

} // namespace chronos::cluster
