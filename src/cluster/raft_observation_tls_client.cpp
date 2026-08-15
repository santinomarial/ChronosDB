#include "chronos/cluster/raft_observation_tls_client.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <new>
#include <optional>
#include <stdexcept>
#include <utility>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status status(common::StatusCode code, const char* message) {
  return {code, message};
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      RaftObservationTlsClient::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] RaftObservationTlsClient::TimePoint
deadline_after(const RaftObservationTlsClient::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration =
      std::chrono::duration_cast<RaftObservationTlsClient::TimePoint::duration>(timeout);
  return now > RaftObservationTlsClient::TimePoint::max() - duration
             ? RaftObservationTlsClient::TimePoint::max()
             : now + duration;
}

} // namespace

class RaftObservationTlsClient::Impl {
public:
  Impl(network::TlsSocket socket, RaftObservationFrameWriteCursor request,
       RaftObservationResponseReader response_reader, RaftObservationTlsClientConfig config,
       const TimePoint now)
      : socket_(std::move(socket)), request_(std::move(request)),
        response_reader_(std::move(response_reader)), config_(config),
        deadline_(deadline_after(now, config.limits.handshake_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (state_ != RaftObservationTlsClientState::kFailed) {
      failure_ = std::move(failure);
      state_ = RaftObservationTlsClientState::kFailed;
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
                         "Raft observation server principal is not authenticated"));
    auto authorized = config_.node_authorizer->authorize_node(authenticated->principal_id,
                                                              config_.request.target_node_id);
    if (!authorized.has_value())
      return fail(authorized.error());
    if (!*authorized)
      return fail(status(common::StatusCode::kUnauthenticated,
                         "TLS principal cannot claim the observation target"));
    state_ = RaftObservationTlsClientState::kWritingRequest;
    interest_ = {.want_write = true};
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

  [[nodiscard]] common::Status write(const bool readable, const bool writable) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    auto progress = socket_.write(request_.pending_write());
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
                         "Raft observation request write made no progress"));
    const common::Status consumed = request_.consume_written(progress->bytes_transferred);
    if (!consumed.is_ok())
      return fail(consumed);
    interest_ = request_.complete() ? RaftObservationTlsInterest{.want_read = true}
                                    : RaftObservationTlsInterest{.want_write = true};
    if (request_.complete())
      state_ = RaftObservationTlsClientState::kReadingResponse;
    return common::Status::ok();
  }

  [[nodiscard]] common::Status read(const bool readable, const bool writable) {
    if ((!interest_.want_read || (!readable && socket_.pending_plaintext_bytes() == 0U)) &&
        (!interest_.want_write || !writable))
      return common::Status::ok();
    auto progress = socket_.read(scratch_);
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
                         "Raft observation response read made no progress"));
    auto step =
        response_reader_.consume(common::ByteView{scratch_}.first(progress->bytes_transferred));
    if (!step.has_value())
      return fail(step.error());
    if (step->consumed_bytes != progress->bytes_transferred)
      return fail(status(common::StatusCode::kCorruption,
                         "Raft observation response has a coalesced suffix"));
    auto* response = step->response.transform([](auto& value) { return &value; }).value_or(nullptr);
    if (response == nullptr) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (response->source_node_id != config_.request.target_node_id ||
        response->target_node_id != config_.request.source_node_id ||
        response->group_id != config_.request.group_id ||
        response->correlation_id != config_.request.correlation_id) {
      return fail(
          status(common::StatusCode::kCorruption, "Raft observation response correlation differs"));
    }
    response_ = std::move(*response);
    state_ = RaftObservationTlsClientState::kComplete;
    interest_ = {};
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  RaftObservationFrameWriteCursor request_;
  RaftObservationResponseReader response_reader_;
  RaftObservationTlsClientConfig config_;
  TimePoint deadline_;
  RaftObservationTlsClientState state_{RaftObservationTlsClientState::kHandshaking};
  RaftObservationTlsInterest interest_{.want_write = true};
  std::array<std::byte, 4096U> scratch_{};
  std::optional<RaftObservationResponse> response_;
  common::Status failure_{common::StatusCode::kInternal,
                          "Raft observation TLS client has not failed"};
};

RaftObservationTlsClient::RaftObservationTlsClient(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftObservationTlsClient::~RaftObservationTlsClient() = default;
RaftObservationTlsClient::RaftObservationTlsClient(RaftObservationTlsClient&&) noexcept = default;
RaftObservationTlsClient&
RaftObservationTlsClient::operator=(RaftObservationTlsClient&&) noexcept = default;

common::Result<RaftObservationTlsClient>
RaftObservationTlsClient::create(network::TlsSocket socket,
                                 const RaftObservationTlsClientConfig config, const TimePoint now) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr ||
      !valid_timeout(config.limits.handshake_timeout) ||
      !valid_timeout(config.limits.exchange_timeout)) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft observation TLS configuration is invalid"));
  }
  auto encoded = encode_raft_observation_request_v1(config.request);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  auto writer =
      RaftObservationFrameWriteCursor::create(std::move(*encoded), config.limits.transport);
  if (!writer.has_value())
    return common::make_unexpected(writer.error());
  auto reader = RaftObservationResponseReader::create(config.limits.transport);
  if (!reader.has_value())
    return common::make_unexpected(reader.error());
  try {
    return RaftObservationTlsClient{std::make_unique<Impl>(std::move(socket), std::move(*writer),
                                                           std::move(*reader), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted, "Raft observation TLS allocation failed"));
  }
}

common::Status RaftObservationTlsClient::on_ready(const bool readable, const bool writable,
                                                  const TimePoint now) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft observation TLS client is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == RaftObservationTlsClientState::kFailed)
    return impl.failure_;
  if (impl.state_ == RaftObservationTlsClientState::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_)
    return impl.fail(status(common::StatusCode::kUnavailable,
                            impl.state_ == RaftObservationTlsClientState::kHandshaking
                                ? "Raft observation TLS handshake timed out"
                                : "Raft observation TLS exchange timed out"));
  if (impl.state_ == RaftObservationTlsClientState::kHandshaking)
    return impl.handshake(readable, writable, now);
  if (impl.state_ == RaftObservationTlsClientState::kWritingRequest)
    return impl.write(readable, writable);
  return impl.read(readable, writable);
}

RaftObservationTlsClientState RaftObservationTlsClient::state() const noexcept {
  return implementation_ ? implementation_->state_ : RaftObservationTlsClientState::kFailed;
}

RaftObservationTlsInterest RaftObservationTlsClient::interest() const noexcept {
  return implementation_ ? implementation_->interest_ : RaftObservationTlsInterest{};
}

RaftObservationTlsClient::TimePoint RaftObservationTlsClient::deadline() const noexcept {
  return implementation_ ? implementation_->deadline_ : TimePoint{};
}

common::Result<raft::RaftGroupObservation> RaftObservationTlsClient::result() const {
  if (!implementation_ || implementation_->state_ != RaftObservationTlsClientState::kComplete) {
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "Raft observation result is unavailable"));
  }
  const auto* response =
      implementation_->response_.transform([](const auto& value) { return &value; })
          .value_or(nullptr);
  if (response == nullptr)
    return common::make_unexpected(
        status(common::StatusCode::kInternal, "Raft observation response is unavailable"));
  if (response->status_code != common::StatusCode::kOk) {
    return common::make_unexpected(status(response->status_code, "Remote Raft observation failed"));
  }
  const auto* observation =
      response->observation.transform([](const auto& value) { return &value; }).value_or(nullptr);
  if (observation == nullptr)
    return common::make_unexpected(status(common::StatusCode::kCorruption,
                                          "Successful Raft observation response has no payload"));
  try {
    return *observation;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft observation result allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft observation result exceeds containers"));
  }
}

const common::Status& RaftObservationTlsClient::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "Raft observation TLS client is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::cluster
