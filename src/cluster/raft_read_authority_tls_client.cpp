#include "chronos/cluster/raft_read_authority_tls_client.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <new>
#include <optional>
#include <stdexcept>
#include <utility>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      RaftReadAuthorityTlsClient::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] RaftReadAuthorityTlsClient::TimePoint
deadline_after(const RaftReadAuthorityTlsClient::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration =
      std::chrono::duration_cast<RaftReadAuthorityTlsClient::TimePoint::duration>(timeout);
  return now > RaftReadAuthorityTlsClient::TimePoint::max() - duration
             ? RaftReadAuthorityTlsClient::TimePoint::max()
             : now + duration;
}

} // namespace

class RaftReadAuthorityTlsClient::Impl {
public:
  Impl(network::TlsSocket socket, RaftReadAuthorityFrameWriteCursor request,
       RaftReadAuthorityResponseReader response_reader, RaftReadAuthorityTlsClientConfig config,
       const TimePoint now)
      : socket_(std::move(socket)), request_(std::move(request)),
        response_reader_(std::move(response_reader)), config_(config),
        deadline_(deadline_after(now, config.limits.handshake_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (state_ != RaftReadAuthorityTlsClientState::kFailed) {
      failure_ = std::move(failure);
      state_ = RaftReadAuthorityTlsClientState::kFailed;
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
                         "Raft read-authority server principal is not authenticated"));
    }
    auto authorized = config_.node_authorizer->authorize_node(authenticated->principal_id,
                                                              config_.request.target_node_id);
    if (!authorized.has_value())
      return fail(authorized.error());
    if (!*authorized) {
      return fail(status(common::StatusCode::kUnauthenticated,
                         "TLS principal cannot claim the read-authority target"));
    }
    state_ = RaftReadAuthorityTlsClientState::kWritingRequest;
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

  [[nodiscard]] common::Status write(const bool readable, const bool writable) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    auto progress = socket_.write(request_.pending_write());
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
                         "Raft read-authority request write made no progress"));
    }
    const common::Status consumed = request_.consume_written(progress->bytes_transferred);
    if (!consumed.is_ok())
      return fail(consumed);
    interest_ = request_.complete() ? RaftReadAuthorityTlsInterest{.want_read = true}
                                    : RaftReadAuthorityTlsInterest{.want_write = true};
    if (request_.complete())
      state_ = RaftReadAuthorityTlsClientState::kReadingResponse;
    return common::Status::ok();
  }

  [[nodiscard]] common::Status read(const bool readable, const bool writable) {
    if ((!interest_.want_read || (!readable && socket_.pending_plaintext_bytes() == 0U)) &&
        (!interest_.want_write || !writable)) {
      return common::Status::ok();
    }
    auto progress = socket_.read(scratch_);
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
                         "Raft read-authority response read made no progress"));
    }
    auto step =
        response_reader_.consume(common::ByteView{scratch_}.first(progress->bytes_transferred));
    if (!step.has_value())
      return fail(step.error());
    if (step->consumed_bytes != progress->bytes_transferred) {
      return fail(status(common::StatusCode::kCorruption,
                         "Raft read-authority response has a coalesced suffix"));
    }
    auto* response = step->response.transform([](auto& value) { return &value; }).value_or(nullptr);
    if (response == nullptr) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (response->source_node_id != config_.request.target_node_id ||
        response->target_node_id != config_.request.source_node_id ||
        response->group_id != config_.request.group_id ||
        response->correlation_id != config_.request.correlation_id) {
      return fail(status(common::StatusCode::kCorruption,
                         "Raft read-authority response correlation differs"));
    }
    response_ = std::move(*response);
    state_ = RaftReadAuthorityTlsClientState::kComplete;
    interest_ = {};
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  RaftReadAuthorityFrameWriteCursor request_;
  RaftReadAuthorityResponseReader response_reader_;
  RaftReadAuthorityTlsClientConfig config_;
  TimePoint deadline_;
  RaftReadAuthorityTlsClientState state_{RaftReadAuthorityTlsClientState::kHandshaking};
  RaftReadAuthorityTlsInterest interest_{.want_write = true};
  std::array<std::byte, 4096U> scratch_{};
  std::optional<RaftReadAuthorityResponse> response_;
  common::Status failure_{common::StatusCode::kInternal,
                          "Raft read-authority TLS client has not failed"};
};

RaftReadAuthorityTlsClient::RaftReadAuthorityTlsClient(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftReadAuthorityTlsClient::~RaftReadAuthorityTlsClient() = default;
RaftReadAuthorityTlsClient::RaftReadAuthorityTlsClient(RaftReadAuthorityTlsClient&&) noexcept =
    default;
RaftReadAuthorityTlsClient&
RaftReadAuthorityTlsClient::operator=(RaftReadAuthorityTlsClient&&) noexcept = default;

common::Result<RaftReadAuthorityTlsClient> RaftReadAuthorityTlsClient::create(
    network::TlsSocket socket, const RaftReadAuthorityTlsClientConfig config, const TimePoint now) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr ||
      !valid_timeout(config.limits.handshake_timeout) ||
      !valid_timeout(config.limits.exchange_timeout)) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft read-authority TLS configuration is invalid"));
  }
  auto encoded = encode_raft_read_authority_request_v1(config.request);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  auto writer =
      RaftReadAuthorityFrameWriteCursor::create(std::move(*encoded), config.limits.transport);
  if (!writer.has_value())
    return common::make_unexpected(writer.error());
  auto reader = RaftReadAuthorityResponseReader::create(config.limits.transport);
  if (!reader.has_value())
    return common::make_unexpected(reader.error());
  try {
    return RaftReadAuthorityTlsClient{std::make_unique<Impl>(std::move(socket), std::move(*writer),
                                                             std::move(*reader), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft read-authority TLS allocation failed"));
  }
}

common::Status RaftReadAuthorityTlsClient::on_ready(const bool readable, const bool writable,
                                                    const TimePoint now) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft read-authority TLS client is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == RaftReadAuthorityTlsClientState::kFailed)
    return impl.failure_;
  if (impl.state_ == RaftReadAuthorityTlsClientState::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_) {
    return impl.fail(status(common::StatusCode::kUnavailable,
                            impl.state_ == RaftReadAuthorityTlsClientState::kHandshaking
                                ? "Raft read-authority TLS handshake timed out"
                                : "Raft read-authority TLS exchange timed out"));
  }
  if (impl.state_ == RaftReadAuthorityTlsClientState::kHandshaking)
    return impl.handshake(readable, writable, now);
  if (impl.state_ == RaftReadAuthorityTlsClientState::kWritingRequest)
    return impl.write(readable, writable);
  return impl.read(readable, writable);
}

RaftReadAuthorityTlsClientState RaftReadAuthorityTlsClient::state() const noexcept {
  return implementation_ ? implementation_->state_ : RaftReadAuthorityTlsClientState::kFailed;
}

RaftReadAuthorityTlsInterest RaftReadAuthorityTlsClient::interest() const noexcept {
  return implementation_ ? implementation_->interest_ : RaftReadAuthorityTlsInterest{};
}

RaftReadAuthorityTlsClient::TimePoint RaftReadAuthorityTlsClient::deadline() const noexcept {
  return implementation_ ? implementation_->deadline_ : TimePoint{};
}

common::Result<RaftReadAuthority> RaftReadAuthorityTlsClient::result() const {
  if (!implementation_ || implementation_->state_ != RaftReadAuthorityTlsClientState::kComplete) {
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "Raft read-authority result is unavailable"));
  }
  const auto* response =
      implementation_->response_.transform([](const auto& value) { return &value; })
          .value_or(nullptr);
  if (response == nullptr) {
    return common::make_unexpected(
        status(common::StatusCode::kInternal, "Raft read-authority response is unavailable"));
  }
  if (response->status_code != common::StatusCode::kOk) {
    return common::make_unexpected(
        status(response->status_code, "Remote Raft read-authority acquisition failed"));
  }
  const auto* authority =
      response->authority.transform([](const auto& value) { return &value; }).value_or(nullptr);
  if (authority == nullptr) {
    return common::make_unexpected(status(common::StatusCode::kCorruption,
                                          "Successful Raft read-authority response has no proof"));
  }
  try {
    return *authority;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft read-authority result allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft read-authority result exceeds containers"));
  }
}

const common::Status& RaftReadAuthorityTlsClient::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "Raft read-authority TLS client is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::cluster
