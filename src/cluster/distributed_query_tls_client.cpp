#include "chronos/cluster/distributed_query_tls_client.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <new>
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

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      DistributedQueryTlsClient::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] DistributedQueryTlsClient::TimePoint
deadline_after(const DistributedQueryTlsClient::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration =
      std::chrono::duration_cast<DistributedQueryTlsClient::TimePoint::duration>(timeout);
  if (now > DistributedQueryTlsClient::TimePoint::max() - duration)
    return DistributedQueryTlsClient::TimePoint::max();
  return now + duration;
}

} // namespace

class DistributedQueryTlsClient::Impl {
public:
  Impl(network::TlsSocket socket, DistributedQueryFrameWriteCursor request,
       const raft::NodeId target_node_id, DistributedQueryTlsClientConfig config,
       const TimePoint now) noexcept
      : socket_(std::move(socket)), request_(std::move(request)), target_node_id_(target_node_id),
        config_(config), deadline_(deadline_after(now, config.limits.handshake_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) noexcept {
    if (state_ != DistributedQueryTlsClientState::kFailed) {
      failure_ = std::move(status);
      state_ = DistributedQueryTlsClientState::kFailed;
      interest_ = {};
    }
    return failure_;
  }

  [[nodiscard]] common::Status authenticate_server(const TimePoint now) {
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
      return fail(unauthenticated("distributed query server principal is not authenticated"));
    auto authorized =
        config_.node_authorizer->authorize_node(authentication->principal_id, target_node_id_);
    if (!authorized.has_value())
      return fail(authorized.error());
    if (!*authorized)
      return fail(unauthenticated("TLS server principal cannot claim the query target node"));
    state_ = DistributedQueryTlsClientState::kWritingRequest;
    interest_ = {.want_write = true};
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
    return authenticate_server(now);
  }

  [[nodiscard]] common::Status advance_write(const bool readable, const bool writable) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    auto progress = socket_.write(request_.pending_write());
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
      return fail(unavailable("distributed query request write made no progress"));
    const common::Status consumed = request_.consume_written(progress->bytes_transferred);
    if (!consumed.is_ok())
      return fail(consumed);
    if (request_.complete()) {
      state_ = DistributedQueryTlsClientState::kReadingResponse;
      interest_ = {.want_read = true};
    } else {
      interest_ = {.want_write = true};
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status advance_read(const bool readable, const bool writable) {
    if ((!interest_.want_read || (!readable && socket_.pending_plaintext_bytes() == 0U)) &&
        (!interest_.want_write || !writable)) {
      return common::Status::ok();
    }
    if (response_size_ == response_bytes_.size())
      return fail(corruption("distributed query response exceeds its fixed carrier bound"));
    auto progress = socket_.read(common::MutableByteView{response_bytes_}.subspan(response_size_));
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
      return fail(unavailable("distributed query response read made no progress"));
    const common::ByteView received =
        common::ByteView{response_bytes_}.subspan(response_size_, progress->bytes_transferred);
    auto step = response_reader_.consume(received);
    if (!step.has_value())
      return fail(step.error());
    response_size_ += progress->bytes_transferred;
    if (step->consumed_bytes != progress->bytes_transferred)
      return fail(corruption("distributed query TLS response has a coalesced suffix"));
    if (step->response.has_value()) {
      state_ = DistributedQueryTlsClientState::kComplete;
      interest_ = {};
    } else {
      interest_ = {.want_read = true};
    }
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  DistributedQueryFrameWriteCursor request_;
  raft::NodeId target_node_id_{};
  DistributedQueryTlsClientConfig config_;
  TimePoint deadline_{};
  DistributedQueryTlsClientState state_{DistributedQueryTlsClientState::kHandshaking};
  DistributedQueryTlsInterest interest_{.want_write = true};
  DistributedQueryResponseReader response_reader_;
  std::array<std::byte, kMaximumDistributedQueryResponseSize> response_bytes_{};
  std::size_t response_size_{};
  common::Status failure_{common::StatusCode::kInternal,
                          "distributed query TLS carrier has not failed"};
};

DistributedQueryTlsClient::DistributedQueryTlsClient(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedQueryTlsClient::~DistributedQueryTlsClient() = default;
DistributedQueryTlsClient::DistributedQueryTlsClient(DistributedQueryTlsClient&&) noexcept =
    default;
DistributedQueryTlsClient&
DistributedQueryTlsClient::operator=(DistributedQueryTlsClient&&) noexcept = default;

common::Result<DistributedQueryTlsClient>
DistributedQueryTlsClient::create(network::TlsSocket socket, DistributedQueryAttempt attempt,
                                  const DistributedQueryTlsClientConfig config,
                                  const TimePoint now) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr ||
      !valid_timeout(config.limits.handshake_timeout) ||
      !valid_timeout(config.limits.exchange_timeout) || attempt.attempt_number == 0U ||
      attempt.target_node_id == 0U) {
    return common::make_unexpected(
        invalid("distributed query TLS client configuration is invalid"));
  }
  auto decoded = decode_distributed_query_request_v1(attempt.request_bytes);
  if (!decoded.has_value())
    return common::make_unexpected(decoded.error());
  if (decoded->target_node_id != attempt.target_node_id)
    return common::make_unexpected(invalid("distributed query TLS attempt target is inconsistent"));
  auto request = DistributedQueryFrameWriteCursor::create(std::move(attempt.request_bytes));
  if (!request.has_value())
    return common::make_unexpected(request.error());
  try {
    return DistributedQueryTlsClient{std::make_unique<Impl>(std::move(socket), std::move(*request),
                                                            attempt.target_node_id, config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed query TLS client allocation failed"));
  }
}

common::Status DistributedQueryTlsClient::on_ready(const bool readable, const bool writable,
                                                   const TimePoint now) {
  if (!implementation_)
    return invalid("distributed query TLS client is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedQueryTlsClientState::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedQueryTlsClientState::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_)
    return impl.fail(unavailable(impl.state_ == DistributedQueryTlsClientState::kHandshaking
                                     ? "distributed query TLS handshake timed out"
                                     : "distributed query TLS exchange timed out"));
  if (impl.state_ == DistributedQueryTlsClientState::kHandshaking)
    return impl.advance_handshake(readable, writable, now);
  if (impl.state_ == DistributedQueryTlsClientState::kWritingRequest)
    return impl.advance_write(readable, writable);
  return impl.advance_read(readable, writable);
}

DistributedQueryTlsClientState DistributedQueryTlsClient::state() const noexcept {
  return implementation_ ? implementation_->state_ : DistributedQueryTlsClientState::kFailed;
}

DistributedQueryTlsInterest DistributedQueryTlsClient::interest() const noexcept {
  return implementation_ ? implementation_->interest_ : DistributedQueryTlsInterest{};
}

common::Result<common::ByteView> DistributedQueryTlsClient::response_bytes() const {
  if (!implementation_ || implementation_->state_ != DistributedQueryTlsClientState::kComplete)
    return common::make_unexpected(
        invalid("distributed query TLS response is unavailable before completion"));
  return common::ByteView{implementation_->response_bytes_}.first(implementation_->response_size_);
}

const common::Status& DistributedQueryTlsClient::failure() const noexcept {
  static const common::Status empty_failure{common::StatusCode::kInvalidArgument,
                                            "distributed query TLS client is empty"};
  return implementation_ ? implementation_->failure_ : empty_failure;
}

} // namespace chronos::cluster
