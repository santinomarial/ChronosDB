#include "chronos/cluster/distributed_vector_query_tls_v2.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <new>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

inline constexpr std::size_t kTlsScratchSize = std::size_t{16U} * 1024U;

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

template <typename TimePoint>
[[nodiscard]] bool valid_limits(const DistributedVectorQueryTlsLimitsV2& limits) noexcept {
  const auto maximum =
      std::chrono::duration_cast<std::chrono::milliseconds>(TimePoint::duration::max());
  constexpr std::size_t kMinimumResponseBytes =
      kDistributedVectorQueryResponseV2HeaderSize + kDistributedVectorQueryResponseV2TrailerSize;
  return limits.handshake_timeout.count() > 0 && limits.handshake_timeout <= maximum &&
         limits.exchange_timeout.count() > 0 && limits.exchange_timeout <= maximum &&
         limits.maximum_response_frames > 0U &&
         limits.maximum_response_frames <= query::kMaximumDistributedCoordinatorMessages &&
         limits.maximum_response_bytes >= kMinimumResponseBytes &&
         limits.maximum_response_bytes <= kMaximumDistributedVectorQueryV2ResponseBytes;
}

template <typename TimePoint>
[[nodiscard]] TimePoint deadline_after(const TimePoint now,
                                       const std::chrono::milliseconds timeout) noexcept {
  const auto duration = std::chrono::duration_cast<typename TimePoint::duration>(timeout);
  return now > TimePoint::max() - duration ? TimePoint::max() : now + duration;
}

struct ClientIdentity {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  common::Uuid query_id;
  schema::TabletId tablet_id;
};

[[nodiscard]] bool terminal_response(const DistributedVectorQueryResponseV2& response) noexcept {
  return response.status_code != common::StatusCode::kOk ||
         (response.payload.has_value() && response.payload->terminal);
}

} // namespace

class DistributedVectorQueryTlsClientV2::Impl {
public:
  Impl(network::TlsSocket socket, DistributedVectorQueryFrameV2WriteCursor request,
       ClientIdentity identity, query::DistributedVectorResultSchema expected_schema,
       const raft::NodeId target, DistributedVectorQueryTlsClientConfigV2 config,
       const TimePoint now) noexcept
      : socket_(std::move(socket)), request_(std::move(request)), identity_(identity),
        target_(target), config_(config),
        deadline_(deadline_after(now, config.limits.handshake_timeout)),
        response_reader_(
            std::move(expected_schema),
            std::min(config.limits.maximum_response_bytes,
                     static_cast<std::size_t>(kMaximumDistributedVectorQueryResponseV2Size))) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ != DistributedVectorQueryTlsStateV2::kFailed) {
      failure_ = std::move(status);
      state_ = DistributedVectorQueryTlsStateV2::kFailed;
      interest_ = {};
      responses_.clear();
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
    if (!authentication->authorized || authentication->principal_id == 0U) {
      return fail(unauthenticated("vector query v2 server principal is not authenticated"));
    }
    auto authorized =
        config_.node_authorizer->authorize_node(authentication->principal_id, target_);
    if (!authorized.has_value())
      return fail(authorized.error());
    if (!*authorized) {
      return fail(unauthenticated("TLS server principal cannot claim vector query v2 target node"));
    }
    state_ = DistributedVectorQueryTlsStateV2::kWritingRequest;
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
      return fail(unavailable("vector query v2 TLS handshake closed"));
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
      return fail(unavailable("vector query v2 request socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("vector query v2 request write made no progress"));
    const common::Status consumed = request_.consume_written(progress->bytes_transferred);
    if (!consumed.is_ok())
      return fail(consumed);
    if (request_.complete()) {
      state_ = DistributedVectorQueryTlsStateV2::kReadingResponses;
      interest_ = {.want_read = true};
    } else {
      interest_ = {.want_write = true};
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status accept_response(DistributedVectorQueryResponseV2 response) {
    if (response.source_node_id != identity_.target_node_id ||
        response.target_node_id != identity_.source_node_id ||
        response.query_id != identity_.query_id || response.tablet_id != identity_.tablet_id) {
      return fail(corruption("vector query v2 TLS response is not correlated"));
    }
    if (responses_.size() == config_.limits.maximum_response_frames)
      return fail(exhausted("vector query v2 TLS response frame limit exceeded"));
    if (response.status_code == common::StatusCode::kOk) {
      if (!response.payload.has_value())
        return fail(corruption("vector query v2 TLS success has no payload"));
      if (response.payload->sequence != responses_.size() + 1U)
        return fail(corruption("vector query v2 TLS response sequence is not contiguous"));
    } else if (!responses_.empty()) {
      return fail(corruption("vector query v2 TLS failure followed successful results"));
    }
    const bool terminal = terminal_response(response);
    try {
      responses_.push_back(std::move(response));
    } catch (const std::bad_alloc&) {
      return fail(exhausted("vector query v2 TLS response allocation failed"));
    } catch (const std::length_error&) {
      return fail(exhausted("vector query v2 TLS response count exceeds container limits"));
    }
    if (terminal) {
      state_ = DistributedVectorQueryTlsStateV2::kComplete;
      interest_ = {};
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status advance_read(const bool readable, const bool writable) {
    if ((!interest_.want_read || (!readable && socket_.pending_plaintext_bytes() == 0U)) &&
        (!interest_.want_write || !writable)) {
      return common::Status::ok();
    }
    auto progress = socket_.read(response_scratch_);
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("vector query v2 response socket closed before terminal"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("vector query v2 response read made no progress"));
    std::size_t offset{};
    const common::ByteView received =
        common::ByteView{response_scratch_}.first(progress->bytes_transferred);
    while (offset < received.size()) {
      auto step = response_reader_.consume(received.subspan(offset));
      if (!step.has_value())
        return fail(step.error());
      if (step->consumed_bytes == 0U)
        return fail(corruption("vector query v2 response reader made no progress"));
      if (step->consumed_bytes > config_.limits.maximum_response_bytes - received_response_bytes_) {
        return fail(exhausted("vector query v2 TLS response byte limit exceeded"));
      }
      received_response_bytes_ += step->consumed_bytes;
      offset += step->consumed_bytes;
      if (step->response.has_value()) {
        common::Status accepted = accept_response(std::move(step->response).value());
        if (!accepted.is_ok())
          return accepted;
        if (state_ == DistributedVectorQueryTlsStateV2::kComplete && offset != received.size()) {
          return fail(corruption("vector query v2 terminal response has a coalesced suffix"));
        }
      }
    }
    if (state_ != DistributedVectorQueryTlsStateV2::kComplete)
      interest_ = {.want_read = true};
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  DistributedVectorQueryFrameV2WriteCursor request_;
  ClientIdentity identity_;
  raft::NodeId target_{};
  DistributedVectorQueryTlsClientConfigV2 config_;
  TimePoint deadline_;
  DistributedVectorQueryTlsStateV2 state_{DistributedVectorQueryTlsStateV2::kHandshaking};
  DistributedVectorQueryTlsInterestV2 interest_{.want_write = true};
  DistributedVectorQueryResponseV2Reader response_reader_;
  std::array<std::byte, kTlsScratchSize> response_scratch_{};
  std::size_t received_response_bytes_{};
  std::vector<DistributedVectorQueryResponseV2> responses_;
  common::Status failure_{common::StatusCode::kInternal,
                          "vector query v2 TLS client has not failed"};
};

class DistributedVectorQueryTlsServerV2::Impl {
public:
  Impl(network::TlsSocket socket, DistributedVectorQueryTlsServerConfigV2 config,
       const TimePoint now) noexcept
      : socket_(std::move(socket)), config_(config),
        deadline_(deadline_after(now, config.limits.handshake_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ != DistributedVectorQueryTlsStateV2::kFailed) {
      failure_ = std::move(status);
      state_ = DistributedVectorQueryTlsStateV2::kFailed;
      interest_ = {};
      response_writers_.clear();
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
    if (!authentication->authorized || authentication->principal_id == 0U) {
      return fail(unauthenticated("vector query v2 client principal is not authenticated"));
    }
    authenticated_peer_ = *authentication;
    state_ = DistributedVectorQueryTlsStateV2::kReadingRequest;
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
      return fail(unavailable("vector query v2 TLS handshake closed"));
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

  [[nodiscard]] common::Status
  validate_response_stream(const std::vector<std::vector<std::byte>>& responses,
                           const DistributedVectorQueryRequestV2& request) {
    if (responses.empty() || responses.size() > config_.limits.maximum_response_frames)
      return exhausted("vector query v2 receiver response count exceeds TLS limit");
    std::size_t total_bytes{};
    for (const auto& response : responses) {
      if (response.size() > config_.limits.maximum_response_bytes - total_bytes)
        return exhausted("vector query v2 receiver response bytes exceed TLS limit");
      total_bytes += response.size();
    }
    try {
      response_writers_.reserve(responses.size());
      for (std::size_t index = 0U; index < responses.size(); ++index) {
        auto decoded = decode_distributed_vector_query_response_v2_exact(
            responses[index], request.dispatch.result_schema);
        if (!decoded.has_value())
          return decoded.error();
        const bool last = index + 1U == responses.size();
        if (decoded->source_node_id != request.target_node_id ||
            decoded->target_node_id != request.source_node_id ||
            decoded->query_id != request.dispatch.dispatch.query_id ||
            decoded->tablet_id != request.dispatch.dispatch.tablet_id) {
          return corruption("vector query v2 receiver response route is not correlated");
        }
        if (decoded->status_code == common::StatusCode::kOk) {
          if (!decoded->payload.has_value())
            return corruption("vector query v2 receiver success response has no payload");
          // The branch above proves presence; this keeps the large payload borrowed.
          // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
          const auto& payload = decoded->payload.value();
          if (payload.sequence != index + 1U || payload.terminal != last) {
            return corruption("vector query v2 receiver response stream is not terminally closed");
          }
        } else if (responses.size() != 1U || decoded->payload.has_value()) {
          return corruption("vector query v2 receiver failure response stream is invalid");
        }
        auto writer = DistributedVectorQueryFrameV2WriteCursor::create_response(
            *decoded, request.dispatch.result_schema);
        if (!writer.has_value())
          return writer.error();
        response_writers_.push_back(std::move(*writer));
      }
    } catch (const std::bad_alloc&) {
      return exhausted("vector query v2 TLS response allocation failed");
    } catch (const std::length_error&) {
      return exhausted("vector query v2 TLS response count exceeds container limits");
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status advance_read(const bool readable, const bool writable) {
    if ((!interest_.want_read || (!readable && socket_.pending_plaintext_bytes() == 0U)) &&
        (!interest_.want_write || !writable)) {
      return common::Status::ok();
    }
    auto progress = socket_.read(request_scratch_);
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("vector query v2 request socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("vector query v2 request read made no progress"));
    const common::ByteView received =
        common::ByteView{request_scratch_}.first(progress->bytes_transferred);
    auto step = request_reader_.consume(received);
    if (!step.has_value())
      return fail(step.error());
    if (step->consumed_bytes != received.size())
      return fail(corruption("vector query v2 request has a coalesced suffix"));
    if (!step->request.has_value()) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    DistributedVectorQueryRequestV2 request = std::move(step->request).value();
    auto encoded_request = encode_distributed_vector_query_request_v2(request);
    if (!encoded_request.has_value())
      return fail(encoded_request.error());
    auto responses = config_.receiver->receive(*encoded_request, *authenticated_peer_);
    if (!responses.has_value())
      return fail(responses.error());
    const common::Status validated = validate_response_stream(*responses, request);
    if (!validated.is_ok())
      return fail(validated);
    state_ = DistributedVectorQueryTlsStateV2::kWritingResponses;
    interest_ = {.want_write = true};
    return common::Status::ok();
  }

  [[nodiscard]] common::Status advance_write(const bool readable, const bool writable) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    auto& writer = response_writers_[response_index_];
    auto progress = socket_.write(writer.pending_write());
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("vector query v2 response socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("vector query v2 response write made no progress"));
    const common::Status consumed = writer.consume_written(progress->bytes_transferred);
    if (!consumed.is_ok())
      return fail(consumed);
    if (writer.complete()) {
      ++response_index_;
      if (response_index_ == response_writers_.size()) {
        state_ = DistributedVectorQueryTlsStateV2::kComplete;
        interest_ = {};
      } else {
        interest_ = {.want_write = true};
      }
    } else {
      interest_ = {.want_write = true};
    }
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  DistributedVectorQueryTlsServerConfigV2 config_;
  TimePoint deadline_;
  DistributedVectorQueryTlsStateV2 state_{DistributedVectorQueryTlsStateV2::kHandshaking};
  DistributedVectorQueryTlsInterestV2 interest_{.want_read = true};
  std::optional<network::PeerAuthenticationResult> authenticated_peer_;
  DistributedVectorQueryRequestV2Reader request_reader_;
  std::array<std::byte, kTlsScratchSize> request_scratch_{};
  std::vector<DistributedVectorQueryFrameV2WriteCursor> response_writers_;
  std::size_t response_index_{};
  common::Status failure_{common::StatusCode::kInternal,
                          "vector query v2 TLS server has not failed"};
};

DistributedVectorQueryTlsClientV2::DistributedVectorQueryTlsClientV2(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorQueryTlsClientV2::~DistributedVectorQueryTlsClientV2() = default;
DistributedVectorQueryTlsClientV2::DistributedVectorQueryTlsClientV2(
    DistributedVectorQueryTlsClientV2&&) noexcept = default;
DistributedVectorQueryTlsClientV2& DistributedVectorQueryTlsClientV2::operator=(
    DistributedVectorQueryTlsClientV2&&) noexcept = default;

common::Result<DistributedVectorQueryTlsClientV2> DistributedVectorQueryTlsClientV2::create(
    network::TlsSocket socket, DistributedVectorQueryAttemptV2 attempt,
    const DistributedVectorQueryTlsClientConfigV2 config, const TimePoint now) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr ||
      !valid_limits<TimePoint>(config.limits) || attempt.attempt_number == 0U ||
      attempt.target_node_id == 0U) {
    return common::make_unexpected(invalid("vector query v2 TLS client configuration is invalid"));
  }
  auto request = decode_distributed_vector_query_request_v2_exact(attempt.request_bytes);
  if (!request.has_value())
    return common::make_unexpected(request.error());
  if (request->target_node_id != attempt.target_node_id) {
    return common::make_unexpected(invalid("vector query v2 TLS attempt target is inconsistent"));
  }
  auto cursor = DistributedVectorQueryFrameV2WriteCursor::create_request(*request);
  if (!cursor.has_value())
    return common::make_unexpected(cursor.error());
  ClientIdentity identity{.source_node_id = request->source_node_id,
                          .target_node_id = request->target_node_id,
                          .query_id = request->dispatch.dispatch.query_id,
                          .tablet_id = request->dispatch.dispatch.tablet_id};
  query::DistributedVectorResultSchema expected_schema = std::move(request->dispatch.result_schema);
  try {
    return DistributedVectorQueryTlsClientV2{
        std::make_unique<Impl>(std::move(socket), std::move(*cursor), identity,
                               std::move(expected_schema), attempt.target_node_id, config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("vector query v2 TLS client allocation failed"));
  }
}

common::Status DistributedVectorQueryTlsClientV2::on_ready(const bool readable, const bool writable,
                                                           const TimePoint now) {
  if (!implementation_)
    return invalid("vector query v2 TLS client is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedVectorQueryTlsStateV2::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedVectorQueryTlsStateV2::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_) {
    return impl.fail(unavailable(impl.state_ == DistributedVectorQueryTlsStateV2::kHandshaking
                                     ? "vector query v2 TLS handshake timed out"
                                     : "vector query v2 TLS exchange timed out"));
  }
  if (impl.state_ == DistributedVectorQueryTlsStateV2::kHandshaking)
    return impl.advance_handshake(readable, writable, now);
  if (impl.state_ == DistributedVectorQueryTlsStateV2::kWritingRequest)
    return impl.advance_write(readable, writable);
  return impl.advance_read(readable, writable);
}

DistributedVectorQueryTlsStateV2 DistributedVectorQueryTlsClientV2::state() const noexcept {
  return implementation_ ? implementation_->state_ : DistributedVectorQueryTlsStateV2::kFailed;
}

DistributedVectorQueryTlsInterestV2 DistributedVectorQueryTlsClientV2::interest() const noexcept {
  return implementation_ ? implementation_->interest_ : DistributedVectorQueryTlsInterestV2{};
}

common::Result<std::span<const DistributedVectorQueryResponseV2>>
DistributedVectorQueryTlsClientV2::responses() const {
  if (!implementation_ || implementation_->state_ != DistributedVectorQueryTlsStateV2::kComplete)
    return common::make_unexpected(invalid("vector query v2 TLS responses are unavailable"));
  return std::span<const DistributedVectorQueryResponseV2>{implementation_->responses_};
}

const common::Status& DistributedVectorQueryTlsClientV2::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "vector query v2 TLS client is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

DistributedVectorQueryTlsServerV2::DistributedVectorQueryTlsServerV2(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorQueryTlsServerV2::~DistributedVectorQueryTlsServerV2() = default;
DistributedVectorQueryTlsServerV2::DistributedVectorQueryTlsServerV2(
    DistributedVectorQueryTlsServerV2&&) noexcept = default;
DistributedVectorQueryTlsServerV2& DistributedVectorQueryTlsServerV2::operator=(
    DistributedVectorQueryTlsServerV2&&) noexcept = default;

common::Result<DistributedVectorQueryTlsServerV2>
DistributedVectorQueryTlsServerV2::create(network::TlsSocket socket,
                                          const DistributedVectorQueryTlsServerConfigV2 config,
                                          const TimePoint now) {
  if (config.authenticator == nullptr || config.receiver == nullptr ||
      !valid_limits<TimePoint>(config.limits)) {
    return common::make_unexpected(invalid("vector query v2 TLS server configuration is invalid"));
  }
  try {
    return DistributedVectorQueryTlsServerV2{
        std::make_unique<Impl>(std::move(socket), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("vector query v2 TLS server allocation failed"));
  }
}

common::Status DistributedVectorQueryTlsServerV2::on_ready(const bool readable, const bool writable,
                                                           const TimePoint now) {
  if (!implementation_)
    return invalid("vector query v2 TLS server is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedVectorQueryTlsStateV2::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedVectorQueryTlsStateV2::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_) {
    return impl.fail(unavailable(impl.state_ == DistributedVectorQueryTlsStateV2::kHandshaking
                                     ? "vector query v2 TLS handshake timed out"
                                     : "vector query v2 TLS exchange timed out"));
  }
  if (impl.state_ == DistributedVectorQueryTlsStateV2::kHandshaking)
    return impl.advance_handshake(readable, writable, now);
  if (impl.state_ == DistributedVectorQueryTlsStateV2::kReadingRequest)
    return impl.advance_read(readable, writable);
  return impl.advance_write(readable, writable);
}

DistributedVectorQueryTlsStateV2 DistributedVectorQueryTlsServerV2::state() const noexcept {
  return implementation_ ? implementation_->state_ : DistributedVectorQueryTlsStateV2::kFailed;
}

DistributedVectorQueryTlsInterestV2 DistributedVectorQueryTlsServerV2::interest() const noexcept {
  return implementation_ ? implementation_->interest_ : DistributedVectorQueryTlsInterestV2{};
}

const common::Status& DistributedVectorQueryTlsServerV2::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "vector query v2 TLS server is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::cluster
