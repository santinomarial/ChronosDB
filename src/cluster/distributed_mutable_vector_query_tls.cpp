#include "chronos/cluster/distributed_mutable_vector_query_tls.hpp"

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
[[nodiscard]] bool valid_limits(const DistributedMutableVectorQueryTlsLimits& limits) noexcept {
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

class DistributedMutableVectorQueryTlsClient::Impl {
public:
  Impl(network::TlsSocket socket, DistributedMutableVectorQueryRequestWriteCursor request,
       const ClientIdentity identity, query::DistributedVectorResultSchema expected_schema,
       const raft::NodeId target, const DistributedMutableVectorQueryTlsClientConfig config,
       const TimePoint now)
      : socket_(std::move(socket)), request_(std::move(request)), identity_(identity),
        target_(target), config_(config),
        deadline_(deadline_after(now, config.limits.handshake_timeout)),
        response_reader_(
            std::move(expected_schema),
            std::min(config.limits.maximum_response_bytes,
                     static_cast<std::size_t>(kMaximumDistributedVectorQueryResponseV2Size))) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ != DistributedMutableVectorQueryTlsState::kFailed) {
      failure_ = std::move(status);
      state_ = DistributedMutableVectorQueryTlsState::kFailed;
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
      return fail(unauthenticated("mutable vector query server principal is not authenticated"));
    }
    auto authorized =
        config_.node_authorizer->authorize_node(authentication->principal_id, target_);
    if (!authorized.has_value())
      return fail(authorized.error());
    if (!*authorized) {
      return fail(
          unauthenticated("TLS server principal cannot claim mutable vector query target node"));
    }
    state_ = DistributedMutableVectorQueryTlsState::kWritingRequest;
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
      return fail(unavailable("mutable vector query TLS handshake closed"));
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
      return fail(unavailable("mutable vector query request socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("mutable vector query request write made no progress"));
    const common::Status consumed = request_.consume_written(progress->bytes_transferred);
    if (!consumed.is_ok())
      return fail(consumed);
    if (request_.complete()) {
      state_ = DistributedMutableVectorQueryTlsState::kReadingResponses;
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
      return fail(corruption("mutable vector query TLS response is not correlated"));
    }
    if (responses_.size() == config_.limits.maximum_response_frames)
      return fail(exhausted("mutable vector query TLS response frame limit exceeded"));
    if (response.status_code == common::StatusCode::kOk) {
      if (!response.payload.has_value())
        return fail(corruption("mutable vector query TLS success has no payload"));
      if (response.payload->sequence != responses_.size() + 1U)
        return fail(corruption("mutable vector query TLS response sequence is not contiguous"));
    } else if (!responses_.empty()) {
      return fail(corruption("mutable vector query TLS failure followed successful results"));
    }
    const bool terminal = terminal_response(response);
    try {
      responses_.push_back(std::move(response));
    } catch (const std::bad_alloc&) {
      return fail(exhausted("mutable vector query TLS response allocation failed"));
    } catch (const std::length_error&) {
      return fail(exhausted("mutable vector query TLS response count exceeds container limits"));
    }
    if (terminal) {
      state_ = DistributedMutableVectorQueryTlsState::kComplete;
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
      return fail(unavailable("mutable vector query response socket closed before terminal"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("mutable vector query response read made no progress"));
    std::size_t offset{};
    const common::ByteView received =
        common::ByteView{response_scratch_}.first(progress->bytes_transferred);
    while (offset < received.size()) {
      auto step = response_reader_.consume(received.subspan(offset));
      if (!step.has_value())
        return fail(step.error());
      if (step->consumed_bytes == 0U)
        return fail(corruption("mutable vector query response reader made no progress"));
      if (step->consumed_bytes > config_.limits.maximum_response_bytes - received_response_bytes_)
        return fail(exhausted("mutable vector query TLS response byte limit exceeded"));
      received_response_bytes_ += step->consumed_bytes;
      offset += step->consumed_bytes;
      if (step->response.has_value()) {
        common::Status accepted = accept_response(std::move(step->response).value());
        if (!accepted.is_ok())
          return accepted;
        if (state_ == DistributedMutableVectorQueryTlsState::kComplete &&
            offset != received.size()) {
          return fail(corruption("mutable vector query terminal response has a coalesced suffix"));
        }
      }
    }
    if (state_ != DistributedMutableVectorQueryTlsState::kComplete)
      interest_ = {.want_read = true};
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  DistributedMutableVectorQueryRequestWriteCursor request_;
  ClientIdentity identity_;
  raft::NodeId target_{};
  DistributedMutableVectorQueryTlsClientConfig config_;
  TimePoint deadline_;
  DistributedMutableVectorQueryTlsState state_{DistributedMutableVectorQueryTlsState::kHandshaking};
  DistributedMutableVectorQueryTlsInterest interest_{.want_write = true};
  DistributedVectorQueryResponseV2Reader response_reader_;
  std::array<std::byte, kTlsScratchSize> response_scratch_{};
  std::size_t received_response_bytes_{};
  std::vector<DistributedVectorQueryResponseV2> responses_;
  common::Status failure_{common::StatusCode::kInternal,
                          "mutable vector query TLS client has not failed"};
};

class DistributedMutableVectorQueryTlsServer::Impl {
public:
  Impl(network::TlsSocket socket, const DistributedMutableVectorQueryTlsServerConfig config,
       const TimePoint now)
      : socket_(std::move(socket)), config_(config),
        deadline_(deadline_after(now, config.limits.handshake_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ != DistributedMutableVectorQueryTlsState::kFailed) {
      failure_ = std::move(status);
      state_ = DistributedMutableVectorQueryTlsState::kFailed;
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
      return fail(unauthenticated("mutable vector query client principal is not authenticated"));
    }
    authenticated_peer_ = *authentication;
    state_ = DistributedMutableVectorQueryTlsState::kReadingRequest;
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
      return fail(unavailable("mutable vector query TLS handshake closed"));
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
                           const DistributedMutableVectorQueryRequest& request) {
    if (responses.empty() || responses.size() > config_.limits.maximum_response_frames)
      return exhausted("mutable vector query receiver response count exceeds TLS limit");
    std::size_t total_bytes{};
    for (const auto& response : responses) {
      if (response.size() > config_.limits.maximum_response_bytes - total_bytes)
        return exhausted("mutable vector query receiver response bytes exceed TLS limit");
      total_bytes += response.size();
    }
    try {
      response_writers_.reserve(responses.size());
      for (std::size_t index = 0U; index < responses.size(); ++index) {
        auto decoded = decode_distributed_vector_query_response_v2_exact(
            responses[index], request.fragment.result_schema);
        if (!decoded.has_value())
          return decoded.error();
        const bool last = index + 1U == responses.size();
        if (decoded->source_node_id != request.target_node_id ||
            decoded->target_node_id != request.source_node_id ||
            decoded->query_id != request.fragment.query_id ||
            decoded->tablet_id != request.fragment.tablet_id) {
          return corruption("mutable vector query receiver response route is not correlated");
        }
        if (decoded->status_code == common::StatusCode::kOk) {
          if (!decoded->payload.has_value())
            return corruption("mutable vector query receiver success response has no payload");
          const auto& payload =
              decoded->payload.value(); // NOLINT(bugprone-unchecked-optional-access)
          if (payload.sequence != index + 1U || payload.terminal != last) {
            return corruption(
                "mutable vector query receiver response stream is not terminally closed");
          }
        } else if (responses.size() != 1U || decoded->payload.has_value()) {
          return corruption("mutable vector query receiver failure response stream is invalid");
        }
        auto writer = DistributedVectorQueryFrameV2WriteCursor::create_response(
            *decoded, request.fragment.result_schema);
        if (!writer.has_value())
          return writer.error();
        response_writers_.push_back(std::move(*writer));
      }
    } catch (const std::bad_alloc&) {
      return exhausted("mutable vector query TLS response allocation failed");
    } catch (const std::length_error&) {
      return exhausted("mutable vector query TLS response count exceeds container limits");
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
      return fail(unavailable("mutable vector query request socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("mutable vector query request read made no progress"));
    const common::ByteView received =
        common::ByteView{request_scratch_}.first(progress->bytes_transferred);
    auto step = request_reader_.consume(received);
    if (!step.has_value())
      return fail(step.error());
    if (step->consumed_bytes != received.size())
      return fail(corruption("mutable vector query request has a coalesced suffix"));
    if (!step->request.has_value()) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    DistributedMutableVectorQueryRequest request = std::move(step->request).value();
    auto encoded_request = encode_distributed_mutable_vector_query_request(request);
    if (!encoded_request.has_value())
      return fail(encoded_request.error());
    auto responses = config_.receiver->receive(*encoded_request, *authenticated_peer_);
    if (!responses.has_value())
      return fail(responses.error());
    const common::Status validated = validate_response_stream(*responses, request);
    if (!validated.is_ok())
      return fail(validated);
    state_ = DistributedMutableVectorQueryTlsState::kWritingResponses;
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
      return fail(unavailable("mutable vector query response socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("mutable vector query response write made no progress"));
    const common::Status consumed = writer.consume_written(progress->bytes_transferred);
    if (!consumed.is_ok())
      return fail(consumed);
    if (writer.complete()) {
      ++response_index_;
      if (response_index_ == response_writers_.size()) {
        state_ = DistributedMutableVectorQueryTlsState::kComplete;
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
  DistributedMutableVectorQueryTlsServerConfig config_;
  TimePoint deadline_;
  DistributedMutableVectorQueryTlsState state_{DistributedMutableVectorQueryTlsState::kHandshaking};
  DistributedMutableVectorQueryTlsInterest interest_{.want_read = true};
  std::optional<network::PeerAuthenticationResult> authenticated_peer_;
  DistributedMutableVectorQueryRequestReader request_reader_;
  std::array<std::byte, kTlsScratchSize> request_scratch_{};
  std::vector<DistributedVectorQueryFrameV2WriteCursor> response_writers_;
  std::size_t response_index_{};
  common::Status failure_{common::StatusCode::kInternal,
                          "mutable vector query TLS server has not failed"};
};

DistributedMutableVectorQueryTlsClient::DistributedMutableVectorQueryTlsClient(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedMutableVectorQueryTlsClient::~DistributedMutableVectorQueryTlsClient() = default;
DistributedMutableVectorQueryTlsClient::DistributedMutableVectorQueryTlsClient(
    DistributedMutableVectorQueryTlsClient&&) noexcept = default;
DistributedMutableVectorQueryTlsClient& DistributedMutableVectorQueryTlsClient::operator=(
    DistributedMutableVectorQueryTlsClient&&) noexcept = default;

common::Result<DistributedMutableVectorQueryTlsClient>
DistributedMutableVectorQueryTlsClient::create(
    network::TlsSocket socket, DistributedMutableVectorQueryAttempt attempt,
    const DistributedMutableVectorQueryTlsClientConfig config, const TimePoint now) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr ||
      !valid_limits<TimePoint>(config.limits) || attempt.attempt_number == 0U ||
      attempt.target_node_id == 0U) {
    return common::make_unexpected(
        invalid("mutable vector query TLS client configuration is invalid"));
  }
  try {
    auto request = decode_distributed_mutable_vector_query_request_exact(attempt.request_bytes);
    if (!request.has_value())
      return common::make_unexpected(request.error());
    if (request->target_node_id != attempt.target_node_id) {
      return common::make_unexpected(
          invalid("mutable vector query TLS attempt target is inconsistent"));
    }
    auto cursor = DistributedMutableVectorQueryRequestWriteCursor::create(*request);
    if (!cursor.has_value())
      return common::make_unexpected(cursor.error());
    const ClientIdentity identity{.source_node_id = request->source_node_id,
                                  .target_node_id = request->target_node_id,
                                  .query_id = request->fragment.query_id,
                                  .tablet_id = request->fragment.tablet_id};
    query::DistributedVectorResultSchema expected_schema =
        std::move(request->fragment.result_schema);
    return DistributedMutableVectorQueryTlsClient{
        std::make_unique<Impl>(std::move(socket), std::move(*cursor), identity,
                               std::move(expected_schema), attempt.target_node_id, config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable vector query TLS client allocation failed"));
  }
}

common::Status DistributedMutableVectorQueryTlsClient::on_ready(const bool readable,
                                                                const bool writable,
                                                                const TimePoint now) {
  if (!implementation_)
    return invalid("mutable vector query TLS client is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedMutableVectorQueryTlsState::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedMutableVectorQueryTlsState::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_) {
    return impl.fail(unavailable(impl.state_ == DistributedMutableVectorQueryTlsState::kHandshaking
                                     ? "mutable vector query TLS handshake timed out"
                                     : "mutable vector query TLS exchange timed out"));
  }
  if (impl.state_ == DistributedMutableVectorQueryTlsState::kHandshaking)
    return impl.advance_handshake(readable, writable, now);
  if (impl.state_ == DistributedMutableVectorQueryTlsState::kWritingRequest)
    return impl.advance_write(readable, writable);
  return impl.advance_read(readable, writable);
}

DistributedMutableVectorQueryTlsState
DistributedMutableVectorQueryTlsClient::state() const noexcept {
  return implementation_ ? implementation_->state_ : DistributedMutableVectorQueryTlsState::kFailed;
}

DistributedMutableVectorQueryTlsInterest
DistributedMutableVectorQueryTlsClient::interest() const noexcept {
  return implementation_ ? implementation_->interest_ : DistributedMutableVectorQueryTlsInterest{};
}

common::Result<std::span<const DistributedVectorQueryResponseV2>>
DistributedMutableVectorQueryTlsClient::responses() const {
  if (!implementation_ ||
      implementation_->state_ != DistributedMutableVectorQueryTlsState::kComplete)
    return common::make_unexpected(invalid("mutable vector query TLS responses are unavailable"));
  return std::span<const DistributedVectorQueryResponseV2>{implementation_->responses_};
}

const common::Status& DistributedMutableVectorQueryTlsClient::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "mutable vector query TLS client is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

DistributedMutableVectorQueryTlsServer::DistributedMutableVectorQueryTlsServer(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedMutableVectorQueryTlsServer::~DistributedMutableVectorQueryTlsServer() = default;
DistributedMutableVectorQueryTlsServer::DistributedMutableVectorQueryTlsServer(
    DistributedMutableVectorQueryTlsServer&&) noexcept = default;
DistributedMutableVectorQueryTlsServer& DistributedMutableVectorQueryTlsServer::operator=(
    DistributedMutableVectorQueryTlsServer&&) noexcept = default;

common::Result<DistributedMutableVectorQueryTlsServer>
DistributedMutableVectorQueryTlsServer::create(
    network::TlsSocket socket, const DistributedMutableVectorQueryTlsServerConfig config,
    const TimePoint now) {
  if (config.authenticator == nullptr || config.receiver == nullptr ||
      !valid_limits<TimePoint>(config.limits)) {
    return common::make_unexpected(
        invalid("mutable vector query TLS server configuration is invalid"));
  }
  try {
    return DistributedMutableVectorQueryTlsServer{
        std::make_unique<Impl>(std::move(socket), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable vector query TLS server allocation failed"));
  }
}

common::Status DistributedMutableVectorQueryTlsServer::on_ready(const bool readable,
                                                                const bool writable,
                                                                const TimePoint now) {
  if (!implementation_)
    return invalid("mutable vector query TLS server is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedMutableVectorQueryTlsState::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedMutableVectorQueryTlsState::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_) {
    return impl.fail(unavailable(impl.state_ == DistributedMutableVectorQueryTlsState::kHandshaking
                                     ? "mutable vector query TLS handshake timed out"
                                     : "mutable vector query TLS exchange timed out"));
  }
  if (impl.state_ == DistributedMutableVectorQueryTlsState::kHandshaking)
    return impl.advance_handshake(readable, writable, now);
  if (impl.state_ == DistributedMutableVectorQueryTlsState::kReadingRequest)
    return impl.advance_read(readable, writable);
  return impl.advance_write(readable, writable);
}

DistributedMutableVectorQueryTlsState
DistributedMutableVectorQueryTlsServer::state() const noexcept {
  return implementation_ ? implementation_->state_ : DistributedMutableVectorQueryTlsState::kFailed;
}

DistributedMutableVectorQueryTlsInterest
DistributedMutableVectorQueryTlsServer::interest() const noexcept {
  return implementation_ ? implementation_->interest_ : DistributedMutableVectorQueryTlsInterest{};
}

const common::Status& DistributedMutableVectorQueryTlsServer::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "mutable vector query TLS server is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::cluster
