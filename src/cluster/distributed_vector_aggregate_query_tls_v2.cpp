#include "chronos/cluster/distributed_vector_aggregate_query_tls_v2.hpp"

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
[[nodiscard]] bool valid_limits(const DistributedVectorAggregateQueryTlsLimitsV2& limits) noexcept {
  const auto maximum =
      std::chrono::duration_cast<std::chrono::milliseconds>(TimePoint::duration::max());
  constexpr std::size_t kMinimumResponseBytes =
      kDistributedVectorAggregateQueryResponseV2HeaderSize +
      kDistributedVectorAggregateQueryResponseV2TrailerSize;
  return limits.handshake_timeout.count() > 0 && limits.handshake_timeout <= maximum &&
         limits.exchange_timeout.count() > 0 && limits.exchange_timeout <= maximum &&
         limits.maximum_response_frames > 0U &&
         limits.maximum_response_frames <= query::kMaximumUngroupedAggregateWidth &&
         limits.maximum_response_bytes >= kMinimumResponseBytes &&
         limits.maximum_response_bytes <= kMaximumDistributedVectorAggregateQueryV2ResponseBytes &&
         limits.payload.maximum_frame_length >=
             query::distributed_vector_aggregate_exchange_format::kMinimumFrameLength &&
         limits.payload.maximum_frame_length <=
             query::distributed_vector_aggregate_exchange_format::kMaximumFrameLength &&
         limits.payload.maximum_aggregates > 0U &&
         limits.payload.maximum_aggregates <=
             query::distributed_vector_aggregate_exchange_format::kMaximumAggregates &&
         limits.payload.state.maximum_frame_length >=
             query::distributed_vector_aggregate_state_format::kMinimumFrameLength &&
         limits.payload.state.maximum_frame_length <=
             query::distributed_vector_aggregate_state_format::kMaximumFrameLength &&
         limits.payload.state.maximum_variable_extremum_bytes > 0U &&
         limits.payload.state.maximum_variable_extremum_bytes <=
             query::distributed_vector_aggregate_state_format::kMaximumExtremumBytes;
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

} // namespace

class DistributedVectorAggregateQueryTlsClientV2::Impl {
public:
  Impl(network::TlsSocket socket, DistributedVectorQueryFrameV2WriteCursor request,
       ClientIdentity identity, const raft::NodeId target,
       std::vector<query::VectorAggregateDefinition>&& definitions,
       query::QueryResourceContext resources,
       const DistributedVectorAggregateQueryTlsClientConfigV2 config, const TimePoint now) noexcept
      : socket_(std::move(socket)), request_(std::move(request)), identity_(identity),
        target_(target), expected_response_frames_(definitions.size()), config_(config),
        deadline_(deadline_after(now, config.limits.handshake_timeout)),
        response_reader_(std::move(definitions), std::move(resources),
                         std::min(config.limits.maximum_response_bytes,
                                  static_cast<std::size_t>(
                                      kMaximumDistributedVectorAggregateQueryResponseV2Size)),
                         config.limits.payload) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ != DistributedVectorAggregateQueryTlsStateV2::kFailed) {
      failure_ = std::move(status);
      state_ = DistributedVectorAggregateQueryTlsStateV2::kFailed;
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
      return fail(
          unauthenticated("vector aggregate query v2 server principal is not authenticated"));
    }
    auto authorized =
        config_.node_authorizer->authorize_node(authentication->principal_id, target_);
    if (!authorized.has_value())
      return fail(authorized.error());
    if (!*authorized) {
      return fail(unauthenticated(
          "TLS server principal cannot claim vector aggregate query v2 target node"));
    }
    state_ = DistributedVectorAggregateQueryTlsStateV2::kWritingRequest;
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
      return fail(unavailable("vector aggregate query v2 TLS handshake closed"));
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
      return fail(unavailable("vector aggregate query v2 request socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("vector aggregate query v2 request write made no progress"));
    const common::Status consumed = request_.consume_written(progress->bytes_transferred);
    if (!consumed.is_ok())
      return fail(consumed);
    if (request_.complete()) {
      state_ = DistributedVectorAggregateQueryTlsStateV2::kReadingResponses;
      interest_ = {.want_read = true};
    } else {
      interest_ = {.want_write = true};
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status accept_response(DistributedVectorAggregateQueryResponseV2 response) {
    if (response.source_node_id != identity_.target_node_id ||
        response.target_node_id != identity_.source_node_id ||
        response.query_id != identity_.query_id || response.tablet_id != identity_.tablet_id) {
      return fail(corruption("vector aggregate query v2 TLS response is not correlated"));
    }
    if (responses_.size() == config_.limits.maximum_response_frames)
      return fail(exhausted("vector aggregate query v2 TLS response frame limit exceeded"));
    bool terminal{};
    if (response.status_code == common::StatusCode::kOk) {
      if (!response.payload.has_value())
        return fail(corruption("vector aggregate query v2 TLS success has no payload"));
      const std::size_t ordinal = responses_.size();
      if (response.payload->sequence != ordinal + 1U ||
          response.payload->aggregate_ordinal != ordinal ||
          response.payload->terminal != (ordinal + 1U == expected_response_frames_)) {
        return fail(corruption("vector aggregate query v2 TLS response sequence is invalid"));
      }
      terminal = ordinal + 1U == expected_response_frames_;
    } else {
      if (!responses_.empty() || response.payload.has_value())
        return fail(corruption("vector aggregate query v2 TLS failure stream is invalid"));
      terminal = true;
    }
    try {
      responses_.push_back(std::move(response));
    } catch (const std::bad_alloc&) {
      return fail(exhausted("vector aggregate query v2 TLS response allocation failed"));
    } catch (const std::length_error&) {
      return fail(exhausted("vector aggregate query v2 TLS response count exceeds limits"));
    }
    if (terminal) {
      state_ = DistributedVectorAggregateQueryTlsStateV2::kComplete;
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
      return fail(unavailable("vector aggregate query v2 response socket closed before terminal"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("vector aggregate query v2 response read made no progress"));
    std::size_t offset{};
    const common::ByteView received =
        common::ByteView{response_scratch_}.first(progress->bytes_transferred);
    while (offset < received.size()) {
      auto step = response_reader_.consume(received.subspan(offset));
      if (!step.has_value())
        return fail(step.error());
      if (step->consumed_bytes == 0U)
        return fail(corruption("vector aggregate query v2 response reader made no progress"));
      if (step->consumed_bytes > config_.limits.maximum_response_bytes - received_response_bytes_)
        return fail(exhausted("vector aggregate query v2 TLS response byte limit exceeded"));
      received_response_bytes_ += step->consumed_bytes;
      offset += step->consumed_bytes;
      if (step->response.has_value()) {
        common::Status accepted = accept_response(std::move(step->response).value());
        if (!accepted.is_ok())
          return accepted;
        if (state_ == DistributedVectorAggregateQueryTlsStateV2::kComplete &&
            offset != received.size()) {
          return fail(corruption("vector aggregate query v2 terminal has a coalesced suffix"));
        }
      }
    }
    if (state_ != DistributedVectorAggregateQueryTlsStateV2::kComplete)
      interest_ = {.want_read = true};
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  DistributedVectorQueryFrameV2WriteCursor request_;
  ClientIdentity identity_;
  raft::NodeId target_{};
  std::size_t expected_response_frames_{};
  DistributedVectorAggregateQueryTlsClientConfigV2 config_;
  TimePoint deadline_;
  DistributedVectorAggregateQueryTlsStateV2 state_{
      DistributedVectorAggregateQueryTlsStateV2::kHandshaking};
  DistributedVectorAggregateQueryTlsInterestV2 interest_{.want_write = true};
  DistributedVectorAggregateQueryResponseV2Reader response_reader_;
  std::array<std::byte, kTlsScratchSize> response_scratch_{};
  std::size_t received_response_bytes_{};
  std::vector<DistributedVectorAggregateQueryResponseV2> responses_;
  common::Status failure_{common::StatusCode::kInternal,
                          "vector aggregate query v2 TLS client has not failed"};
};

class DistributedVectorAggregateQueryTlsServerV2::Impl {
public:
  Impl(network::TlsSocket socket, const DistributedVectorAggregateQueryTlsServerConfigV2 config,
       const TimePoint now) noexcept
      : socket_(std::move(socket)), config_(config),
        deadline_(deadline_after(now, config.limits.handshake_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ != DistributedVectorAggregateQueryTlsStateV2::kFailed) {
      failure_ = std::move(status);
      state_ = DistributedVectorAggregateQueryTlsStateV2::kFailed;
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
      return fail(
          unauthenticated("vector aggregate query v2 client principal is not authenticated"));
    }
    authenticated_peer_ = *authentication;
    state_ = DistributedVectorAggregateQueryTlsStateV2::kReadingRequest;
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
      return fail(unavailable("vector aggregate query v2 TLS handshake closed"));
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
  validate_response_stream(DistributedVectorAggregateQueryBoundResponsesV2 bound,
                           const DistributedVectorQueryRequestV2& request) {
    common::Status definitions_status = validate_distributed_vector_aggregate_query_definitions_v2(
        request.dispatch, bound.definitions);
    if (!definitions_status.is_ok())
      return definitions_status;
    if (bound.definitions.size() > config_.limits.payload.maximum_aggregates ||
        bound.encoded_responses.empty() ||
        bound.encoded_responses.size() > config_.limits.maximum_response_frames) {
      return exhausted("vector aggregate query v2 receiver response count exceeds TLS limit");
    }
    std::size_t total_bytes{};
    for (const auto& response : bound.encoded_responses) {
      if (response.size() > config_.limits.maximum_response_bytes - total_bytes)
        return exhausted("vector aggregate query v2 receiver response bytes exceed TLS limit");
      total_bytes += response.size();
    }
    auto resources = query::QueryResourceContext::create(config_.limits.maximum_response_bytes);
    if (!resources.has_value())
      return resources.error();
    try {
      response_writers_.reserve(bound.encoded_responses.size());
      for (std::size_t index = 0U; index < bound.encoded_responses.size(); ++index) {
        auto decoded = decode_distributed_vector_aggregate_query_response_v2_exact(
            bound.encoded_responses[index], bound.definitions, *resources, config_.limits.payload);
        if (!decoded.has_value())
          return decoded.error();
        if (decoded->source_node_id != request.target_node_id ||
            decoded->target_node_id != request.source_node_id ||
            decoded->query_id != request.dispatch.dispatch.query_id ||
            decoded->tablet_id != request.dispatch.dispatch.tablet_id) {
          return corruption("vector aggregate query v2 receiver response is not correlated");
        }
        const bool last = index + 1U == bound.encoded_responses.size();
        if (decoded->status_code == common::StatusCode::kOk) {
          if (bound.encoded_responses.size() != bound.definitions.size() ||
              !decoded->payload.has_value()) {
            return corruption("vector aggregate query v2 receiver success vector is incomplete");
          }
          // The immediately preceding guard proves presence while keeping the state borrowed.
          // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
          const auto& payload = *decoded->payload;
          if (payload.sequence != index + 1U || payload.aggregate_ordinal != index ||
              payload.terminal != last) {
            return corruption("vector aggregate query v2 receiver success vector is invalid");
          }
        } else if (bound.encoded_responses.size() != 1U || decoded->payload.has_value()) {
          return corruption("vector aggregate query v2 receiver failure response is invalid");
        }
        auto writer = DistributedVectorAggregateQueryResponseV2WriteCursor::create(
            *decoded, bound.definitions);
        if (!writer.has_value())
          return writer.error();
        response_writers_.push_back(std::move(*writer));
      }
    } catch (const std::bad_alloc&) {
      return exhausted("vector aggregate query v2 TLS response allocation failed");
    } catch (const std::length_error&) {
      return exhausted("vector aggregate query v2 TLS response count exceeds limits");
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
      return fail(unavailable("vector aggregate query v2 request socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("vector aggregate query v2 request read made no progress"));
    const common::ByteView received =
        common::ByteView{request_scratch_}.first(progress->bytes_transferred);
    auto step = request_reader_.consume(received);
    if (!step.has_value())
      return fail(step.error());
    if (step->consumed_bytes != received.size())
      return fail(corruption("vector aggregate query v2 request has a coalesced suffix"));
    if (!step->request.has_value()) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    DistributedVectorQueryRequestV2 request = std::move(step->request).value();
    auto encoded_request = encode_distributed_vector_query_request_v2(request);
    if (!encoded_request.has_value())
      return fail(encoded_request.error());
    auto bound = config_.receiver->receive_bound(*encoded_request, *authenticated_peer_);
    if (!bound.has_value())
      return fail(bound.error());
    const common::Status validated = validate_response_stream(std::move(*bound), request);
    if (!validated.is_ok())
      return fail(validated);
    state_ = DistributedVectorAggregateQueryTlsStateV2::kWritingResponses;
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
      return fail(unavailable("vector aggregate query v2 response socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("vector aggregate query v2 response write made no progress"));
    const common::Status consumed = writer.consume_written(progress->bytes_transferred);
    if (!consumed.is_ok())
      return fail(consumed);
    if (writer.complete()) {
      ++response_index_;
      if (response_index_ == response_writers_.size()) {
        state_ = DistributedVectorAggregateQueryTlsStateV2::kComplete;
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
  DistributedVectorAggregateQueryTlsServerConfigV2 config_;
  TimePoint deadline_;
  DistributedVectorAggregateQueryTlsStateV2 state_{
      DistributedVectorAggregateQueryTlsStateV2::kHandshaking};
  DistributedVectorAggregateQueryTlsInterestV2 interest_{.want_read = true};
  std::optional<network::PeerAuthenticationResult> authenticated_peer_;
  DistributedVectorQueryRequestV2Reader request_reader_;
  std::array<std::byte, kTlsScratchSize> request_scratch_{};
  std::vector<DistributedVectorAggregateQueryResponseV2WriteCursor> response_writers_;
  std::size_t response_index_{};
  common::Status failure_{common::StatusCode::kInternal,
                          "vector aggregate query v2 TLS server has not failed"};
};

DistributedVectorAggregateQueryTlsClientV2::DistributedVectorAggregateQueryTlsClientV2(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorAggregateQueryTlsClientV2::~DistributedVectorAggregateQueryTlsClientV2() = default;
DistributedVectorAggregateQueryTlsClientV2::DistributedVectorAggregateQueryTlsClientV2(
    DistributedVectorAggregateQueryTlsClientV2&&) noexcept = default;
DistributedVectorAggregateQueryTlsClientV2& DistributedVectorAggregateQueryTlsClientV2::operator=(
    DistributedVectorAggregateQueryTlsClientV2&&) noexcept = default;

common::Result<DistributedVectorAggregateQueryTlsClientV2>
DistributedVectorAggregateQueryTlsClientV2::create(
    network::TlsSocket socket, DistributedVectorAggregateQueryAttemptV2 attempt,
    std::vector<query::VectorAggregateDefinition>&& definitions,
    query::QueryResourceContext resources,
    const DistributedVectorAggregateQueryTlsClientConfigV2 config, const TimePoint now) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr ||
      !valid_limits<TimePoint>(config.limits) || attempt.attempt_number == 0U ||
      attempt.target_node_id == 0U || definitions.size() > config.limits.maximum_response_frames ||
      definitions.size() > config.limits.payload.maximum_aggregates) {
    return common::make_unexpected(
        invalid("vector aggregate query v2 TLS client configuration is invalid"));
  }
  auto request = decode_distributed_vector_query_request_v2_exact(attempt.request_bytes);
  if (!request.has_value())
    return common::make_unexpected(request.error());
  if (request->target_node_id != attempt.target_node_id) {
    return common::make_unexpected(
        invalid("vector aggregate query v2 TLS attempt target is inconsistent"));
  }
  const common::Status definitions_status =
      validate_distributed_vector_aggregate_query_definitions_v2(request->dispatch, definitions);
  if (!definitions_status.is_ok())
    return common::make_unexpected(definitions_status);
  auto cursor = DistributedVectorQueryFrameV2WriteCursor::create_request(*request);
  if (!cursor.has_value())
    return common::make_unexpected(cursor.error());
  ClientIdentity identity{.source_node_id = request->source_node_id,
                          .target_node_id = request->target_node_id,
                          .query_id = request->dispatch.dispatch.query_id,
                          .tablet_id = request->dispatch.dispatch.tablet_id};
  try {
    return DistributedVectorAggregateQueryTlsClientV2{std::make_unique<Impl>(
        std::move(socket), std::move(*cursor), identity, attempt.target_node_id,
        std::move(definitions), std::move(resources), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("vector aggregate query v2 TLS client allocation failed"));
  }
}

common::Status DistributedVectorAggregateQueryTlsClientV2::on_ready(const bool readable,
                                                                    const bool writable,
                                                                    const TimePoint now) {
  if (!implementation_)
    return invalid("vector aggregate query v2 TLS client is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedVectorAggregateQueryTlsStateV2::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedVectorAggregateQueryTlsStateV2::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_) {
    return impl.fail(
        unavailable(impl.state_ == DistributedVectorAggregateQueryTlsStateV2::kHandshaking
                        ? "vector aggregate query v2 TLS handshake timed out"
                        : "vector aggregate query v2 TLS exchange timed out"));
  }
  if (impl.state_ == DistributedVectorAggregateQueryTlsStateV2::kHandshaking)
    return impl.advance_handshake(readable, writable, now);
  if (impl.state_ == DistributedVectorAggregateQueryTlsStateV2::kWritingRequest)
    return impl.advance_write(readable, writable);
  return impl.advance_read(readable, writable);
}

DistributedVectorAggregateQueryTlsStateV2
DistributedVectorAggregateQueryTlsClientV2::state() const noexcept {
  return implementation_ ? implementation_->state_
                         : DistributedVectorAggregateQueryTlsStateV2::kFailed;
}

DistributedVectorAggregateQueryTlsInterestV2
DistributedVectorAggregateQueryTlsClientV2::interest() const noexcept {
  return implementation_ ? implementation_->interest_
                         : DistributedVectorAggregateQueryTlsInterestV2{};
}

common::Result<std::span<const DistributedVectorAggregateQueryResponseV2>>
DistributedVectorAggregateQueryTlsClientV2::responses() const {
  if (!implementation_ ||
      implementation_->state_ != DistributedVectorAggregateQueryTlsStateV2::kComplete) {
    return common::make_unexpected(
        invalid("vector aggregate query v2 TLS responses are unavailable"));
  }
  return std::span<const DistributedVectorAggregateQueryResponseV2>{implementation_->responses_};
}

const common::Status& DistributedVectorAggregateQueryTlsClientV2::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "vector aggregate query v2 TLS client is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

DistributedVectorAggregateQueryTlsServerV2::DistributedVectorAggregateQueryTlsServerV2(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorAggregateQueryTlsServerV2::~DistributedVectorAggregateQueryTlsServerV2() = default;
DistributedVectorAggregateQueryTlsServerV2::DistributedVectorAggregateQueryTlsServerV2(
    DistributedVectorAggregateQueryTlsServerV2&&) noexcept = default;
DistributedVectorAggregateQueryTlsServerV2& DistributedVectorAggregateQueryTlsServerV2::operator=(
    DistributedVectorAggregateQueryTlsServerV2&&) noexcept = default;

common::Result<DistributedVectorAggregateQueryTlsServerV2>
DistributedVectorAggregateQueryTlsServerV2::create(
    network::TlsSocket socket, const DistributedVectorAggregateQueryTlsServerConfigV2 config,
    const TimePoint now) {
  if (config.authenticator == nullptr || config.receiver == nullptr ||
      !valid_limits<TimePoint>(config.limits)) {
    return common::make_unexpected(
        invalid("vector aggregate query v2 TLS server configuration is invalid"));
  }
  try {
    return DistributedVectorAggregateQueryTlsServerV2{
        std::make_unique<Impl>(std::move(socket), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("vector aggregate query v2 TLS server allocation failed"));
  }
}

common::Status DistributedVectorAggregateQueryTlsServerV2::on_ready(const bool readable,
                                                                    const bool writable,
                                                                    const TimePoint now) {
  if (!implementation_)
    return invalid("vector aggregate query v2 TLS server is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedVectorAggregateQueryTlsStateV2::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedVectorAggregateQueryTlsStateV2::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_) {
    return impl.fail(
        unavailable(impl.state_ == DistributedVectorAggregateQueryTlsStateV2::kHandshaking
                        ? "vector aggregate query v2 TLS handshake timed out"
                        : "vector aggregate query v2 TLS exchange timed out"));
  }
  if (impl.state_ == DistributedVectorAggregateQueryTlsStateV2::kHandshaking)
    return impl.advance_handshake(readable, writable, now);
  if (impl.state_ == DistributedVectorAggregateQueryTlsStateV2::kReadingRequest)
    return impl.advance_read(readable, writable);
  return impl.advance_write(readable, writable);
}

DistributedVectorAggregateQueryTlsStateV2
DistributedVectorAggregateQueryTlsServerV2::state() const noexcept {
  return implementation_ ? implementation_->state_
                         : DistributedVectorAggregateQueryTlsStateV2::kFailed;
}

DistributedVectorAggregateQueryTlsInterestV2
DistributedVectorAggregateQueryTlsServerV2::interest() const noexcept {
  return implementation_ ? implementation_->interest_
                         : DistributedVectorAggregateQueryTlsInterestV2{};
}

const common::Status& DistributedVectorAggregateQueryTlsServerV2::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "vector aggregate query v2 TLS server is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::cluster
