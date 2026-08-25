#include "chronos/cluster/distributed_vector_grouped_aggregate_query_tls_v2.hpp"

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
[[nodiscard]] bool
valid_limits(const DistributedVectorGroupedAggregateQueryTlsLimitsV2& limits) noexcept {
  const auto maximum =
      std::chrono::duration_cast<std::chrono::milliseconds>(TimePoint::duration::max());
  constexpr std::size_t kMinimumResponseBytes =
      kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize +
      kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize;
  return limits.handshake_timeout.count() > 0 && limits.handshake_timeout <= maximum &&
         limits.exchange_timeout.count() > 0 && limits.exchange_timeout <= maximum &&
         limits.maximum_response_frames > 0U &&
         limits.maximum_response_frames <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups &&
         limits.maximum_response_frames <= limits.payload.maximum_groups &&
         limits.maximum_response_bytes >= kMinimumResponseBytes &&
         limits.maximum_response_bytes <=
             kMaximumDistributedVectorGroupedAggregateQueryV2ResponseBytes &&
         limits.payload.maximum_frame_length >=
             query::distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength &&
         limits.payload.maximum_frame_length <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumFrameLength &&
         limits.payload.maximum_key_payload_bytes > 0U &&
         limits.payload.maximum_key_payload_bytes <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumKeyPayloadBytes &&
         limits.payload.maximum_groups > 0U &&
         limits.payload.maximum_groups <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups &&
         limits.payload.maximum_group_keys > 0U &&
         limits.payload.maximum_group_keys <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroupKeys &&
         limits.payload.maximum_aggregates <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumAggregates &&
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

class DistributedVectorGroupedAggregateQueryTlsClientV2::Impl {
public:
  Impl(network::TlsSocket socket, DistributedVectorQueryFrameV2WriteCursor request,
       ClientIdentity identity, const raft::NodeId target,
       std::vector<query::VectorGroupKeyDefinition>&& keys,
       std::vector<query::VectorAggregateDefinition>&& aggregates,
       query::QueryResourceContext resources,
       const DistributedVectorGroupedAggregateQueryTlsClientConfigV2 config, const TimePoint now)
      : socket_(std::move(socket)), request_(std::move(request)), identity_(identity),
        target_(target), config_(config),
        deadline_(deadline_after(now, config.limits.handshake_timeout)),
        response_reader_(
            std::move(keys), std::move(aggregates), std::move(resources),
            std::min(config.limits.maximum_response_bytes,
                     static_cast<std::size_t>(
                         kMaximumDistributedVectorGroupedAggregateQueryResponseV2Size)),
            config.limits.payload) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ != DistributedVectorGroupedAggregateQueryTlsStateV2::kFailed) {
      failure_ = std::move(status);
      state_ = DistributedVectorGroupedAggregateQueryTlsStateV2::kFailed;
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
      return fail(unauthenticated(
          "vector grouped aggregate query v2 server principal is not authenticated"));
    }
    auto authorized =
        config_.node_authorizer->authorize_node(authentication->principal_id, target_);
    if (!authorized.has_value())
      return fail(authorized.error());
    if (!*authorized) {
      return fail(unauthenticated(
          "TLS server principal cannot claim vector grouped aggregate query v2 target node"));
    }
    state_ = DistributedVectorGroupedAggregateQueryTlsStateV2::kWritingRequest;
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
      return fail(unavailable("vector grouped aggregate query v2 TLS handshake closed"));
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
      return fail(unavailable("vector grouped aggregate query v2 request socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("vector grouped aggregate query v2 request write made no progress"));
    const common::Status consumed = request_.consume_written(progress->bytes_transferred);
    if (!consumed.is_ok())
      return fail(consumed);
    if (request_.complete()) {
      state_ = DistributedVectorGroupedAggregateQueryTlsStateV2::kReadingResponses;
      interest_ = {.want_read = true};
    } else {
      interest_ = {.want_write = true};
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status
  accept_response(DistributedVectorGroupedAggregateQueryResponseV2 response) {
    if (response.source_node_id != identity_.target_node_id ||
        response.target_node_id != identity_.source_node_id ||
        response.query_id != identity_.query_id || response.tablet_id != identity_.tablet_id) {
      return fail(corruption("vector grouped aggregate query v2 TLS response is not correlated"));
    }
    if (responses_.size() == config_.limits.maximum_response_frames)
      return fail(exhausted("vector grouped aggregate query v2 TLS response frame limit exceeded"));
    bool terminal{};
    if (response.status_code == common::StatusCode::kOk) {
      if (!response.payload.has_value())
        return fail(corruption("vector grouped aggregate query v2 TLS success has no payload"));
      const std::size_t ordinal = responses_.size();
      const auto& position = response.payload->position();
      const bool valid_empty = position.empty && ordinal == 0U && position.group_count == 0U &&
                               position.group_ordinal == 0U && position.sequence == 1U &&
                               position.terminal;
      const bool valid_group = !position.empty && position.group_count > 0U &&
                               position.group_count <= config_.limits.maximum_response_frames &&
                               position.group_ordinal == ordinal &&
                               position.sequence == ordinal + 1U &&
                               position.terminal == (ordinal + 1U == position.group_count);
      if ((!valid_empty && !valid_group) ||
          (expected_group_count_.has_value() && position.group_count != *expected_group_count_)) {
        return fail(
            corruption("vector grouped aggregate query v2 TLS response sequence is invalid"));
      }
      if (!expected_group_count_.has_value())
        expected_group_count_ = position.group_count;
      terminal = position.terminal;
    } else {
      if (!responses_.empty() || response.payload.has_value())
        return fail(corruption("vector grouped aggregate query v2 TLS failure stream is invalid"));
      terminal = true;
    }
    try {
      responses_.push_back(std::move(response));
    } catch (const std::bad_alloc&) {
      return fail(exhausted("vector grouped aggregate query v2 TLS response allocation failed"));
    } catch (const std::length_error&) {
      return fail(exhausted("vector grouped aggregate query v2 TLS response count exceeds limits"));
    }
    if (terminal) {
      state_ = DistributedVectorGroupedAggregateQueryTlsStateV2::kComplete;
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
      return fail(
          unavailable("vector grouped aggregate query v2 response socket closed before terminal"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("vector grouped aggregate query v2 response read made no progress"));
    std::size_t offset{};
    const common::ByteView received =
        common::ByteView{response_scratch_}.first(progress->bytes_transferred);
    while (offset < received.size()) {
      auto step = response_reader_.consume(received.subspan(offset));
      if (!step.has_value())
        return fail(step.error());
      if (step->consumed_bytes == 0U)
        return fail(
            corruption("vector grouped aggregate query v2 response reader made no progress"));
      if (step->consumed_bytes > config_.limits.maximum_response_bytes - received_response_bytes_)
        return fail(
            exhausted("vector grouped aggregate query v2 TLS response byte limit exceeded"));
      received_response_bytes_ += step->consumed_bytes;
      offset += step->consumed_bytes;
      if (step->response.has_value()) {
        common::Status accepted = accept_response(std::move(step->response).value());
        if (!accepted.is_ok())
          return accepted;
        if (state_ == DistributedVectorGroupedAggregateQueryTlsStateV2::kComplete &&
            offset != received.size()) {
          return fail(
              corruption("vector grouped aggregate query v2 terminal has a coalesced suffix"));
        }
      }
    }
    if (state_ != DistributedVectorGroupedAggregateQueryTlsStateV2::kComplete)
      interest_ = {.want_read = true};
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  DistributedVectorQueryFrameV2WriteCursor request_;
  ClientIdentity identity_;
  raft::NodeId target_{};
  std::optional<std::uint32_t> expected_group_count_;
  DistributedVectorGroupedAggregateQueryTlsClientConfigV2 config_;
  TimePoint deadline_;
  DistributedVectorGroupedAggregateQueryTlsStateV2 state_{
      DistributedVectorGroupedAggregateQueryTlsStateV2::kHandshaking};
  DistributedVectorGroupedAggregateQueryTlsInterestV2 interest_{.want_write = true};
  DistributedVectorGroupedAggregateQueryResponseV2Reader response_reader_;
  std::array<std::byte, kTlsScratchSize> response_scratch_{};
  std::size_t received_response_bytes_{};
  std::vector<DistributedVectorGroupedAggregateQueryResponseV2> responses_;
  common::Status failure_{common::StatusCode::kInternal,
                          "vector grouped aggregate query v2 TLS client has not failed"};
};

class DistributedVectorGroupedAggregateQueryTlsServerV2::Impl {
public:
  Impl(network::TlsSocket socket,
       const DistributedVectorGroupedAggregateQueryTlsServerConfigV2 config,
       const TimePoint now) noexcept
      : socket_(std::move(socket)), config_(config),
        deadline_(deadline_after(now, config.limits.handshake_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ != DistributedVectorGroupedAggregateQueryTlsStateV2::kFailed) {
      failure_ = std::move(status);
      state_ = DistributedVectorGroupedAggregateQueryTlsStateV2::kFailed;
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
      return fail(unauthenticated(
          "vector grouped aggregate query v2 client principal is not authenticated"));
    }
    authenticated_peer_ = *authentication;
    state_ = DistributedVectorGroupedAggregateQueryTlsStateV2::kReadingRequest;
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
      return fail(unavailable("vector grouped aggregate query v2 TLS handshake closed"));
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
  validate_response_stream(DistributedVectorGroupedAggregateQueryBoundResponsesV2 bound,
                           const DistributedVectorQueryRequestV2& request) {
    common::Status authority_status =
        validate_distributed_vector_grouped_aggregate_query_authority_v2(
            request.dispatch, bound.authority.keys, bound.authority.aggregates);
    if (!authority_status.is_ok())
      return authority_status;
    if (bound.authority.keys.size() > config_.limits.payload.maximum_group_keys ||
        bound.authority.aggregates.size() > config_.limits.payload.maximum_aggregates ||
        bound.encoded_responses.empty() ||
        bound.encoded_responses.size() > config_.limits.maximum_response_frames) {
      return exhausted(
          "vector grouped aggregate query v2 receiver response count exceeds TLS limit");
    }
    std::size_t total_bytes{};
    for (const auto& response : bound.encoded_responses) {
      if (response.size() > config_.limits.maximum_response_bytes - total_bytes)
        return exhausted(
            "vector grouped aggregate query v2 receiver response bytes exceed TLS limit");
      total_bytes += response.size();
    }
    auto resources = query::QueryResourceContext::create(config_.limits.maximum_response_bytes);
    if (!resources.has_value())
      return resources.error();
    try {
      response_writers_.reserve(bound.encoded_responses.size());
      for (std::size_t index = 0U; index < bound.encoded_responses.size(); ++index) {
        auto decoded = decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
            bound.encoded_responses[index], bound.authority.keys, bound.authority.aggregates,
            *resources, config_.limits.payload);
        if (!decoded.has_value())
          return decoded.error();
        if (decoded->source_node_id != request.target_node_id ||
            decoded->target_node_id != request.source_node_id ||
            decoded->query_id != request.dispatch.dispatch.query_id ||
            decoded->tablet_id != request.dispatch.dispatch.tablet_id) {
          return corruption(
              "vector grouped aggregate query v2 receiver response is not correlated");
        }
        const bool last = index + 1U == bound.encoded_responses.size();
        if (decoded->status_code == common::StatusCode::kOk) {
          if (!decoded->payload.has_value())
            return corruption(
                "vector grouped aggregate query v2 receiver success vector is incomplete");
          // The immediately preceding guard proves presence while keeping the state borrowed.
          // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
          const auto& position = decoded->payload->position();
          const bool valid_empty = position.empty && bound.encoded_responses.size() == 1U &&
                                   position.group_count == 0U && position.group_ordinal == 0U &&
                                   position.sequence == 1U && position.terminal;
          const bool valid_groups = !position.empty &&
                                    position.group_count == bound.encoded_responses.size() &&
                                    position.group_ordinal == index &&
                                    position.sequence == index + 1U && position.terminal == last;
          if (!valid_empty && !valid_groups) {
            return corruption(
                "vector grouped aggregate query v2 receiver success vector is invalid");
          }
        } else if (bound.encoded_responses.size() != 1U || decoded->payload.has_value()) {
          return corruption(
              "vector grouped aggregate query v2 receiver failure response is invalid");
        }
        auto writer = DistributedVectorGroupedAggregateQueryResponseV2WriteCursor::create(
            *decoded, bound.authority.keys, bound.authority.aggregates);
        if (!writer.has_value())
          return writer.error();
        response_writers_.push_back(std::move(*writer));
      }
    } catch (const std::bad_alloc&) {
      return exhausted("vector grouped aggregate query v2 TLS response allocation failed");
    } catch (const std::length_error&) {
      return exhausted("vector grouped aggregate query v2 TLS response count exceeds limits");
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
      return fail(unavailable("vector grouped aggregate query v2 request socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("vector grouped aggregate query v2 request read made no progress"));
    const common::ByteView received =
        common::ByteView{request_scratch_}.first(progress->bytes_transferred);
    auto step = request_reader_.consume(received);
    if (!step.has_value())
      return fail(step.error());
    if (step->consumed_bytes != received.size())
      return fail(corruption("vector grouped aggregate query v2 request has a coalesced suffix"));
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
    state_ = DistributedVectorGroupedAggregateQueryTlsStateV2::kWritingResponses;
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
      return fail(unavailable("vector grouped aggregate query v2 response socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("vector grouped aggregate query v2 response write made no progress"));
    const common::Status consumed = writer.consume_written(progress->bytes_transferred);
    if (!consumed.is_ok())
      return fail(consumed);
    if (writer.complete()) {
      ++response_index_;
      if (response_index_ == response_writers_.size()) {
        state_ = DistributedVectorGroupedAggregateQueryTlsStateV2::kComplete;
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
  DistributedVectorGroupedAggregateQueryTlsServerConfigV2 config_;
  TimePoint deadline_;
  DistributedVectorGroupedAggregateQueryTlsStateV2 state_{
      DistributedVectorGroupedAggregateQueryTlsStateV2::kHandshaking};
  DistributedVectorGroupedAggregateQueryTlsInterestV2 interest_{.want_read = true};
  std::optional<network::PeerAuthenticationResult> authenticated_peer_;
  DistributedVectorQueryRequestV2Reader request_reader_;
  std::array<std::byte, kTlsScratchSize> request_scratch_{};
  std::vector<DistributedVectorGroupedAggregateQueryResponseV2WriteCursor> response_writers_;
  std::size_t response_index_{};
  common::Status failure_{common::StatusCode::kInternal,
                          "vector grouped aggregate query v2 TLS server has not failed"};
};

DistributedVectorGroupedAggregateQueryTlsClientV2::
    DistributedVectorGroupedAggregateQueryTlsClientV2(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateQueryTlsClientV2::
    ~DistributedVectorGroupedAggregateQueryTlsClientV2() = default;
DistributedVectorGroupedAggregateQueryTlsClientV2::
    DistributedVectorGroupedAggregateQueryTlsClientV2(
        DistributedVectorGroupedAggregateQueryTlsClientV2&&) noexcept = default;
DistributedVectorGroupedAggregateQueryTlsClientV2&
DistributedVectorGroupedAggregateQueryTlsClientV2::operator=(
    DistributedVectorGroupedAggregateQueryTlsClientV2&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateQueryTlsClientV2>
DistributedVectorGroupedAggregateQueryTlsClientV2::create(
    network::TlsSocket socket, DistributedVectorGroupedAggregateQueryAttemptV2 attempt,
    std::vector<query::VectorGroupKeyDefinition>&& keys,
    std::vector<query::VectorAggregateDefinition>&& aggregates,
    query::QueryResourceContext resources,
    const DistributedVectorGroupedAggregateQueryTlsClientConfigV2 config, const TimePoint now) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr ||
      !valid_limits<TimePoint>(config.limits) || attempt.attempt_number == 0U ||
      attempt.target_node_id == 0U || keys.size() > config.limits.payload.maximum_group_keys ||
      aggregates.size() > config.limits.payload.maximum_aggregates) {
    return common::make_unexpected(
        invalid("vector grouped aggregate query v2 TLS client configuration is invalid"));
  }
  auto request = decode_distributed_vector_query_request_v2_exact(attempt.request_bytes);
  if (!request.has_value())
    return common::make_unexpected(request.error());
  if (request->target_node_id != attempt.target_node_id) {
    return common::make_unexpected(
        invalid("vector grouped aggregate query v2 TLS attempt target is inconsistent"));
  }
  const common::Status authority_status =
      validate_distributed_vector_grouped_aggregate_query_authority_v2(request->dispatch, keys,
                                                                       aggregates);
  if (!authority_status.is_ok())
    return common::make_unexpected(authority_status);
  auto cursor = DistributedVectorQueryFrameV2WriteCursor::create_request(*request);
  if (!cursor.has_value())
    return common::make_unexpected(cursor.error());
  ClientIdentity identity{.source_node_id = request->source_node_id,
                          .target_node_id = request->target_node_id,
                          .query_id = request->dispatch.dispatch.query_id,
                          .tablet_id = request->dispatch.dispatch.tablet_id};
  try {
    return DistributedVectorGroupedAggregateQueryTlsClientV2{std::make_unique<Impl>(
        std::move(socket), std::move(*cursor), identity, attempt.target_node_id, std::move(keys),
        std::move(aggregates), std::move(resources), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("vector grouped aggregate query v2 TLS client allocation failed"));
  }
}

common::Status DistributedVectorGroupedAggregateQueryTlsClientV2::on_ready(const bool readable,
                                                                           const bool writable,
                                                                           const TimePoint now) {
  if (!implementation_)
    return invalid("vector grouped aggregate query v2 TLS client is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedVectorGroupedAggregateQueryTlsStateV2::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedVectorGroupedAggregateQueryTlsStateV2::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_) {
    return impl.fail(
        unavailable(impl.state_ == DistributedVectorGroupedAggregateQueryTlsStateV2::kHandshaking
                        ? "vector grouped aggregate query v2 TLS handshake timed out"
                        : "vector grouped aggregate query v2 TLS exchange timed out"));
  }
  if (impl.state_ == DistributedVectorGroupedAggregateQueryTlsStateV2::kHandshaking)
    return impl.advance_handshake(readable, writable, now);
  if (impl.state_ == DistributedVectorGroupedAggregateQueryTlsStateV2::kWritingRequest)
    return impl.advance_write(readable, writable);
  return impl.advance_read(readable, writable);
}

DistributedVectorGroupedAggregateQueryTlsStateV2
DistributedVectorGroupedAggregateQueryTlsClientV2::state() const noexcept {
  return implementation_ ? implementation_->state_
                         : DistributedVectorGroupedAggregateQueryTlsStateV2::kFailed;
}

DistributedVectorGroupedAggregateQueryTlsInterestV2
DistributedVectorGroupedAggregateQueryTlsClientV2::interest() const noexcept {
  return implementation_ ? implementation_->interest_
                         : DistributedVectorGroupedAggregateQueryTlsInterestV2{};
}

common::Result<std::span<const DistributedVectorGroupedAggregateQueryResponseV2>>
DistributedVectorGroupedAggregateQueryTlsClientV2::responses() const {
  if (!implementation_ ||
      implementation_->state_ != DistributedVectorGroupedAggregateQueryTlsStateV2::kComplete) {
    return common::make_unexpected(
        invalid("vector grouped aggregate query v2 TLS responses are unavailable"));
  }
  return std::span<const DistributedVectorGroupedAggregateQueryResponseV2>{
      implementation_->responses_};
}

const common::Status& DistributedVectorGroupedAggregateQueryTlsClientV2::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "vector grouped aggregate query v2 TLS client is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

DistributedVectorGroupedAggregateQueryTlsServerV2::
    DistributedVectorGroupedAggregateQueryTlsServerV2(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateQueryTlsServerV2::
    ~DistributedVectorGroupedAggregateQueryTlsServerV2() = default;
DistributedVectorGroupedAggregateQueryTlsServerV2::
    DistributedVectorGroupedAggregateQueryTlsServerV2(
        DistributedVectorGroupedAggregateQueryTlsServerV2&&) noexcept = default;
DistributedVectorGroupedAggregateQueryTlsServerV2&
DistributedVectorGroupedAggregateQueryTlsServerV2::operator=(
    DistributedVectorGroupedAggregateQueryTlsServerV2&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateQueryTlsServerV2>
DistributedVectorGroupedAggregateQueryTlsServerV2::create(
    network::TlsSocket socket, const DistributedVectorGroupedAggregateQueryTlsServerConfigV2 config,
    const TimePoint now) {
  if (config.authenticator == nullptr || config.receiver == nullptr ||
      !valid_limits<TimePoint>(config.limits)) {
    return common::make_unexpected(
        invalid("vector grouped aggregate query v2 TLS server configuration is invalid"));
  }
  try {
    return DistributedVectorGroupedAggregateQueryTlsServerV2{
        std::make_unique<Impl>(std::move(socket), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("vector grouped aggregate query v2 TLS server allocation failed"));
  }
}

common::Status DistributedVectorGroupedAggregateQueryTlsServerV2::on_ready(const bool readable,
                                                                           const bool writable,
                                                                           const TimePoint now) {
  if (!implementation_)
    return invalid("vector grouped aggregate query v2 TLS server is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedVectorGroupedAggregateQueryTlsStateV2::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedVectorGroupedAggregateQueryTlsStateV2::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_) {
    return impl.fail(
        unavailable(impl.state_ == DistributedVectorGroupedAggregateQueryTlsStateV2::kHandshaking
                        ? "vector grouped aggregate query v2 TLS handshake timed out"
                        : "vector grouped aggregate query v2 TLS exchange timed out"));
  }
  if (impl.state_ == DistributedVectorGroupedAggregateQueryTlsStateV2::kHandshaking)
    return impl.advance_handshake(readable, writable, now);
  if (impl.state_ == DistributedVectorGroupedAggregateQueryTlsStateV2::kReadingRequest)
    return impl.advance_read(readable, writable);
  return impl.advance_write(readable, writable);
}

DistributedVectorGroupedAggregateQueryTlsStateV2
DistributedVectorGroupedAggregateQueryTlsServerV2::state() const noexcept {
  return implementation_ ? implementation_->state_
                         : DistributedVectorGroupedAggregateQueryTlsStateV2::kFailed;
}

DistributedVectorGroupedAggregateQueryTlsInterestV2
DistributedVectorGroupedAggregateQueryTlsServerV2::interest() const noexcept {
  return implementation_ ? implementation_->interest_
                         : DistributedVectorGroupedAggregateQueryTlsInterestV2{};
}

const common::Status& DistributedVectorGroupedAggregateQueryTlsServerV2::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "vector grouped aggregate query v2 TLS server is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::cluster
