#include "chronos/cluster/distributed_mutable_query_control_tls.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <new>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

inline constexpr std::size_t kScratchSize = std::size_t{16U} * 1024U;

[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}

[[nodiscard]] bool valid_limits(const DistributedMutableQueryControlTlsServerLimits& limits) {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      DistributedMutableQueryControlTlsServer::TimePoint::duration::max());
  constexpr std::size_t minimum_response_bytes =
      kDistributedVectorQueryResponseV2HeaderSize + kDistributedVectorQueryResponseV2TrailerSize;
  constexpr std::size_t minimum_grouped_response_bytes =
      kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize +
      kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize;
  return limits.handshake_timeout.count() > 0 && limits.handshake_timeout <= maximum &&
         limits.exchange_timeout.count() > 0 && limits.exchange_timeout <= maximum &&
         limits.maximum_mutable_response_frames > 0U &&
         limits.maximum_mutable_response_frames <= query::kMaximumDistributedCoordinatorMessages &&
         limits.maximum_mutable_response_bytes >= minimum_response_bytes &&
         limits.maximum_mutable_response_bytes <= kMaximumDistributedVectorQueryV2ResponseBytes &&
         limits.maximum_mutable_grouped_response_frames > 0U &&
         limits.maximum_mutable_grouped_response_frames <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups &&
         limits.maximum_mutable_grouped_response_frames <=
             limits.mutable_grouped_payload.maximum_groups &&
         limits.maximum_mutable_grouped_response_bytes >= minimum_grouped_response_bytes &&
         limits.maximum_mutable_grouped_response_bytes <=
             kMaximumDistributedVectorGroupedAggregateQueryV2ResponseBytes &&
         limits.maximum_mutable_grouped_decode_memory_bytes > 0U &&
         limits.maximum_mutable_grouped_decode_memory_bytes <=
             kMaximumDistributedVectorGroupedAggregateQueryV2DecodeMemoryBytes &&
         limits.mutable_grouped_payload.maximum_frame_length >=
             query::distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength &&
         limits.mutable_grouped_payload.maximum_frame_length <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumFrameLength &&
         limits.mutable_grouped_payload.maximum_key_payload_bytes > 0U &&
         limits.mutable_grouped_payload.maximum_key_payload_bytes <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumKeyPayloadBytes &&
         limits.mutable_grouped_payload.maximum_group_keys > 0U &&
         limits.mutable_grouped_payload.maximum_group_keys <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroupKeys &&
         limits.mutable_grouped_payload.maximum_aggregates <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumAggregates &&
         limits.mutable_grouped_payload.state.maximum_frame_length >=
             query::distributed_vector_aggregate_state_format::kMinimumFrameLength &&
         limits.mutable_grouped_payload.state.maximum_frame_length <=
             query::distributed_vector_aggregate_state_format::kMaximumFrameLength &&
         limits.mutable_grouped_payload.state.maximum_variable_extremum_bytes > 0U &&
         limits.mutable_grouped_payload.state.maximum_variable_extremum_bytes <=
             query::distributed_vector_aggregate_state_format::kMaximumExtremumBytes &&
         validate_distributed_vector_grouped_aggregate_shuffle_job_control_decode_limits(
             limits.grouped_shuffle_job_control)
             .is_ok();
}

[[nodiscard]] DistributedMutableQueryControlTlsServer::TimePoint
deadline_after(const DistributedMutableQueryControlTlsServer::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration =
      std::chrono::duration_cast<DistributedMutableQueryControlTlsServer::TimePoint::duration>(
          timeout);
  return now > DistributedMutableQueryControlTlsServer::TimePoint::max() - duration
             ? DistributedMutableQueryControlTlsServer::TimePoint::max()
             : now + duration;
}

} // namespace

class DistributedMutableQueryControlTlsServer::Impl {
public:
  Impl(network::TlsSocket socket, DistributedMutableQueryControlTlsServerConfig config,
       DistributedVectorGroupedAggregateShuffleJobControlRequestReader job_reader,
       const TimePoint now)
      : socket_(std::move(socket)), config_(config),
        deadline_(deadline_after(now, config.limits.handshake_timeout)),
        job_reader_(std::move(job_reader)) {}

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (state_ != DistributedMutableQueryControlTlsServerState::kFailed) {
      failure_ = std::move(failure);
      state_ = DistributedMutableQueryControlTlsServerState::kFailed;
      interest_ = {};
      mutable_writers_.clear();
      mutable_grouped_writers_.clear();
      authority_writer_.reset();
      job_writer_.reset();
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
                         "query-control client principal is not authenticated"));
    }
    authenticated_peer_ = *authenticated;
    state_ = DistributedMutableQueryControlTlsServerState::kReadingProtocol;
    interest_ = {.want_read = true};
    deadline_ = deadline_after(now, config_.limits.exchange_timeout);
    return read_protocol(false, false, now);
  }

  [[nodiscard]] common::Status handshake(const bool readable, const bool writable,
                                         const TimePoint now) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    auto progress = socket_.handshake();
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed) {
      return fail(status(common::StatusCode::kUnavailable, "query-control TLS handshake closed"));
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

  [[nodiscard]] common::Status read_protocol(const bool readable, const bool writable,
                                             const TimePoint now) {
    if ((!interest_.want_read || (!readable && socket_.pending_plaintext_bytes() == 0U)) &&
        (!interest_.want_write || !writable)) {
      return common::Status::ok();
    }
    auto progress = socket_.read(common::MutableByteView{protocol_magic_}.subspan(protocol_bytes_));
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(status(common::StatusCode::kUnavailable, "query-control protocol socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(
          status(common::StatusCode::kUnavailable, "query-control protocol read made no progress"));
    protocol_bytes_ += progress->bytes_transferred;
    if (protocol_bytes_ != protocol_magic_.size()) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }

    const common::ByteView magic{protocol_magic_};
    if (std::ranges::equal(magic, kDistributedMutableVectorQueryRequestMagicV1)) {
      protocol_ = DistributedMutableQueryControlProtocol::kMutableVectorQuery;
      auto consumed = mutable_reader_.consume(magic);
      if (!consumed.has_value())
        return fail(consumed.error());
      if (consumed->consumed_bytes != magic.size() || consumed->request.has_value())
        return fail(status(common::StatusCode::kCorruption,
                           "mutable query-control magic prefix is invalid"));
    } else if (std::ranges::equal(magic, kRaftReadAuthorityRequestMagicV1)) {
      protocol_ = DistributedMutableQueryControlProtocol::kRaftReadAuthority;
      auto consumed = authority_reader_.consume(magic);
      if (!consumed.has_value())
        return fail(consumed.error());
      if (consumed->consumed_bytes != magic.size() || consumed->request.has_value())
        return fail(status(common::StatusCode::kCorruption,
                           "read-authority query-control magic prefix is invalid"));
    } else if (
        config_.grouped_shuffle_job_service != nullptr &&
        (std::ranges::equal(
             magic,
             distributed_vector_grouped_aggregate_shuffle_job_control_format::kRequestMagic) ||
         std::ranges::equal(
             magic,
             distributed_vector_grouped_aggregate_shuffle_job_control_v2_format::kRequestMagic) ||
         std::ranges::equal(
             magic,
             distributed_vector_grouped_aggregate_shuffle_job_control_v3_format::kRequestMagic) ||
         std::ranges::equal(
             magic,
             distributed_vector_grouped_aggregate_shuffle_job_control_v4_format::kRequestMagic))) {
      protocol_ = DistributedMutableQueryControlProtocol::kGroupedShuffleJobControl;
      auto consumed = job_reader_.consume(magic);
      if (!consumed.has_value())
        return fail(consumed.error());
      if (consumed->consumed_bytes != magic.size() || consumed->request.has_value())
        return fail(status(common::StatusCode::kCorruption,
                           "grouped shuffle job-control magic prefix is invalid"));
    } else {
      return fail(status(common::StatusCode::kNotSupported,
                         "query-control application protocol is unsupported"));
    }
    state_ = DistributedMutableQueryControlTlsServerState::kReadingRequest;
    interest_ = {.want_read = true};
    return read_request(false, false, now);
  }

  [[nodiscard]] common::Status
  validate_mutable_responses(const std::vector<std::vector<std::byte>>& responses,
                             const DistributedMutableVectorQueryRequest& request) {
    if (responses.empty() || responses.size() > config_.limits.maximum_mutable_response_frames) {
      return status(common::StatusCode::kResourceExhausted,
                    "query-control mutable response count exceeds limit");
    }
    std::size_t total_bytes{};
    try {
      mutable_writers_.reserve(responses.size());
      for (std::size_t index = 0U; index < responses.size(); ++index) {
        if (responses[index].size() > config_.limits.maximum_mutable_response_bytes - total_bytes) {
          return status(common::StatusCode::kResourceExhausted,
                        "query-control mutable response bytes exceed limit");
        }
        total_bytes += responses[index].size();
        auto decoded = decode_distributed_vector_query_response_v2_exact(
            responses[index], request.fragment.result_schema);
        if (!decoded.has_value())
          return decoded.error();
        const bool last = index + 1U == responses.size();
        if (decoded->source_node_id != request.target_node_id ||
            decoded->target_node_id != request.source_node_id ||
            decoded->query_id != request.fragment.query_id ||
            decoded->tablet_id != request.fragment.tablet_id) {
          return status(common::StatusCode::kCorruption,
                        "query-control mutable response route is not correlated");
        }
        if (decoded->status_code == common::StatusCode::kOk) {
          if (!decoded->payload.has_value())
            return status(common::StatusCode::kCorruption,
                          "query-control mutable success response has no payload");
          const auto& payload =
              decoded->payload.value(); // NOLINT(bugprone-unchecked-optional-access)
          if (payload.sequence != index + 1U || payload.terminal != last) {
            return status(common::StatusCode::kCorruption,
                          "query-control mutable response stream is not terminally closed");
          }
        } else if (responses.size() != 1U || decoded->payload.has_value()) {
          return status(common::StatusCode::kCorruption,
                        "query-control mutable failure response stream is invalid");
        }
        auto writer = DistributedVectorQueryFrameV2WriteCursor::create_response(
            *decoded, request.fragment.result_schema);
        if (!writer.has_value())
          return writer.error();
        mutable_writers_.push_back(std::move(*writer));
      }
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return status(common::StatusCode::kResourceExhausted,
                    "query-control mutable response allocation failed");
    } catch (const std::length_error&) {
      return status(common::StatusCode::kResourceExhausted,
                    "query-control mutable response count exceeds container limits");
    }
  }

  [[nodiscard]] common::Status
  accept_mutable_request(const DistributedMutableVectorQueryRequest& request) {
    auto encoded = encode_distributed_mutable_vector_query_request(request);
    if (!encoded.has_value())
      return fail(encoded.error());
    const auto* authenticated =
        authenticated_peer_.transform([](const auto& value) { return &value; }).value_or(nullptr);
    if (authenticated == nullptr)
      return fail(
          status(common::StatusCode::kInternal, "query-control authenticated peer is unavailable"));
    if (request.fragment.plan.mode == query::DistributedVectorPlanMode::kGroupedAggregate)
      return accept_mutable_grouped_request(request, *encoded, *authenticated);
    auto responses = config_.mutable_receiver->receive(*encoded, *authenticated);
    if (!responses.has_value())
      return fail(responses.error());
    const common::Status valid = validate_mutable_responses(*responses, request);
    if (!valid.is_ok())
      return fail(valid);
    state_ = DistributedMutableQueryControlTlsServerState::kWritingResponse;
    interest_ = {.want_write = true};
    return common::Status::ok();
  }

  [[nodiscard]] common::Status validate_mutable_grouped_responses(
      DistributedMutableVectorGroupedAggregateQueryBoundResponses bound,
      const DistributedMutableVectorQueryRequest& request) {
    common::Status authority_status =
        validate_distributed_mutable_vector_grouped_aggregate_query_authority(
            request.fragment, bound.authority.keys, bound.authority.aggregates);
    if (!authority_status.is_ok())
      return authority_status;
    if (bound.authority.keys.size() > config_.limits.mutable_grouped_payload.maximum_group_keys ||
        bound.authority.aggregates.size() >
            config_.limits.mutable_grouped_payload.maximum_aggregates ||
        bound.encoded_responses.empty() ||
        bound.encoded_responses.size() > config_.limits.maximum_mutable_grouped_response_frames) {
      return status(common::StatusCode::kResourceExhausted,
                    "query-control mutable grouped response count exceeds limit");
    }
    std::size_t total_bytes{};
    for (const auto& response : bound.encoded_responses) {
      if (response.size() > config_.limits.maximum_mutable_grouped_response_bytes - total_bytes) {
        return status(common::StatusCode::kResourceExhausted,
                      "query-control mutable grouped response bytes exceed limit");
      }
      total_bytes += response.size();
    }
    auto resources = query::QueryResourceContext::create(
        config_.limits.maximum_mutable_grouped_decode_memory_bytes);
    if (!resources.has_value())
      return resources.error();
    try {
      mutable_grouped_writers_.reserve(bound.encoded_responses.size());
      for (std::size_t index = 0U; index < bound.encoded_responses.size(); ++index) {
        auto decoded = decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
            bound.encoded_responses[index], bound.authority.keys, bound.authority.aggregates,
            *resources, config_.limits.mutable_grouped_payload);
        if (!decoded.has_value())
          return decoded.error();
        if (decoded->source_node_id != request.target_node_id ||
            decoded->target_node_id != request.source_node_id ||
            decoded->query_id != request.fragment.query_id ||
            decoded->tablet_id != request.fragment.tablet_id) {
          return status(common::StatusCode::kCorruption,
                        "query-control mutable grouped response route is not correlated");
        }
        const bool last = index + 1U == bound.encoded_responses.size();
        if (decoded->status_code == common::StatusCode::kOk) {
          if (!decoded->payload.has_value()) {
            return status(common::StatusCode::kCorruption,
                          "query-control mutable grouped success response has no payload");
          }
          const auto& position = decoded->payload->position();
          const bool valid_empty = position.empty && bound.encoded_responses.size() == 1U &&
                                   position.group_count == 0U && position.group_ordinal == 0U &&
                                   position.sequence == 1U && position.terminal;
          const bool valid_groups = !position.empty &&
                                    position.group_count == bound.encoded_responses.size() &&
                                    position.group_ordinal == index &&
                                    position.sequence == index + 1U && position.terminal == last;
          if (!valid_empty && !valid_groups) {
            return status(common::StatusCode::kCorruption,
                          "query-control mutable grouped success response is invalid");
          }
        } else if (bound.encoded_responses.size() != 1U || decoded->payload.has_value()) {
          return status(common::StatusCode::kCorruption,
                        "query-control mutable grouped failure response is invalid");
        }
        auto writer = DistributedVectorGroupedAggregateQueryResponseV2WriteCursor::create(
            *decoded, bound.authority.keys, bound.authority.aggregates);
        if (!writer.has_value())
          return writer.error();
        mutable_grouped_writers_.push_back(std::move(*writer));
      }
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return status(common::StatusCode::kResourceExhausted,
                    "query-control mutable grouped response allocation failed");
    } catch (const std::length_error&) {
      return status(common::StatusCode::kResourceExhausted,
                    "query-control mutable grouped response exceeds container limits");
    }
  }

  [[nodiscard]] common::Status
  accept_mutable_grouped_request(const DistributedMutableVectorQueryRequest& request,
                                 const common::ByteView encoded,
                                 const network::PeerAuthenticationResult& authenticated) {
    auto responses = config_.mutable_grouped_receiver->receive_bound(encoded, authenticated);
    if (!responses.has_value())
      return fail(responses.error());
    const common::Status valid = validate_mutable_grouped_responses(std::move(*responses), request);
    if (!valid.is_ok())
      return fail(valid);
    protocol_ = DistributedMutableQueryControlProtocol::kMutableVectorGroupedAggregateQuery;
    state_ = DistributedMutableQueryControlTlsServerState::kWritingResponse;
    interest_ = {.want_write = true};
    return common::Status::ok();
  }

  [[nodiscard]] common::Status accept_authority_request(const RaftReadAuthorityRequest& request) {
    auto encoded = encode_raft_read_authority_request_v1(request);
    if (!encoded.has_value())
      return fail(encoded.error());
    const auto* authenticated =
        authenticated_peer_.transform([](const auto& value) { return &value; }).value_or(nullptr);
    if (authenticated == nullptr)
      return fail(
          status(common::StatusCode::kInternal, "query-control authenticated peer is unavailable"));
    auto response = config_.read_authority_receiver->receive(*encoded, *authenticated);
    if (!response.has_value())
      return fail(response.error());
    auto writer = RaftReadAuthorityFrameWriteCursor::create(
        std::move(*response), config_.limits.read_authority_transport);
    if (!writer.has_value())
      return fail(writer.error());
    authority_writer_.emplace(std::move(*writer));
    state_ = DistributedMutableQueryControlTlsServerState::kWritingResponse;
    interest_ = {.want_write = true};
    return common::Status::ok();
  }

  [[nodiscard]] common::Status accept_grouped_shuffle_job_request(
      DistributedVectorGroupedAggregateShuffleJobControlRequest request, const TimePoint now) {
    const auto* authenticated =
        authenticated_peer_.transform([](const auto& value) { return &value; }).value_or(nullptr);
    if (authenticated == nullptr)
      return fail(
          status(common::StatusCode::kInternal, "query-control authenticated peer is unavailable"));
    try {
      auto response =
          config_.grouped_shuffle_job_service->receive(std::move(request), *authenticated, now);
      if (!response.has_value())
        return fail(response.error());
      common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse> encoded =
          response->action ==
                  DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes
              ? encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v2(
                    *response)
          : response->action == DistributedVectorGroupedAggregateShuffleJobControlAction::kCancel
              ? encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v3(
                    *response)
          : response->action ==
                  DistributedVectorGroupedAggregateShuffleJobControlAction::kRenewLease
              ? encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v4(
                    *response)
              : encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1(
                    *response);
      if (!encoded.has_value())
        return fail(encoded.error());
      job_writer_.emplace(
          DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor::create(
              std::move(*encoded)));
      state_ = DistributedMutableQueryControlTlsServerState::kWritingResponse;
      interest_ = {.want_write = true};
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return fail(status(common::StatusCode::kResourceExhausted,
                         "query-control grouped shuffle job allocation failed"));
    } catch (...) {
      return fail(
          status(common::StatusCode::kInternal, "query-control grouped shuffle job service threw"));
    }
  }

  [[nodiscard]] common::Status read_request(const bool readable, const bool writable,
                                            const TimePoint now) {
    if ((!interest_.want_read || (!readable && socket_.pending_plaintext_bytes() == 0U)) &&
        (!interest_.want_write || !writable)) {
      return common::Status::ok();
    }
    auto progress = socket_.read(request_scratch_);
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(status(common::StatusCode::kUnavailable, "query-control request socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(
          status(common::StatusCode::kUnavailable, "query-control request read made no progress"));
    const common::ByteView received =
        common::ByteView{request_scratch_}.first(progress->bytes_transferred);
    if (protocol_ == DistributedMutableQueryControlProtocol::kMutableVectorQuery) {
      auto step = mutable_reader_.consume(received);
      if (!step.has_value())
        return fail(step.error());
      if (step->consumed_bytes != received.size())
        return fail(status(common::StatusCode::kCorruption,
                           "query-control mutable request has a coalesced suffix"));
      if (step->request.has_value()) {
        return accept_mutable_request(
            step->request.value()); // NOLINT(bugprone-unchecked-optional-access)
      }
    } else if (protocol_ == DistributedMutableQueryControlProtocol::kRaftReadAuthority) {
      auto step = authority_reader_.consume(received);
      if (!step.has_value())
        return fail(step.error());
      if (step->consumed_bytes != received.size())
        return fail(status(common::StatusCode::kCorruption,
                           "query-control authority request has a coalesced suffix"));
      if (step->request.has_value()) {
        return accept_authority_request(
            step->request.value()); // NOLINT(bugprone-unchecked-optional-access)
      }
    } else if (protocol_ == DistributedMutableQueryControlProtocol::kGroupedShuffleJobControl) {
      auto step = job_reader_.consume(received);
      if (!step.has_value())
        return fail(step.error());
      if (step->consumed_bytes != received.size())
        return fail(status(common::StatusCode::kCorruption,
                           "query-control grouped shuffle job request has a coalesced suffix"));
      if (step->request.has_value())
        return accept_grouped_shuffle_job_request(std::move(*step->request), now);
    } else {
      return fail(
          status(common::StatusCode::kInternal, "query-control request protocol is unavailable"));
    }
    interest_ = {.want_read = true};
    return common::Status::ok();
  }

  [[nodiscard]] common::Status write_response(const bool readable, const bool writable) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    common::ByteView pending;
    if (protocol_ == DistributedMutableQueryControlProtocol::kMutableVectorQuery) {
      if (mutable_writer_index_ >= mutable_writers_.size())
        return fail(status(common::StatusCode::kInternal,
                           "query-control mutable response writer is unavailable"));
      pending = mutable_writers_[mutable_writer_index_].pending_write();
    } else if (protocol_ ==
               DistributedMutableQueryControlProtocol::kMutableVectorGroupedAggregateQuery) {
      if (mutable_grouped_writer_index_ >= mutable_grouped_writers_.size())
        return fail(status(common::StatusCode::kInternal,
                           "query-control mutable grouped response writer is unavailable"));
      pending = mutable_grouped_writers_[mutable_grouped_writer_index_].pending_write();
    } else if (protocol_ == DistributedMutableQueryControlProtocol::kRaftReadAuthority &&
               authority_writer_.has_value()) {
      pending = authority_writer_->pending_write();
    } else if (protocol_ == DistributedMutableQueryControlProtocol::kGroupedShuffleJobControl &&
               job_writer_.has_value()) {
      pending = job_writer_->pending_write();
    } else {
      return fail(
          status(common::StatusCode::kInternal, "query-control response writer is unavailable"));
    }
    auto progress = socket_.write(pending);
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(status(common::StatusCode::kUnavailable, "query-control response socket closed"));
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
                         "query-control response write made no progress"));

    bool complete{};
    if (protocol_ == DistributedMutableQueryControlProtocol::kMutableVectorQuery) {
      auto& writer = mutable_writers_[mutable_writer_index_];
      const common::Status consumed = writer.consume_written(progress->bytes_transferred);
      if (!consumed.is_ok())
        return fail(consumed);
      if (writer.complete())
        ++mutable_writer_index_;
      complete = mutable_writer_index_ == mutable_writers_.size();
    } else if (protocol_ ==
               DistributedMutableQueryControlProtocol::kMutableVectorGroupedAggregateQuery) {
      auto& writer = mutable_grouped_writers_[mutable_grouped_writer_index_];
      const common::Status consumed = writer.consume_written(progress->bytes_transferred);
      if (!consumed.is_ok())
        return fail(consumed);
      if (writer.complete())
        ++mutable_grouped_writer_index_;
      complete = mutable_grouped_writer_index_ == mutable_grouped_writers_.size();
    } else if (protocol_ == DistributedMutableQueryControlProtocol::kRaftReadAuthority) {
      const common::Status consumed =
          authority_writer_->consume_written(progress->bytes_transferred);
      if (!consumed.is_ok())
        return fail(consumed);
      complete = authority_writer_->complete();
    } else {
      const common::Status consumed = job_writer_->consume_written(progress->bytes_transferred);
      if (!consumed.is_ok())
        return fail(consumed);
      complete = job_writer_->complete();
    }
    if (complete) {
      state_ = DistributedMutableQueryControlTlsServerState::kComplete;
      interest_ = {};
    } else {
      interest_ = {.want_write = true};
    }
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  DistributedMutableQueryControlTlsServerConfig config_;
  TimePoint deadline_;
  DistributedMutableQueryControlTlsServerState state_{
      DistributedMutableQueryControlTlsServerState::kHandshaking};
  DistributedMutableQueryControlProtocol protocol_{
      DistributedMutableQueryControlProtocol::kUndetermined};
  DistributedMutableQueryControlTlsInterest interest_{.want_read = true};
  std::optional<network::PeerAuthenticationResult> authenticated_peer_;
  std::array<std::byte, 8U> protocol_magic_{};
  std::size_t protocol_bytes_{};
  DistributedMutableVectorQueryRequestReader mutable_reader_;
  RaftReadAuthorityRequestReader authority_reader_;
  DistributedVectorGroupedAggregateShuffleJobControlRequestReader job_reader_;
  std::array<std::byte, kScratchSize> request_scratch_{};
  std::vector<DistributedVectorQueryFrameV2WriteCursor> mutable_writers_;
  std::size_t mutable_writer_index_{};
  std::vector<DistributedVectorGroupedAggregateQueryResponseV2WriteCursor> mutable_grouped_writers_;
  std::size_t mutable_grouped_writer_index_{};
  std::optional<RaftReadAuthorityFrameWriteCursor> authority_writer_;
  std::optional<DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor> job_writer_;
  common::Status failure_{common::StatusCode::kInternal, "query-control TLS server has not failed"};
};

DistributedMutableQueryControlTlsServer::DistributedMutableQueryControlTlsServer(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedMutableQueryControlTlsServer::~DistributedMutableQueryControlTlsServer() = default;
DistributedMutableQueryControlTlsServer::DistributedMutableQueryControlTlsServer(
    DistributedMutableQueryControlTlsServer&&) noexcept = default;
DistributedMutableQueryControlTlsServer& DistributedMutableQueryControlTlsServer::operator=(
    DistributedMutableQueryControlTlsServer&&) noexcept = default;

common::Result<DistributedMutableQueryControlTlsServer>
DistributedMutableQueryControlTlsServer::create(
    network::TlsSocket socket, const DistributedMutableQueryControlTlsServerConfig config,
    const TimePoint now) {
  if (config.authenticator == nullptr || config.mutable_receiver == nullptr ||
      config.mutable_grouped_receiver == nullptr || config.read_authority_receiver == nullptr ||
      !valid_limits(config.limits)) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "query-control TLS server configuration is invalid"));
  }
  auto authority_limits =
      RaftReadAuthorityResponseReader::create(config.limits.read_authority_transport);
  if (!authority_limits.has_value())
    return common::make_unexpected(authority_limits.error());
  auto job_reader = DistributedVectorGroupedAggregateShuffleJobControlRequestReader::create(
      config.limits.grouped_shuffle_job_control);
  if (!job_reader.has_value())
    return common::make_unexpected(job_reader.error());
  try {
    return DistributedMutableQueryControlTlsServer{
        std::make_unique<Impl>(std::move(socket), config, std::move(*job_reader), now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "query-control TLS server allocation failed"));
  }
}

common::Status DistributedMutableQueryControlTlsServer::on_ready(const bool readable,
                                                                 const bool writable,
                                                                 const TimePoint now) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "query-control TLS server is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedMutableQueryControlTlsServerState::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedMutableQueryControlTlsServerState::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_) {
    return impl.fail(
        status(common::StatusCode::kUnavailable,
               impl.state_ == DistributedMutableQueryControlTlsServerState::kHandshaking
                   ? "query-control TLS handshake timed out"
                   : "query-control TLS exchange timed out"));
  }
  if (impl.state_ == DistributedMutableQueryControlTlsServerState::kHandshaking)
    return impl.handshake(readable, writable, now);
  if (impl.state_ == DistributedMutableQueryControlTlsServerState::kReadingProtocol)
    return impl.read_protocol(readable, writable, now);
  if (impl.state_ == DistributedMutableQueryControlTlsServerState::kReadingRequest)
    return impl.read_request(readable, writable, now);
  return impl.write_response(readable, writable);
}

DistributedMutableQueryControlTlsServerState
DistributedMutableQueryControlTlsServer::state() const noexcept {
  return implementation_ ? implementation_->state_
                         : DistributedMutableQueryControlTlsServerState::kFailed;
}

DistributedMutableQueryControlProtocol
DistributedMutableQueryControlTlsServer::protocol() const noexcept {
  return implementation_ ? implementation_->protocol_
                         : DistributedMutableQueryControlProtocol::kUndetermined;
}

DistributedMutableQueryControlTlsInterest
DistributedMutableQueryControlTlsServer::interest() const noexcept {
  return implementation_ ? implementation_->interest_ : DistributedMutableQueryControlTlsInterest{};
}

const common::Status& DistributedMutableQueryControlTlsServer::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "query-control TLS server is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::cluster
