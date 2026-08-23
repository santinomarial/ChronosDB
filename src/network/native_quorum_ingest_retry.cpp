#include "chronos/network/native_quorum_ingest_retry.hpp"

#include <algorithm>
#include <memory>
#include <new>
#include <utility>

namespace chronos::network {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

template <typename Value>
[[nodiscard]] Value* optional_pointer(std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
}

template <typename Value>
[[nodiscard]] const Value* optional_pointer(const std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
}

[[nodiscard]] NativeClientConfig client_config(const ConnectionBufferConfig& buffers) {
  return {.buffers = buffers,
          .maximum_in_flight_requests = 1U,
          .minimum_protocol_major = kProtocolV2Major,
          .maximum_protocol_major = kProtocolV2Major,
          .maximum_protocol_minor = kProtocolV2LatestMinor,
          .requested_feature_bits =
              kProtocolV2QuorumSyncFeature | kProtocolV2LeaderRedirectFeature};
}

[[nodiscard]] common::Status server_failure(const ProtocolErrorCode code) {
  switch (code) {
  case ProtocolErrorCode::kOverloaded:
  case ProtocolErrorCode::kUnknownRequest:
    return {common::StatusCode::kUnavailable, "native QUORUM_SYNC server rejected the attempt"};
  case ProtocolErrorCode::kCancelled:
    return {common::StatusCode::kCancelled, "native QUORUM_SYNC attempt was cancelled"};
  case ProtocolErrorCode::kUnauthorized:
    return {common::StatusCode::kUnauthenticated, "native QUORUM_SYNC attempt was not authorized"};
  case ProtocolErrorCode::kMalformedFrame:
  case ProtocolErrorCode::kUnsupportedVersion:
  case ProtocolErrorCode::kInvalidState:
  case ProtocolErrorCode::kDuplicateRequest:
  case ProtocolErrorCode::kInvalidRequest:
    return invalid("native QUORUM_SYNC server rejected the request");
  case ProtocolErrorCode::kExecutionFailure:
  case ProtocolErrorCode::kInternal:
    return {common::StatusCode::kInternal, "native QUORUM_SYNC server execution failed"};
  }
  return {common::StatusCode::kInternal, "native QUORUM_SYNC server error is unknown"};
}

} // namespace

class NativeQuorumIngestRetry::Impl {
public:
  Impl(NativeLeaderRedirectRouter owned_router, ConnectionBufferConfig configured_buffers,
       std::vector<std::byte> append) noexcept
      : router(std::move(owned_router)), buffers(configured_buffers),
        encoded_columnar_append(std::move(append)) {}

  [[nodiscard]] common::Result<NativeClientSession> make_attempt() const {
    auto created = NativeClientSession::create(client_config(buffers));
    if (!created.has_value())
      return common::make_unexpected(created.error());
    if (const common::Status status = created->queue_handshake(); !status.is_ok())
      return common::make_unexpected(status);
    return created;
  }

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state == NativeQuorumIngestRetryState::kRunning) {
      state = NativeQuorumIngestRetryState::kFailed;
      failure_status = std::move(status);
      if (client.has_value())
        client->close();
    }
    return failure_status;
  }

  [[nodiscard]] NativeLeaderRoute current_route() const noexcept {
    const auto found = std::ranges::lower_bound(router.routes(), router.current_node_id(), {},
                                                &NativeLeaderRoute::node_id);
    return *found;
  }

  NativeLeaderRedirectRouter router;
  ConnectionBufferConfig buffers;
  std::vector<std::byte> encoded_columnar_append;
  std::optional<NativeClientSession> client;
  std::optional<std::uint64_t> request_id;
  std::optional<QuorumSyncIngestAcknowledgement> acknowledgement;
  NativeQuorumIngestRetryState state{NativeQuorumIngestRetryState::kRunning};
  std::size_t attempts_started{};
  common::Status failure_status{common::StatusCode::kInternal,
                                "native QUORUM_SYNC retry has not failed"};
};

NativeQuorumIngestRetry::NativeQuorumIngestRetry(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}

NativeQuorumIngestRetry::~NativeQuorumIngestRetry() = default;
NativeQuorumIngestRetry::NativeQuorumIngestRetry(NativeQuorumIngestRetry&&) noexcept = default;
NativeQuorumIngestRetry&
NativeQuorumIngestRetry::operator=(NativeQuorumIngestRetry&&) noexcept = default;

common::Result<NativeQuorumIngestRetry>
NativeQuorumIngestRetry::create(NativeQuorumIngestRetryConfig config,
                                std::vector<std::byte> encoded_columnar_append) {
  auto validated = encode_ingest_request(
      DurabilityMode::kQuorumSync, encoded_columnar_append,
      {.protocol_major = kProtocolV2Major,
       .protocol_minor = kProtocolV2LatestMinor,
       .feature_bits = kProtocolV2QuorumSyncFeature | kProtocolV2LeaderRedirectFeature},
      config.buffers.protocol);
  if (!validated.has_value())
    return common::make_unexpected(validated.error());
  auto router = NativeLeaderRedirectRouter::create(std::move(config.routing));
  if (!router.has_value())
    return common::make_unexpected(router.error());
  try {
    auto implementation = std::make_unique<Impl>(std::move(*router), config.buffers,
                                                 std::move(encoded_columnar_append));
    auto attempt = implementation->make_attempt();
    if (!attempt.has_value())
      return common::make_unexpected(attempt.error());
    implementation->client.emplace(std::move(*attempt));
    implementation->attempts_started = 1U;
    return NativeQuorumIngestRetry{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("native QUORUM_SYNC retry allocation failed"));
  }
}

common::ByteView NativeQuorumIngestRetry::pending_write() const noexcept {
  if (!implementation_ || implementation_->state != NativeQuorumIngestRetryState::kRunning)
    return {};
  const NativeClientSession* client = optional_pointer(implementation_->client);
  return client != nullptr ? client->pending_write() : common::ByteView{};
}

common::Status NativeQuorumIngestRetry::consume_written(const std::size_t bytes) {
  if (!implementation_ || implementation_->state != NativeQuorumIngestRetryState::kRunning)
    return invalid("native QUORUM_SYNC retry is not writable");
  NativeClientSession* client = optional_pointer(implementation_->client);
  return client != nullptr ? client->consume_written(bytes)
                           : invalid("native QUORUM_SYNC retry has no client session");
}

common::Result<NativeQuorumIngestRetryProgress>
NativeQuorumIngestRetry::receive(const common::ByteView bytes) {
  if (!implementation_ || implementation_->state != NativeQuorumIngestRetryState::kRunning) {
    return common::make_unexpected(invalid("native QUORUM_SYNC retry is not receiving"));
  }
  Impl& impl = *implementation_;
  NativeClientSession* client = optional_pointer(impl.client);
  if (client == nullptr)
    return common::make_unexpected(impl.fail(invalid("native QUORUM_SYNC retry has no client")));
  auto frames = client->receive(bytes);
  if (!frames.has_value())
    return common::make_unexpected(impl.fail(frames.error()));

  NativeQuorumIngestRetryProgress progress{.attempt_number = impl.attempts_started};
  for (const Frame& frame : *frames) {
    if (frame.header.message_type == MessageType::kServerHello && !impl.request_id.has_value()) {
      constexpr std::uint64_t kRequiredFeatures =
          kProtocolV2QuorumSyncFeature | kProtocolV2LeaderRedirectFeature;
      if ((client->negotiated_feature_bits() & kRequiredFeatures) != kRequiredFeatures) {
        return common::make_unexpected(
            impl.fail(invalid("native QUORUM_SYNC retry features were not negotiated")));
      }
      auto request =
          client->queue_ingest(DurabilityMode::kQuorumSync, impl.encoded_columnar_append);
      if (!request.has_value())
        return common::make_unexpected(impl.fail(request.error()));
      impl.request_id = *request;
      continue;
    }
    if (!impl.request_id.has_value() || frame.header.request_id != *impl.request_id)
      continue;
    if (frame.header.message_type == MessageType::kQuorumSyncIngestAcknowledgement) {
      auto acknowledgement = decode_quorum_sync_ingest_acknowledgement(frame.payload);
      const auto prior = impl.router.last_authority();
      if (!acknowledgement.has_value() || acknowledgement->group_id != impl.router.group_id() ||
          acknowledgement->leader_node_id != impl.router.current_node_id() ||
          (prior.has_value() && acknowledgement->leader_term < prior->leader_term)) {
        return common::make_unexpected(
            impl.fail(invalid("native QUORUM_SYNC acknowledgement authority is invalid")));
      }
      impl.acknowledgement = *acknowledgement;
      impl.state = NativeQuorumIngestRetryState::kComplete;
      progress.acknowledgement = *acknowledgement;
      return progress;
    }
    if (frame.header.message_type == MessageType::kLeaderRedirect) {
      auto redirect = decode_leader_redirect(frame.payload);
      if (!redirect.has_value())
        return common::make_unexpected(impl.fail(redirect.error()));
      auto candidate = impl.make_attempt();
      if (!candidate.has_value())
        return common::make_unexpected(impl.fail(candidate.error()));
      auto target = impl.router.accept(*redirect);
      if (!target.has_value())
        return common::make_unexpected(impl.fail(target.error()));
      impl.client.emplace(std::move(*candidate));
      impl.request_id.reset();
      ++impl.attempts_started;
      progress.reconnect_required = true;
      progress.attempt_number = impl.attempts_started;
      return progress;
    }
    if (frame.header.message_type == MessageType::kError) {
      auto error = decode_error_message(frame.payload, impl.buffers.protocol);
      return common::make_unexpected(
          impl.fail(error.has_value() ? server_failure(error->code) : error.error()));
    }
  }
  return progress;
}

NativeQuorumIngestRetryState NativeQuorumIngestRetry::state() const noexcept {
  return implementation_ ? implementation_->state : NativeQuorumIngestRetryState::kFailed;
}

NativeLeaderRoute NativeQuorumIngestRetry::current_route() const noexcept {
  return implementation_ ? implementation_->current_route() : NativeLeaderRoute{};
}

std::size_t NativeQuorumIngestRetry::attempts_started() const noexcept {
  return implementation_ ? implementation_->attempts_started : 0U;
}

common::Result<QuorumSyncIngestAcknowledgement> NativeQuorumIngestRetry::result() const {
  if (!implementation_ || implementation_->state != NativeQuorumIngestRetryState::kComplete) {
    return common::make_unexpected(invalid("native QUORUM_SYNC result is not available"));
  }
  const QuorumSyncIngestAcknowledgement* acknowledgement =
      optional_pointer(implementation_->acknowledgement);
  return acknowledgement != nullptr
             ? common::Result<QuorumSyncIngestAcknowledgement>{*acknowledgement}
             : common::make_unexpected(invalid("native QUORUM_SYNC result is missing"));
}

const common::Status& NativeQuorumIngestRetry::failure() const {
  static const common::Status empty{common::StatusCode::kInternal,
                                    "native QUORUM_SYNC retry is empty"};
  return implementation_ ? implementation_->failure_status : empty;
}

} // namespace chronos::network
