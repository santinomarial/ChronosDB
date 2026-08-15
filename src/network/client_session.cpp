#include "chronos/network/client_session.hpp"

#include <algorithm>
#include <limits>
#include <new>
#include <span>
#include <utility>

namespace chronos::network {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}
[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

} // namespace

NativeClientSession::NativeClientSession(NativeClientConfig config, ConnectionBuffers buffers,
                                         std::vector<ActiveRequest> active) noexcept
    : config_(config), buffers_(std::move(buffers)), active_(std::move(active)) {}

common::Result<NativeClientSession> NativeClientSession::create(const NativeClientConfig& config) {
  if (config.maximum_in_flight_requests == 0U || config.maximum_in_flight_requests > 65'536U)
    return common::make_unexpected(invalid("client in-flight request limit is invalid"));
  if (config.minimum_protocol_major == 0U ||
      config.minimum_protocol_major > config.maximum_protocol_major ||
      config.maximum_protocol_major > kProtocolLatestMajor ||
      config.maximum_protocol_minor > kProtocolLatestMinor ||
      (config.requested_feature_bits & ~kProtocolV2SupportedFeatureBits) != 0U ||
      (config.maximum_protocol_major == kProtocolMajor &&
       ((config.maximum_protocol_minor == 0U && config.requested_feature_bits != 0U) ||
        (config.requested_feature_bits &
         (kProtocolV2QuorumSyncFeature | kProtocolV2LeaderRedirectFeature)) != 0U)))
    return common::make_unexpected(invalid("client protocol extension configuration is invalid"));
  auto buffers = ConnectionBuffers::create(config.buffers);
  if (!buffers.has_value())
    return common::make_unexpected(buffers.error());
  try {
    std::vector<ActiveRequest> active;
    active.reserve(config.maximum_in_flight_requests);
    return NativeClientSession{config, std::move(*buffers), std::move(active)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("client session allocation failed"));
  }
}

common::Status NativeClientSession::queue_frame(const MessageType type,
                                                const std::uint64_t request_id,
                                                const common::ByteView payload,
                                                const std::uint32_t flags) {
  const std::uint16_t major =
      type == MessageType::kClientHello ? kProtocolMajor : negotiated_major_;
  const std::uint16_t minor = type == MessageType::kClientHello ? 0U : negotiated_minor_;
  auto encoded = encode_frame({.protocol_major = major,
                               .protocol_minor = minor,
                               .message_type = type,
                               .flags = flags,
                               .request_id = request_id},
                              payload, config_.buffers.protocol);
  if (!encoded.has_value())
    return encoded.error();
  return buffers_.enqueue(std::move(*encoded));
}

common::Status NativeClientSession::queue_handshake() {
  if (phase_ != ClientSessionPhase::kCreated)
    return invalid("client handshake can be queued exactly once");
  auto payload =
      encode_client_hello({.minimum_major = config_.minimum_protocol_major,
                           .maximum_major = config_.maximum_protocol_major,
                           .maximum_minor = config_.maximum_protocol_minor,
                           .feature_bits = config_.requested_feature_bits,
                           .maximum_payload_size = config_.buffers.protocol.maximum_payload_size});
  if (!payload.has_value())
    return payload.error();
  const common::Status status = queue_frame(MessageType::kClientHello, 0U, *payload);
  if (status.is_ok())
    phase_ = ClientSessionPhase::kAwaitingServerHello;
  return status;
}

common::Result<std::uint64_t> NativeClientSession::queue_query(const std::string_view sql) {
  if (phase_ != ClientSessionPhase::kActive)
    return common::make_unexpected(invalid("client session is not active"));
  if (active_.size() == config_.maximum_in_flight_requests ||
      last_request_id_ == std::numeric_limits<std::uint64_t>::max())
    return common::make_unexpected(exhausted("client request admission is full"));
  auto payload =
      encode_query_request(sql, {.maximum_payload_size = negotiated_maximum_payload_size_});
  if (!payload.has_value())
    return common::make_unexpected(payload.error());
  const std::uint64_t request_id = last_request_id_ + 1U;
  if (const common::Status status = queue_frame(MessageType::kQueryRequest, request_id, *payload);
      !status.is_ok())
    return common::make_unexpected(status);
  active_.push_back({.id = request_id, .type = MessageType::kQueryRequest});
  last_request_id_ = request_id;
  return request_id;
}

common::Result<std::uint64_t>
NativeClientSession::queue_subscription(const common::Uuid& subscription_id,
                                        const std::string_view sql) {
  if (phase_ != ClientSessionPhase::kActive ||
      (negotiated_feature_bits_ & kProtocolV1SubscriptionFeature) == 0U)
    return common::make_unexpected(invalid("client subscription feature is not active"));
  if (active_.size() == config_.maximum_in_flight_requests ||
      last_request_id_ == std::numeric_limits<std::uint64_t>::max())
    return common::make_unexpected(exhausted("client request admission is full"));
  const common::ByteView query = std::as_bytes(std::span{sql.data(), sql.size()});
  auto payload = encode_subscription_request(
      {.mode = SubscriptionStartMode::kNewQuery, .subscription_id = subscription_id, .body = query},
      {.protocol = {.maximum_payload_size = negotiated_maximum_payload_size_}});
  if (!payload.has_value())
    return common::make_unexpected(payload.error());
  const std::uint64_t request_id = last_request_id_ + 1U;
  if (const common::Status status =
          queue_frame(MessageType::kSubscribeRequest, request_id, *payload);
      !status.is_ok())
    return common::make_unexpected(status);
  active_.push_back({.id = request_id, .type = MessageType::kSubscribeRequest});
  last_request_id_ = request_id;
  return request_id;
}

common::Result<std::uint64_t>
NativeClientSession::queue_subscription_resume(const common::Uuid& subscription_id,
                                               const common::ByteView resume_token) {
  if (phase_ != ClientSessionPhase::kActive ||
      (negotiated_feature_bits_ & kProtocolV1SubscriptionFeature) == 0U)
    return common::make_unexpected(invalid("client subscription feature is not active"));
  if (active_.size() == config_.maximum_in_flight_requests ||
      last_request_id_ == std::numeric_limits<std::uint64_t>::max())
    return common::make_unexpected(exhausted("client request admission is full"));
  auto payload = encode_subscription_request(
      {.mode = SubscriptionStartMode::kResume,
       .subscription_id = subscription_id,
       .body = resume_token},
      {.protocol = {.maximum_payload_size = negotiated_maximum_payload_size_}});
  if (!payload.has_value())
    return common::make_unexpected(payload.error());
  const std::uint64_t request_id = last_request_id_ + 1U;
  if (const common::Status status =
          queue_frame(MessageType::kSubscribeRequest, request_id, *payload);
      !status.is_ok())
    return common::make_unexpected(status);
  active_.push_back({.id = request_id, .type = MessageType::kSubscribeRequest});
  last_request_id_ = request_id;
  return request_id;
}

common::Status NativeClientSession::queue_subscription_acknowledgement(
    const std::uint64_t request_id, const SubscriptionAcknowledgement& acknowledgement) {
  if (phase_ != ClientSessionPhase::kActive)
    return invalid("client session is not active");
  const auto found = std::ranges::find_if(
      active_, [&](const ActiveRequest& item) { return item.id == request_id; });
  if (found == active_.end() || found->type != MessageType::kSubscribeRequest ||
      !found->subscription_ready || found->cancellation_requested ||
      acknowledgement.delivery_sequence == 0U ||
      acknowledgement.delivery_sequence < found->subscription_last_acknowledged ||
      acknowledgement.delivery_sequence > found->subscription_last_delivery)
    return invalid("client subscription acknowledgement state is invalid");
  auto payload = encode_subscription_acknowledgement(acknowledgement);
  if (!payload.has_value())
    return payload.error();
  const common::Status status =
      queue_frame(MessageType::kSubscriptionAcknowledge, request_id, *payload);
  if (status.is_ok())
    found->subscription_last_acknowledged = acknowledgement.delivery_sequence;
  return status;
}

common::Result<std::uint64_t>
NativeClientSession::queue_ingest(const DurabilityMode durability,
                                  const common::ByteView encoded_columnar_append) {
  if (phase_ != ClientSessionPhase::kActive)
    return common::make_unexpected(invalid("client session is not active"));
  if (durability == DurabilityMode::kQuorumSync &&
      (negotiated_major_ != kProtocolV2Major ||
       (negotiated_feature_bits_ & kProtocolV2QuorumSyncFeature) == 0U))
    return common::make_unexpected(invalid("client QUORUM_SYNC feature is not active"));
  if (active_.size() == config_.maximum_in_flight_requests ||
      last_request_id_ == std::numeric_limits<std::uint64_t>::max())
    return common::make_unexpected(exhausted("client request admission is full"));
  auto payload = encode_ingest_request(durability, encoded_columnar_append,
                                       {.protocol_major = negotiated_major_,
                                        .protocol_minor = negotiated_minor_,
                                        .feature_bits = negotiated_feature_bits_},
                                       {.maximum_payload_size = negotiated_maximum_payload_size_});
  if (!payload.has_value())
    return common::make_unexpected(payload.error());
  const std::uint64_t request_id = last_request_id_ + 1U;
  if (const common::Status status = queue_frame(MessageType::kIngestRequest, request_id, *payload);
      !status.is_ok())
    return common::make_unexpected(status);
  active_.push_back(
      {.id = request_id, .type = MessageType::kIngestRequest, .durability = durability});
  last_request_id_ = request_id;
  return request_id;
}

common::Status NativeClientSession::queue_cancel(const std::uint64_t request_id) {
  if (phase_ != ClientSessionPhase::kActive || request_id == 0U || request_id > last_request_id_)
    return invalid("client cancellation identity is invalid");
  common::Status status = queue_frame(MessageType::kCancel, request_id, {});
  if (!status.is_ok())
    return status;
  const auto found = std::ranges::find_if(
      active_, [&](const ActiveRequest& item) { return item.id == request_id; });
  if (found != active_.end()) {
    if (found->type == MessageType::kSubscribeRequest)
      found->cancellation_requested = true;
    else
      erase_active(static_cast<std::size_t>(found - active_.begin()));
  }
  return common::Status::ok();
}

common::Status NativeClientSession::queue_ping() {
  if (phase_ != ClientSessionPhase::kActive)
    return invalid("client session is not active");
  return queue_frame(MessageType::kPing, 0U, {});
}

common::Status NativeClientSession::accept_server_frame(const Frame& frame) {
  if (frame.header.payload_size != frame.payload.size())
    return invalid("server frame header and payload disagree");
  if (phase_ == ClientSessionPhase::kAwaitingServerHello) {
    if (frame.header.message_type != MessageType::kServerHello || frame.header.request_id != 0U ||
        frame.header.protocol_major != kProtocolMajor || frame.header.protocol_minor != 0U)
      return invalid("SERVER_HELLO with request ID zero is required");
    const auto hello = decode_server_hello(frame.payload);
    if (!hello.has_value() || hello->selected_major < config_.minimum_protocol_major ||
        hello->selected_major > config_.maximum_protocol_major ||
        hello->selected_minor > config_.maximum_protocol_minor ||
        (hello->feature_bits & ~config_.requested_feature_bits) != 0U ||
        hello->maximum_payload_size > config_.buffers.protocol.maximum_payload_size)
      return invalid("SERVER_HELLO negotiation is invalid");
    negotiated_major_ = hello->selected_major;
    negotiated_minor_ = hello->selected_minor;
    negotiated_feature_bits_ = hello->feature_bits;
    negotiated_maximum_payload_size_ = hello->maximum_payload_size;
    phase_ = ClientSessionPhase::kActive;
    return common::Status::ok();
  }
  if (phase_ != ClientSessionPhase::kActive)
    return invalid("client session is not accepting frames");
  if (frame.header.protocol_major != negotiated_major_ ||
      frame.header.protocol_minor != negotiated_minor_)
    return invalid("server frame version does not match the negotiated version");
  if (frame.payload.size() > negotiated_maximum_payload_size_)
    return exhausted("server payload exceeds negotiated limit");
  if (frame.header.message_type == MessageType::kPong)
    return frame.header.request_id == 0U && frame.payload.empty()
               ? common::Status::ok()
               : invalid("PONG payload is invalid");
  const auto found = std::ranges::find_if(
      active_, [&](const ActiveRequest& item) { return item.id == frame.header.request_id; });
  if (found == active_.end())
    return invalid("server response does not name an active request");
  const auto offset = static_cast<std::size_t>(found - active_.begin());
  switch (frame.header.message_type) {
  case MessageType::kQueryResult:
    if ((found->type != MessageType::kQueryRequest &&
         found->type != MessageType::kSubscribeRequest) ||
        found->query_result_ended || found->subscription_ready || found->cancellation_requested ||
        !decode_query_result_batch(
             frame.payload,
             {.protocol = {.maximum_payload_size = negotiated_maximum_payload_size_}})
             .has_value())
      return invalid("QUERY_RESULT response is invalid");
    found->query_result_ended = (frame.header.flags & kFrameFlagEndStream) != 0U;
    found->query_result_started = true;
    return common::Status::ok();
  case MessageType::kQueryEnd:
    if (found->type != MessageType::kQueryRequest || !found->query_result_ended ||
        !frame.payload.empty())
      return invalid("QUERY_END response is invalid");
    erase_active(offset);
    return common::Status::ok();
  case MessageType::kIngestAcknowledgement: {
    const auto acknowledgement =
        decode_ingest_acknowledgement(frame.payload, {.protocol_major = negotiated_major_,
                                                      .protocol_minor = negotiated_minor_,
                                                      .feature_bits = negotiated_feature_bits_});
    if (found->type != MessageType::kIngestRequest || !acknowledgement.has_value() ||
        found->durability == DurabilityMode::kQuorumSync ||
        acknowledgement->requested_durability != found->durability)
      return invalid("INGEST_ACKNOWLEDGEMENT response is invalid");
    erase_active(offset);
    return common::Status::ok();
  }
  case MessageType::kQuorumSyncIngestAcknowledgement: {
    const auto acknowledgement = decode_quorum_sync_ingest_acknowledgement(frame.payload);
    if (found->type != MessageType::kIngestRequest ||
        found->durability != DurabilityMode::kQuorumSync || !acknowledgement.has_value())
      return invalid("QUORUM_SYNC acknowledgement response is invalid");
    erase_active(offset);
    return common::Status::ok();
  }
  case MessageType::kLeaderRedirect:
    if ((negotiated_feature_bits_ & kProtocolV2LeaderRedirectFeature) == 0U ||
        (found->type != MessageType::kIngestRequest && found->type != MessageType::kQueryRequest) ||
        found->query_result_started || !decode_leader_redirect(frame.payload).has_value())
      return invalid("LEADER_REDIRECT response is invalid");
    erase_active(offset);
    return common::Status::ok();
  case MessageType::kSubscriptionReady:
    if (found->type != MessageType::kSubscribeRequest || !found->query_result_ended ||
        found->subscription_ready || found->cancellation_requested ||
        !decode_subscription_ready(
             frame.payload,
             {.protocol = {.maximum_payload_size = negotiated_maximum_payload_size_}})
             .has_value())
      return invalid("SUBSCRIPTION_READY response is invalid");
    found->subscription_ready = true;
    return common::Status::ok();
  case MessageType::kSubscriptionChange: {
    const auto change = decode_subscription_change(
        frame.payload, {.protocol = {.maximum_payload_size = negotiated_maximum_payload_size_}});
    if (found->type != MessageType::kSubscribeRequest || !found->subscription_ready ||
        found->cancellation_requested || !change.has_value() ||
        (found->subscription_last_delivery != 0U &&
         change->delivery_sequence != found->subscription_last_delivery + 1U))
      return invalid("SUBSCRIPTION_CHANGE response is invalid");
    found->subscription_last_delivery = change->delivery_sequence;
    return common::Status::ok();
  }
  case MessageType::kSubscriptionCheckpoint: {
    const auto checkpoint = decode_subscription_checkpoint(
        frame.payload, {.protocol = {.maximum_payload_size = negotiated_maximum_payload_size_}});
    if (found->type != MessageType::kSubscribeRequest || !found->subscription_ready ||
        found->cancellation_requested || !checkpoint.has_value() ||
        checkpoint->acknowledged_delivery_sequence > found->subscription_last_acknowledged ||
        checkpoint->acknowledged_delivery_sequence < found->subscription_last_checkpoint)
      return invalid("SUBSCRIPTION_CHECKPOINT response is invalid");
    found->subscription_last_checkpoint = checkpoint->acknowledged_delivery_sequence;
    return common::Status::ok();
  }
  case MessageType::kSubscriptionEnd:
    if (found->type != MessageType::kSubscribeRequest ||
        !decode_subscription_end(
             frame.payload,
             {.protocol = {.maximum_payload_size = negotiated_maximum_payload_size_}})
             .has_value())
      return invalid("SUBSCRIPTION_END response is invalid");
    erase_active(offset);
    return common::Status::ok();
  case MessageType::kError:
    if (!decode_error_message(frame.payload,
                              {.maximum_payload_size = negotiated_maximum_payload_size_})
             .has_value())
      return invalid("ERROR response is invalid");
    erase_active(offset);
    return common::Status::ok();
  default:
    return invalid("server message direction is invalid");
  }
}

common::Result<std::vector<Frame>> NativeClientSession::receive(const common::ByteView bytes) {
  auto frames = buffers_.receive(bytes);
  if (!frames.has_value()) {
    close();
    return common::make_unexpected(frames.error());
  }
  for (const Frame& frame : *frames) {
    if (const common::Status status = accept_server_frame(frame); !status.is_ok()) {
      close();
      return common::make_unexpected(status);
    }
  }
  return frames;
}

void NativeClientSession::erase_active(const std::size_t offset) noexcept {
  active_.erase(active_.begin() + static_cast<std::ptrdiff_t>(offset));
}
common::ByteView NativeClientSession::pending_write() const noexcept {
  return buffers_.pending_write();
}
common::Status NativeClientSession::consume_written(const std::size_t bytes) noexcept {
  return buffers_.consume_written(bytes);
}
void NativeClientSession::close() noexcept {
  buffers_.clear();
  active_.clear();
  phase_ = ClientSessionPhase::kClosed;
}
ClientSessionPhase NativeClientSession::phase() const noexcept {
  return phase_;
}
std::size_t NativeClientSession::in_flight_requests() const noexcept {
  return active_.size();
}
std::uint32_t NativeClientSession::negotiated_maximum_payload_size() const noexcept {
  return negotiated_maximum_payload_size_;
}
std::uint16_t NativeClientSession::negotiated_major() const noexcept {
  return negotiated_major_;
}
std::uint16_t NativeClientSession::negotiated_minor() const noexcept {
  return negotiated_minor_;
}
std::uint64_t NativeClientSession::negotiated_feature_bits() const noexcept {
  return negotiated_feature_bits_;
}

} // namespace chronos::network
