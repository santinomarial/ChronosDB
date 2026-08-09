#include "chronos/network/connection_state.hpp"

#include <algorithm>
#include <new>
#include <string>
#include <utility>

namespace chronos::network {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] bool server_only_type(const MessageType type) noexcept {
  return type == MessageType::kServerHello || type == MessageType::kIngestAcknowledgement ||
         type == MessageType::kQueryResult || type == MessageType::kQueryEnd ||
         type == MessageType::kSubscriptionReady || type == MessageType::kSubscriptionChange ||
         type == MessageType::kSubscriptionCheckpoint || type == MessageType::kSubscriptionEnd ||
         type == MessageType::kError || type == MessageType::kPong;
}

} // namespace

ServerConnectionState::ServerConnectionState(
    ConnectionStateConfig config, std::vector<std::uint64_t> active_requests,
    std::vector<MessageType> active_request_types, std::vector<bool> query_result_ended,
    std::vector<bool> subscription_ready, std::vector<bool> cancellation_requested,
    std::vector<std::uint64_t> subscription_last_delivery,
    std::vector<std::uint64_t> subscription_last_acknowledged,
    std::vector<std::uint64_t> subscription_last_checkpoint) noexcept
    : config_(config), active_requests_(std::move(active_requests)),
      active_request_types_(std::move(active_request_types)),
      query_result_ended_(std::move(query_result_ended)),
      subscription_ready_(std::move(subscription_ready)),
      cancellation_requested_(std::move(cancellation_requested)),
      subscription_last_delivery_(std::move(subscription_last_delivery)),
      subscription_last_acknowledged_(std::move(subscription_last_acknowledged)),
      subscription_last_checkpoint_(std::move(subscription_last_checkpoint)) {}

common::Result<ServerConnectionState>
ServerConnectionState::create(const ConnectionStateConfig& config) {
  if (const common::Status status = validate_protocol_limits(config.limits); !status.is_ok())
    return common::make_unexpected(status);
  if (config.maximum_in_flight_requests == 0U || config.maximum_in_flight_requests > 65'536U)
    return common::make_unexpected(invalid("connection in-flight request limit is invalid"));
  if ((config.supported_feature_bits & ~kProtocolV1SupportedFeatureBits) != 0U)
    return common::make_unexpected(invalid("connection feature mask contains unknown bits"));
  try {
    std::vector<std::uint64_t> active;
    std::vector<MessageType> active_types;
    std::vector<bool> result_ended;
    std::vector<bool> subscription_ready;
    std::vector<bool> cancellation_requested;
    std::vector<std::uint64_t> last_delivery;
    std::vector<std::uint64_t> last_acknowledged;
    std::vector<std::uint64_t> last_checkpoint;
    active.reserve(config.maximum_in_flight_requests);
    active_types.reserve(config.maximum_in_flight_requests);
    result_ended.reserve(config.maximum_in_flight_requests);
    subscription_ready.reserve(config.maximum_in_flight_requests);
    cancellation_requested.reserve(config.maximum_in_flight_requests);
    last_delivery.reserve(config.maximum_in_flight_requests);
    last_acknowledged.reserve(config.maximum_in_flight_requests);
    last_checkpoint.reserve(config.maximum_in_flight_requests);
    return ServerConnectionState{config,
                                 std::move(active),
                                 std::move(active_types),
                                 std::move(result_ended),
                                 std::move(subscription_ready),
                                 std::move(cancellation_requested),
                                 std::move(last_delivery),
                                 std::move(last_acknowledged),
                                 std::move(last_checkpoint)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("connection state allocation failed"));
  }
}

common::Result<InboundAction> ServerConnectionState::accept(const Frame& frame) {
  if (phase_ == ConnectionPhase::kClosed)
    return common::make_unexpected(invalid("connection is closed"));
  if (frame.header.payload_size != frame.payload.size())
    return common::make_unexpected(invalid("frame header and owned payload disagree"));
  if (server_only_type(frame.header.message_type))
    return common::make_unexpected(invalid("client sent a server-only message"));

  if (phase_ == ConnectionPhase::kAwaitingHello) {
    if (frame.header.message_type != MessageType::kClientHello || frame.header.request_id != 0U ||
        frame.header.protocol_minor != 0U)
      return common::make_unexpected(
          invalid("CLIENT_HELLO with request ID zero is required first"));
    const auto hello = decode_client_hello(frame.payload);
    if (!hello.has_value())
      return common::make_unexpected(hello.error());
    if (hello->minimum_major > kProtocolMajor || hello->maximum_major < kProtocolMajor)
      return common::make_unexpected(invalid("CLIENT_HELLO has no compatible Protocol v1 version"));
    negotiated_minor_ = std::min(hello->maximum_minor, kProtocolLatestMinor);
    negotiated_feature_bits_ =
        negotiated_minor_ == 0U ? 0U : hello->feature_bits & config_.supported_feature_bits;
    negotiated_maximum_payload_size_ =
        std::min(config_.limits.maximum_payload_size, hello->maximum_payload_size);
    phase_ = ConnectionPhase::kActive;
    return InboundAction{.kind = InboundActionKind::kHandshake,
                         .negotiated_maximum_payload_size = negotiated_maximum_payload_size_,
                         .negotiated_minor = negotiated_minor_,
                         .negotiated_feature_bits = negotiated_feature_bits_};
  }

  if (frame.header.message_type == MessageType::kClientHello)
    return common::make_unexpected(invalid("CLIENT_HELLO cannot be repeated"));
  if (frame.header.protocol_minor != negotiated_minor_)
    return common::make_unexpected(invalid("message minor does not match the negotiated version"));
  if (frame.payload.size() > negotiated_maximum_payload_size_)
    return common::make_unexpected(exhausted("message exceeds the negotiated payload limit"));
  if (frame.header.message_type == MessageType::kPing) {
    if (frame.header.request_id != 0U || !frame.payload.empty())
      return common::make_unexpected(invalid("PING requires request ID zero and an empty payload"));
    return InboundAction{.kind = InboundActionKind::kPing};
  }
  if (frame.header.message_type == MessageType::kSubscriptionAcknowledge) {
    if ((negotiated_feature_bits_ & kProtocolV1SubscriptionFeature) == 0U ||
        frame.header.request_id == 0U)
      return common::make_unexpected(invalid("subscription acknowledgement is not negotiated"));
    const auto found = std::ranges::find(active_requests_, frame.header.request_id);
    if (found == active_requests_.end())
      return common::make_unexpected(invalid("subscription acknowledgement request is inactive"));
    const std::size_t offset = static_cast<std::size_t>(found - active_requests_.begin());
    const auto acknowledgement = decode_subscription_acknowledgement(frame.payload);
    if (active_request_types_[offset] != MessageType::kSubscribeRequest ||
        !subscription_ready_[offset] || cancellation_requested_[offset] ||
        !acknowledgement.has_value() ||
        acknowledgement->delivery_sequence < subscription_last_acknowledged_[offset] ||
        acknowledgement->delivery_sequence > subscription_last_delivery_[offset]) {
      return common::make_unexpected(invalid("subscription acknowledgement state is invalid"));
    }
    subscription_last_acknowledged_[offset] = acknowledgement->delivery_sequence;
    return InboundAction{.kind = InboundActionKind::kSubscriptionAcknowledge,
                         .request_id = frame.header.request_id,
                         .acknowledged_delivery_sequence = acknowledgement->delivery_sequence};
  }
  if (frame.header.message_type == MessageType::kCancel) {
    if (frame.header.request_id == 0U || !frame.payload.empty() ||
        frame.header.request_id > last_request_id_)
      return common::make_unexpected(invalid("CANCEL request identity is invalid"));
    const auto found = std::ranges::find(active_requests_, frame.header.request_id);
    bool newly_active_cancellation = false;
    if (found != active_requests_.end()) {
      const auto offset = static_cast<std::size_t>(found - active_requests_.begin());
      if (active_request_types_[offset] == MessageType::kSubscribeRequest) {
        newly_active_cancellation = !cancellation_requested_[offset];
        cancellation_requested_[offset] = true;
      } else {
        newly_active_cancellation = true;
        erase_active(offset);
      }
    }
    return InboundAction{.kind = InboundActionKind::kCancel,
                         .request_id = frame.header.request_id,
                         .cancellation_was_active = newly_active_cancellation};
  }
  if (frame.header.message_type != MessageType::kIngestRequest &&
      frame.header.message_type != MessageType::kQueryRequest &&
      frame.header.message_type != MessageType::kSubscribeRequest)
    return common::make_unexpected(invalid("message type is invalid for an active client"));
  if (frame.header.request_id == 0U || frame.header.request_id <= last_request_id_)
    return common::make_unexpected(
        invalid("request IDs must increase strictly and cannot be reused"));
  if (active_requests_.size() == config_.maximum_in_flight_requests)
    return common::make_unexpected(exhausted("connection in-flight request limit is full"));
  if (frame.header.message_type == MessageType::kSubscribeRequest &&
      (negotiated_feature_bits_ & kProtocolV1SubscriptionFeature) == 0U) {
    return common::make_unexpected(invalid("subscription feature was not negotiated"));
  }
  if (frame.header.message_type == MessageType::kIngestRequest) {
    if (!decode_ingest_request(frame.payload,
                               {.maximum_payload_size = negotiated_maximum_payload_size_})
             .has_value())
      return common::make_unexpected(invalid("INGEST_REQUEST payload is invalid"));
  } else if (frame.header.message_type == MessageType::kQueryRequest &&
             !decode_query_request(frame.payload,
                                   {.maximum_payload_size = negotiated_maximum_payload_size_})
                  .has_value()) {
    return common::make_unexpected(invalid("QUERY_REQUEST payload is invalid"));
  } else if (frame.header.message_type == MessageType::kSubscribeRequest &&
             !decode_subscription_request(
                  frame.payload,
                  {.protocol = {.maximum_payload_size = negotiated_maximum_payload_size_}})
                  .has_value()) {
    return common::make_unexpected(invalid("SUBSCRIBE_REQUEST payload is invalid"));
  }
  active_requests_.push_back(frame.header.request_id);
  active_request_types_.push_back(frame.header.message_type);
  query_result_ended_.push_back(false);
  subscription_ready_.push_back(false);
  cancellation_requested_.push_back(false);
  subscription_last_delivery_.push_back(0U);
  subscription_last_acknowledged_.push_back(0U);
  subscription_last_checkpoint_.push_back(0U);
  last_request_id_ = frame.header.request_id;
  const InboundActionKind kind =
      frame.header.message_type == MessageType::kIngestRequest  ? InboundActionKind::kIngest
      : frame.header.message_type == MessageType::kQueryRequest ? InboundActionKind::kQuery
                                                                : InboundActionKind::kSubscribe;
  return InboundAction{.kind = kind, .request_id = frame.header.request_id};
}

common::Status ServerConnectionState::accept_response(const Frame& frame) {
  const auto found = std::ranges::find(active_requests_, frame.header.request_id);
  if (phase_ != ConnectionPhase::kActive || found == active_requests_.end())
    return invalid("response does not name an active request");
  const auto offset = static_cast<std::size_t>(found - active_requests_.begin());
  const MessageType request_type = active_request_types_[offset];
  switch (frame.header.message_type) {
  case MessageType::kQueryResult:
    if ((request_type != MessageType::kQueryRequest &&
         request_type != MessageType::kSubscribeRequest) ||
        query_result_ended_[offset] || subscription_ready_[offset] ||
        cancellation_requested_[offset] || (frame.header.flags & ~kFrameFlagEndStream) != 0U)
      return invalid("QUERY_RESULT response state is invalid");
    query_result_ended_[offset] = (frame.header.flags & kFrameFlagEndStream) != 0U;
    return common::Status::ok();
  case MessageType::kQueryEnd:
    if (request_type != MessageType::kQueryRequest || !query_result_ended_[offset])
      return invalid("QUERY_END requires a completed result stream");
    erase_active(offset);
    return common::Status::ok();
  case MessageType::kIngestAcknowledgement:
    if (request_type != MessageType::kIngestRequest)
      return invalid("ingest acknowledgement response state is invalid");
    erase_active(offset);
    return common::Status::ok();
  case MessageType::kSubscriptionReady:
    if (request_type != MessageType::kSubscribeRequest || !query_result_ended_[offset] ||
        subscription_ready_[offset] || cancellation_requested_[offset] ||
        !decode_subscription_ready(
             frame.payload,
             {.protocol = {.maximum_payload_size = negotiated_maximum_payload_size_}})
             .has_value())
      return invalid("SUBSCRIPTION_READY response state is invalid");
    subscription_ready_[offset] = true;
    return common::Status::ok();
  case MessageType::kSubscriptionChange: {
    const auto change = decode_subscription_change(
        frame.payload, {.protocol = {.maximum_payload_size = negotiated_maximum_payload_size_}});
    if (request_type != MessageType::kSubscribeRequest || !subscription_ready_[offset] ||
        cancellation_requested_[offset] || !change.has_value() ||
        (subscription_last_delivery_[offset] != 0U &&
         change->delivery_sequence != subscription_last_delivery_[offset] + 1U))
      return invalid("SUBSCRIPTION_CHANGE response state is invalid");
    subscription_last_delivery_[offset] = change->delivery_sequence;
    return common::Status::ok();
  }
  case MessageType::kSubscriptionCheckpoint: {
    const auto checkpoint = decode_subscription_checkpoint(
        frame.payload, {.protocol = {.maximum_payload_size = negotiated_maximum_payload_size_}});
    if (request_type != MessageType::kSubscribeRequest || !subscription_ready_[offset] ||
        cancellation_requested_[offset] || !checkpoint.has_value() ||
        checkpoint->acknowledged_delivery_sequence > subscription_last_acknowledged_[offset] ||
        checkpoint->acknowledged_delivery_sequence < subscription_last_checkpoint_[offset])
      return invalid("SUBSCRIPTION_CHECKPOINT response state is invalid");
    subscription_last_checkpoint_[offset] = checkpoint->acknowledged_delivery_sequence;
    return common::Status::ok();
  }
  case MessageType::kSubscriptionEnd:
    if (request_type != MessageType::kSubscribeRequest ||
        !decode_subscription_end(
             frame.payload,
             {.protocol = {.maximum_payload_size = negotiated_maximum_payload_size_}})
             .has_value())
      return invalid("SUBSCRIPTION_END response state is invalid");
    erase_active(offset);
    return common::Status::ok();
  case MessageType::kError:
    erase_active(offset);
    return common::Status::ok();
  default:
    return invalid("message type is not a shard response");
  }
}

void ServerConnectionState::erase_active(const std::size_t offset) noexcept {
  active_requests_.erase(active_requests_.begin() + static_cast<std::ptrdiff_t>(offset));
  active_request_types_.erase(active_request_types_.begin() + static_cast<std::ptrdiff_t>(offset));
  query_result_ended_.erase(query_result_ended_.begin() + static_cast<std::ptrdiff_t>(offset));
  subscription_ready_.erase(subscription_ready_.begin() + static_cast<std::ptrdiff_t>(offset));
  cancellation_requested_.erase(cancellation_requested_.begin() +
                                static_cast<std::ptrdiff_t>(offset));
  subscription_last_delivery_.erase(subscription_last_delivery_.begin() +
                                    static_cast<std::ptrdiff_t>(offset));
  subscription_last_acknowledged_.erase(subscription_last_acknowledged_.begin() +
                                        static_cast<std::ptrdiff_t>(offset));
  subscription_last_checkpoint_.erase(subscription_last_checkpoint_.begin() +
                                      static_cast<std::ptrdiff_t>(offset));
}

bool ServerConnectionState::complete(const std::uint64_t request_id) noexcept {
  const auto found = std::ranges::find(active_requests_, request_id);
  if (found == active_requests_.end())
    return false;
  const auto offset = static_cast<std::size_t>(found - active_requests_.begin());
  erase_active(offset);
  return true;
}

void ServerConnectionState::close() noexcept {
  active_requests_.clear();
  active_request_types_.clear();
  query_result_ended_.clear();
  subscription_ready_.clear();
  cancellation_requested_.clear();
  subscription_last_delivery_.clear();
  subscription_last_acknowledged_.clear();
  subscription_last_checkpoint_.clear();
  phase_ = ConnectionPhase::kClosed;
}

ConnectionPhase ServerConnectionState::phase() const noexcept {
  return phase_;
}
std::size_t ServerConnectionState::in_flight_requests() const noexcept {
  return active_requests_.size();
}
std::uint64_t ServerConnectionState::last_request_id() const noexcept {
  return last_request_id_;
}
std::uint32_t ServerConnectionState::negotiated_maximum_payload_size() const noexcept {
  return negotiated_maximum_payload_size_;
}
std::uint16_t ServerConnectionState::negotiated_minor() const noexcept {
  return negotiated_minor_;
}
std::uint64_t ServerConnectionState::negotiated_feature_bits() const noexcept {
  return negotiated_feature_bits_;
}
std::span<const std::uint64_t> ServerConnectionState::active_request_ids() const noexcept {
  return active_requests_;
}
std::optional<MessageType>
ServerConnectionState::active_request_type(const std::uint64_t request_id) const noexcept {
  const auto found = std::ranges::find(active_requests_, request_id);
  if (found == active_requests_.end())
    return std::nullopt;
  return active_request_types_[static_cast<std::size_t>(found - active_requests_.begin())];
}

} // namespace chronos::network
