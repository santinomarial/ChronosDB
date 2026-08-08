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
         type == MessageType::kError || type == MessageType::kPong;
}

} // namespace

ServerConnectionState::ServerConnectionState(ConnectionStateConfig config,
                                             std::vector<std::uint64_t> active_requests,
                                             std::vector<MessageType> active_request_types,
                                             std::vector<bool> query_result_ended) noexcept
    : config_(config), active_requests_(std::move(active_requests)),
      active_request_types_(std::move(active_request_types)),
      query_result_ended_(std::move(query_result_ended)) {}

common::Result<ServerConnectionState>
ServerConnectionState::create(const ConnectionStateConfig& config) {
  if (const common::Status status = validate_protocol_limits(config.limits); !status.is_ok())
    return common::make_unexpected(status);
  if (config.maximum_in_flight_requests == 0U || config.maximum_in_flight_requests > 65'536U)
    return common::make_unexpected(invalid("connection in-flight request limit is invalid"));
  try {
    std::vector<std::uint64_t> active;
    std::vector<MessageType> active_types;
    std::vector<bool> result_ended;
    active.reserve(config.maximum_in_flight_requests);
    active_types.reserve(config.maximum_in_flight_requests);
    result_ended.reserve(config.maximum_in_flight_requests);
    return ServerConnectionState{config, std::move(active), std::move(active_types),
                                 std::move(result_ended)};
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
    if (frame.header.message_type != MessageType::kClientHello || frame.header.request_id != 0U)
      return common::make_unexpected(
          invalid("CLIENT_HELLO with request ID zero is required first"));
    const auto hello = decode_client_hello(frame.payload);
    if (!hello.has_value())
      return common::make_unexpected(hello.error());
    if (hello->minimum_major > kProtocolMajor || hello->maximum_major < kProtocolMajor ||
        hello->maximum_minor < kProtocolMinor)
      return common::make_unexpected(invalid("CLIENT_HELLO has no compatible Protocol v1 version"));
    negotiated_maximum_payload_size_ =
        std::min(config_.limits.maximum_payload_size, hello->maximum_payload_size);
    phase_ = ConnectionPhase::kActive;
    return InboundAction{.kind = InboundActionKind::kHandshake,
                         .negotiated_maximum_payload_size = negotiated_maximum_payload_size_};
  }

  if (frame.header.message_type == MessageType::kClientHello)
    return common::make_unexpected(invalid("CLIENT_HELLO cannot be repeated"));
  if (frame.payload.size() > negotiated_maximum_payload_size_)
    return common::make_unexpected(exhausted("message exceeds the negotiated payload limit"));
  if (frame.header.message_type == MessageType::kPing) {
    if (frame.header.request_id != 0U || !frame.payload.empty())
      return common::make_unexpected(invalid("PING requires request ID zero and an empty payload"));
    return InboundAction{.kind = InboundActionKind::kPing};
  }
  if (frame.header.message_type == MessageType::kCancel) {
    if (frame.header.request_id == 0U || !frame.payload.empty() ||
        frame.header.request_id > last_request_id_)
      return common::make_unexpected(invalid("CANCEL request identity is invalid"));
    const auto found = std::ranges::find(active_requests_, frame.header.request_id);
    const bool active = found != active_requests_.end();
    if (active) {
      const auto offset = static_cast<std::size_t>(found - active_requests_.begin());
      erase_active(offset);
    }
    return InboundAction{.kind = InboundActionKind::kCancel,
                         .request_id = frame.header.request_id,
                         .cancellation_was_active = active};
  }
  if (frame.header.message_type != MessageType::kIngestRequest &&
      frame.header.message_type != MessageType::kQueryRequest)
    return common::make_unexpected(invalid("message type is invalid for an active client"));
  if (frame.header.request_id == 0U || frame.header.request_id <= last_request_id_)
    return common::make_unexpected(
        invalid("request IDs must increase strictly and cannot be reused"));
  if (active_requests_.size() == config_.maximum_in_flight_requests)
    return common::make_unexpected(exhausted("connection in-flight request limit is full"));
  if (frame.header.message_type == MessageType::kIngestRequest) {
    if (!decode_ingest_request(frame.payload,
                               {.maximum_payload_size = negotiated_maximum_payload_size_})
             .has_value())
      return common::make_unexpected(invalid("INGEST_REQUEST payload is invalid"));
  } else if (!decode_query_request(frame.payload,
                                   {.maximum_payload_size = negotiated_maximum_payload_size_})
                  .has_value()) {
    return common::make_unexpected(invalid("QUERY_REQUEST payload is invalid"));
  }
  active_requests_.push_back(frame.header.request_id);
  active_request_types_.push_back(frame.header.message_type);
  query_result_ended_.push_back(false);
  last_request_id_ = frame.header.request_id;
  return InboundAction{.kind = frame.header.message_type == MessageType::kIngestRequest
                                   ? InboundActionKind::kIngest
                                   : InboundActionKind::kQuery,
                       .request_id = frame.header.request_id};
}

common::Status ServerConnectionState::accept_response(const Frame& frame) {
  const auto found = std::ranges::find(active_requests_, frame.header.request_id);
  if (phase_ != ConnectionPhase::kActive || found == active_requests_.end())
    return invalid("response does not name an active request");
  const auto offset = static_cast<std::size_t>(found - active_requests_.begin());
  const MessageType request_type = active_request_types_[offset];
  switch (frame.header.message_type) {
  case MessageType::kQueryResult:
    if (request_type != MessageType::kQueryRequest || query_result_ended_[offset] ||
        (frame.header.flags & ~kFrameFlagEndStream) != 0U)
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
