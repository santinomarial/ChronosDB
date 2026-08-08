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
                                             std::vector<std::uint64_t> active_requests) noexcept
    : config_(config), active_requests_(std::move(active_requests)) {}

common::Result<ServerConnectionState>
ServerConnectionState::create(const ConnectionStateConfig& config) {
  if (const common::Status status = validate_protocol_limits(config.limits); !status.is_ok())
    return common::make_unexpected(status);
  if (config.maximum_in_flight_requests == 0U || config.maximum_in_flight_requests > 65'536U)
    return common::make_unexpected(invalid("connection in-flight request limit is invalid"));
  try {
    std::vector<std::uint64_t> active;
    active.reserve(config.maximum_in_flight_requests);
    return ServerConnectionState{config, std::move(active)};
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
    if (active)
      active_requests_.erase(found);
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
  last_request_id_ = frame.header.request_id;
  return InboundAction{.kind = frame.header.message_type == MessageType::kIngestRequest
                                   ? InboundActionKind::kIngest
                                   : InboundActionKind::kQuery,
                       .request_id = frame.header.request_id};
}

bool ServerConnectionState::complete(const std::uint64_t request_id) noexcept {
  const auto found = std::ranges::find(active_requests_, request_id);
  if (found == active_requests_.end())
    return false;
  active_requests_.erase(found);
  return true;
}

void ServerConnectionState::close() noexcept {
  active_requests_.clear();
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

} // namespace chronos::network
