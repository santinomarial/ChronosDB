#include "chronos/network/client_session.hpp"

#include <algorithm>
#include <limits>
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

} // namespace

NativeClientSession::NativeClientSession(NativeClientConfig config, ConnectionBuffers buffers,
                                         std::vector<ActiveRequest> active) noexcept
    : config_(config), buffers_(std::move(buffers)), active_(std::move(active)) {}

common::Result<NativeClientSession> NativeClientSession::create(const NativeClientConfig& config) {
  if (config.maximum_in_flight_requests == 0U || config.maximum_in_flight_requests > 65'536U)
    return common::make_unexpected(invalid("client in-flight request limit is invalid"));
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
  auto encoded = encode_frame({.message_type = type, .flags = flags, .request_id = request_id},
                              payload, config_.buffers.protocol);
  if (!encoded.has_value())
    return encoded.error();
  return buffers_.enqueue(std::move(*encoded));
}

common::Status NativeClientSession::queue_handshake() {
  if (phase_ != ClientSessionPhase::kCreated)
    return invalid("client handshake can be queued exactly once");
  auto payload =
      encode_client_hello({.maximum_payload_size = config_.buffers.protocol.maximum_payload_size});
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
NativeClientSession::queue_ingest(const DurabilityMode durability,
                                  const common::ByteView encoded_columnar_append) {
  if (phase_ != ClientSessionPhase::kActive)
    return common::make_unexpected(invalid("client session is not active"));
  if (active_.size() == config_.maximum_in_flight_requests ||
      last_request_id_ == std::numeric_limits<std::uint64_t>::max())
    return common::make_unexpected(exhausted("client request admission is full"));
  auto payload = encode_ingest_request(durability, encoded_columnar_append,
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
  if (found != active_.end())
    erase_active(static_cast<std::size_t>(found - active_.begin()));
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
    if (frame.header.message_type != MessageType::kServerHello || frame.header.request_id != 0U)
      return invalid("SERVER_HELLO with request ID zero is required");
    const auto hello = decode_server_hello(frame.payload);
    if (!hello.has_value() ||
        hello->maximum_payload_size > config_.buffers.protocol.maximum_payload_size)
      return invalid("SERVER_HELLO negotiation is invalid");
    negotiated_maximum_payload_size_ = hello->maximum_payload_size;
    phase_ = ClientSessionPhase::kActive;
    return common::Status::ok();
  }
  if (phase_ != ClientSessionPhase::kActive)
    return invalid("client session is not accepting frames");
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
    if (found->type != MessageType::kQueryRequest || found->query_result_ended ||
        !decode_query_result_batch(
             frame.payload,
             {.protocol = {.maximum_payload_size = negotiated_maximum_payload_size_}})
             .has_value())
      return invalid("QUERY_RESULT response is invalid");
    found->query_result_ended = (frame.header.flags & kFrameFlagEndStream) != 0U;
    return common::Status::ok();
  case MessageType::kQueryEnd:
    if (found->type != MessageType::kQueryRequest || !found->query_result_ended ||
        !frame.payload.empty())
      return invalid("QUERY_END response is invalid");
    erase_active(offset);
    return common::Status::ok();
  case MessageType::kIngestAcknowledgement: {
    const auto acknowledgement = decode_ingest_acknowledgement(frame.payload);
    if (found->type != MessageType::kIngestRequest || !acknowledgement.has_value() ||
        acknowledgement->requested_durability != found->durability)
      return invalid("INGEST_ACKNOWLEDGEMENT response is invalid");
    erase_active(offset);
    return common::Status::ok();
  }
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

} // namespace chronos::network
