#include "chronos/network/client_session.hpp"
#include "chronos/network/connection_state.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/protocol.hpp"
#include "chronos/network/subscription_messages.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <span>

namespace {

void exercise_message(const chronos::network::Frame& frame) {
  using namespace chronos::network;
  const IngestProtocolContext ingest_context{.protocol_major = frame.header.protocol_major,
                                             .protocol_minor = frame.header.protocol_minor,
                                             .feature_bits =
                                                 frame.header.protocol_major == kProtocolV2Major
                                                     ? kProtocolV2SupportedFeatureBits
                                                     : kProtocolV1SupportedFeatureBits};
  switch (frame.header.message_type) {
  case MessageType::kClientHello:
    static_cast<void>(decode_client_hello(frame.payload));
    break;
  case MessageType::kServerHello:
    static_cast<void>(decode_server_hello(frame.payload));
    break;
  case MessageType::kIngestRequest:
    static_cast<void>(decode_ingest_request(frame.payload, ingest_context));
    break;
  case MessageType::kIngestAcknowledgement:
    static_cast<void>(decode_ingest_acknowledgement(frame.payload, ingest_context));
    break;
  case MessageType::kQuorumSyncIngestAcknowledgement:
    static_cast<void>(decode_quorum_sync_ingest_acknowledgement(frame.payload));
    break;
  case MessageType::kQueryRequest:
    static_cast<void>(decode_query_request(frame.payload));
    break;
  case MessageType::kQueryResult:
    static_cast<void>(decode_query_result_batch(frame.payload));
    break;
  case MessageType::kSubscribeRequest:
    static_cast<void>(decode_subscription_request(frame.payload));
    break;
  case MessageType::kSubscriptionReady:
    static_cast<void>(decode_subscription_ready(frame.payload));
    break;
  case MessageType::kSubscriptionChange:
    static_cast<void>(decode_subscription_change(frame.payload));
    static_cast<void>(decode_subscription_change(frame.payload,
                                                 {.protocol_major = frame.header.protocol_major,
                                                  .protocol_minor = frame.header.protocol_minor,
                                                  .feature_bits = kProtocolV1SubscriptionFeature}));
    break;
  case MessageType::kSubscriptionAcknowledge:
    static_cast<void>(decode_subscription_acknowledgement(frame.payload));
    break;
  case MessageType::kSubscriptionCheckpoint:
    static_cast<void>(decode_subscription_checkpoint(frame.payload));
    break;
  case MessageType::kSubscriptionEnd:
    static_cast<void>(decode_subscription_end(frame.payload));
    break;
  case MessageType::kError:
    static_cast<void>(decode_error_message(frame.payload));
    break;
  case MessageType::kQueryEnd:
  case MessageType::kCancel:
  case MessageType::kPing:
  case MessageType::kPong:
    break;
  }
}

void exercise(const chronos::common::ByteView bytes) {
  using namespace chronos::network;
  constexpr ProtocolLimits kLimits{.maximum_payload_size = 65'536U};
  const auto decoded = decode_frame(bytes, kLimits);
  if (decoded.has_value()) {
    const auto encoded = encode_frame({.protocol_major = decoded->header.protocol_major,
                                       .protocol_minor = decoded->header.protocol_minor,
                                       .message_type = decoded->header.message_type,
                                       .flags = decoded->header.flags,
                                       .request_id = decoded->header.request_id},
                                      decoded->payload, kLimits);
    if (!encoded.has_value() || !std::ranges::equal(*encoded, bytes))
      std::abort();
    exercise_message(*decoded);
    ConnectionStateConfig state_config;
    state_config.limits = kLimits;
    auto state = ServerConnectionState::create(state_config);
    if (!state.has_value())
      std::abort();
    static_cast<void>(state->accept(*decoded));
  }

  ConnectionBufferConfig buffer_config;
  buffer_config.protocol = kLimits;
  buffer_config.maximum_inbound_buffer_bytes = kFrameHeaderSize + kLimits.maximum_payload_size;
  auto buffers = ConnectionBuffers::create(buffer_config);
  if (!buffers.has_value())
    std::abort();
  const std::size_t split =
      bytes.empty()
          ? 0U
          : std::min(static_cast<std::size_t>(std::to_integer<std::uint8_t>(bytes.front())),
                     bytes.size());
  const auto first = buffers->receive(bytes.first(split));
  if (!first.has_value())
    return;
  const auto second = buffers->receive(bytes.subspan(split));
  if (second.has_value()) {
    for (const Frame& frame : *first)
      exercise_message(frame);
    for (const Frame& frame : *second)
      exercise_message(frame);
  }

  NativeClientConfig client_config;
  client_config.buffers = buffer_config;
  auto client = NativeClientSession::create(client_config);
  if (!client.has_value() || !client->queue_handshake().is_ok())
    std::abort();
  static_cast<void>(client->consume_written(client->pending_write().size()));
  static_cast<void>(client->receive(bytes));
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const chronos::common::ByteView bytes =
      chronos::common::byte_view(std::span<const std::uint8_t>{data, size});
  exercise(bytes);

  const auto hello_payload =
      chronos::network::encode_client_hello({.maximum_payload_size = 65'536U});
  if (!hello_payload.has_value())
    std::abort();
  auto hello =
      chronos::network::encode_frame({.message_type = chronos::network::MessageType::kClientHello},
                                     *hello_payload, {.maximum_payload_size = 65'536U});
  if (!hello.has_value())
    std::abort();
  if (!bytes.empty())
    (*hello)[std::to_integer<std::uint8_t>(bytes.front()) % hello->size()] ^= std::byte{1U};
  exercise(*hello);
  return 0;
}
