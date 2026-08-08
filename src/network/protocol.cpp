#include "chronos/network/protocol.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/common/status.hpp"

#include <algorithm>
#include <limits>
#include <new>
#include <string>
#include <utility>

namespace chronos::network {
namespace {

constexpr std::size_t kHeaderCrcOffset = 36U;

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corrupt(std::string message) {
  return {common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status exhausted() {
  return {common::StatusCode::kResourceExhausted, "protocol frame allocation failed"};
}

[[nodiscard]] bool is_known_message_type(const std::uint16_t raw) noexcept {
  switch (static_cast<MessageType>(raw)) {
  case MessageType::kClientHello:
  case MessageType::kServerHello:
  case MessageType::kIngestRequest:
  case MessageType::kIngestAcknowledgement:
  case MessageType::kQueryRequest:
  case MessageType::kQueryResult:
  case MessageType::kQueryEnd:
  case MessageType::kCancel:
  case MessageType::kError:
  case MessageType::kPing:
  case MessageType::kPong:
    return true;
  }
  return false;
}

[[nodiscard]] common::Status validate_flags(const MessageType type, const std::uint32_t flags) {
  if ((flags & ~kKnownFrameFlags) != 0U)
    return corrupt("protocol frame contains unknown required flags");
  if ((flags & kFrameFlagEndStream) != 0U && type != MessageType::kQueryResult)
    return corrupt("END_STREAM is valid only on QUERY_RESULT");
  return common::Status::ok();
}

} // namespace

common::Status validate_protocol_limits(const ProtocolLimits& limits) {
  if (limits.maximum_payload_size == 0U)
    return invalid("protocol maximum payload size must be nonzero");
  if (limits.maximum_payload_size > kDefaultMaximumPayloadSize)
    return invalid("protocol maximum payload size exceeds the Protocol v1 ceiling");
  return common::Status::ok();
}

common::Result<std::size_t> encoded_frame_size(const std::size_t payload_size,
                                               const ProtocolLimits& limits) {
  if (const common::Status status = validate_protocol_limits(limits); !status.is_ok())
    return common::make_unexpected(status);
  if (payload_size > limits.maximum_payload_size ||
      payload_size > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(invalid("protocol payload exceeds the configured frame limit"));
  }
  const auto size = common::checked_add(kFrameHeaderSize, payload_size);
  if (!size.has_value())
    return common::make_unexpected(invalid("protocol frame size overflows size_t"));
  return *size;
}

common::Result<std::vector<std::byte>> encode_frame(const FrameDescriptor& descriptor,
                                                    const common::ByteView payload,
                                                    const ProtocolLimits& limits) {
  const MessageType message_type = descriptor.message_type;
  const std::uint32_t flags = descriptor.flags;
  if (!is_known_message_type(static_cast<std::uint16_t>(message_type)))
    return common::make_unexpected(invalid("protocol message type is unassigned"));
  if (const common::Status status = validate_flags(message_type, flags); !status.is_ok())
    return common::make_unexpected(invalid(status.message()));
  const common::Result<std::size_t> frame_size = encoded_frame_size(payload.size(), limits);
  if (!frame_size.has_value())
    return common::make_unexpected(frame_size.error());
  try {
    std::vector<std::byte> bytes(*frame_size);
    common::ByteWriter writer{bytes};
    if (const common::Status status = writer.write_u32_le(kProtocolMagic); !status.is_ok())
      return common::make_unexpected(status);
    if (const common::Status status = writer.write_u16_le(kProtocolMajor); !status.is_ok())
      return common::make_unexpected(status);
    if (const common::Status status = writer.write_u16_le(kProtocolMinor); !status.is_ok())
      return common::make_unexpected(status);
    if (const common::Status status =
            writer.write_u16_le(static_cast<std::uint16_t>(kFrameHeaderSize));
        !status.is_ok())
      return common::make_unexpected(status);
    if (const common::Status status = writer.write_u16_le(static_cast<std::uint16_t>(message_type));
        !status.is_ok())
      return common::make_unexpected(status);
    if (const common::Status status = writer.write_u32_le(flags); !status.is_ok())
      return common::make_unexpected(status);
    if (const common::Status status = writer.write_u64_le(descriptor.request_id); !status.is_ok())
      return common::make_unexpected(status);
    if (const common::Status status =
            writer.write_u32_le(static_cast<std::uint32_t>(payload.size()));
        !status.is_ok())
      return common::make_unexpected(status);
    if (const common::Status status = writer.write_u32_le(common::crc32c(payload)); !status.is_ok())
      return common::make_unexpected(status);
    if (const common::Status status = writer.write_u32_le(0U); !status.is_ok())
      return common::make_unexpected(status);
    if (const common::Status status =
            writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
        !status.is_ok())
      return common::make_unexpected(status);
    if (const common::Status status = writer.write_exact(payload); !status.is_ok())
      return common::make_unexpected(status);
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted());
  }
}

common::Result<FrameHeader> decode_frame_header(const common::ByteView bytes,
                                                const ProtocolLimits& limits) {
  if (const common::Status status = validate_protocol_limits(limits); !status.is_ok())
    return common::make_unexpected(status);
  if (bytes.size() < kFrameHeaderSize)
    return common::make_unexpected(corrupt("protocol frame header is truncated"));
  common::ByteReader reader{bytes.first(kFrameHeaderSize)};
  const auto magic = reader.read_u32_le();
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_size = reader.read_u16_le();
  const auto raw_type = reader.read_u16_le();
  const auto flags = reader.read_u32_le();
  const auto request_id = reader.read_u64_le();
  const auto payload_size = reader.read_u32_le();
  const auto payload_crc = reader.read_u32_le();
  const auto reserved = reader.read_u32_le();
  const auto stored_header_crc = reader.read_u32_le();
  if (!magic.has_value() || !major.has_value() || !minor.has_value() || !header_size.has_value() ||
      !raw_type.has_value() || !flags.has_value() || !request_id.has_value() ||
      !payload_size.has_value() || !payload_crc.has_value() || !reserved.has_value() ||
      !stored_header_crc.has_value()) {
    return common::make_unexpected(corrupt("protocol frame header cannot be decoded"));
  }
  if (*magic != kProtocolMagic)
    return common::make_unexpected(corrupt("protocol frame magic does not match Protocol v1"));
  if (*major != kProtocolMajor || *minor > kProtocolMinor)
    return common::make_unexpected(corrupt("protocol frame version is unsupported"));
  if (*header_size != kFrameHeaderSize)
    return common::make_unexpected(corrupt("protocol frame header size is unsupported"));
  if (!is_known_message_type(*raw_type))
    return common::make_unexpected(corrupt("protocol frame message type is unassigned"));
  if (*reserved != 0U)
    return common::make_unexpected(corrupt("protocol frame reserved field is nonzero"));
  if (*payload_size > limits.maximum_payload_size)
    return common::make_unexpected(corrupt("protocol payload exceeds the configured frame limit"));
  if (common::crc32c(bytes.first(kHeaderCrcOffset)) != *stored_header_crc)
    return common::make_unexpected(corrupt("protocol frame header CRC32C does not match"));
  const MessageType type = static_cast<MessageType>(*raw_type);
  if (const common::Status status = validate_flags(type, *flags); !status.is_ok())
    return common::make_unexpected(status);
  return FrameHeader{.protocol_major = *major,
                     .protocol_minor = *minor,
                     .message_type = type,
                     .flags = *flags,
                     .request_id = *request_id,
                     .payload_size = *payload_size,
                     .payload_crc32c = *payload_crc};
}

common::Result<Frame> decode_frame(const common::ByteView bytes, const ProtocolLimits& limits) {
  const common::Result<FrameHeader> header = decode_frame_header(bytes, limits);
  if (!header.has_value())
    return common::make_unexpected(header.error());
  const common::Result<std::size_t> frame_size = encoded_frame_size(header->payload_size, limits);
  if (!frame_size.has_value())
    return common::make_unexpected(frame_size.error());
  if (bytes.size() != *frame_size)
    return common::make_unexpected(corrupt("protocol frame length is not exact"));
  const common::ByteView payload = bytes.subspan(kFrameHeaderSize, header->payload_size);
  if (common::crc32c(payload) != header->payload_crc32c)
    return common::make_unexpected(corrupt("protocol payload CRC32C does not match"));
  try {
    return Frame{.header = *header,
                 .payload = std::vector<std::byte>(payload.begin(), payload.end())};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted());
  }
}

} // namespace chronos::network
