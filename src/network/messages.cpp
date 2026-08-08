#include "chronos/network/messages.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/schema/utf8.hpp"

#include <limits>
#include <new>
#include <string>
#include <utility>

namespace chronos::network {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corrupt(std::string message) {
  return {common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status exhausted() {
  return {common::StatusCode::kResourceExhausted, "protocol message allocation failed"};
}

[[nodiscard]] bool valid_durability(const std::uint8_t value) noexcept {
  return value == static_cast<std::uint8_t>(DurabilityMode::kAsync) ||
         value == static_cast<std::uint8_t>(DurabilityMode::kLocalSync);
}

[[nodiscard]] common::Result<std::vector<std::byte>> allocated(const std::size_t size) {
  try {
    return std::vector<std::byte>(size);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted());
  }
}

[[nodiscard]] common::Result<std::size_t> variable_payload_size(const std::size_t envelope,
                                                                const std::size_t body,
                                                                const ProtocolLimits& limits) {
  if (const common::Status status = validate_protocol_limits(limits); !status.is_ok())
    return common::make_unexpected(status);
  const auto size = common::checked_add(envelope, body);
  if (!size.has_value() || *size > limits.maximum_payload_size ||
      body > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(
        invalid("protocol message exceeds the configured payload limit"));
  }
  return *size;
}

[[nodiscard]] common::Status validate_hello_range(const ClientHello& hello) {
  if (hello.minimum_major == 0U || hello.minimum_major > hello.maximum_major)
    return invalid("CLIENT_HELLO protocol range is invalid");
  if ((hello.feature_bits & ~kProtocolV1FeatureBits) != 0U)
    return invalid("CLIENT_HELLO requests unknown feature bits");
  return validate_protocol_limits({.maximum_payload_size = hello.maximum_payload_size});
}

[[nodiscard]] common::Status validate_server_hello(const ServerHello& hello) {
  if (hello.selected_major != kProtocolMajor || hello.selected_minor != kProtocolMinor ||
      hello.feature_bits != kProtocolV1FeatureBits)
    return invalid("SERVER_HELLO selection is unsupported");
  return validate_protocol_limits({.maximum_payload_size = hello.maximum_payload_size});
}

[[nodiscard]] common::Status
validate_acknowledgement(const IngestAcknowledgement& acknowledgement) {
  if (!valid_durability(static_cast<std::uint8_t>(acknowledgement.requested_durability)) ||
      !valid_durability(static_cast<std::uint8_t>(acknowledgement.effective_durability)) ||
      (acknowledgement.outcome != IngestOutcome::kApplied &&
       acknowledgement.outcome != IngestOutcome::kMatchingRetry))
    return invalid("INGEST_ACKNOWLEDGEMENT fields are unassigned");
  if (acknowledgement.outcome == IngestOutcome::kApplied &&
      (acknowledgement.record_sequence == 0U || acknowledgement.segment_number == 0U))
    return invalid("applied acknowledgement requires a WAL position");
  if (acknowledgement.outcome == IngestOutcome::kMatchingRetry &&
      (acknowledgement.record_sequence != 0U || acknowledgement.segment_number != 0U ||
       acknowledgement.byte_offset != 0U))
    return invalid("matching retry acknowledgement cannot invent a WAL position");
  return common::Status::ok();
}

} // namespace

common::Result<std::vector<std::byte>> encode_client_hello(const ClientHello& hello) {
  if (const common::Status status = validate_hello_range(hello); !status.is_ok())
    return common::make_unexpected(status);
  auto bytes = allocated(kHelloPayloadSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::ByteWriter writer{*bytes};
  if (const common::Status status = writer.write_u16_le(kMessagePayloadFormat); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u16_le(hello.minimum_major); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u16_le(hello.maximum_major); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u16_le(hello.maximum_minor); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u64_le(hello.feature_bits); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u32_le(hello.maximum_payload_size);
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u32_le(0U); !status.is_ok())
    return common::make_unexpected(status);
  return bytes;
}

common::Result<ClientHello> decode_client_hello(const common::ByteView payload) {
  if (payload.size() != kHelloPayloadSize)
    return common::make_unexpected(corrupt("CLIENT_HELLO payload size is not canonical"));
  common::ByteReader reader{payload};
  const auto format = reader.read_u16_le();
  const auto minimum_major = reader.read_u16_le();
  const auto maximum_major = reader.read_u16_le();
  const auto maximum_minor = reader.read_u16_le();
  const auto features = reader.read_u64_le();
  const auto maximum_payload = reader.read_u32_le();
  const auto reserved = reader.read_u32_le();
  if (!format.has_value() || !minimum_major.has_value() || !maximum_major.has_value() ||
      !maximum_minor.has_value() || !features.has_value() || !maximum_payload.has_value() ||
      !reserved.has_value())
    return common::make_unexpected(corrupt("CLIENT_HELLO payload is truncated"));
  if (*format != kMessagePayloadFormat || *reserved != 0U)
    return common::make_unexpected(corrupt("CLIENT_HELLO payload is unsupported"));
  ClientHello hello{.minimum_major = *minimum_major,
                    .maximum_major = *maximum_major,
                    .maximum_minor = *maximum_minor,
                    .feature_bits = *features,
                    .maximum_payload_size = *maximum_payload};
  if (const common::Status status = validate_hello_range(hello); !status.is_ok())
    return common::make_unexpected(corrupt(status.message()));
  return hello;
}

common::Result<std::vector<std::byte>> encode_server_hello(const ServerHello& hello) {
  if (const common::Status status = validate_server_hello(hello); !status.is_ok())
    return common::make_unexpected(status);
  auto bytes = allocated(kHelloPayloadSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::ByteWriter writer{*bytes};
  if (const common::Status status = writer.write_u16_le(kMessagePayloadFormat); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u16_le(hello.selected_major); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u16_le(hello.selected_minor); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u16_le(0U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u64_le(hello.feature_bits); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u32_le(hello.maximum_payload_size);
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u32_le(0U); !status.is_ok())
    return common::make_unexpected(status);
  return bytes;
}

common::Result<ServerHello> decode_server_hello(const common::ByteView payload) {
  if (payload.size() != kHelloPayloadSize)
    return common::make_unexpected(corrupt("SERVER_HELLO payload size is not canonical"));
  common::ByteReader reader{payload};
  const auto format = reader.read_u16_le();
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto reserved16 = reader.read_u16_le();
  const auto features = reader.read_u64_le();
  const auto maximum_payload = reader.read_u32_le();
  const auto reserved32 = reader.read_u32_le();
  if (!format.has_value() || !major.has_value() || !minor.has_value() || !reserved16.has_value() ||
      !features.has_value() || !maximum_payload.has_value() || !reserved32.has_value())
    return common::make_unexpected(corrupt("SERVER_HELLO payload is truncated"));
  ServerHello hello{.selected_major = *major,
                    .selected_minor = *minor,
                    .feature_bits = *features,
                    .maximum_payload_size = *maximum_payload};
  if (*format != kMessagePayloadFormat || *reserved16 != 0U || *reserved32 != 0U ||
      !validate_server_hello(hello).is_ok())
    return common::make_unexpected(corrupt("SERVER_HELLO payload is unsupported"));
  return hello;
}

common::Result<std::vector<std::byte>>
encode_ingest_request(const DurabilityMode durability,
                      const common::ByteView encoded_columnar_append,
                      const ProtocolLimits& limits) {
  if (!valid_durability(static_cast<std::uint8_t>(durability)))
    return common::make_unexpected(invalid("INGEST_REQUEST durability mode is unassigned"));
  const auto size =
      variable_payload_size(kIngestEnvelopeSize, encoded_columnar_append.size(), limits);
  if (!size.has_value())
    return common::make_unexpected(size.error());
  auto bytes = allocated(*size);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::ByteWriter writer{*bytes};
  if (const common::Status status = writer.write_u16_le(kMessagePayloadFormat); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u8(static_cast<std::uint8_t>(durability));
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u8(0U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status =
          writer.write_u32_le(static_cast<std::uint32_t>(encoded_columnar_append.size()));
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_exact(encoded_columnar_append); !status.is_ok())
    return common::make_unexpected(status);
  return bytes;
}

common::Result<IngestRequestView> decode_ingest_request(const common::ByteView payload,
                                                        const ProtocolLimits& limits) {
  if (payload.size() < kIngestEnvelopeSize || payload.size() > limits.maximum_payload_size)
    return common::make_unexpected(corrupt("INGEST_REQUEST payload size is invalid"));
  common::ByteReader reader{payload};
  const auto format = reader.read_u16_le();
  const auto durability = reader.read_u8();
  const auto reserved = reader.read_u8();
  const auto body_size = reader.read_u32_le();
  if (!format.has_value() || !durability.has_value() || !reserved.has_value() ||
      !body_size.has_value() || *format != kMessagePayloadFormat || *reserved != 0U ||
      !valid_durability(*durability) || *body_size != reader.remaining())
    return common::make_unexpected(corrupt("INGEST_REQUEST envelope is invalid"));
  return IngestRequestView{.durability = static_cast<DurabilityMode>(*durability),
                           .encoded_columnar_append = *reader.read_exact(*body_size)};
}

common::Result<std::vector<std::byte>>
encode_ingest_acknowledgement(const IngestAcknowledgement& acknowledgement) {
  if (const common::Status status = validate_acknowledgement(acknowledgement); !status.is_ok())
    return common::make_unexpected(status);
  auto bytes = allocated(kIngestAcknowledgementSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::ByteWriter writer{*bytes};
  if (const common::Status status = writer.write_u16_le(kMessagePayloadFormat); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status =
          writer.write_u8(static_cast<std::uint8_t>(acknowledgement.requested_durability));
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status =
          writer.write_u8(static_cast<std::uint8_t>(acknowledgement.effective_durability));
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status =
          writer.write_u8(static_cast<std::uint8_t>(acknowledgement.outcome));
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.zero_fill(3U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u64_le(acknowledgement.record_sequence);
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u64_le(acknowledgement.segment_number);
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u64_le(acknowledgement.byte_offset);
      !status.is_ok())
    return common::make_unexpected(status);
  return bytes;
}

common::Result<IngestAcknowledgement>
decode_ingest_acknowledgement(const common::ByteView payload) {
  if (payload.size() != kIngestAcknowledgementSize)
    return common::make_unexpected(corrupt("INGEST_ACKNOWLEDGEMENT payload size is not canonical"));
  common::ByteReader reader{payload};
  const auto format = reader.read_u16_le();
  const auto requested = reader.read_u8();
  const auto effective = reader.read_u8();
  const auto outcome = reader.read_u8();
  const auto reserved = reader.read_exact(3U);
  const auto sequence = reader.read_u64_le();
  const auto segment = reader.read_u64_le();
  const auto offset = reader.read_u64_le();
  if (!format.has_value() || !requested.has_value() || !effective.has_value() ||
      !outcome.has_value() || !reserved.has_value() || !sequence.has_value() ||
      !segment.has_value() || !offset.has_value() || *format != kMessagePayloadFormat ||
      !valid_durability(*requested) || !valid_durability(*effective) ||
      (*outcome != static_cast<std::uint8_t>(IngestOutcome::kApplied) &&
       *outcome != static_cast<std::uint8_t>(IngestOutcome::kMatchingRetry)) ||
      (*reserved)[0] != std::byte{0} || (*reserved)[1] != std::byte{0} ||
      (*reserved)[2] != std::byte{0})
    return common::make_unexpected(corrupt("INGEST_ACKNOWLEDGEMENT payload is invalid"));
  IngestAcknowledgement value{.requested_durability = static_cast<DurabilityMode>(*requested),
                              .effective_durability = static_cast<DurabilityMode>(*effective),
                              .outcome = static_cast<IngestOutcome>(*outcome),
                              .record_sequence = *sequence,
                              .segment_number = *segment,
                              .byte_offset = *offset};
  if (!validate_acknowledgement(value).is_ok())
    return common::make_unexpected(corrupt("INGEST_ACKNOWLEDGEMENT semantics are invalid"));
  return value;
}

common::Result<std::vector<std::byte>> encode_query_request(const std::string_view sql,
                                                            const ProtocolLimits& limits) {
  const common::ByteView bytes_view = std::as_bytes(std::span{sql.data(), sql.size()});
  if (sql.empty() || !schema::is_valid_utf8(bytes_view))
    return common::make_unexpected(invalid("QUERY_REQUEST SQL must be nonempty valid UTF-8"));
  const auto size = variable_payload_size(kQueryEnvelopeSize, sql.size(), limits);
  if (!size.has_value())
    return common::make_unexpected(size.error());
  auto bytes = allocated(*size);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::ByteWriter writer{*bytes};
  if (const common::Status status = writer.write_u16_le(kMessagePayloadFormat); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u16_le(0U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u32_le(static_cast<std::uint32_t>(sql.size()));
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_exact(bytes_view); !status.is_ok())
    return common::make_unexpected(status);
  return bytes;
}

common::Result<common::ByteView> decode_query_request(const common::ByteView payload,
                                                      const ProtocolLimits& limits) {
  if (payload.size() < kQueryEnvelopeSize || payload.size() > limits.maximum_payload_size)
    return common::make_unexpected(corrupt("QUERY_REQUEST payload size is invalid"));
  common::ByteReader reader{payload};
  const auto format = reader.read_u16_le();
  const auto reserved = reader.read_u16_le();
  const auto size = reader.read_u32_le();
  if (!format.has_value() || !reserved.has_value() || !size.has_value() ||
      *format != kMessagePayloadFormat || *reserved != 0U || *size == 0U ||
      *size != reader.remaining())
    return common::make_unexpected(corrupt("QUERY_REQUEST envelope is invalid"));
  const common::ByteView sql = *reader.read_exact(*size);
  if (!schema::is_valid_utf8(sql))
    return common::make_unexpected(corrupt("QUERY_REQUEST SQL is invalid UTF-8"));
  return sql;
}

common::Result<std::vector<std::byte>> encode_error_message(const ProtocolErrorCode code,
                                                            const std::string_view message,
                                                            const ProtocolLimits& limits) {
  const common::ByteView message_bytes = std::as_bytes(std::span{message.data(), message.size()});
  if (message.empty() || !schema::is_valid_utf8(message_bytes))
    return common::make_unexpected(invalid("ERROR message must be nonempty valid UTF-8"));
  const auto size = variable_payload_size(kErrorEnvelopeSize, message.size(), limits);
  if (!size.has_value())
    return common::make_unexpected(size.error());
  auto bytes = allocated(*size);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::ByteWriter writer{*bytes};
  if (const common::Status status = writer.write_u16_le(kMessagePayloadFormat); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u16_le(static_cast<std::uint16_t>(code));
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u32_le(static_cast<std::uint32_t>(message.size()));
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_exact(message_bytes); !status.is_ok())
    return common::make_unexpected(status);
  return bytes;
}

common::Result<ErrorMessageView> decode_error_message(const common::ByteView payload,
                                                      const ProtocolLimits& limits) {
  if (payload.size() < kErrorEnvelopeSize || payload.size() > limits.maximum_payload_size)
    return common::make_unexpected(corrupt("ERROR payload size is invalid"));
  common::ByteReader reader{payload};
  const auto format = reader.read_u16_le();
  const auto code = reader.read_u16_le();
  const auto size = reader.read_u32_le();
  if (!format.has_value() || !code.has_value() || !size.has_value() ||
      *format != kMessagePayloadFormat ||
      *code < static_cast<std::uint16_t>(ProtocolErrorCode::kMalformedFrame) ||
      *code > static_cast<std::uint16_t>(ProtocolErrorCode::kInternal) || *size == 0U ||
      *size != reader.remaining())
    return common::make_unexpected(corrupt("ERROR envelope is invalid"));
  const common::ByteView message = *reader.read_exact(*size);
  if (!schema::is_valid_utf8(message))
    return common::make_unexpected(corrupt("ERROR message is invalid UTF-8"));
  return ErrorMessageView{.code = static_cast<ProtocolErrorCode>(*code), .message = message};
}

} // namespace chronos::network
