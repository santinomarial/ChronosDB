#include "chronos/network/messages.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/schema/utf8.hpp"

#include <algorithm>
#include <array>
#include <limits>
#include <new>
#include <optional>
#include <span>
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

[[nodiscard]] bool valid_v1_durability(const std::uint8_t value) noexcept {
  return value == static_cast<std::uint8_t>(DurabilityMode::kAsync) ||
         value == static_cast<std::uint8_t>(DurabilityMode::kLocalSync);
}

[[nodiscard]] std::uint64_t supported_features(const std::uint16_t major) noexcept {
  return major == kProtocolV2Major ? kProtocolV2SupportedFeatureBits
                                   : kProtocolV1SupportedFeatureBits;
}

[[nodiscard]] bool valid_ingest_context(const IngestProtocolContext& context) noexcept {
  const bool version = (context.protocol_major == kProtocolMajor &&
                        context.protocol_minor <= kProtocolLatestMinor) ||
                       (context.protocol_major == kProtocolV2Major &&
                        context.protocol_minor <= kProtocolV2LatestMinor);
  return version && (context.feature_bits & ~supported_features(context.protocol_major)) == 0U &&
         (context.protocol_major != kProtocolMajor || context.protocol_minor != 0U ||
          context.feature_bits == 0U);
}

[[nodiscard]] bool valid_durability(const std::uint8_t value,
                                    const IngestProtocolContext& context) noexcept {
  return valid_v1_durability(value) ||
         (value == static_cast<std::uint8_t>(DurabilityMode::kQuorumSync) &&
          context.protocol_major == kProtocolV2Major &&
          (context.feature_bits & kProtocolV2QuorumSyncFeature) != 0U);
}

[[nodiscard]] std::optional<std::size_t>
fixed_query_cell_size(const schema::LogicalTypeKind kind) noexcept {
  switch (kind) {
  case schema::LogicalTypeKind::kBool:
  case schema::LogicalTypeKind::kInt8:
  case schema::LogicalTypeKind::kUInt8:
    return 1U;
  case schema::LogicalTypeKind::kInt16:
  case schema::LogicalTypeKind::kUInt16:
    return 2U;
  case schema::LogicalTypeKind::kInt32:
  case schema::LogicalTypeKind::kUInt32:
  case schema::LogicalTypeKind::kFloat32:
  case schema::LogicalTypeKind::kDate:
    return 4U;
  case schema::LogicalTypeKind::kInt64:
  case schema::LogicalTypeKind::kUInt64:
  case schema::LogicalTypeKind::kFloat64:
  case schema::LogicalTypeKind::kTimestampNs:
    return 8U;
  case schema::LogicalTypeKind::kDecimal:
  case schema::LogicalTypeKind::kUuid:
    return 16U;
  case schema::LogicalTypeKind::kSymbol:
  case schema::LogicalTypeKind::kString:
  case schema::LogicalTypeKind::kBinary:
    return std::nullopt;
  }
  return std::nullopt;
}

[[nodiscard]] bool decimal_fits_precision(const common::ByteView bytes,
                                          const std::uint16_t precision) noexcept {
  if (bytes.size() != 16U)
    return false;
  std::array<std::uint8_t, 16> magnitude{};
  for (std::size_t index = 0U; index < magnitude.size(); ++index)
    magnitude[index] = std::to_integer<std::uint8_t>(bytes[index]);
  if ((magnitude.back() & 0x80U) != 0U) {
    std::uint16_t carry = 1U;
    for (std::uint8_t& byte : magnitude) {
      const auto inverted = static_cast<std::uint8_t>(~static_cast<unsigned int>(byte));
      const std::uint16_t value = static_cast<std::uint16_t>(inverted) + carry;
      byte = static_cast<std::uint8_t>(value & 0xffU);
      carry = static_cast<std::uint16_t>(value >> 8U);
    }
  }
  std::array<std::uint8_t, 16> limit{};
  limit.front() = 1U;
  for (std::uint16_t digit = 0U; digit < precision; ++digit) {
    std::uint16_t carry = 0U;
    for (std::uint8_t& byte : limit) {
      const unsigned int value = static_cast<unsigned int>(byte) * 10U + carry;
      byte = static_cast<std::uint8_t>(value & 0xffU);
      carry = static_cast<std::uint16_t>(value >> 8U);
    }
  }
  for (std::size_t index = magnitude.size(); index > 0U; --index) {
    if (magnitude[index - 1U] != limit[index - 1U])
      return magnitude[index - 1U] < limit[index - 1U];
  }
  return false;
}

[[nodiscard]] common::Status validate_query_cell(const QueryResultColumn& column,
                                                 const QueryResultCell& cell) {
  if (cell.is_null)
    return column.nullable && cell.value.empty()
               ? common::Status::ok()
               : invalid("query result NULL cell is not canonical");
  if (const auto fixed = fixed_query_cell_size(column.type.kind()); fixed.has_value()) {
    if (cell.value.size() != *fixed)
      return invalid("query result fixed-width cell size is invalid");
    if (column.type.kind() == schema::LogicalTypeKind::kBool &&
        cell.value.front() != std::byte{0} && cell.value.front() != std::byte{1})
      return invalid("query result Boolean cell is not canonical");
    if (column.type.kind() == schema::LogicalTypeKind::kDecimal &&
        !decimal_fits_precision(cell.value, column.type.parameter_0()))
      return invalid("query result decimal exceeds its declared precision");
  } else if ((column.type.kind() == schema::LogicalTypeKind::kString ||
              column.type.kind() == schema::LogicalTypeKind::kSymbol) &&
             !schema::is_valid_utf8(cell.value)) {
    return invalid("query result text cell is not valid UTF-8");
  }
  if (cell.value.size() >= kQueryResultNullCellLength)
    return invalid("query result cell is too large");
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_query_result_limits(const QueryResultLimits& limits) {
  if (const common::Status status = validate_protocol_limits(limits.protocol); !status.is_ok())
    return status;
  if (limits.maximum_rows == 0U || limits.maximum_columns == 0U || limits.maximum_columns > 4096U ||
      limits.maximum_column_name_bytes == 0U || limits.maximum_column_name_bytes > 65'536U)
    return invalid("query result limits are invalid");
  return common::Status::ok();
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
  if (hello.minimum_major == 0U || hello.minimum_major > hello.maximum_major ||
      hello.maximum_major > kProtocolLatestMajor || hello.maximum_minor > kProtocolLatestMinor)
    return invalid("CLIENT_HELLO protocol range is invalid");
  if ((hello.feature_bits & ~kProtocolV2SupportedFeatureBits) != 0U)
    return invalid("CLIENT_HELLO requests unknown feature bits");
  if (hello.maximum_major == kProtocolMajor && hello.maximum_minor == 0U &&
      hello.feature_bits != 0U)
    return invalid("CLIENT_HELLO Protocol 1.0 cannot request extension features");
  if (hello.maximum_major == kProtocolMajor &&
      (hello.feature_bits & (kProtocolV2QuorumSyncFeature | kProtocolV2LeaderRedirectFeature)) !=
          0U)
    return invalid("CLIENT_HELLO Protocol v1 cannot request Protocol v2 features");
  return validate_protocol_limits({.maximum_payload_size = hello.maximum_payload_size});
}

[[nodiscard]] common::Status validate_server_hello(const ServerHello& hello) {
  const bool version =
      (hello.selected_major == kProtocolMajor && hello.selected_minor <= kProtocolLatestMinor) ||
      (hello.selected_major == kProtocolV2Major && hello.selected_minor <= kProtocolV2LatestMinor);
  if (!version || (hello.feature_bits & ~supported_features(hello.selected_major)) != 0U ||
      (hello.selected_major == kProtocolMajor && hello.selected_minor == 0U &&
       hello.feature_bits != 0U))
    return invalid("SERVER_HELLO selection is unsupported");
  return validate_protocol_limits({.maximum_payload_size = hello.maximum_payload_size});
}

[[nodiscard]] common::Status
validate_acknowledgement(const IngestAcknowledgement& acknowledgement) {
  if (!valid_v1_durability(static_cast<std::uint8_t>(acknowledgement.requested_durability)) ||
      !valid_v1_durability(static_cast<std::uint8_t>(acknowledgement.effective_durability)) ||
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

[[nodiscard]] common::Status
validate_quorum_sync_acknowledgement(const QuorumSyncIngestAcknowledgement& acknowledgement) {
  if (acknowledgement.requested_durability != DurabilityMode::kQuorumSync ||
      acknowledgement.effective_durability != DurabilityMode::kQuorumSync ||
      (acknowledgement.outcome != IngestOutcome::kApplied &&
       acknowledgement.outcome != IngestOutcome::kMatchingRetry) ||
      acknowledgement.group_id.is_nil() || acknowledgement.leader_node_id == 0U ||
      acknowledgement.leader_term == 0U || acknowledgement.log_index == 0U ||
      acknowledgement.entry_term == 0U || acknowledgement.local_durable_physical_sequence == 0U)
    return invalid("QUORUM_SYNC acknowledgement fields are invalid");
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_leader_redirect(const LeaderRedirect& redirect) {
  if (redirect.group_id.is_nil() || redirect.leader_node_id == 0U || redirect.leader_term == 0U ||
      redirect.placement_epoch == 0U)
    return invalid("LEADER_REDIRECT fields are invalid");
  return common::Status::ok();
}

} // namespace

QueryResultBatchView::QueryResultBatchView(std::uint32_t rows,
                                           std::vector<QueryResultColumn> columns,
                                           std::vector<QueryResultCell> cells) noexcept
    : rows_(rows), columns_(std::move(columns)), cells_(std::move(cells)) {}

std::uint32_t QueryResultBatchView::row_count() const noexcept {
  return rows_;
}
std::span<const QueryResultColumn> QueryResultBatchView::columns() const noexcept {
  return columns_;
}
const QueryResultCell* QueryResultBatchView::cell(const std::uint32_t row,
                                                  const std::size_t column) const noexcept {
  if (row >= rows_ || column >= columns_.size())
    return nullptr;
  return &cells_[static_cast<std::size_t>(row) * columns_.size() + column];
}

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
  return encode_ingest_request(durability, encoded_columnar_append, IngestProtocolContext{},
                               limits);
}

common::Result<std::vector<std::byte>>
encode_ingest_request(const DurabilityMode durability,
                      const common::ByteView encoded_columnar_append,
                      const IngestProtocolContext& context, const ProtocolLimits& limits) {
  if (!valid_ingest_context(context) ||
      !valid_durability(static_cast<std::uint8_t>(durability), context))
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
  return decode_ingest_request(payload, IngestProtocolContext{}, limits);
}

common::Result<IngestRequestView> decode_ingest_request(const common::ByteView payload,
                                                        const IngestProtocolContext& context,
                                                        const ProtocolLimits& limits) {
  if (payload.size() < kIngestEnvelopeSize || payload.size() > limits.maximum_payload_size)
    return common::make_unexpected(corrupt("INGEST_REQUEST payload size is invalid"));
  if (!valid_ingest_context(context))
    return common::make_unexpected(corrupt("INGEST_REQUEST protocol context is invalid"));
  common::ByteReader reader{payload};
  const auto format = reader.read_u16_le();
  const auto durability = reader.read_u8();
  const auto reserved = reader.read_u8();
  const auto body_size = reader.read_u32_le();
  if (!format.has_value() || !durability.has_value() || !reserved.has_value() ||
      !body_size.has_value() || *format != kMessagePayloadFormat || *reserved != 0U ||
      !valid_durability(*durability, context) || *body_size != reader.remaining())
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

common::Result<std::vector<std::byte>>
encode_ingest_acknowledgement(const IngestAcknowledgement& acknowledgement,
                              const IngestProtocolContext& context) {
  if (!valid_ingest_context(context))
    return common::make_unexpected(invalid("INGEST_ACKNOWLEDGEMENT protocol context is invalid"));
  return encode_ingest_acknowledgement(acknowledgement);
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
      !valid_v1_durability(*requested) || !valid_v1_durability(*effective) ||
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

common::Result<IngestAcknowledgement>
decode_ingest_acknowledgement(const common::ByteView payload,
                              const IngestProtocolContext& context) {
  if (!valid_ingest_context(context))
    return common::make_unexpected(corrupt("INGEST_ACKNOWLEDGEMENT protocol context is invalid"));
  return decode_ingest_acknowledgement(payload);
}

common::Result<std::vector<std::byte>>
encode_quorum_sync_ingest_acknowledgement(const QuorumSyncIngestAcknowledgement& acknowledgement) {
  if (const common::Status status = validate_quorum_sync_acknowledgement(acknowledgement);
      !status.is_ok())
    return common::make_unexpected(status);
  auto bytes = allocated(kQuorumSyncIngestAcknowledgementSize);
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
  if (const common::Status status = writer.write_exact(acknowledgement.group_id.bytes());
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u64_le(acknowledgement.leader_node_id);
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u64_le(acknowledgement.leader_term);
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u64_le(acknowledgement.log_index); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u64_le(acknowledgement.entry_term);
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status =
          writer.write_u64_le(acknowledgement.local_durable_physical_sequence);
      !status.is_ok())
    return common::make_unexpected(status);
  return bytes;
}

common::Result<QuorumSyncIngestAcknowledgement>
decode_quorum_sync_ingest_acknowledgement(const common::ByteView payload) {
  if (payload.size() != kQuorumSyncIngestAcknowledgementSize)
    return common::make_unexpected(
        corrupt("QUORUM_SYNC acknowledgement payload size is not canonical"));
  common::ByteReader reader{payload};
  const auto format = reader.read_u16_le();
  const auto requested = reader.read_u8();
  const auto effective = reader.read_u8();
  const auto outcome = reader.read_u8();
  const auto reserved = reader.read_exact(3U);
  const auto group = reader.read_exact(common::Uuid::kSize);
  const auto leader = reader.read_u64_le();
  const auto leader_term = reader.read_u64_le();
  const auto log_index = reader.read_u64_le();
  const auto entry_term = reader.read_u64_le();
  const auto physical_sequence = reader.read_u64_le();
  if (!format.has_value() || !requested.has_value() || !effective.has_value() ||
      !outcome.has_value() || !reserved.has_value() || !group.has_value() || !leader.has_value() ||
      !leader_term.has_value() || !log_index.has_value() || !entry_term.has_value() ||
      !physical_sequence.has_value() || *format != kMessagePayloadFormat ||
      (*reserved)[0] != std::byte{0} || (*reserved)[1] != std::byte{0} ||
      (*reserved)[2] != std::byte{0})
    return common::make_unexpected(corrupt("QUORUM_SYNC acknowledgement payload is invalid"));
  common::Uuid::Bytes group_bytes{};
  std::ranges::copy(*group, group_bytes.begin());
  const common::Uuid group_id{group_bytes};
  QuorumSyncIngestAcknowledgement value{
      .requested_durability = static_cast<DurabilityMode>(*requested),
      .effective_durability = static_cast<DurabilityMode>(*effective),
      .outcome = static_cast<IngestOutcome>(*outcome),
      .group_id = group_id,
      .leader_node_id = *leader,
      .leader_term = *leader_term,
      .log_index = *log_index,
      .entry_term = *entry_term,
      .local_durable_physical_sequence = *physical_sequence};
  if (!validate_quorum_sync_acknowledgement(value).is_ok())
    return common::make_unexpected(corrupt("QUORUM_SYNC acknowledgement semantics are invalid"));
  return value;
}

common::Result<std::vector<std::byte>> encode_leader_redirect(const LeaderRedirect& redirect) {
  if (const common::Status status = validate_leader_redirect(redirect); !status.is_ok())
    return common::make_unexpected(status);
  auto bytes = allocated(kLeaderRedirectSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::ByteWriter writer{*bytes};
  if (const common::Status status = writer.write_u16_le(kMessagePayloadFormat); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.zero_fill(6U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_exact(redirect.group_id.bytes()); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u64_le(redirect.leader_node_id); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u64_le(redirect.leader_term); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u64_le(redirect.placement_epoch); !status.is_ok())
    return common::make_unexpected(status);
  return bytes;
}

common::Result<LeaderRedirect> decode_leader_redirect(const common::ByteView payload) {
  if (payload.size() != kLeaderRedirectSize)
    return common::make_unexpected(corrupt("LEADER_REDIRECT payload size is not canonical"));
  common::ByteReader reader{payload};
  const auto format = reader.read_u16_le();
  const auto reserved = reader.read_exact(6U);
  const auto group = reader.read_exact(common::Uuid::kSize);
  const auto leader_node = reader.read_u64_le();
  const auto leader_term = reader.read_u64_le();
  const auto placement_epoch = reader.read_u64_le();
  if (!format.has_value() || !reserved.has_value() || !group.has_value() ||
      !leader_node.has_value() || !leader_term.has_value() || !placement_epoch.has_value())
    return common::make_unexpected(corrupt("LEADER_REDIRECT payload is truncated"));
  if (*format != kMessagePayloadFormat ||
      !std::ranges::all_of(*reserved, [](const std::byte byte) { return byte == std::byte{0}; }))
    return common::make_unexpected(corrupt("LEADER_REDIRECT payload is unsupported"));
  common::Uuid::Bytes group_bytes{};
  std::ranges::copy(*group, group_bytes.begin());
  LeaderRedirect redirect{.group_id = common::Uuid{group_bytes},
                          .leader_node_id = *leader_node,
                          .leader_term = *leader_term,
                          .placement_epoch = *placement_epoch};
  if (!validate_leader_redirect(redirect).is_ok())
    return common::make_unexpected(corrupt("LEADER_REDIRECT semantics are invalid"));
  return redirect;
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

common::Result<std::vector<std::byte>> encode_query_result_batch(
    const std::uint32_t rows, const std::span<const QueryResultColumn> columns,
    const std::span<const QueryResultCell> cells, const QueryResultLimits& limits) {
  if (const common::Status status = validate_query_result_limits(limits); !status.is_ok())
    return common::make_unexpected(status);
  const auto cell_count = common::checked_multiply(static_cast<std::size_t>(rows), columns.size());
  if (rows > limits.maximum_rows || columns.empty() || columns.size() > limits.maximum_columns ||
      !cell_count.has_value() || *cell_count != cells.size())
    return common::make_unexpected(invalid("query result batch shape is invalid"));

  std::size_t descriptor_bytes = 0U;
  std::size_t total = kQueryResultEnvelopeSize;
  for (const QueryResultColumn& column : columns) {
    const common::ByteView name = std::as_bytes(std::span{column.name.data(), column.name.size()});
    if (name.empty() || name.size() > limits.maximum_column_name_bytes ||
        !schema::is_valid_utf8(name))
      return common::make_unexpected(invalid("query result column name is invalid"));
    const auto next_descriptors =
        common::checked_add(descriptor_bytes, kQueryResultColumnEnvelopeSize + name.size());
    if (!next_descriptors.has_value())
      return common::make_unexpected(invalid("query result descriptor size overflowed"));
    descriptor_bytes = *next_descriptors;
  }
  const auto after_descriptors = common::checked_add(total, descriptor_bytes);
  if (!after_descriptors.has_value())
    return common::make_unexpected(invalid("query result descriptor size overflowed"));
  total = *after_descriptors;
  for (std::size_t index = 0U; index < cells.size(); ++index) {
    const QueryResultColumn& column = columns[index % columns.size()];
    if (const common::Status status = validate_query_cell(column, cells[index]); !status.is_ok())
      return common::make_unexpected(status);
    const auto next = common::checked_add(total, std::size_t{4U} + cells[index].value.size());
    if (!next.has_value())
      return common::make_unexpected(invalid("query result cell size overflowed"));
    total = *next;
  }
  if (descriptor_bytes > std::numeric_limits<std::uint32_t>::max() ||
      total > limits.protocol.maximum_payload_size)
    return common::make_unexpected(invalid("query result batch exceeds the payload limit"));
  auto bytes = allocated(total);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::ByteWriter writer{*bytes};
  if (!writer.write_u16_le(kMessagePayloadFormat).is_ok() || !writer.write_u16_le(0U).is_ok() ||
      !writer.write_u32_le(rows).is_ok() ||
      !writer.write_u32_le(static_cast<std::uint32_t>(columns.size())).is_ok() ||
      !writer.write_u32_le(static_cast<std::uint32_t>(descriptor_bytes)).is_ok())
    return common::make_unexpected(invalid("query result batch planning mismatch"));
  for (const QueryResultColumn& column : columns) {
    const common::ByteView name = std::as_bytes(std::span{column.name.data(), column.name.size()});
    if (!writer.write_u16_le(column.type.code()).is_ok() ||
        !writer.write_u16_le(column.type.parameter_0()).is_ok() ||
        !writer.write_u16_le(column.type.parameter_1()).is_ok() ||
        !writer.write_u8(column.nullable ? 1U : 0U).is_ok() || !writer.write_u8(0U).is_ok() ||
        !writer.write_u32_le(static_cast<std::uint32_t>(name.size())).is_ok() ||
        !writer.write_u32_le(0U).is_ok() || !writer.write_exact(name).is_ok())
      return common::make_unexpected(invalid("query result descriptor planning mismatch"));
  }
  for (const QueryResultCell& cell : cells) {
    const std::uint32_t length =
        cell.is_null ? kQueryResultNullCellLength : static_cast<std::uint32_t>(cell.value.size());
    if (!writer.write_u32_le(length).is_ok() || !writer.write_exact(cell.value).is_ok())
      return common::make_unexpected(invalid("query result cell planning mismatch"));
  }
  return bytes;
}

common::Result<QueryResultBatchView> decode_query_result_batch(const common::ByteView payload,
                                                               const QueryResultLimits& limits) {
  if (const common::Status status = validate_query_result_limits(limits); !status.is_ok())
    return common::make_unexpected(status);
  if (payload.size() < kQueryResultEnvelopeSize ||
      payload.size() > limits.protocol.maximum_payload_size)
    return common::make_unexpected(corrupt("QUERY_RESULT payload size is invalid"));
  common::ByteReader reader{payload};
  const auto format = reader.read_u16_le();
  const auto flags = reader.read_u16_le();
  const auto rows = reader.read_u32_le();
  const auto column_count = reader.read_u32_le();
  const auto descriptor_bytes = reader.read_u32_le();
  if (!format.has_value() || !flags.has_value() || !rows.has_value() || !column_count.has_value() ||
      !descriptor_bytes.has_value() || *format != kMessagePayloadFormat || *flags != 0U ||
      *rows > limits.maximum_rows || *column_count == 0U ||
      *column_count > limits.maximum_columns || *descriptor_bytes > reader.remaining())
    return common::make_unexpected(corrupt("QUERY_RESULT envelope is invalid"));
  auto descriptors = reader.read_subreader(*descriptor_bytes);
  if (!descriptors.has_value())
    return common::make_unexpected(corrupt("QUERY_RESULT descriptors are truncated"));
  try {
    std::vector<QueryResultColumn> columns;
    columns.reserve(*column_count);
    for (std::uint32_t index = 0U; index < *column_count; ++index) {
      const auto code = descriptors->read_u16_le();
      const auto parameter_0 = descriptors->read_u16_le();
      const auto parameter_1 = descriptors->read_u16_le();
      const auto nullable = descriptors->read_u8();
      const auto reserved_0 = descriptors->read_u8();
      const auto name_size = descriptors->read_u32_le();
      const auto reserved_1 = descriptors->read_u32_le();
      if (!code.has_value() || !parameter_0.has_value() || !parameter_1.has_value() ||
          !nullable.has_value() || !reserved_0.has_value() || !name_size.has_value() ||
          !reserved_1.has_value() || *nullable > 1U || *reserved_0 != 0U || *reserved_1 != 0U ||
          *name_size == 0U || *name_size > limits.maximum_column_name_bytes)
        return common::make_unexpected(corrupt("QUERY_RESULT column descriptor is invalid"));
      const auto kind = schema::logical_type_kind_from_code(*code);
      if (!kind.has_value())
        return common::make_unexpected(corrupt("QUERY_RESULT logical type is unassigned"));
      const auto type = schema::LogicalType::create(*kind, *parameter_0, *parameter_1);
      const auto name = descriptors->read_exact(*name_size);
      if (!type.has_value() || !name.has_value() || !schema::is_valid_utf8(*name))
        return common::make_unexpected(corrupt("QUERY_RESULT column descriptor is invalid"));
      // Character bytes may be inspected through char by the C++ aliasing rules.
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
      const std::string_view name_view{reinterpret_cast<const char*>(name->data()), name->size()};
      columns.push_back({.name = name_view, .type = *type, .nullable = *nullable == 1U});
    }
    if (!descriptors->empty())
      return common::make_unexpected(corrupt("QUERY_RESULT descriptor length is not canonical"));
    const auto cell_count = common::checked_multiply(static_cast<std::size_t>(*rows),
                                                     static_cast<std::size_t>(*column_count));
    if (!cell_count.has_value())
      return common::make_unexpected(corrupt("QUERY_RESULT cell count overflowed"));
    const std::size_t number_of_cells = cell_count.value();
    std::vector<QueryResultCell> cells;
    cells.reserve(number_of_cells);
    for (std::size_t index = 0U; index < number_of_cells; ++index) {
      const auto length = reader.read_u32_le();
      if (!length.has_value())
        return common::make_unexpected(corrupt("QUERY_RESULT cell is truncated"));
      const bool is_null = *length == kQueryResultNullCellLength;
      const auto value = reader.read_exact(is_null ? 0U : *length);
      if (!value.has_value())
        return common::make_unexpected(corrupt("QUERY_RESULT cell is truncated"));
      QueryResultCell cell{.is_null = is_null, .value = *value};
      if (!validate_query_cell(columns[index % columns.size()], cell).is_ok())
        return common::make_unexpected(corrupt("QUERY_RESULT cell is not canonical"));
      cells.push_back(cell);
    }
    if (!reader.empty())
      return common::make_unexpected(corrupt("QUERY_RESULT payload has trailing bytes"));
    return QueryResultBatchView{*rows, std::move(columns), std::move(cells)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted());
  }
}

} // namespace chronos::network
