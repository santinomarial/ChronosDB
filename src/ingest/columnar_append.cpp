#include "chronos/ingest/columnar_append.hpp"

#include "chronos/common/checked_math.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

using namespace columnar_append_v1;

[[nodiscard]] common::Status invalid_argument(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] ColumnarAppendDecodeError incomplete(std::string message,
                                                    const std::size_t required_size) {
  return ColumnarAppendDecodeError{
      ColumnarAppendDecodeErrorKind::kIncomplete,
      common::Status{common::StatusCode::kOutOfRange, std::move(message)}, required_size};
}

[[nodiscard]] ColumnarAppendDecodeError corruption(std::string message) {
  return ColumnarAppendDecodeError{
      ColumnarAppendDecodeErrorKind::kCorruption,
      common::Status{common::StatusCode::kCorruption, std::move(message)}};
}

[[nodiscard]] ColumnarAppendDecodeError unsupported(std::string message) {
  return ColumnarAppendDecodeError{
      ColumnarAppendDecodeErrorKind::kUnsupported,
      common::Status{common::StatusCode::kNotSupported, std::move(message)}};
}

[[nodiscard]] ColumnarAppendDecodeError limit_exceeded(std::string message) {
  return ColumnarAppendDecodeError{
      ColumnarAppendDecodeErrorKind::kResourceLimit,
      common::Status{common::StatusCode::kResourceExhausted, std::move(message)}};
}

[[nodiscard]] ColumnarAppendDecodeError internal_error(common::Status status) {
  return ColumnarAppendDecodeError{ColumnarAppendDecodeErrorKind::kInternal, std::move(status)};
}

void store_u32_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint32_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
}

void store_u64_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint64_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
}

[[nodiscard]] std::uint32_t load_u32_le(const common::ByteView bytes,
                                        const std::size_t offset) noexcept {
  std::uint32_t value = 0U;
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= std::to_integer<std::uint32_t>(bytes[offset + index]) << (index * 8U);
  }
  return value;
}

[[nodiscard]] std::uint64_t load_u64_le(const common::ByteView bytes,
                                        const std::size_t offset) noexcept {
  std::uint64_t value = 0U;
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= std::to_integer<std::uint64_t>(bytes[offset + index]) << (index * 8U);
  }
  return value;
}

template <typename Identifier>
[[nodiscard]] common::Result<Identifier> read_identifier(const common::ByteView bytes,
                                                         const std::size_t offset) {
  common::Uuid::Bytes value{};
  std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(offset), value.size(), value.begin());
  return Identifier::from_bytes(value);
}

template <typename Identifier>
void store_identifier(const common::MutableByteView bytes, const std::size_t offset,
                      const Identifier& identifier) noexcept {
  std::copy(identifier.bytes().begin(), identifier.bytes().end(),
            bytes.begin() + static_cast<std::ptrdiff_t>(offset));
}

[[nodiscard]] std::array<std::byte, sizeof(std::uint32_t)>
encode_u32_le(const std::uint32_t value) noexcept {
  std::array<std::byte, sizeof(value)> bytes{};
  store_u32_le(bytes, 0U, value);
  return bytes;
}

[[nodiscard]] std::array<std::byte, sizeof(std::uint64_t)>
encode_u64_le(const std::uint64_t value) noexcept {
  std::array<std::byte, sizeof(value)> bytes{};
  store_u64_le(bytes, 0U, value);
  return bytes;
}

[[nodiscard]] ColumnarAppendDecodeError
map_batch_error(const columnar::ColumnarBatchDecodeError& error) {
  switch (error.kind()) {
  case columnar::ColumnarBatchDecodeErrorKind::kIncomplete:
  case columnar::ColumnarBatchDecodeErrorKind::kInvalid:
    return corruption(std::string{"embedded Columnar Batch v1 is invalid: "} +
                      error.status().message());
  case columnar::ColumnarBatchDecodeErrorKind::kUnsupported:
    return unsupported(std::string{"embedded Columnar Batch v1 is unsupported: "} +
                       error.status().message());
  case columnar::ColumnarBatchDecodeErrorKind::kResourceLimit:
    return limit_exceeded(std::string{"embedded Columnar Batch v1 exceeds a decode limit: "} +
                          error.status().message());
  }
  return internal_error(common::Status{common::StatusCode::kInternal,
                                       "unknown embedded batch decode error"});
}

} // namespace

common::Result<Sha256Digest>
compute_columnar_append_v1_request_digest(const ColumnarAppendDigestInput& input) {
  if (input.encoded_batch.size() > columnar::format::kMaximumEmbeddedBatchLength ||
      input.encoded_batch.size() > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kOutOfRange, "Columnar Batch v1 exceeds the digest preimage limit"});
  }

  const auto application_format = encode_u32_le(kApplicationFormat);
  const auto application_kind = encode_u32_le(kApplicationKind);
  const auto mutation_kind = encode_u32_le(kMutationKindAppendRows);
  const auto schema_version = encode_u64_le(input.schema_version.value());
  const auto batch_length = encode_u32_le(static_cast<std::uint32_t>(input.encoded_batch.size()));
  const std::array<common::ByteView, 10U> fragments{
      kRequestDigestDomain, application_format, application_kind, mutation_kind,
      input.table_id.bytes(), input.tablet_id.bytes(), input.schema_id.bytes(), schema_version,
      batch_length, input.encoded_batch};
  return sha256(fragments);
}

common::Result<wal::EncodedApplicationPayload>
encode_columnar_append_v1(const ColumnarAppendEncodeInput& input,
                          const columnar::EncodedColumnarBatch& batch) {
  if (batch.size() > columnar::format::kMaximumEmbeddedBatchLength) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kOutOfRange, "Columnar Batch v1 exceeds the command limit"});
  }
  const columnar::ColumnarBatchDecodeResult decoded =
      columnar::decode_columnar_batch_v1_exact(batch.bytes());
  if (!decoded.has_value()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInternal, "owned Columnar Batch v1 failed exact validation"});
  }

  const common::Result<Sha256Digest> digest = compute_columnar_append_v1_request_digest(
      ColumnarAppendDigestInput{.table_id = decoded->table_id(),
                                .tablet_id = input.tablet_id,
                                .schema_id = decoded->schema_id(),
                                .schema_version = decoded->schema_version(),
                                .encoded_batch = batch.bytes()});
  if (!digest.has_value()) {
    return common::make_unexpected(digest.error());
  }

  std::vector<std::byte> body(kCommandHeaderLength + batch.size(), std::byte{0});
  const common::MutableByteView output{body};
  store_u32_le(output, kCommandHeaderLengthOffset,
               static_cast<std::uint32_t>(kCommandHeaderLength));
  store_u32_le(output, kMutationKindOffset, kMutationKindAppendRows);
  store_u32_le(output, kDigestAlgorithmOffset, kDigestAlgorithmSha256);
  store_identifier(output, kClientIdOffset, input.client_id);
  store_identifier(output, kClientBatchIdOffset, input.client_batch_id);
  store_identifier(output, kTableIdOffset, decoded->table_id());
  store_identifier(output, kTabletIdOffset, input.tablet_id);
  store_identifier(output, kSchemaIdOffset, decoded->schema_id());
  store_u64_le(output, kSchemaVersionOffset, decoded->schema_version().value());
  store_u32_le(output, kRowCountOffset, decoded->row_count());
  store_u32_le(output, kBatchLengthOffset, static_cast<std::uint32_t>(batch.size()));
  std::copy(digest->bytes().begin(), digest->bytes().end(),
            output.begin() + static_cast<std::ptrdiff_t>(kRequestDigestOffset));
  store_u32_le(output, kOutcomeCodeOffset, kOutcomeCodeApplied);
  store_u32_le(output, kOutcomeRowCountOffset, decoded->row_count());
  std::memcpy(body.data() + kBatchOffset, batch.bytes().data(), batch.size());

  return wal::encode_application_payload(wal::ApplicationEnvelopeInput{
      .application_format = kApplicationFormat,
      .application_kind = kApplicationKind,
      .application_flags = kApplicationFlags,
      .application_body = body});
}

ColumnarAppendDecodeError::ColumnarAppendDecodeError(const ColumnarAppendDecodeErrorKind kind,
                                                     common::Status status,
                                                     const std::size_t required_size) noexcept
    : kind_(kind), status_(std::move(status)), required_size_(required_size) {}

DecodedColumnarAppendView::DecodedColumnarAppendView(
    ClientId client_id, ClientBatchId client_batch_id, schema::TableId table_id,
    schema::TabletId tablet_id, schema::SchemaId schema_id,
    const schema::SchemaVersion schema_version, const std::uint32_t row_count,
    Sha256Digest request_digest, columnar::DecodedColumnarBatchView batch,
    const common::ByteView encoded_payload) noexcept
    : client_id_(client_id), client_batch_id_(client_batch_id), table_id_(table_id),
      tablet_id_(tablet_id), schema_id_(schema_id), schema_version_(schema_version),
      row_count_(row_count), request_digest_(request_digest), batch_(std::move(batch)),
      encoded_payload_(encoded_payload) {}

const columnar::DecodedColumnarBatchView& DecodedColumnarAppendView::batch() const noexcept {
  return batch_;
}

common::ByteView DecodedColumnarAppendView::encoded_payload() const noexcept {
  return encoded_payload_;
}

ColumnarAppendDecodeResult
decode_columnar_append_v1_prefix(const common::ByteView bytes,
                                 const ColumnarAppendDecodeLimits limits) {
  if (limits.max_application_payload_length == 0U ||
      limits.max_application_payload_length > kMaximumApplicationPayloadLength ||
      limits.batch.max_batch_length == 0U ||
      limits.batch.max_batch_length > columnar::format::kMaximumEmbeddedBatchLength ||
      limits.batch.max_rows == 0U || limits.batch.max_columns == 0U ||
      limits.batch.max_columns > columnar::format::kMaximumColumnCount) {
    return std::unexpected(limit_exceeded("COLUMNAR_APPEND decode limits are outside v1 bounds"));
  }
  if (bytes.size() < wal::kApplicationEnvelopeSize) {
    return std::unexpected(incomplete("COLUMNAR_APPEND requires a 16-byte application envelope",
                                      wal::kApplicationEnvelopeSize));
  }
  const common::Result<wal::DecodedApplicationEnvelope> envelope =
      wal::decode_application_payload(bytes);
  if (!envelope.has_value()) {
    return std::unexpected(corruption(envelope.error().message()));
  }
  if (envelope->application_format != kApplicationFormat ||
      envelope->application_kind != kApplicationKind ||
      envelope->application_flags != kApplicationFlags) {
    return std::unexpected(unsupported("WAL application envelope is not COLUMNAR_APPEND v1"));
  }
  if (bytes.size() < kApplicationPayloadHeaderLength) {
    return std::unexpected(incomplete("COLUMNAR_APPEND requires its complete 160-byte header",
                                      kApplicationPayloadHeaderLength));
  }

  const common::ByteView header =
      bytes.subspan(wal::kApplicationEnvelopeSize, kCommandHeaderLength);
  if (load_u32_le(header, kCommandHeaderLengthOffset) != kCommandHeaderLength) {
    return std::unexpected(corruption("COLUMNAR_APPEND header length is invalid"));
  }
  if (load_u32_le(header, kCommandFlagsOffset) != 0U ||
      load_u32_le(header, kMutationKindOffset) != kMutationKindAppendRows ||
      load_u32_le(header, kDigestAlgorithmOffset) != kDigestAlgorithmSha256 ||
      load_u32_le(header, kOutcomeCodeOffset) != kOutcomeCodeApplied ||
      load_u32_le(header, kOutcomeFlagsOffset) != 0U) {
    return std::unexpected(unsupported("COLUMNAR_APPEND contains unsupported required semantics"));
  }
  if (load_u32_le(header, kReservedOffset) != 0U) {
    return std::unexpected(corruption("COLUMNAR_APPEND reserved field is nonzero"));
  }

  const std::uint32_t batch_length = load_u32_le(header, kBatchLengthOffset);
  if (batch_length == 0U || batch_length > columnar::format::kMaximumEmbeddedBatchLength) {
    return std::unexpected(corruption("COLUMNAR_APPEND batch length is outside v1 bounds"));
  }
  const std::optional<std::size_t> total_length = common::checked_add(
      kApplicationPayloadHeaderLength, static_cast<std::size_t>(batch_length));
  if (!total_length.has_value() || *total_length > kMaximumApplicationPayloadLength) {
    return std::unexpected(corruption("COLUMNAR_APPEND payload length is outside v1 bounds"));
  }
  if (*total_length > limits.max_application_payload_length ||
      batch_length > limits.batch.max_batch_length) {
    return std::unexpected(limit_exceeded("COLUMNAR_APPEND exceeds configured decode limits"));
  }
  if (bytes.size() < *total_length) {
    return std::unexpected(
        incomplete("complete COLUMNAR_APPEND extends beyond input", *total_length));
  }

  const common::Result<ClientId> client_id = read_identifier<ClientId>(header, kClientIdOffset);
  const common::Result<ClientBatchId> client_batch_id =
      read_identifier<ClientBatchId>(header, kClientBatchIdOffset);
  const common::Result<schema::TableId> table_id =
      read_identifier<schema::TableId>(header, kTableIdOffset);
  const common::Result<schema::TabletId> tablet_id =
      read_identifier<schema::TabletId>(header, kTabletIdOffset);
  const common::Result<schema::SchemaId> schema_id =
      read_identifier<schema::SchemaId>(header, kSchemaIdOffset);
  const common::Result<schema::SchemaVersion> schema_version =
      schema::SchemaVersion::from_value(load_u64_le(header, kSchemaVersionOffset));
  if (!client_id.has_value() || !client_batch_id.has_value() || !table_id.has_value() ||
      !tablet_id.has_value() || !schema_id.has_value() || !schema_version.has_value()) {
    return std::unexpected(corruption("COLUMNAR_APPEND contains a zero identity or schema version"));
  }

  const std::uint32_t row_count = load_u32_le(header, kRowCountOffset);
  if (row_count == 0U || load_u32_le(header, kOutcomeRowCountOffset) != row_count) {
    return std::unexpected(corruption("COLUMNAR_APPEND row-count metadata is invalid"));
  }
  const common::ByteView encoded_batch =
      bytes.subspan(kApplicationPayloadHeaderLength, batch_length);
  columnar::ColumnarBatchDecodeResult batch =
      columnar::decode_columnar_batch_v1_exact(encoded_batch, limits.batch);
  if (!batch.has_value()) {
    return std::unexpected(map_batch_error(batch.error()));
  }
  if (batch->table_id() != *table_id || batch->schema_id() != *schema_id ||
      batch->schema_version() != *schema_version || batch->row_count() != row_count) {
    return std::unexpected(
        corruption("COLUMNAR_APPEND metadata does not agree with the embedded batch"));
  }

  Sha256Digest::Bytes digest_bytes{};
  std::copy_n(header.begin() + static_cast<std::ptrdiff_t>(kRequestDigestOffset),
              digest_bytes.size(), digest_bytes.begin());
  const Sha256Digest request_digest{digest_bytes};
  const common::Result<Sha256Digest> expected_digest =
      compute_columnar_append_v1_request_digest(ColumnarAppendDigestInput{
          .table_id = *table_id,
          .tablet_id = *tablet_id,
          .schema_id = *schema_id,
          .schema_version = *schema_version,
          .encoded_batch = encoded_batch});
  if (!expected_digest.has_value()) {
    return std::unexpected(internal_error(expected_digest.error()));
  }
  if (request_digest != *expected_digest) {
    return std::unexpected(corruption("COLUMNAR_APPEND request digest mismatch"));
  }

  return DecodedColumnarAppendView{*client_id, *client_batch_id, *table_id, *tablet_id,
                                   *schema_id, *schema_version, row_count, request_digest,
                                   std::move(*batch), bytes.first(*total_length)};
}

ColumnarAppendDecodeResult
decode_columnar_append_v1_exact(const common::ByteView bytes,
                                const ColumnarAppendDecodeLimits limits) {
  ColumnarAppendDecodeResult decoded = decode_columnar_append_v1_prefix(bytes, limits);
  if (!decoded.has_value()) {
    return decoded;
  }
  if (decoded->encoded_payload().size() != bytes.size()) {
    return std::unexpected(corruption("exact COLUMNAR_APPEND input contains trailing bytes"));
  }
  return decoded;
}

ColumnarAppendDecodeResult
decode_columnar_append_v1_record(const wal::DecodedRecord& record,
                                 const ColumnarAppendDecodeLimits limits) {
  if (record.header.record_format != wal::kRecordFormat ||
      record.header.record_type != wal::kApplicationEntryRecordType) {
    return std::unexpected(unsupported("WAL record is not an application-entry v1 record"));
  }
  if (record.header.payload_length != record.payload.size()) {
    return std::unexpected(corruption("WAL record payload length does not match its decoded view"));
  }
  return decode_columnar_append_v1_exact(record.payload, limits);
}

common::Status validate_columnar_append_schema(const DecodedColumnarAppendView& command,
                                               const schema::TableSchema& schema) {
  if (command.table_id() != schema.table_id() || command.schema_id() != schema.schema_id() ||
      command.schema_version() != schema.version()) {
    return invalid_argument("COLUMNAR_APPEND identity or version does not match the schema");
  }
  return columnar::validate_columnar_batch_schema(command.batch(), schema);
}

} // namespace chronos::ingest
