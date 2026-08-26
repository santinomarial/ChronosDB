#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_transport.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <limits>
#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>

namespace chronos::cluster {
namespace {

namespace Format = distributed_vector_grouped_aggregate_shuffle_result_format;

inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                  std::byte{'V'}, std::byte{'G'}, std::byte{'R'},
                                                  std::byte{'R'}, std::byte{'1'}};
inline constexpr std::uint16_t kTerminalFlag = 1U;
inline constexpr std::size_t kHeaderCrcOffset = 124U;
inline constexpr std::size_t kReservedLength = 36U;

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status unsupported(const char* message) {
  return {common::StatusCode::kNotSupported, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status
validate_limits(const DistributedVectorGroupedAggregateShuffleResultDecodeLimits& limits) {
  if (limits.maximum_frame_length < Format::kHeaderLength + Format::kTrailerLength ||
      limits.maximum_frame_length > Format::kMaximumFrameLength ||
      limits.result_batch.protocol.maximum_payload_size == 0U ||
      limits.result_batch.protocol.maximum_payload_size > network::kDefaultMaximumPayloadSize ||
      limits.result_batch.maximum_rows == 0U || limits.result_batch.maximum_columns == 0U ||
      limits.result_batch.maximum_columns >
          query::distributed_vector_result_schema_format::kMaximumColumns ||
      limits.result_batch.maximum_column_name_bytes == 0U ||
      limits.result_batch.maximum_column_name_bytes > 65'536U) {
    return invalid("grouped shuffle result limits are invalid");
  }
  return common::Status::ok();
}

[[nodiscard]] bool descriptors_match(const network::QueryResultBatchView& batch,
                                     const query::DistributedVectorResultSchema& result_schema) {
  const auto columns = batch.columns();
  if (columns.size() != result_schema.columns.size())
    return false;
  for (std::size_t index = 0U; index < columns.size(); ++index) {
    const auto& expected = result_schema.columns[index];
    if (columns[index].name != expected.name || columns[index].type != expected.type ||
        columns[index].nullable != expected.nullable) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] common::Result<std::uint32_t>
result_schema_fingerprint(const query::DistributedVectorResultSchema& result_schema) {
  auto encoded = query::encode_distributed_vector_result_schema(result_schema);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  const common::ByteView bytes = encoded->bytes();
  return common::crc32c(bytes.first(bytes.size() - sizeof(std::uint32_t)));
}

[[nodiscard]] common::Status
validate_identity(const DistributedVectorGroupedAggregateShuffleResultFrame& frame,
                  const DistributedVectorGroupedAggregateShuffleAuthority& authority,
                  const raft::NodeId coordinator_node_id) {
  if (coordinator_node_id == 0U || frame.query_id != authority.query_id() ||
      frame.source_node_id == 0U || frame.target_node_id != coordinator_node_id ||
      frame.source_node_id == frame.target_node_id ||
      frame.partition_count != authority.partition_count() ||
      frame.partition_id >= frame.partition_count ||
      frame.hash_version != authority.hash_version() || frame.sequence == 0U) {
    return invalid("grouped shuffle result identity differs from authority");
  }
  const auto destination = authority.destination_node(frame.partition_id);
  if (!destination.has_value() || *destination != frame.source_node_id)
    return invalid("grouped shuffle result source does not own the partition");
  return common::Status::ok();
}

[[nodiscard]] common::Status
validate_batch(const DistributedVectorGroupedAggregateShuffleResultFrame& frame,
               const query::DistributedVectorResultSchema& result_schema,
               const network::QueryResultLimits& limits) {
  common::Status schema = query::validate_distributed_vector_result_schema_value(result_schema);
  if (!schema.is_ok())
    return schema;
  if (result_schema.columns.size() > limits.maximum_columns)
    return exhausted("grouped shuffle result schema exceeds the column limit");
  for (const auto& column : result_schema.columns) {
    if (column.name.size() > limits.maximum_column_name_bytes)
      return exhausted("grouped shuffle result schema exceeds the name limit");
  }
  if (frame.encoded_result_batch.empty()) {
    return frame.terminal ? common::Status::ok()
                          : invalid("grouped shuffle result nonterminal frame is empty");
  }
  auto batch = network::decode_query_result_batch(frame.encoded_result_batch, limits);
  if (!batch.has_value())
    return batch.error();
  if (batch->row_count() == 0U)
    return invalid("grouped shuffle result batch must contain a row");
  return descriptors_match(*batch, result_schema)
             ? common::Status::ok()
             : invalid("grouped shuffle result schema differs from authority");
}

struct ParsedHeader {
  std::size_t total_length{};
  common::Uuid query_id;
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  std::uint32_t partition_id{};
  std::uint32_t partition_count{};
  std::uint16_t hash_version{};
  std::uint64_t sequence{};
  bool terminal{};
  std::uint32_t payload_length{};
  std::uint32_t payload_crc{};
};

[[nodiscard]] common::Result<ParsedHeader>
parse_header(const common::ByteView header,
             const DistributedVectorGroupedAggregateShuffleAuthority& authority,
             const query::DistributedVectorResultSchema& result_schema,
             const raft::NodeId coordinator_node_id,
             const DistributedVectorGroupedAggregateShuffleResultDecodeLimits& limits) {
  if (header.size() != Format::kHeaderLength ||
      !std::ranges::equal(header.first(kMagic.size()), kMagic)) {
    return common::make_unexpected(corruption("grouped shuffle result magic is invalid"));
  }
  const common::Status limit_status = validate_limits(limits);
  if (!limit_status.is_ok())
    return common::make_unexpected(limit_status);
  common::ByteReader crc_reader{header.last(4U)};
  const auto stored_header_crc = crc_reader.read_u32_le();
  if (!stored_header_crc.has_value() ||
      *stored_header_crc != common::crc32c(header.first(kHeaderCrcOffset))) {
    return common::make_unexpected(corruption("grouped shuffle result header checksum differs"));
  }
  common::ByteReader reader{header.subspan(kMagic.size())};
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto total_length = reader.read_u64_le();
  const auto source_node = reader.read_u64_le();
  const auto target_node = reader.read_u64_le();
  const auto query_bytes = reader.read_exact(common::Uuid::kSize);
  const auto partition_id = reader.read_u32_le();
  const auto partition_count = reader.read_u32_le();
  const auto hash_version = reader.read_u16_le();
  const auto flags = reader.read_u16_le();
  const auto sequence = reader.read_u64_le();
  const auto payload_length = reader.read_u32_le();
  const auto payload_crc = reader.read_u32_le();
  const auto schema_crc = reader.read_u32_le();
  const auto reserved = reader.read_exact(kReservedLength);
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !total_length.has_value() || !source_node.has_value() || !target_node.has_value() ||
      !query_bytes.has_value() || !partition_id.has_value() || !partition_count.has_value() ||
      !hash_version.has_value() || !flags.has_value() || !sequence.has_value() ||
      !payload_length.has_value() || !payload_crc.has_value() || !schema_crc.has_value() ||
      !reserved.has_value()) {
    return common::make_unexpected(corruption("grouped shuffle result header is truncated"));
  }
  if (*major != Format::kMajor || *minor != Format::kMinor)
    return common::make_unexpected(unsupported("grouped shuffle result version is unsupported"));
  const auto expected_schema_crc = result_schema_fingerprint(result_schema);
  if (!expected_schema_crc.has_value())
    return common::make_unexpected(expected_schema_crc.error());
  if (*header_length != Format::kHeaderLength || (*flags & ~kTerminalFlag) != 0U ||
      *payload_length > limits.result_batch.protocol.maximum_payload_size ||
      *total_length != Format::kHeaderLength + *payload_length + Format::kTrailerLength ||
      *total_length > limits.maximum_frame_length || *schema_crc != *expected_schema_crc ||
      !std::ranges::all_of(*reserved, [](const std::byte value) { return value == std::byte{}; })) {
    return common::make_unexpected(corruption("grouped shuffle result header is noncanonical"));
  }
  common::Uuid::Bytes owned_query{};
  std::ranges::copy(*query_bytes, owned_query.begin());
  const common::Uuid query_id{owned_query};
  DistributedVectorGroupedAggregateShuffleResultFrame identity{.query_id = query_id,
                                                               .source_node_id = *source_node,
                                                               .target_node_id = *target_node,
                                                               .partition_id = *partition_id,
                                                               .partition_count = *partition_count,
                                                               .hash_version = *hash_version,
                                                               .sequence = *sequence,
                                                               .terminal =
                                                                   (*flags & kTerminalFlag) != 0U};
  if (!validate_identity(identity, authority, coordinator_node_id).is_ok()) {
    return common::make_unexpected(
        corruption("grouped shuffle result header differs from immutable authority"));
  }
  if (*payload_length == 0U && !identity.terminal) {
    return common::make_unexpected(
        corruption("grouped shuffle result nonterminal header is empty"));
  }
  return ParsedHeader{.total_length = static_cast<std::size_t>(*total_length),
                      .query_id = query_id,
                      .source_node_id = *source_node,
                      .target_node_id = *target_node,
                      .partition_id = *partition_id,
                      .partition_count = *partition_count,
                      .hash_version = *hash_version,
                      .sequence = *sequence,
                      .terminal = identity.terminal,
                      .payload_length = *payload_length,
                      .payload_crc = *payload_crc};
}

} // namespace

common::Result<std::vector<std::byte>>
encode_distributed_vector_grouped_aggregate_shuffle_result_frame(
    const DistributedVectorGroupedAggregateShuffleResultFrame& frame,
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema,
    const raft::NodeId coordinator_node_id) {
  const common::Status identity = validate_identity(frame, authority, coordinator_node_id);
  if (!identity.is_ok())
    return common::make_unexpected(identity);
  const common::Status batch = validate_batch(frame, result_schema, {});
  if (!batch.is_ok())
    return common::make_unexpected(batch.code() == common::StatusCode::kResourceExhausted
                                       ? batch
                                       : invalid("grouped shuffle result batch is invalid"));
  if (frame.encoded_result_batch.size() > network::kDefaultMaximumPayloadSize ||
      frame.encoded_result_batch.size() > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(invalid("grouped shuffle result batch is too large"));
  }
  auto schema_crc = result_schema_fingerprint(result_schema);
  if (!schema_crc.has_value())
    return common::make_unexpected(schema_crc.error());
  try {
    const std::size_t total =
        Format::kHeaderLength + frame.encoded_result_batch.size() + Format::kTrailerLength;
    std::vector<std::byte> bytes(total);
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kMagic);
    if (status.is_ok())
      status = writer.write_u16_le(Format::kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(Format::kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(Format::kHeaderLength);
    if (status.is_ok())
      status = writer.write_u64_le(total);
    if (status.is_ok())
      status = writer.write_u64_le(frame.source_node_id);
    if (status.is_ok())
      status = writer.write_u64_le(frame.target_node_id);
    if (status.is_ok())
      status = writer.write_exact(frame.query_id.bytes());
    if (status.is_ok())
      status = writer.write_u32_le(frame.partition_id);
    if (status.is_ok())
      status = writer.write_u32_le(frame.partition_count);
    if (status.is_ok())
      status = writer.write_u16_le(frame.hash_version);
    if (status.is_ok())
      status = writer.write_u16_le(frame.terminal ? kTerminalFlag : 0U);
    if (status.is_ok())
      status = writer.write_u64_le(frame.sequence);
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(frame.encoded_result_batch.size()));
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(frame.encoded_result_batch));
    if (status.is_ok())
      status = writer.write_u32_le(*schema_crc);
    if (status.is_ok())
      status = writer.zero_fill(kReservedLength);
    if (!status.is_ok() || writer.offset() != kHeaderCrcOffset)
      return common::make_unexpected(invalid("grouped shuffle result header size is inconsistent"));
    status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (status.is_ok())
      status = writer.write_exact(frame.encoded_result_batch);
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(total - 4U)));
    if (!status.is_ok() || !writer.full())
      return common::make_unexpected(invalid("grouped shuffle result frame size is inconsistent"));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle result encode allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle result frame exceeds limits"));
  }
}

common::Result<DistributedVectorGroupedAggregateShuffleResultFrame>
decode_distributed_vector_grouped_aggregate_shuffle_result_frame_exact(
    const common::ByteView bytes,
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema,
    const raft::NodeId coordinator_node_id,
    const DistributedVectorGroupedAggregateShuffleResultDecodeLimits limits) {
  const common::Status limit_status = validate_limits(limits);
  if (!limit_status.is_ok())
    return common::make_unexpected(limit_status);
  if (bytes.size() < Format::kHeaderLength + Format::kTrailerLength ||
      bytes.size() > Format::kMaximumFrameLength)
    return common::make_unexpected(corruption("grouped shuffle result length is invalid"));
  const auto header = parse_header(bytes.first(Format::kHeaderLength), authority, result_schema,
                                   coordinator_node_id, limits);
  if (!header.has_value())
    return common::make_unexpected(header.error());
  if (header->total_length != bytes.size())
    return common::make_unexpected(corruption("grouped shuffle result frame is not exact"));
  const common::ByteView payload = bytes.subspan(Format::kHeaderLength, header->payload_length);
  if (common::crc32c(payload) != header->payload_crc)
    return common::make_unexpected(corruption("grouped shuffle result payload checksum differs"));
  common::ByteReader trailer{bytes.last(4U)};
  const auto stored_frame_crc = trailer.read_u32_le();
  if (!stored_frame_crc.has_value() ||
      *stored_frame_crc != common::crc32c(bytes.first(bytes.size() - 4U))) {
    return common::make_unexpected(corruption("grouped shuffle result frame checksum differs"));
  }
  try {
    DistributedVectorGroupedAggregateShuffleResultFrame result{
        .query_id = header->query_id,
        .source_node_id = header->source_node_id,
        .target_node_id = header->target_node_id,
        .partition_id = header->partition_id,
        .partition_count = header->partition_count,
        .hash_version = header->hash_version,
        .sequence = header->sequence,
        .terminal = header->terminal,
        .encoded_result_batch = {payload.begin(), payload.end()}};
    const common::Status batch = validate_batch(result, result_schema, limits.result_batch);
    if (!batch.is_ok()) {
      return common::make_unexpected(batch.code() == common::StatusCode::kResourceExhausted
                                         ? batch
                                         : corruption("grouped shuffle result batch is invalid"));
    }
    return result;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle result decode allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle result decode exceeds limits"));
  }
}

DistributedVectorGroupedAggregateShuffleResultReader::
    DistributedVectorGroupedAggregateShuffleResultReader(
        const DistributedVectorGroupedAggregateShuffleAuthority& authority,
        const query::DistributedVectorResultSchema& result_schema,
        const raft::NodeId coordinator_node_id,
        const DistributedVectorGroupedAggregateShuffleResultDecodeLimits limits) noexcept
    : authority_(authority), result_schema_(result_schema),
      coordinator_node_id_(coordinator_node_id), limits_(limits) {}

common::Result<DistributedVectorGroupedAggregateShuffleResultReadStep>
DistributedVectorGroupedAggregateShuffleResultReader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  const common::Status limit_status = validate_limits(limits_);
  if (!limit_status.is_ok())
    return common::make_unexpected(limit_status);
  std::size_t consumed{};
  try {
    if (frame_.empty()) {
      const std::size_t copied = std::min(bytes.size(), header_.size() - header_bytes_);
      std::ranges::copy(bytes.first(copied),
                        header_.begin() + static_cast<std::ptrdiff_t>(header_bytes_));
      header_bytes_ += copied;
      consumed += copied;
      if (header_bytes_ != header_.size())
        return DistributedVectorGroupedAggregateShuffleResultReadStep{.consumed_bytes = consumed};
      const auto parsed = parse_header(header_, authority_.get(), result_schema_.get(),
                                       coordinator_node_id_, limits_);
      if (!parsed.has_value()) {
        failure_ = parsed.error();
        return common::make_unexpected(*failure_);
      }
      frame_.resize(parsed->total_length);
      std::ranges::copy(header_, frame_.begin());
      frame_bytes_ = header_.size();
    }
    const std::size_t available = bytes.size() - consumed;
    const std::size_t copied = std::min(available, frame_.size() - frame_bytes_);
    std::ranges::copy(bytes.subspan(consumed, copied),
                      frame_.begin() + static_cast<std::ptrdiff_t>(frame_bytes_));
    frame_bytes_ += copied;
    consumed += copied;
    if (frame_bytes_ != frame_.size())
      return DistributedVectorGroupedAggregateShuffleResultReadStep{.consumed_bytes = consumed};
    auto decoded = decode_distributed_vector_grouped_aggregate_shuffle_result_frame_exact(
        frame_, authority_.get(), result_schema_.get(), coordinator_node_id_, limits_);
    if (!decoded.has_value()) {
      failure_ = decoded.error();
      return common::make_unexpected(*failure_);
    }
    frame_.clear();
    frame_bytes_ = 0U;
    header_bytes_ = 0U;
    return DistributedVectorGroupedAggregateShuffleResultReadStep{.consumed_bytes = consumed,
                                                                  .frame = std::move(*decoded)};
  } catch (const std::bad_alloc&) {
    failure_ = exhausted("grouped shuffle result reader allocation failed");
    return common::make_unexpected(*failure_);
  } catch (const std::length_error&) {
    failure_ = exhausted("grouped shuffle result reader exceeds limits");
    return common::make_unexpected(*failure_);
  }
}

std::size_t DistributedVectorGroupedAggregateShuffleResultReader::buffered_bytes() const noexcept {
  return frame_.empty() ? header_bytes_ : frame_bytes_;
}

bool DistributedVectorGroupedAggregateShuffleResultReader::failed() const noexcept {
  return failure_.has_value();
}

DistributedVectorGroupedAggregateShuffleResultWriteCursor::
    DistributedVectorGroupedAggregateShuffleResultWriteCursor(
        std::vector<std::byte> encoded) noexcept
    : encoded_(std::move(encoded)) {}

DistributedVectorGroupedAggregateShuffleResultWriteCursor::
    DistributedVectorGroupedAggregateShuffleResultWriteCursor(
        DistributedVectorGroupedAggregateShuffleResultWriteCursor&& other) noexcept
    : encoded_(std::move(other.encoded_)), written_bytes_(other.written_bytes_) {
  other.written_bytes_ = other.encoded_.size();
}

DistributedVectorGroupedAggregateShuffleResultWriteCursor&
DistributedVectorGroupedAggregateShuffleResultWriteCursor::operator=(
    DistributedVectorGroupedAggregateShuffleResultWriteCursor&& other) noexcept {
  if (this != &other) {
    encoded_ = std::move(other.encoded_);
    written_bytes_ = other.written_bytes_;
    other.written_bytes_ = other.encoded_.size();
  }
  return *this;
}

common::Result<DistributedVectorGroupedAggregateShuffleResultWriteCursor>
DistributedVectorGroupedAggregateShuffleResultWriteCursor::create(
    const DistributedVectorGroupedAggregateShuffleResultFrame& frame,
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema,
    const raft::NodeId coordinator_node_id) {
  auto encoded = encode_distributed_vector_grouped_aggregate_shuffle_result_frame(
      frame, authority, result_schema, coordinator_node_id);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return DistributedVectorGroupedAggregateShuffleResultWriteCursor{std::move(*encoded)};
}

common::ByteView
DistributedVectorGroupedAggregateShuffleResultWriteCursor::pending_write() const noexcept {
  return common::ByteView{encoded_}.subspan(written_bytes_);
}

common::Status DistributedVectorGroupedAggregateShuffleResultWriteCursor::consume_written(
    const std::size_t bytes) noexcept {
  if (bytes > encoded_.size() - written_bytes_)
    return invalid("grouped shuffle result write progress exceeds pending bytes");
  written_bytes_ += bytes;
  return common::Status::ok();
}

std::size_t
DistributedVectorGroupedAggregateShuffleResultWriteCursor::written_bytes() const noexcept {
  return written_bytes_;
}

bool DistributedVectorGroupedAggregateShuffleResultWriteCursor::complete() const noexcept {
  return written_bytes_ == encoded_.size();
}

} // namespace chronos::cluster
