#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_ack.hpp"

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_stream.hpp"
#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <new>
#include <ranges>
#include <utility>

namespace chronos::cluster {
namespace {

inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                  std::byte{'V'}, std::byte{'G'}, std::byte{'R'},
                                                  std::byte{'K'}, std::byte{'1'}};
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderCrcOffset = 124U;

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

[[nodiscard]] common::Result<common::Uuid> read_uuid(common::ByteReader& reader) {
  const auto bytes = reader.read_exact(common::Uuid::kSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::Uuid::Bytes owned{};
  std::ranges::copy(*bytes, owned.begin());
  return common::Uuid{owned};
}

[[nodiscard]] common::Result<std::uint32_t>
schema_fingerprint(const query::DistributedVectorResultSchema& result_schema) {
  auto encoded = query::encode_distributed_vector_result_schema(result_schema);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  const common::ByteView bytes = encoded->bytes();
  return common::crc32c(bytes.first(bytes.size() - sizeof(std::uint32_t)));
}

[[nodiscard]] common::Status
validate_ack(const DistributedVectorGroupedAggregateShuffleResultAckV1& ack,
             const DistributedVectorGroupedAggregateShuffleAuthority& authority,
             const query::DistributedVectorResultSchema& result_schema,
             const raft::NodeId coordinator_node_id) {
  common::Status schema_status =
      query::validate_distributed_vector_result_schema_value(result_schema);
  if (!schema_status.is_ok())
    return schema_status;
  if (coordinator_node_id == 0U || ack.query_id != authority.query_id() ||
      ack.source_node_id == 0U || ack.target_node_id != coordinator_node_id ||
      ack.source_node_id == ack.target_node_id ||
      ack.partition_count != authority.partition_count() ||
      ack.partition_id >= ack.partition_count || ack.hash_version != authority.hash_version() ||
      ack.accepted_frames == 0U ||
      ack.accepted_frames > kMaximumDistributedVectorGroupedAggregateShuffleResultStreamFrames ||
      ack.accepted_bytes <
          distributed_vector_grouped_aggregate_shuffle_result_format::kHeaderLength +
              distributed_vector_grouped_aggregate_shuffle_result_format::kTrailerLength ||
      ack.accepted_bytes > kMaximumDistributedVectorGroupedAggregateShuffleResultStreamBytes) {
    return invalid("grouped shuffle result acknowledgment identity or extent is invalid");
  }
  const auto destination = authority.destination_node(ack.partition_id);
  return destination.has_value() && *destination == ack.source_node_id
             ? common::Status::ok()
             : invalid("grouped shuffle result acknowledgment source differs from authority");
}

} // namespace

common::Result<std::vector<std::byte>>
encode_distributed_vector_grouped_aggregate_shuffle_result_ack_v1(
    const DistributedVectorGroupedAggregateShuffleResultAckV1& ack,
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema,
    const raft::NodeId coordinator_node_id) {
  const common::Status valid = validate_ack(ack, authority, result_schema, coordinator_node_id);
  if (!valid.is_ok())
    return common::make_unexpected(valid);
  auto schema_crc = schema_fingerprint(result_schema);
  if (!schema_crc.has_value())
    return common::make_unexpected(schema_crc.error());
  try {
    std::vector<std::byte> bytes(kDistributedVectorGroupedAggregateShuffleResultAckV1Size);
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kMagic);
    if (status.is_ok())
      status = writer.write_u16_le(kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(kDistributedVectorGroupedAggregateShuffleResultAckV1HeaderSize);
    if (status.is_ok())
      status = writer.write_u64_le(kDistributedVectorGroupedAggregateShuffleResultAckV1Size);
    if (status.is_ok())
      status = writer.write_u64_le(ack.target_node_id);
    if (status.is_ok())
      status = writer.write_u64_le(ack.source_node_id);
    if (status.is_ok())
      status = writer.write_exact(ack.query_id.bytes());
    if (status.is_ok())
      status = writer.write_u32_le(ack.partition_id);
    if (status.is_ok())
      status = writer.write_u32_le(ack.partition_count);
    if (status.is_ok())
      status = writer.write_u16_le(ack.hash_version);
    if (status.is_ok())
      status = writer.zero_fill(2U);
    if (status.is_ok())
      status = writer.write_u32_le(ack.accepted_frames);
    if (status.is_ok())
      status = writer.write_u64_le(ack.accepted_bytes);
    if (status.is_ok())
      status = writer.write_u32_le(*schema_crc);
    if (status.is_ok())
      status = writer.zero_fill(40U);
    if (!status.is_ok() || writer.offset() != kHeaderCrcOffset)
      return common::make_unexpected(
          invalid("grouped shuffle result acknowledgment header is invalid"));
    status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(128U)));
    if (!status.is_ok() || !writer.full())
      return common::make_unexpected(
          invalid("grouped shuffle result acknowledgment size is invalid"));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped shuffle result acknowledgment allocation failed"));
  }
}

common::Result<DistributedVectorGroupedAggregateShuffleResultAckV1>
decode_distributed_vector_grouped_aggregate_shuffle_result_ack_v1_exact(
    const common::ByteView bytes,
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema,
    const raft::NodeId coordinator_node_id) {
  if (bytes.size() != kDistributedVectorGroupedAggregateShuffleResultAckV1Size ||
      !std::ranges::equal(bytes.first(kMagic.size()), kMagic)) {
    return common::make_unexpected(
        corruption("grouped shuffle result acknowledgment framing is invalid"));
  }
  common::ByteReader header_crc_reader{bytes.subspan(kHeaderCrcOffset, 4U)};
  const auto header_crc = header_crc_reader.read_u32_le();
  common::ByteReader frame_crc_reader{bytes.last(4U)};
  const auto frame_crc = frame_crc_reader.read_u32_le();
  if (!header_crc.has_value() || *header_crc != common::crc32c(bytes.first(kHeaderCrcOffset)) ||
      !frame_crc.has_value() || *frame_crc != common::crc32c(bytes.first(128U))) {
    return common::make_unexpected(
        corruption("grouped shuffle result acknowledgment checksum differs"));
  }
  common::ByteReader reader{bytes.subspan(kMagic.size())};
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto total_length = reader.read_u64_le();
  const auto ack_source = reader.read_u64_le();
  const auto ack_target = reader.read_u64_le();
  const auto query_id = read_uuid(reader);
  const auto partition_id = reader.read_u32_le();
  const auto partition_count = reader.read_u32_le();
  const auto hash_version = reader.read_u16_le();
  const auto flags = reader.read_u16_le();
  const auto accepted_frames = reader.read_u32_le();
  const auto accepted_bytes = reader.read_u64_le();
  const auto schema_crc = reader.read_u32_le();
  const auto reserved = reader.read_exact(40U);
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !total_length.has_value() || !ack_source.has_value() || !ack_target.has_value() ||
      !query_id.has_value() || !partition_id.has_value() || !partition_count.has_value() ||
      !hash_version.has_value() || !flags.has_value() || !accepted_frames.has_value() ||
      !accepted_bytes.has_value() || !schema_crc.has_value() || !reserved.has_value()) {
    return common::make_unexpected(
        corruption("grouped shuffle result acknowledgment header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(
        unsupported("grouped shuffle result acknowledgment version unsupported"));
  auto expected_schema_crc = schema_fingerprint(result_schema);
  if (!expected_schema_crc.has_value())
    return common::make_unexpected(expected_schema_crc.error());
  if (*header_length != kDistributedVectorGroupedAggregateShuffleResultAckV1HeaderSize ||
      *total_length != kDistributedVectorGroupedAggregateShuffleResultAckV1Size || *flags != 0U ||
      *schema_crc != *expected_schema_crc ||
      !std::ranges::all_of(*reserved, [](const std::byte value) { return value == std::byte{}; })) {
    return common::make_unexpected(
        corruption("grouped shuffle result acknowledgment header is noncanonical"));
  }
  DistributedVectorGroupedAggregateShuffleResultAckV1 ack{.query_id = *query_id,
                                                          .source_node_id = *ack_target,
                                                          .target_node_id = *ack_source,
                                                          .partition_id = *partition_id,
                                                          .partition_count = *partition_count,
                                                          .hash_version = *hash_version,
                                                          .accepted_frames = *accepted_frames,
                                                          .accepted_bytes = *accepted_bytes};
  if (!validate_ack(ack, authority, result_schema, coordinator_node_id).is_ok()) {
    return common::make_unexpected(
        corruption("grouped shuffle result acknowledgment differs from authority"));
  }
  return ack;
}

DistributedVectorGroupedAggregateShuffleResultAckV1Reader::
    DistributedVectorGroupedAggregateShuffleResultAckV1Reader(
        const DistributedVectorGroupedAggregateShuffleAuthority& authority,
        const query::DistributedVectorResultSchema& result_schema,
        const raft::NodeId coordinator_node_id) noexcept
    : authority_(authority), result_schema_(result_schema),
      coordinator_node_id_(coordinator_node_id) {}

common::Result<DistributedVectorGroupedAggregateShuffleResultAckV1ReadStep>
DistributedVectorGroupedAggregateShuffleResultAckV1Reader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  if (complete_) {
    if (!bytes.empty()) {
      failure_ = corruption("grouped shuffle result acknowledgment has a suffix");
      return common::make_unexpected(*failure_);
    }
    return DistributedVectorGroupedAggregateShuffleResultAckV1ReadStep{};
  }
  const std::size_t copied = std::min(bytes.size(), bytes_.size() - buffered_bytes_);
  std::ranges::copy(bytes.first(copied),
                    bytes_.begin() + static_cast<std::ptrdiff_t>(buffered_bytes_));
  buffered_bytes_ += copied;
  if (buffered_bytes_ != bytes_.size())
    return DistributedVectorGroupedAggregateShuffleResultAckV1ReadStep{.consumed_bytes = copied};
  auto decoded = decode_distributed_vector_grouped_aggregate_shuffle_result_ack_v1_exact(
      bytes_, authority_, result_schema_, coordinator_node_id_);
  if (!decoded.has_value()) {
    failure_ = decoded.error();
    return common::make_unexpected(*failure_);
  }
  complete_ = true;
  return DistributedVectorGroupedAggregateShuffleResultAckV1ReadStep{.consumed_bytes = copied,
                                                                     .ack = *decoded};
}

std::size_t
DistributedVectorGroupedAggregateShuffleResultAckV1Reader::buffered_bytes() const noexcept {
  return buffered_bytes_;
}

bool DistributedVectorGroupedAggregateShuffleResultAckV1Reader::failed() const noexcept {
  return failure_.has_value();
}

DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor::
    DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor(
        std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor::
    DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor(
        DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor&& other) noexcept
    : bytes_(std::move(other.bytes_)), written_bytes_(other.written_bytes_) {
  other.written_bytes_ = other.bytes_.size();
}

DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor&
DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor::operator=(
    DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor&& other) noexcept {
  if (this != &other) {
    bytes_ = std::move(other.bytes_);
    written_bytes_ = other.written_bytes_;
    other.written_bytes_ = other.bytes_.size();
  }
  return *this;
}

common::Result<DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor>
DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor::create(
    const DistributedVectorGroupedAggregateShuffleResultAckV1& ack,
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema,
    const raft::NodeId coordinator_node_id) {
  auto encoded = encode_distributed_vector_grouped_aggregate_shuffle_result_ack_v1(
      ack, authority, result_schema, coordinator_node_id);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor{std::move(*encoded)};
}

common::ByteView
DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor::pending_write() const noexcept {
  return common::ByteView{bytes_}.subspan(written_bytes_);
}

common::Status DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor::consume_written(
    const std::size_t bytes) noexcept {
  if (bytes > bytes_.size() - written_bytes_)
    return invalid("written bytes exceed grouped shuffle result acknowledgment");
  written_bytes_ += bytes;
  return common::Status::ok();
}

bool DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor::complete() const noexcept {
  return written_bytes_ == bytes_.size();
}

std::size_t
DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor::written_bytes() const noexcept {
  return written_bytes_;
}

} // namespace chronos::cluster
