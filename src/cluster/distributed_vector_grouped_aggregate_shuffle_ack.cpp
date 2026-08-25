#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_ack.hpp"

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_stream.hpp"
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
                                                  std::byte{'V'}, std::byte{'G'}, std::byte{'A'},
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

[[nodiscard]] common::Status
validate_ack(const DistributedVectorGroupedAggregateShuffleAckV1& ack,
             const DistributedVectorGroupedAggregateShuffleAuthority& authority) {
  if (ack.query_id != authority.query_id() || ack.partition_count != authority.partition_count() ||
      ack.edge.source_node_id == 0U || ack.edge.target_node_id == 0U ||
      ack.edge.source_node_id == ack.edge.target_node_id || ack.accepted_frames == 0U ||
      ack.accepted_frames >
          query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups ||
      ack.accepted_bytes <
          kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize +
              query::distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength +
              kDistributedVectorGroupedAggregateShuffleFrameV1TrailerSize ||
      ack.accepted_bytes > kMaximumDistributedVectorGroupedAggregateShuffleStreamBytes) {
    return invalid("grouped shuffle acknowledgment identity or extent is invalid");
  }
  return authority.validate_edge(ack.edge);
}

} // namespace

common::Result<std::vector<std::byte>> encode_distributed_vector_grouped_aggregate_shuffle_ack_v1(
    const DistributedVectorGroupedAggregateShuffleAckV1& ack,
    const DistributedVectorGroupedAggregateShuffleAuthority& authority) {
  common::Status valid = validate_ack(ack, authority);
  if (!valid.is_ok())
    return common::make_unexpected(valid);
  try {
    std::vector<std::byte> bytes(kDistributedVectorGroupedAggregateShuffleAckV1Size);
    common::ByteWriter writer{bytes};
    common::Status write = writer.write_exact(kMagic);
    if (write.is_ok())
      write = writer.write_u16_le(kMajor);
    if (write.is_ok())
      write = writer.write_u16_le(kMinor);
    if (write.is_ok())
      write = writer.write_u32_le(kDistributedVectorGroupedAggregateShuffleAckV1HeaderSize);
    if (write.is_ok())
      write = writer.write_u64_le(kDistributedVectorGroupedAggregateShuffleAckV1Size);
    if (write.is_ok())
      write = writer.write_u64_le(ack.edge.target_node_id);
    if (write.is_ok())
      write = writer.write_u64_le(ack.edge.source_node_id);
    if (write.is_ok())
      write = writer.write_exact(ack.query_id.bytes());
    if (write.is_ok())
      write = writer.write_exact(ack.edge.tablet_id.bytes());
    if (write.is_ok())
      write = writer.write_u32_le(ack.edge.partition_id);
    if (write.is_ok())
      write = writer.write_u32_le(ack.partition_count);
    if (write.is_ok())
      write = writer.write_u16_le(ack.edge.hash_version);
    if (write.is_ok())
      write = writer.zero_fill(2U);
    if (write.is_ok())
      write = writer.write_u32_le(ack.accepted_frames);
    if (write.is_ok())
      write = writer.write_u64_le(ack.accepted_bytes);
    if (write.is_ok())
      write = writer.zero_fill(28U);
    if (!write.is_ok() || writer.offset() != kHeaderCrcOffset)
      return common::make_unexpected(invalid("grouped shuffle acknowledgment header is invalid"));
    write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(128U)));
    if (!write.is_ok() || !writer.full())
      return common::make_unexpected(invalid("grouped shuffle acknowledgment size is invalid"));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle acknowledgment allocation failed"));
  }
}

common::Result<DistributedVectorGroupedAggregateShuffleAckV1>
decode_distributed_vector_grouped_aggregate_shuffle_ack_v1_exact(
    const common::ByteView bytes,
    const DistributedVectorGroupedAggregateShuffleAuthority& authority) {
  if (bytes.size() != kDistributedVectorGroupedAggregateShuffleAckV1Size ||
      !std::ranges::equal(bytes.first(kMagic.size()), kMagic)) {
    return common::make_unexpected(corruption("grouped shuffle acknowledgment framing is invalid"));
  }
  common::ByteReader header_crc_reader{bytes.subspan(kHeaderCrcOffset, 4U)};
  const auto header_crc = header_crc_reader.read_u32_le();
  common::ByteReader frame_crc_reader{bytes.last(4U)};
  const auto frame_crc = frame_crc_reader.read_u32_le();
  if (!header_crc.has_value() || *header_crc != common::crc32c(bytes.first(kHeaderCrcOffset)) ||
      !frame_crc.has_value() || *frame_crc != common::crc32c(bytes.first(128U))) {
    return common::make_unexpected(corruption("grouped shuffle acknowledgment checksum differs"));
  }
  common::ByteReader reader{bytes.subspan(kMagic.size())};
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto total_length = reader.read_u64_le();
  const auto ack_source = reader.read_u64_le();
  const auto ack_target = reader.read_u64_le();
  const auto query_id = read_uuid(reader);
  const auto tablet_uuid = read_uuid(reader);
  const auto partition_id = reader.read_u32_le();
  const auto partition_count = reader.read_u32_le();
  const auto hash_version = reader.read_u16_le();
  const auto flags = reader.read_u16_le();
  const auto accepted_frames = reader.read_u32_le();
  const auto accepted_bytes = reader.read_u64_le();
  const auto reserved = reader.read_exact(28U);
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !total_length.has_value() || !ack_source.has_value() || !ack_target.has_value() ||
      !query_id.has_value() || !tablet_uuid.has_value() || !partition_id.has_value() ||
      !partition_count.has_value() || !hash_version.has_value() || !flags.has_value() ||
      !accepted_frames.has_value() || !accepted_bytes.has_value() || !reserved.has_value()) {
    return common::make_unexpected(
        corruption("grouped shuffle acknowledgment header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(
        unsupported("grouped shuffle acknowledgment version unsupported"));
  if (*header_length != kDistributedVectorGroupedAggregateShuffleAckV1HeaderSize ||
      *total_length != kDistributedVectorGroupedAggregateShuffleAckV1Size || *flags != 0U ||
      !std::ranges::all_of(*reserved, [](const std::byte value) { return value == std::byte{}; })) {
    return common::make_unexpected(
        corruption("grouped shuffle acknowledgment header is noncanonical"));
  }
  auto tablet_id = schema::TabletId::from_uuid(*tablet_uuid);
  if (!tablet_id.has_value())
    return common::make_unexpected(corruption("grouped shuffle acknowledgment tablet is invalid"));
  DistributedVectorGroupedAggregateShuffleAckV1 ack{.query_id = *query_id,
                                                    .edge = {.tablet_id = *tablet_id,
                                                             .partition_id = *partition_id,
                                                             .source_node_id = *ack_target,
                                                             .target_node_id = *ack_source,
                                                             .hash_version = *hash_version},
                                                    .partition_count = *partition_count,
                                                    .accepted_frames = *accepted_frames,
                                                    .accepted_bytes = *accepted_bytes};
  if (!validate_ack(ack, authority).is_ok())
    return common::make_unexpected(
        corruption("grouped shuffle acknowledgment differs from authority"));
  return ack;
}

DistributedVectorGroupedAggregateShuffleAckV1Reader::
    DistributedVectorGroupedAggregateShuffleAckV1Reader(
        const DistributedVectorGroupedAggregateShuffleAuthority& authority) noexcept
    : authority_(authority) {}

common::Result<DistributedVectorGroupedAggregateShuffleAckV1ReadStep>
DistributedVectorGroupedAggregateShuffleAckV1Reader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  if (complete_ && !bytes.empty()) {
    failure_ = corruption("grouped shuffle acknowledgment has a suffix");
    return common::make_unexpected(*failure_);
  }
  const std::size_t copied = std::min(bytes.size(), bytes_.size() - buffered_bytes_);
  std::ranges::copy(bytes.first(copied),
                    bytes_.begin() + static_cast<std::ptrdiff_t>(buffered_bytes_));
  buffered_bytes_ += copied;
  if (buffered_bytes_ != bytes_.size())
    return DistributedVectorGroupedAggregateShuffleAckV1ReadStep{.consumed_bytes = copied};
  auto decoded =
      decode_distributed_vector_grouped_aggregate_shuffle_ack_v1_exact(bytes_, authority_);
  if (!decoded.has_value()) {
    failure_ = decoded.error();
    return common::make_unexpected(*failure_);
  }
  complete_ = true;
  return DistributedVectorGroupedAggregateShuffleAckV1ReadStep{.consumed_bytes = copied,
                                                               .ack = *decoded};
}

std::size_t DistributedVectorGroupedAggregateShuffleAckV1Reader::buffered_bytes() const noexcept {
  return buffered_bytes_;
}

bool DistributedVectorGroupedAggregateShuffleAckV1Reader::failed() const noexcept {
  return failure_.has_value();
}

DistributedVectorGroupedAggregateShuffleAckV1WriteCursor::
    DistributedVectorGroupedAggregateShuffleAckV1WriteCursor(std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

DistributedVectorGroupedAggregateShuffleAckV1WriteCursor::
    DistributedVectorGroupedAggregateShuffleAckV1WriteCursor(
        DistributedVectorGroupedAggregateShuffleAckV1WriteCursor&& other) noexcept
    : bytes_(std::move(other.bytes_)), written_bytes_(other.written_bytes_) {
  other.written_bytes_ = other.bytes_.size();
}

DistributedVectorGroupedAggregateShuffleAckV1WriteCursor&
DistributedVectorGroupedAggregateShuffleAckV1WriteCursor::operator=(
    DistributedVectorGroupedAggregateShuffleAckV1WriteCursor&& other) noexcept {
  if (this != &other) {
    bytes_ = std::move(other.bytes_);
    written_bytes_ = other.written_bytes_;
    other.written_bytes_ = other.bytes_.size();
  }
  return *this;
}

common::Result<DistributedVectorGroupedAggregateShuffleAckV1WriteCursor>
DistributedVectorGroupedAggregateShuffleAckV1WriteCursor::create(
    const DistributedVectorGroupedAggregateShuffleAckV1& ack,
    const DistributedVectorGroupedAggregateShuffleAuthority& authority) {
  auto encoded = encode_distributed_vector_grouped_aggregate_shuffle_ack_v1(ack, authority);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return DistributedVectorGroupedAggregateShuffleAckV1WriteCursor{std::move(*encoded)};
}

common::ByteView
DistributedVectorGroupedAggregateShuffleAckV1WriteCursor::pending_write() const noexcept {
  return common::ByteView{bytes_}.subspan(written_bytes_);
}

common::Status
DistributedVectorGroupedAggregateShuffleAckV1WriteCursor::consume_written(const std::size_t bytes) {
  if (bytes > bytes_.size() - written_bytes_)
    return invalid("written bytes exceed grouped shuffle acknowledgment");
  written_bytes_ += bytes;
  return common::Status::ok();
}

bool DistributedVectorGroupedAggregateShuffleAckV1WriteCursor::complete() const noexcept {
  return written_bytes_ == bytes_.size();
}

std::size_t
DistributedVectorGroupedAggregateShuffleAckV1WriteCursor::written_bytes() const noexcept {
  return written_bytes_;
}

} // namespace chronos::cluster
