#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_transport.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>

namespace chronos::cluster {
namespace {

inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                  std::byte{'V'}, std::byte{'G'}, std::byte{'S'},
                                                  std::byte{'F'}, std::byte{'1'}};
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

[[nodiscard]] bool valid_payload_limits(
    const query::DistributedVectorGroupedAggregateExchangeDecodeLimits& limits) noexcept {
  return limits.maximum_frame_length >=
             query::distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength &&
         limits.maximum_frame_length <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumFrameLength &&
         limits.maximum_key_payload_bytes > 0U &&
         limits.maximum_key_payload_bytes <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumKeyPayloadBytes &&
         limits.maximum_groups > 0U &&
         limits.maximum_groups <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups &&
         limits.maximum_group_keys > 0U &&
         limits.maximum_group_keys <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroupKeys &&
         limits.maximum_aggregates <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumAggregates &&
         limits.state.maximum_frame_length >=
             query::distributed_vector_aggregate_state_format::kMinimumFrameLength &&
         limits.state.maximum_frame_length <=
             query::distributed_vector_aggregate_state_format::kMaximumFrameLength &&
         limits.state.maximum_variable_extremum_bytes > 0U &&
         limits.state.maximum_variable_extremum_bytes <=
             query::distributed_vector_aggregate_state_format::kMaximumExtremumBytes;
}

[[nodiscard]] common::Result<common::Uuid> read_uuid(common::ByteReader& reader) {
  const auto bytes = reader.read_exact(common::Uuid::kSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::Uuid::Bytes owned{};
  std::ranges::copy(*bytes, owned.begin());
  return common::Uuid{owned};
}

[[nodiscard]] common::Result<schema::TabletId> read_tablet(common::ByteReader& reader) {
  const auto value = read_uuid(reader);
  return value.has_value() ? schema::TabletId::from_uuid(*value)
                           : common::make_unexpected(value.error());
}

struct ParsedHeader {
  std::size_t total_length{};
  common::Uuid query_id;
  DistributedVectorGroupedAggregateShuffleEdge edge;
  std::uint32_t partition_count{};
  std::uint32_t payload_length{};
  std::uint32_t payload_crc{};
};

[[nodiscard]] common::Result<ParsedHeader>
parse_header(const common::ByteView header,
             const DistributedVectorGroupedAggregateShuffleAuthority& authority,
             const query::DistributedVectorGroupedAggregateExchangeDecodeLimits& payload_limits) {
  if (header.size() != kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize ||
      !std::ranges::equal(header.first(kMagic.size()), kMagic)) {
    return common::make_unexpected(corruption("grouped shuffle frame header magic is invalid"));
  }
  if (!valid_payload_limits(payload_limits))
    return common::make_unexpected(invalid("grouped shuffle payload limits are invalid"));
  common::ByteReader crc_reader{header.last(4U)};
  const auto stored_header_crc = crc_reader.read_u32_le();
  if (!stored_header_crc.has_value() ||
      *stored_header_crc != common::crc32c(header.first(kHeaderCrcOffset))) {
    return common::make_unexpected(corruption("grouped shuffle frame header checksum differs"));
  }
  common::ByteReader reader{header.subspan(kMagic.size())};
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto total_length = reader.read_u64_le();
  const auto source_node = reader.read_u64_le();
  const auto target_node = reader.read_u64_le();
  const auto query_id = read_uuid(reader);
  const auto tablet_id = read_tablet(reader);
  const auto partition_id = reader.read_u32_le();
  const auto partition_count = reader.read_u32_le();
  const auto hash_version = reader.read_u16_le();
  const auto flags = reader.read_u16_le();
  const auto payload_length = reader.read_u32_le();
  const auto payload_crc = reader.read_u32_le();
  const auto reserved = reader.read_exact(32U);
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !total_length.has_value() || !source_node.has_value() || !target_node.has_value() ||
      !query_id.has_value() || !tablet_id.has_value() || !partition_id.has_value() ||
      !partition_count.has_value() || !hash_version.has_value() || !flags.has_value() ||
      !payload_length.has_value() || !payload_crc.has_value() || !reserved.has_value()) {
    return common::make_unexpected(corruption("grouped shuffle frame header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(unsupported("grouped shuffle frame version is unsupported"));
  if (*header_length != kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize ||
      *source_node == 0U || *target_node == 0U || *source_node == *target_node ||
      query_id->is_nil() || tablet_id->uuid().is_nil() || *partition_count == 0U ||
      *partition_id >= *partition_count || *flags != 0U ||
      *payload_length <
          query::distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength ||
      *payload_length >
          query::distributed_vector_grouped_aggregate_exchange_format::kMaximumFrameLength ||
      *payload_length > payload_limits.maximum_frame_length ||
      *total_length != kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize +
                           *payload_length +
                           kDistributedVectorGroupedAggregateShuffleFrameV1TrailerSize ||
      *total_length > kMaximumDistributedVectorGroupedAggregateShuffleFrameV1Size ||
      !std::ranges::all_of(*reserved, [](const std::byte value) { return value == std::byte{}; })) {
    return common::make_unexpected(corruption("grouped shuffle frame header is noncanonical"));
  }
  const DistributedVectorGroupedAggregateShuffleEdge edge{.tablet_id = *tablet_id,
                                                          .partition_id = *partition_id,
                                                          .source_node_id = *source_node,
                                                          .target_node_id = *target_node,
                                                          .hash_version = *hash_version};
  if (*query_id != authority.query_id() || *partition_count != authority.partition_count() ||
      !authority.validate_edge(edge).is_ok()) {
    return common::make_unexpected(
        corruption("grouped shuffle frame differs from immutable authority"));
  }
  return ParsedHeader{.total_length = static_cast<std::size_t>(*total_length),
                      .query_id = *query_id,
                      .edge = edge,
                      .partition_count = *partition_count,
                      .payload_length = *payload_length,
                      .payload_crc = *payload_crc};
}

[[nodiscard]] common::Status
validate_frame(const DistributedVectorGroupedAggregateShuffleFrameV1& frame,
               const DistributedVectorGroupedAggregateShuffleAuthority& authority) {
  if (frame.query_id != authority.query_id() ||
      frame.partition_count != authority.partition_count() ||
      frame.edge.source_node_id == frame.edge.target_node_id)
    return invalid("grouped shuffle frame identity differs from authority");
  common::Status edge = authority.validate_edge(frame.edge);
  if (!edge.is_ok())
    return edge;
  const auto& position = frame.payload.position();
  if (position.query_id != frame.query_id || position.tablet_id != frame.edge.tablet_id)
    return invalid("grouped shuffle payload correlation differs from outer frame");
  if (!position.empty) {
    const auto hash = query::canonical_vector_group_key_hash_v1(authority.key_definitions(),
                                                                frame.payload.keys());
    if (!hash.has_value())
      return hash.error();
    if (*hash % frame.partition_count != frame.edge.partition_id)
      return invalid("grouped shuffle payload key is routed to the wrong partition");
  }
  return common::Status::ok();
}

} // namespace

common::Result<std::vector<std::byte>> encode_distributed_vector_grouped_aggregate_shuffle_frame_v1(
    const DistributedVectorGroupedAggregateShuffleFrameV1& frame,
    const DistributedVectorGroupedAggregateShuffleAuthority& authority) {
  const common::Status valid = validate_frame(frame, authority);
  if (!valid.is_ok())
    return common::make_unexpected(valid);
  auto payload = query::encode_distributed_vector_grouped_aggregate_exchange_message(
      frame.payload, authority.key_definitions(), authority.aggregate_definitions());
  if (!payload.has_value())
    return common::make_unexpected(payload.error());
  try {
    const std::size_t total = kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize +
                              payload->bytes().size() +
                              kDistributedVectorGroupedAggregateShuffleFrameV1TrailerSize;
    std::vector<std::byte> bytes(total);
    common::ByteWriter writer{bytes};
    common::Status write = writer.write_exact(kMagic);
    if (write.is_ok())
      write = writer.write_u16_le(kMajor);
    if (write.is_ok())
      write = writer.write_u16_le(kMinor);
    if (write.is_ok())
      write = writer.write_u32_le(kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize);
    if (write.is_ok())
      write = writer.write_u64_le(total);
    if (write.is_ok())
      write = writer.write_u64_le(frame.edge.source_node_id);
    if (write.is_ok())
      write = writer.write_u64_le(frame.edge.target_node_id);
    if (write.is_ok())
      write = writer.write_exact(frame.query_id.bytes());
    if (write.is_ok())
      write = writer.write_exact(frame.edge.tablet_id.bytes());
    if (write.is_ok())
      write = writer.write_u32_le(frame.edge.partition_id);
    if (write.is_ok())
      write = writer.write_u32_le(frame.partition_count);
    if (write.is_ok())
      write = writer.write_u16_le(frame.edge.hash_version);
    if (write.is_ok())
      write = writer.zero_fill(2U);
    if (write.is_ok())
      write = writer.write_u32_le(static_cast<std::uint32_t>(payload->bytes().size()));
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(payload->bytes()));
    if (write.is_ok())
      write = writer.zero_fill(32U);
    if (!write.is_ok() || writer.offset() != kHeaderCrcOffset)
      return common::make_unexpected(invalid("grouped shuffle frame header size is inconsistent"));
    write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (write.is_ok())
      write = writer.write_exact(payload->bytes());
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(total - 4U)));
    if (!write.is_ok() || !writer.full())
      return common::make_unexpected(invalid("grouped shuffle frame size is inconsistent"));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle frame allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle frame exceeds container limits"));
  }
}

common::Result<DistributedVectorGroupedAggregateShuffleFrameV1>
decode_distributed_vector_grouped_aggregate_shuffle_frame_v1_exact(
    const common::ByteView bytes,
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::QueryResourceContext& resources,
    const query::DistributedVectorGroupedAggregateExchangeDecodeLimits limits) {
  constexpr std::size_t kMinimum =
      kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize +
      query::distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength +
      kDistributedVectorGroupedAggregateShuffleFrameV1TrailerSize;
  if (!valid_payload_limits(limits))
    return common::make_unexpected(invalid("grouped shuffle payload limits are invalid"));
  if (bytes.size() < kMinimum ||
      bytes.size() > kMaximumDistributedVectorGroupedAggregateShuffleFrameV1Size)
    return common::make_unexpected(corruption("grouped shuffle frame length is invalid"));
  const auto header = parse_header(
      bytes.first(kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize), authority, limits);
  if (!header.has_value())
    return common::make_unexpected(header.error());
  if (header->total_length != bytes.size())
    return common::make_unexpected(corruption("grouped shuffle frame is not exact"));
  const common::ByteView payload = bytes.subspan(
      kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize, header->payload_length);
  if (common::crc32c(payload) != header->payload_crc)
    return common::make_unexpected(corruption("grouped shuffle payload checksum differs"));
  common::ByteReader trailer{bytes.last(4U)};
  const auto stored_frame_crc = trailer.read_u32_le();
  if (!stored_frame_crc.has_value() ||
      *stored_frame_crc != common::crc32c(bytes.first(bytes.size() - 4U))) {
    return common::make_unexpected(corruption("grouped shuffle frame checksum differs"));
  }
  auto decoded = query::decode_distributed_vector_grouped_aggregate_exchange_message_exact(
      payload, authority.key_definitions(), authority.aggregate_definitions(), resources, limits);
  if (!decoded.has_value())
    return common::make_unexpected(decoded.error());
  DistributedVectorGroupedAggregateShuffleFrameV1 result{.query_id = header->query_id,
                                                         .edge = header->edge,
                                                         .partition_count = header->partition_count,
                                                         .payload = std::move(*decoded)};
  if (!validate_frame(result, authority).is_ok())
    return common::make_unexpected(corruption("grouped shuffle payload differs from authority"));
  return result;
}

DistributedVectorGroupedAggregateShuffleFrameV1Reader::
    DistributedVectorGroupedAggregateShuffleFrameV1Reader(
        const DistributedVectorGroupedAggregateShuffleAuthority& authority,
        query::QueryResourceContext resources, const std::size_t maximum_frame_length,
        const query::DistributedVectorGroupedAggregateExchangeDecodeLimits payload_limits) noexcept
    : authority_(authority), resources_(std::move(resources)),
      maximum_frame_length_(maximum_frame_length), payload_limits_(payload_limits) {}

common::Result<DistributedVectorGroupedAggregateShuffleFrameV1ReadStep>
DistributedVectorGroupedAggregateShuffleFrameV1Reader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  constexpr std::size_t kMinimum =
      kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize +
      query::distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength +
      kDistributedVectorGroupedAggregateShuffleFrameV1TrailerSize;
  if (!valid_payload_limits(payload_limits_) || maximum_frame_length_ < kMinimum ||
      maximum_frame_length_ > kMaximumDistributedVectorGroupedAggregateShuffleFrameV1Size) {
    return common::make_unexpected(invalid("grouped shuffle frame reader limits are invalid"));
  }
  std::size_t consumed{};
  if (frame_.empty()) {
    const std::size_t copied = std::min(bytes.size(), header_.size() - header_bytes_);
    std::ranges::copy(bytes.first(copied),
                      header_.begin() + static_cast<std::ptrdiff_t>(header_bytes_));
    header_bytes_ += copied;
    consumed += copied;
    if (header_bytes_ != header_.size())
      return DistributedVectorGroupedAggregateShuffleFrameV1ReadStep{.consumed_bytes = consumed};
    const auto parsed = parse_header(header_, authority_.get(), payload_limits_);
    if (!parsed.has_value()) {
      failure_ = parsed.error();
      return common::make_unexpected(*failure_);
    }
    if (parsed->total_length > maximum_frame_length_) {
      failure_ = exhausted("grouped shuffle frame exceeds reader limit");
      return common::make_unexpected(*failure_);
    }
    try {
      frame_.resize(parsed->total_length);
    } catch (const std::bad_alloc&) {
      failure_ = exhausted("grouped shuffle frame reader allocation failed");
      return common::make_unexpected(*failure_);
    } catch (const std::length_error&) {
      failure_ = exhausted("grouped shuffle frame reader exceeds container limits");
      return common::make_unexpected(*failure_);
    }
    std::ranges::copy(header_, frame_.begin());
    frame_bytes_ = header_.size();
  }
  const common::ByteView remaining = bytes.subspan(consumed);
  const std::size_t copied = std::min(remaining.size(), frame_.size() - frame_bytes_);
  std::ranges::copy(remaining.first(copied),
                    frame_.begin() + static_cast<std::ptrdiff_t>(frame_bytes_));
  frame_bytes_ += copied;
  consumed += copied;
  if (frame_bytes_ != frame_.size())
    return DistributedVectorGroupedAggregateShuffleFrameV1ReadStep{.consumed_bytes = consumed};
  auto decoded = decode_distributed_vector_grouped_aggregate_shuffle_frame_v1_exact(
      frame_, authority_.get(), resources_, payload_limits_);
  if (!decoded.has_value()) {
    failure_ = decoded.error();
    return common::make_unexpected(*failure_);
  }
  DistributedVectorGroupedAggregateShuffleFrameV1 result = std::move(*decoded);
  frame_.clear();
  frame_bytes_ = 0U;
  header_bytes_ = 0U;
  return DistributedVectorGroupedAggregateShuffleFrameV1ReadStep{.consumed_bytes = consumed,
                                                                 .frame = std::move(result)};
}

std::size_t DistributedVectorGroupedAggregateShuffleFrameV1Reader::buffered_bytes() const noexcept {
  return frame_.empty() ? header_bytes_ : frame_bytes_;
}

bool DistributedVectorGroupedAggregateShuffleFrameV1Reader::failed() const noexcept {
  return failure_.has_value();
}

DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor::
    DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor(
        std::vector<std::byte> encoded_frame) noexcept
    : encoded_frame_(std::move(encoded_frame)) {}

DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor::
    DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor(
        DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor&& other) noexcept
    : encoded_frame_(std::move(other.encoded_frame_)), written_bytes_(other.written_bytes_) {
  other.written_bytes_ = other.encoded_frame_.size();
}

DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor&
DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor::operator=(
    DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor&& other) noexcept {
  if (this != &other) {
    encoded_frame_ = std::move(other.encoded_frame_);
    written_bytes_ = other.written_bytes_;
    other.written_bytes_ = other.encoded_frame_.size();
  }
  return *this;
}

common::Result<DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor>
DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor::create(
    const DistributedVectorGroupedAggregateShuffleFrameV1& frame,
    const DistributedVectorGroupedAggregateShuffleAuthority& authority) {
  auto encoded = encode_distributed_vector_grouped_aggregate_shuffle_frame_v1(frame, authority);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor{std::move(*encoded)};
}

common::ByteView
DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor::pending_write() const noexcept {
  return common::ByteView{encoded_frame_}.subspan(written_bytes_);
}

common::Status DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor::consume_written(
    const std::size_t bytes) noexcept {
  if (bytes > encoded_frame_.size() - written_bytes_)
    return invalid("written bytes exceed grouped shuffle frame");
  written_bytes_ += bytes;
  return common::Status::ok();
}

std::size_t
DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor::written_bytes() const noexcept {
  return written_bytes_;
}

bool DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor::complete() const noexcept {
  return written_bytes_ == encoded_frame_.size();
}

} // namespace chronos::cluster
