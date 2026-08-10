#include "chronos/raft/tablet_movement_snapshot_chunk.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                           std::byte{'M'}, std::byte{'C'}, std::byte{'H'},
                                           std::byte{'K'}, std::byte{0U}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;
constexpr std::size_t kHeaderCrcOffset = 120U;

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}
[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}
[[nodiscard]] common::Status unsupported(const char* message) {
  return {common::StatusCode::kNotSupported, message};
}

[[nodiscard]] bool valid_limits(const TabletMovementSnapshotChunkCodecLimits& limits) {
  return limits.maximum_snapshot_bytes > 0U && limits.maximum_chunk_bytes > 0U &&
         limits.maximum_chunk_bytes <= limits.maximum_snapshot_bytes &&
         limits.maximum_encoded_bytes >= kTabletMovementSnapshotChunkHeaderSize + 1U +
                                             kTabletMovementSnapshotChunkTrailerSize &&
         limits.maximum_encoded_bytes <= kMaximumTabletMovementSnapshotChunkSize &&
         limits.maximum_chunk_bytes <= limits.maximum_encoded_bytes -
                                           kTabletMovementSnapshotChunkHeaderSize -
                                           kTabletMovementSnapshotChunkTrailerSize;
}

[[nodiscard]] bool valid_session(const TabletMovementSnapshotSession& session,
                                 const TabletMovementSnapshotChunkCodecLimits& limits) {
  return !session.tablet_id.uuid().is_nil() && session.placement_epoch != 0U &&
         session.source_node != 0U && session.target_node != 0U &&
         session.source_node != session.target_node && session.snapshot.manifest_generation != 0U &&
         session.snapshot.applied_index != 0U && session.snapshot.applied_term != 0U &&
         session.snapshot.total_bytes != 0U &&
         session.snapshot.total_bytes <= limits.maximum_snapshot_bytes;
}

void store_u32(std::span<std::byte> bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t ordinal = 0U; ordinal < sizeof(value); ++ordinal)
    bytes[offset + ordinal] = static_cast<std::byte>(value >> (ordinal * 8U));
}
[[nodiscard]] std::uint16_t load_u16(const common::ByteView bytes, const std::size_t offset) {
  return static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset])) |
         static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset + 1U]) << 8U);
}
[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes, const std::size_t offset) {
  std::uint32_t value{};
  for (std::size_t ordinal = 0U; ordinal < sizeof(value); ++ordinal)
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[offset + ordinal]))
             << (ordinal * 8U);
  return value;
}
[[nodiscard]] std::uint64_t load_u64(const common::ByteView bytes, const std::size_t offset) {
  std::uint64_t value{};
  for (std::size_t ordinal = 0U; ordinal < sizeof(value); ++ordinal)
    value |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[offset + ordinal]))
             << (ordinal * 8U);
  return value;
}
[[nodiscard]] std::uint32_t header_crc(const common::ByteView header) {
  std::array<std::byte, kTabletMovementSnapshotChunkHeaderSize> copy{};
  std::ranges::copy(header, copy.begin());
  std::fill_n(copy.begin() + static_cast<std::ptrdiff_t>(kHeaderCrcOffset), sizeof(std::uint32_t),
              std::byte{0U});
  return common::crc32c(copy);
}

} // namespace

common::Result<std::vector<std::byte>>
encode_tablet_movement_snapshot_chunk_v1(const TabletMovementSnapshotChunk& chunk,
                                         const TabletMovementSnapshotChunkCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("movement chunk codec limits are invalid"));
  if (!valid_session(chunk.session, limits) || chunk.bytes.empty() ||
      chunk.bytes.size() > limits.maximum_chunk_bytes ||
      chunk.offset > chunk.session.snapshot.total_bytes ||
      chunk.bytes.size() > chunk.session.snapshot.total_bytes - chunk.offset) {
    return common::make_unexpected(invalid("movement chunk identity or bounds are invalid"));
  }
  const std::size_t total_size = kTabletMovementSnapshotChunkHeaderSize + chunk.bytes.size() +
                                 kTabletMovementSnapshotChunkTrailerSize;
  std::vector<std::byte> output(total_size, std::byte{0U});
  common::ByteWriter writer{
      std::span<std::byte>{output}.first(kTabletMovementSnapshotChunkHeaderSize)};
  const auto& session = chunk.session;
  for (const common::Status& status :
       {writer.write_exact(kMagic),
        writer.write_u16_le(kMajor),
        writer.write_u16_le(kMinor),
        writer.write_u32_le(kTabletMovementSnapshotChunkHeaderSize),
        writer.write_u64_le(total_size),
        writer.write_exact(session.tablet_id.bytes()),
        writer.write_u64_le(session.placement_epoch),
        writer.write_u64_le(session.source_node),
        writer.write_u64_le(session.target_node),
        writer.write_u64_le(session.snapshot.manifest_generation),
        writer.write_u64_le(session.snapshot.applied_index),
        writer.write_u64_le(session.snapshot.applied_term),
        writer.write_u64_le(session.snapshot.total_bytes),
        writer.write_u32_le(session.snapshot.content_crc32c),
        writer.zero_fill(4U),
        writer.write_u64_le(chunk.offset),
        writer.write_u32_le(static_cast<std::uint32_t>(chunk.bytes.size())),
        writer.write_u32_le(common::crc32c(chunk.bytes)),
        writer.write_u32_le(0U),
        writer.zero_fill(4U)}) {
    if (!status.is_ok())
      return common::make_unexpected(status);
  }
  std::ranges::copy(chunk.bytes, output.begin() + static_cast<std::ptrdiff_t>(
                                                      kTabletMovementSnapshotChunkHeaderSize));
  store_u32(output, kHeaderCrcOffset,
            header_crc(common::ByteView{output}.first(kTabletMovementSnapshotChunkHeaderSize)));
  store_u32(output, total_size - kTabletMovementSnapshotChunkTrailerSize,
            common::crc32c(common::ByteView{output}.first(
                total_size - kTabletMovementSnapshotChunkTrailerSize)));
  return output;
}

common::Result<TabletMovementSnapshotChunk>
decode_tablet_movement_snapshot_chunk_v1(const common::ByteView bytes,
                                         const TabletMovementSnapshotChunkCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("movement chunk codec limits are invalid"));
  if (bytes.size() <
          kTabletMovementSnapshotChunkHeaderSize + 1U + kTabletMovementSnapshotChunkTrailerSize ||
      bytes.size() > limits.maximum_encoded_bytes) {
    return common::make_unexpected(corruption("movement chunk size is invalid"));
  }
  const common::ByteView header = bytes.first(kTabletMovementSnapshotChunkHeaderSize);
  if (header_crc(header) != load_u32(header, kHeaderCrcOffset))
    return common::make_unexpected(corruption("movement chunk header checksum mismatch"));
  if (!std::ranges::equal(header.first(kMagic.size()), kMagic) || load_u16(header, 8U) != kMajor ||
      load_u16(header, 10U) != kMinor) {
    return common::make_unexpected(unsupported("movement chunk version is unsupported"));
  }
  common::Uuid::Bytes tablet_bytes{};
  std::ranges::copy(header.subspan(24U, tablet_bytes.size()), tablet_bytes.begin());
  auto tablet_id = schema::TabletId::from_bytes(tablet_bytes);
  const std::uint64_t epoch = load_u64(header, 40U);
  const std::uint64_t source = load_u64(header, 48U);
  const std::uint64_t target = load_u64(header, 56U);
  const std::uint64_t manifest = load_u64(header, 64U);
  const std::uint64_t applied_index = load_u64(header, 72U);
  const std::uint64_t applied_term = load_u64(header, 80U);
  const std::uint64_t snapshot_size = load_u64(header, 88U);
  const std::uint32_t content_crc = load_u32(header, 96U);
  const std::uint64_t offset = load_u64(header, 104U);
  const std::uint32_t payload_size = load_u32(header, 112U);
  if (load_u32(header, 12U) != kTabletMovementSnapshotChunkHeaderSize ||
      load_u64(header, 16U) != bytes.size() || !tablet_id.has_value() ||
      load_u32(header, 100U) != 0U || load_u32(header, 124U) != 0U || payload_size == 0U ||
      payload_size > limits.maximum_chunk_bytes ||
      payload_size != bytes.size() - kTabletMovementSnapshotChunkHeaderSize -
                          kTabletMovementSnapshotChunkTrailerSize ||
      snapshot_size > std::numeric_limits<std::size_t>::max()) {
    return common::make_unexpected(corruption("movement chunk header is invalid"));
  }
  TabletMovementSnapshotSession session{
      *tablet_id, epoch, source, target,
      SnapshotTransferMetadata{manifest, applied_index, applied_term,
                               static_cast<std::size_t>(snapshot_size), content_crc}};
  const common::ByteView payload =
      bytes.subspan(kTabletMovementSnapshotChunkHeaderSize, payload_size);
  if (!valid_session(session, limits) || offset > session.snapshot.total_bytes ||
      payload.size() > session.snapshot.total_bytes - offset ||
      common::crc32c(payload) != load_u32(header, 116U) ||
      common::crc32c(bytes.first(bytes.size() - kTabletMovementSnapshotChunkTrailerSize)) !=
          load_u32(bytes, bytes.size() - kTabletMovementSnapshotChunkTrailerSize)) {
    return common::make_unexpected(corruption("movement chunk semantics or checksum are invalid"));
  }
  return TabletMovementSnapshotChunk{session, offset,
                                     std::vector<std::byte>{payload.begin(), payload.end()}};
}

} // namespace chronos::raft
