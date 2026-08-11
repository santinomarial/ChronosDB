#include "chronos/cluster/tablet_physical_part_chunk.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <ranges>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                           std::byte{'P'}, std::byte{'C'}, std::byte{'H'},
                                           std::byte{'K'}, std::byte{0U}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;
constexpr std::size_t kHeaderCrcOffset = 176U;

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

[[nodiscard]] bool valid_limits(const TabletPhysicalPartChunkCodecLimits& limits) noexcept {
  return limits.maximum_object_bytes > 0U &&
         limits.maximum_object_bytes <= cseg::format::kMaximumFileLength &&
         limits.maximum_chunk_bytes > 0U &&
         limits.maximum_chunk_bytes <= limits.maximum_object_bytes &&
         limits.maximum_encoded_bytes >=
             kTabletPhysicalPartChunkHeaderSize + 1U + kTabletPhysicalPartChunkTrailerSize &&
         limits.maximum_encoded_bytes <= kMaximumTabletPhysicalPartChunkSize &&
         limits.maximum_chunk_bytes <= limits.maximum_encoded_bytes -
                                           kTabletPhysicalPartChunkHeaderSize -
                                           kTabletPhysicalPartChunkTrailerSize;
}

[[nodiscard]] bool valid_session(const TabletPhysicalPartTransferSession& session,
                                 const TabletPhysicalPartChunkCodecLimits& limits) noexcept {
  return !session.table_id.uuid().is_nil() && !session.tablet_id.uuid().is_nil() &&
         !session.group_id.is_nil() && session.placement_epoch != 0U && session.source_node != 0U &&
         session.target_node != 0U && session.source_node != session.target_node &&
         session.manifest_generation != 0U && !session.part_id.uuid().is_nil() &&
         session.total_bytes != 0U && session.total_bytes <= limits.maximum_object_bytes;
}

void store_u32(std::span<std::byte> bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

[[nodiscard]] std::uint16_t load_u16(const common::ByteView bytes, const std::size_t offset) {
  return static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset])) |
         static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset + 1U]) << 8U);
}

[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes, const std::size_t offset) {
  std::uint32_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[offset + index]))
             << (index * 8U);
  }
  return value;
}

[[nodiscard]] std::uint64_t load_u64(const common::ByteView bytes, const std::size_t offset) {
  std::uint64_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[offset + index]))
             << (index * 8U);
  }
  return value;
}

[[nodiscard]] std::uint32_t header_crc(const common::ByteView header) {
  std::array<std::byte, kTabletPhysicalPartChunkHeaderSize> copy{};
  std::ranges::copy(header, copy.begin());
  std::fill_n(copy.begin() + static_cast<std::ptrdiff_t>(kHeaderCrcOffset), sizeof(std::uint32_t),
              std::byte{0U});
  return common::crc32c(copy);
}

template <typename Identity>
[[nodiscard]] common::Result<Identity> read_identity(common::ByteReader& reader) {
  auto bytes = reader.read_exact(common::Uuid::kSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::Uuid::Bytes identity{};
  std::ranges::copy(*bytes, identity.begin());
  return Identity::from_bytes(identity);
}

[[nodiscard]] common::Result<common::Uuid> read_uuid(common::ByteReader& reader) {
  auto bytes = reader.read_exact(common::Uuid::kSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::Uuid::Bytes identity{};
  std::ranges::copy(*bytes, identity.begin());
  return common::Uuid{identity};
}

} // namespace

common::Result<std::vector<std::byte>>
encode_tablet_physical_part_chunk_v1(const TabletPhysicalPartChunk& chunk,
                                     const TabletPhysicalPartChunkCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("physical part chunk codec limits are invalid"));
  if (!valid_session(chunk.session, limits) || chunk.bytes.empty() ||
      chunk.bytes.size() > limits.maximum_chunk_bytes || chunk.offset > chunk.session.total_bytes ||
      chunk.bytes.size() > chunk.session.total_bytes - chunk.offset) {
    return common::make_unexpected(invalid("physical part chunk identity or bounds are invalid"));
  }
  try {
    const std::size_t total_size = kTabletPhysicalPartChunkHeaderSize + chunk.bytes.size() +
                                   kTabletPhysicalPartChunkTrailerSize;
    std::vector<std::byte> output(total_size, std::byte{0U});
    common::ByteWriter writer{
        std::span<std::byte>{output}.first(kTabletPhysicalPartChunkHeaderSize)};
    const auto& session = chunk.session;
    for (const common::Status& status :
         {writer.write_exact(kMagic),
          writer.write_u16_le(kMajor),
          writer.write_u16_le(kMinor),
          writer.write_u32_le(kTabletPhysicalPartChunkHeaderSize),
          writer.write_u64_le(total_size),
          writer.write_exact(session.table_id.bytes()),
          writer.write_exact(session.tablet_id.bytes()),
          writer.write_exact(session.group_id.bytes()),
          writer.write_u64_le(session.placement_epoch),
          writer.write_u64_le(session.source_node),
          writer.write_u64_le(session.target_node),
          writer.write_u64_le(session.manifest_generation),
          writer.write_exact(session.part_id.bytes()),
          writer.write_u64_le(session.total_bytes),
          writer.write_exact(session.content_sha256.bytes()),
          writer.write_u64_le(chunk.offset),
          writer.write_u32_le(static_cast<std::uint32_t>(chunk.bytes.size())),
          writer.write_u32_le(common::crc32c(chunk.bytes)),
          writer.write_u32_le(0U),
          writer.zero_fill(12U)}) {
      if (!status.is_ok())
        return common::make_unexpected(status);
    }
    std::ranges::copy(chunk.bytes, output.begin() + static_cast<std::ptrdiff_t>(
                                                        kTabletPhysicalPartChunkHeaderSize));
    store_u32(output, kHeaderCrcOffset,
              header_crc(common::ByteView{output}.first(kTabletPhysicalPartChunkHeaderSize)));
    store_u32(output, total_size - kTabletPhysicalPartChunkTrailerSize,
              common::crc32c(common::ByteView{output}.first(total_size -
                                                            kTabletPhysicalPartChunkTrailerSize)));
    return output;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("physical part chunk allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("physical part chunk exceeded container limits"));
  }
}

common::Result<TabletPhysicalPartChunk>
decode_tablet_physical_part_chunk_v1(const common::ByteView bytes,
                                     const TabletPhysicalPartChunkCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("physical part chunk codec limits are invalid"));
  if (bytes.size() <
          kTabletPhysicalPartChunkHeaderSize + 1U + kTabletPhysicalPartChunkTrailerSize ||
      bytes.size() > limits.maximum_encoded_bytes) {
    return common::make_unexpected(corruption("physical part chunk size is invalid"));
  }
  const common::ByteView header = bytes.first(kTabletPhysicalPartChunkHeaderSize);
  if (header_crc(header) != load_u32(header, kHeaderCrcOffset))
    return common::make_unexpected(corruption("physical part chunk header checksum mismatch"));
  if (!std::ranges::equal(header.first(kMagic.size()), kMagic) || load_u16(header, 8U) != kMajor ||
      load_u16(header, 10U) != kMinor) {
    return common::make_unexpected(unsupported("physical part chunk version is unsupported"));
  }
  common::ByteReader reader{header.subspan(24U)};
  auto table_id = read_identity<schema::TableId>(reader);
  auto tablet_id = read_identity<schema::TabletId>(reader);
  auto group_id = read_uuid(reader);
  auto epoch = reader.read_u64_le();
  auto source = reader.read_u64_le();
  auto target = reader.read_u64_le();
  auto manifest = reader.read_u64_le();
  auto part_id = read_identity<cseg::PartId>(reader);
  auto object_size = reader.read_u64_le();
  auto digest = reader.read_exact(ingest::Sha256Digest::kSize);
  auto offset = reader.read_u64_le();
  auto payload_size = reader.read_u32_le();
  auto payload_crc = reader.read_u32_le();
  auto stored_header_crc = reader.read_u32_le();
  auto reserved = reader.read_exact(12U);
  if (load_u32(header, 12U) != kTabletPhysicalPartChunkHeaderSize ||
      load_u64(header, 16U) != bytes.size() || !table_id.has_value() || !tablet_id.has_value() ||
      !group_id.has_value() || !epoch.has_value() || !source.has_value() || !target.has_value() ||
      !manifest.has_value() || !part_id.has_value() || !object_size.has_value() ||
      !digest.has_value() || !offset.has_value() || !payload_size.has_value() ||
      !payload_crc.has_value() || !stored_header_crc.has_value() || !reserved.has_value() ||
      *stored_header_crc != load_u32(header, kHeaderCrcOffset) ||
      std::ranges::any_of(*reserved,
                          [](const std::byte value) { return value != std::byte{0U}; }) ||
      *payload_size == 0U || *payload_size > limits.maximum_chunk_bytes ||
      *payload_size !=
          bytes.size() - kTabletPhysicalPartChunkHeaderSize - kTabletPhysicalPartChunkTrailerSize) {
    return common::make_unexpected(corruption("physical part chunk header is invalid"));
  }
  ingest::Sha256Digest::Bytes digest_bytes{};
  std::ranges::copy(*digest, digest_bytes.begin());
  TabletPhysicalPartTransferSession session{.table_id = *table_id,
                                            .tablet_id = *tablet_id,
                                            .group_id = *group_id,
                                            .placement_epoch = *epoch,
                                            .source_node = *source,
                                            .target_node = *target,
                                            .manifest_generation = *manifest,
                                            .part_id = *part_id,
                                            .total_bytes = *object_size,
                                            .content_sha256 = ingest::Sha256Digest{digest_bytes}};
  const common::ByteView payload = bytes.subspan(kTabletPhysicalPartChunkHeaderSize, *payload_size);
  if (!valid_session(session, limits) || *offset > session.total_bytes ||
      payload.size() > session.total_bytes - *offset || common::crc32c(payload) != *payload_crc ||
      common::crc32c(bytes.first(bytes.size() - kTabletPhysicalPartChunkTrailerSize)) !=
          load_u32(bytes, bytes.size() - kTabletPhysicalPartChunkTrailerSize)) {
    return common::make_unexpected(
        corruption("physical part chunk semantics or checksum are invalid"));
  }
  try {
    return TabletPhysicalPartChunk{session, *offset,
                                   std::vector<std::byte>{payload.begin(), payload.end()}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("physical part chunk decode allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("physical part chunk decode exceeded container limits"));
  }
}

} // namespace chronos::cluster
