#include "chronos/tiering/cold_manifest.hpp"

#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/schema/utf8.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <new>
#include <optional>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::tiering {
namespace {

struct ColdManifestLayout {
  std::uint64_t locations_offset{};
  std::uint64_t keys_offset{};
  std::uint64_t keys_length{};
  std::uint64_t keys_end{};
  std::uint64_t trailer_offset{};
  std::uint64_t total_length{};
};

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] ColdLocationManifestDecodeError incomplete(const char* message,
                                                         const std::uint64_t required_size) {
  return {ColdLocationManifestDecodeErrorKind::kIncomplete,
          {common::StatusCode::kCorruption, message},
          required_size};
}

[[nodiscard]] ColdLocationManifestDecodeError corruption(const char* message) {
  return {ColdLocationManifestDecodeErrorKind::kCorruption,
          {common::StatusCode::kCorruption, message}};
}

[[nodiscard]] ColdLocationManifestDecodeError unsupported(const char* message) {
  return {ColdLocationManifestDecodeErrorKind::kUnsupported,
          {common::StatusCode::kNotSupported, message}};
}

[[nodiscard]] ColdLocationManifestDecodeError resource_limit(const char* message) {
  return {ColdLocationManifestDecodeErrorKind::kResourceLimit,
          {common::StatusCode::kResourceExhausted, message}};
}

[[nodiscard]] bool is_zero(const common::ByteView bytes) noexcept {
  return std::ranges::all_of(bytes, [](const std::byte value) { return value == std::byte{0}; });
}

[[nodiscard]] std::uint16_t load_u16(const common::ByteView bytes,
                                     const std::size_t offset) noexcept {
  return static_cast<std::uint16_t>(std::to_integer<std::uint16_t>(bytes[offset]) |
                                    (std::to_integer<std::uint16_t>(bytes[offset + 1U]) << 8U));
}

[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes,
                                     const std::size_t offset) noexcept {
  std::uint32_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    value |= std::to_integer<std::uint32_t>(bytes[offset + index]) << (index * 8U);
  return value;
}

[[nodiscard]] std::uint64_t load_u64(const common::ByteView bytes,
                                     const std::size_t offset) noexcept {
  std::uint64_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    value |= std::to_integer<std::uint64_t>(bytes[offset + index]) << (index * 8U);
  return value;
}

[[nodiscard]] bool valid_object_key(const std::string_view key) noexcept {
  if (key.empty() || key.size() > cold_manifest_format::kMaximumObjectKeyLength ||
      !schema::is_valid_utf8(key)) {
    return false;
  }
  return std::ranges::none_of(key, [](const char character) {
    const auto value = static_cast<unsigned char>(character);
    return value <= 0x1FU || value == 0x7FU;
  });
}

[[nodiscard]] common::Status
validate_locations(const std::span<const ColdPartLocationDescriptor> locations) {
  if (locations.size() > cold_manifest_format::kMaximumLocationCount)
    return invalid("cold-location manifest has too many descriptors");
  if (!std::ranges::is_sorted(locations, {}, &ColdPartLocationDescriptor::part_id) ||
      std::ranges::adjacent_find(locations, {}, &ColdPartLocationDescriptor::part_id) !=
          locations.end()) {
    return invalid("cold-location manifest part identities are not strictly sorted");
  }
  std::set<std::string_view> object_keys;
  std::uint64_t key_bytes{};
  for (const ColdPartLocationDescriptor& location : locations) {
    if (location.file_length == 0U || !valid_object_key(location.object_key) ||
        !object_keys.insert(location.object_key).second) {
      return invalid("cold-location manifest descriptor is invalid or aliases an object key");
    }
    const auto next =
        common::checked_add(key_bytes, static_cast<std::uint64_t>(location.object_key.size()));
    if (!next.has_value() || *next > cold_manifest_format::kMaximumKeyBytes)
      return invalid("cold-location manifest object-key table exceeds format bounds");
    key_bytes = *next;
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<ColdManifestLayout> plan_layout(const std::uint64_t location_count,
                                                             const std::uint64_t keys_length) {
  if (location_count > cold_manifest_format::kMaximumLocationCount ||
      keys_length > cold_manifest_format::kMaximumKeyBytes) {
    return common::make_unexpected(invalid("cold-location manifest layout exceeds format bounds"));
  }
  const std::uint64_t locations_offset = cold_manifest_format::kHeaderLength;
  const auto descriptor_bytes = common::checked_multiply(
      location_count, static_cast<std::uint64_t>(cold_manifest_format::kDescriptorLength));
  const auto keys_offset = descriptor_bytes.has_value()
                               ? common::checked_add(locations_offset, *descriptor_bytes)
                               : std::nullopt;
  const auto keys_end =
      keys_offset.has_value() ? common::checked_add(*keys_offset, keys_length) : std::nullopt;
  if (!keys_end.has_value())
    return common::make_unexpected(exhausted("cold-location manifest layout overflowed"));
  auto trailer_offset = common::checked_align_up(*keys_end, cold_manifest_format::kAlignment);
  if (!trailer_offset.has_value())
    return common::make_unexpected(exhausted("cold-location manifest alignment overflowed"));
  const auto total_length = common::checked_add(
      *trailer_offset, static_cast<std::uint64_t>(cold_manifest_format::kTrailerLength));
  if (!total_length.has_value() || *total_length > cold_manifest_format::kMaximumFileLength)
    return common::make_unexpected(exhausted("cold-location manifest exceeds file limit"));
  return ColdManifestLayout{locations_offset, *keys_offset,    keys_length,
                            *keys_end,        *trailer_offset, *total_length};
}

[[nodiscard]] common::Status write_u32_at(const common::MutableByteView bytes,
                                          const std::size_t offset, const std::uint32_t value) {
  common::ByteWriter writer{bytes.subspan(offset, sizeof(value))};
  return writer.write_u32_le(value);
}

[[nodiscard]] common::Result<manifest::DatabaseId> parse_database_id(const common::ByteView bytes,
                                                                     const std::size_t offset) {
  common::Uuid::Bytes id{};
  std::ranges::copy(bytes.subspan(offset, id.size()), id.begin());
  return manifest::DatabaseId::from_bytes(id);
}

[[nodiscard]] common::Result<cseg::PartId> parse_part_id(const common::ByteView bytes) {
  common::Uuid::Bytes id{};
  std::ranges::copy(bytes.first(id.size()), id.begin());
  return cseg::PartId::from_bytes(id);
}

[[nodiscard]] common::Uuid parse_uuid(const common::ByteView bytes,
                                      const std::size_t offset) noexcept {
  common::Uuid::Bytes id{};
  std::ranges::copy(bytes.subspan(offset, id.size()), id.begin());
  return common::Uuid{id};
}

} // namespace

ColdLocationManifestDecodeError::ColdLocationManifestDecodeError(
    const ColdLocationManifestDecodeErrorKind kind, common::Status status,
    const std::uint64_t required_size) noexcept
    : kind_(kind), status_(std::move(status)), required_size_(required_size) {}

ColdLocationManifestDecodeErrorKind ColdLocationManifestDecodeError::kind() const noexcept {
  return kind_;
}

const common::Status& ColdLocationManifestDecodeError::status() const noexcept {
  return status_;
}

std::uint64_t ColdLocationManifestDecodeError::required_size() const noexcept {
  return required_size_;
}

EncodedColdLocationManifest::EncodedColdLocationManifest(std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedColdLocationManifest::bytes() const noexcept {
  return bytes_;
}

std::size_t EncodedColdLocationManifest::size() const noexcept {
  return bytes_.size();
}

DecodedColdLocationManifest::DecodedColdLocationManifest(
    const std::uint64_t generation, const std::uint64_t previous_generation,
    const std::uint64_t base_manifest_generation, manifest::DatabaseId database_id,
    common::Uuid object_store_id, std::vector<ColdPartLocationDescriptor> locations,
    const std::size_t encoded_size) noexcept
    : generation_(generation), previous_generation_(previous_generation),
      base_manifest_generation_(base_manifest_generation), database_id_(database_id),
      object_store_id_(object_store_id), locations_(std::move(locations)),
      encoded_size_(encoded_size) {}

std::uint64_t DecodedColdLocationManifest::generation() const noexcept {
  return generation_;
}

std::uint64_t DecodedColdLocationManifest::previous_generation() const noexcept {
  return previous_generation_;
}

std::uint64_t DecodedColdLocationManifest::base_manifest_generation() const noexcept {
  return base_manifest_generation_;
}

const manifest::DatabaseId& DecodedColdLocationManifest::database_id() const noexcept {
  return database_id_;
}

const common::Uuid& DecodedColdLocationManifest::object_store_id() const noexcept {
  return object_store_id_;
}

std::span<const ColdPartLocationDescriptor>
DecodedColdLocationManifest::locations() const noexcept {
  return locations_;
}

std::size_t DecodedColdLocationManifest::encoded_size() const noexcept {
  return encoded_size_;
}

common::Result<EncodedColdLocationManifest>
encode_cold_location_manifest_v1(const ColdLocationManifestEncodeInput& input) {
  try {
    if (input.generation == 0U || input.base_manifest_generation == 0U ||
        input.object_store_id.is_nil()) {
      return common::make_unexpected(invalid("cold-location manifest authority is invalid"));
    }
    const common::Status locations_status = validate_locations(input.locations);
    if (!locations_status.is_ok())
      return common::make_unexpected(locations_status);
    std::uint64_t key_bytes{};
    for (const ColdPartLocationDescriptor& location : input.locations)
      key_bytes += static_cast<std::uint64_t>(location.object_key.size());
    auto layout = plan_layout(input.locations.size(), key_bytes);
    if (!layout.has_value())
      return common::make_unexpected(layout.error());
    if (layout->total_length > std::numeric_limits<std::size_t>::max())
      return common::make_unexpected(exhausted("cold-location manifest is not addressable"));

    std::vector<std::byte> storage(static_cast<std::size_t>(layout->total_length), std::byte{0});
    const common::MutableByteView bytes{storage};
    common::ByteWriter header{bytes.first(cold_manifest_format::kHeaderLength)};
    common::Status written = header.write_exact(cold_manifest_format::kMagic);
    if (written.is_ok())
      written = header.write_u16_le(cold_manifest_format::kFormatMajor);
    if (written.is_ok())
      written = header.write_u16_le(cold_manifest_format::kFormatMinor);
    if (written.is_ok())
      written = header.write_u32_le(cold_manifest_format::kHeaderLength);
    if (written.is_ok())
      written = header.write_u32_le(0U);
    if (written.is_ok())
      written = header.write_u32_le(0U);
    if (written.is_ok())
      written = header.write_u64_le(layout->total_length);
    if (written.is_ok())
      written = header.write_u64_le(input.generation);
    if (written.is_ok())
      written = header.write_u64_le(input.generation == 1U ? 0U : input.generation - 1U);
    if (written.is_ok())
      written = header.write_u64_le(input.base_manifest_generation);
    if (written.is_ok())
      written = header.write_exact(input.database_id.bytes());
    if (written.is_ok())
      written = header.write_exact(input.object_store_id.bytes());
    if (written.is_ok())
      written = header.write_u64_le(input.locations.size());
    if (written.is_ok())
      written = header.write_u64_le(layout->locations_offset);
    if (written.is_ok())
      written = header.write_u64_le(layout->keys_offset);
    if (written.is_ok())
      written = header.write_u64_le(layout->keys_length);
    if (written.is_ok())
      written = header.write_u64_le(layout->trailer_offset);
    if (written.is_ok()) {
      written = header.zero_fill(cold_manifest_format::kHeaderCrc32cOffset - header.offset());
    }
    if (!written.is_ok() || header.offset() != cold_manifest_format::kHeaderCrc32cOffset)
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "cold manifest header layout failed"});
    written = write_u32_at(
        bytes, cold_manifest_format::kHeaderCrc32cOffset,
        common::crc32c(common::ByteView{storage}.first(cold_manifest_format::kHeaderCrc32cOffset)));
    if (!written.is_ok())
      return common::make_unexpected(written);

    std::uint64_t key_offset{};
    for (std::size_t index = 0U; index < input.locations.size(); ++index) {
      const ColdPartLocationDescriptor& location = input.locations[index];
      const std::size_t descriptor_offset = static_cast<std::size_t>(layout->locations_offset) +
                                            index * cold_manifest_format::kDescriptorLength;
      common::ByteWriter descriptor{
          bytes.subspan(descriptor_offset, cold_manifest_format::kDescriptorLength)};
      written = descriptor.write_exact(location.part_id.bytes());
      if (written.is_ok())
        written = descriptor.write_u64_le(location.file_length);
      if (written.is_ok())
        written = descriptor.write_exact(location.content_sha256.bytes());
      if (written.is_ok())
        written = descriptor.write_u64_le(key_offset);
      if (written.is_ok())
        written = descriptor.write_u32_le(static_cast<std::uint32_t>(location.object_key.size()));
      if (written.is_ok())
        written = descriptor.write_u32_le(0U);
      if (written.is_ok())
        written = descriptor.zero_fill(20U);
      if (!written.is_ok() || descriptor.offset() != cold_manifest_format::kDescriptorCrc32cOffset)
        return common::make_unexpected(common::Status{common::StatusCode::kInternal,
                                                      "cold manifest descriptor layout failed"});
      written = descriptor.write_u32_le(common::crc32c(common::ByteView{storage}.subspan(
          descriptor_offset, cold_manifest_format::kDescriptorCrc32cOffset)));
      if (!written.is_ok() || !descriptor.full()) {
        return common::make_unexpected(common::Status{
            common::StatusCode::kInternal, "cold manifest descriptor checksum layout failed"});
      }
      const std::size_t absolute_key_offset =
          static_cast<std::size_t>(layout->keys_offset + key_offset);
      for (std::size_t byte_index = 0U; byte_index < location.object_key.size(); ++byte_index) {
        storage[absolute_key_offset + byte_index] =
            std::byte{static_cast<unsigned char>(location.object_key[byte_index])};
      }
      key_offset += static_cast<std::uint64_t>(location.object_key.size());
    }
    const std::size_t file_crc_offset = storage.size() - cold_manifest_format::kFileCrc32cLength;
    written = write_u32_at(bytes, file_crc_offset,
                           common::crc32c(common::ByteView{storage}.first(file_crc_offset)));
    if (!written.is_ok())
      return common::make_unexpected(written);
    return EncodedColdLocationManifest{std::move(storage)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("cold-location manifest allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("cold-location manifest exceeds container limits"));
  }
}

ColdLocationManifestDecodeResult
decode_cold_location_manifest_v1_prefix(const common::ByteView bytes,
                                        const ColdLocationManifestDecodeLimits limits) {
  if (limits.maximum_file_length == 0U ||
      limits.maximum_file_length > cold_manifest_format::kMaximumFileLength ||
      limits.maximum_locations > cold_manifest_format::kMaximumLocationCount ||
      limits.maximum_key_bytes > cold_manifest_format::kMaximumKeyBytes) {
    return std::unexpected(resource_limit("cold manifest decode limits are invalid"));
  }
  if (bytes.size() < cold_manifest_format::kMagic.size())
    return std::unexpected(
        incomplete("cold manifest requires complete magic", cold_manifest_format::kMagic.size()));
  if (!std::ranges::equal(bytes.first(cold_manifest_format::kMagic.size()),
                          cold_manifest_format::kMagic)) {
    return std::unexpected(corruption("cold manifest magic is invalid"));
  }
  if (bytes.size() < cold_manifest_format::kHeaderLength) {
    return std::unexpected(
        incomplete("cold manifest requires complete header", cold_manifest_format::kHeaderLength));
  }
  const common::ByteView header = bytes.first(cold_manifest_format::kHeaderLength);
  if (common::crc32c(header.first(cold_manifest_format::kHeaderCrc32cOffset)) !=
      load_u32(header, cold_manifest_format::kHeaderCrc32cOffset)) {
    return std::unexpected(corruption("cold manifest header checksum is invalid"));
  }
  const std::uint16_t format_major = load_u16(header, cold_manifest_format::kFormatMajorOffset);
  const std::uint16_t format_minor = load_u16(header, cold_manifest_format::kFormatMinorOffset);
  if (format_major == 0U)
    return std::unexpected(corruption("cold manifest format major is zero"));
  if (format_major != cold_manifest_format::kFormatMajor ||
      format_minor != cold_manifest_format::kFormatMinor) {
    return std::unexpected(unsupported("cold manifest version is unsupported"));
  }
  if (load_u32(header, cold_manifest_format::kFileFlagsOffset) != 0U)
    return std::unexpected(unsupported("cold manifest file flags are unsupported"));
  if (load_u32(header, cold_manifest_format::kHeaderLengthOffset) !=
          cold_manifest_format::kHeaderLength ||
      load_u32(header, cold_manifest_format::kHeaderReserved0Offset) != 0U ||
      !is_zero(header.subspan(cold_manifest_format::kHeaderReserved1Offset,
                              cold_manifest_format::kHeaderCrc32cOffset -
                                  cold_manifest_format::kHeaderReserved1Offset)) ||
      load_u32(header, cold_manifest_format::kHeaderReserved2Offset) != 0U) {
    return std::unexpected(corruption("cold manifest fixed header fields are invalid"));
  }

  const std::uint64_t generation = load_u64(header, cold_manifest_format::kGenerationOffset);
  const std::uint64_t previous = load_u64(header, cold_manifest_format::kPreviousGenerationOffset);
  const std::uint64_t base_generation =
      load_u64(header, cold_manifest_format::kBaseManifestGenerationOffset);
  const std::uint64_t location_count = load_u64(header, cold_manifest_format::kLocationCountOffset);
  const std::uint64_t key_bytes = load_u64(header, cold_manifest_format::kKeysLengthOffset);
  if (generation == 0U || previous != (generation == 1U ? 0U : generation - 1U) ||
      base_generation == 0U || location_count > cold_manifest_format::kMaximumLocationCount ||
      key_bytes > cold_manifest_format::kMaximumKeyBytes) {
    return std::unexpected(corruption("cold manifest authority or counts are invalid"));
  }
  auto layout = plan_layout(location_count, key_bytes);
  if (!layout.has_value() ||
      load_u64(header, cold_manifest_format::kTotalLengthOffset) != layout->total_length ||
      load_u64(header, cold_manifest_format::kLocationsOffsetFieldOffset) !=
          layout->locations_offset ||
      load_u64(header, cold_manifest_format::kKeysOffsetFieldOffset) != layout->keys_offset ||
      load_u64(header, cold_manifest_format::kTrailerOffsetFieldOffset) != layout->trailer_offset) {
    return std::unexpected(corruption("cold manifest layout fields disagree"));
  }
  if (layout->total_length > limits.maximum_file_length ||
      location_count > limits.maximum_locations || key_bytes > limits.maximum_key_bytes) {
    return std::unexpected(resource_limit("cold manifest exceeds configured decode limits"));
  }
  if (layout->total_length > std::numeric_limits<std::size_t>::max())
    return std::unexpected(resource_limit("cold manifest is not addressable on this host"));
  if (bytes.size() < static_cast<std::size_t>(layout->total_length)) {
    return std::unexpected(
        incomplete("cold manifest generation is incomplete", layout->total_length));
  }
  const common::ByteView generation_bytes =
      bytes.first(static_cast<std::size_t>(layout->total_length));
  const std::size_t file_crc_offset =
      generation_bytes.size() - cold_manifest_format::kFileCrc32cLength;
  if (!is_zero(
          generation_bytes.subspan(static_cast<std::size_t>(layout->keys_end),
                                   file_crc_offset - static_cast<std::size_t>(layout->keys_end))) ||
      common::crc32c(generation_bytes.first(file_crc_offset)) !=
          load_u32(generation_bytes, file_crc_offset)) {
    return std::unexpected(corruption("cold manifest trailer or file checksum is invalid"));
  }

  auto database_id = parse_database_id(header, cold_manifest_format::kDatabaseIdOffset);
  const common::Uuid object_store_id =
      parse_uuid(header, cold_manifest_format::kObjectStoreIdOffset);
  if (!database_id.has_value() || object_store_id.is_nil())
    return std::unexpected(corruption("cold manifest identity is zero"));

  try {
    std::vector<ColdPartLocationDescriptor> locations;
    locations.reserve(static_cast<std::size_t>(location_count));
    std::uint64_t expected_key_offset{};
    for (std::uint64_t index = 0U; index < location_count; ++index) {
      const std::size_t descriptor_offset = static_cast<std::size_t>(
          layout->locations_offset + index * cold_manifest_format::kDescriptorLength);
      const common::ByteView descriptor =
          generation_bytes.subspan(descriptor_offset, cold_manifest_format::kDescriptorLength);
      if (common::crc32c(descriptor.first(cold_manifest_format::kDescriptorCrc32cOffset)) !=
          load_u32(descriptor, cold_manifest_format::kDescriptorCrc32cOffset)) {
        return std::unexpected(corruption("cold manifest descriptor checksum is invalid"));
      }
      if (load_u32(descriptor, cold_manifest_format::kDescriptorFlagsOffset) != 0U)
        return std::unexpected(unsupported("cold manifest descriptor flags are unsupported"));
      if (!is_zero(descriptor.subspan(cold_manifest_format::kDescriptorReservedOffset, 20U)))
        return std::unexpected(corruption("cold manifest descriptor reserved bytes are nonzero"));
      auto part_id = parse_part_id(descriptor);
      const std::uint64_t file_length =
          load_u64(descriptor, cold_manifest_format::kDescriptorFileLengthOffset);
      const std::uint64_t key_offset =
          load_u64(descriptor, cold_manifest_format::kDescriptorKeyOffsetOffset);
      const std::uint32_t key_length =
          load_u32(descriptor, cold_manifest_format::kDescriptorKeyLengthOffset);
      if (!part_id.has_value() || file_length == 0U || key_offset != expected_key_offset ||
          key_length == 0U || key_length > cold_manifest_format::kMaximumObjectKeyLength ||
          key_offset > layout->keys_length || key_length > layout->keys_length - key_offset) {
        return std::unexpected(corruption("cold manifest descriptor fields are invalid"));
      }
      ingest::Sha256Digest::Bytes digest{};
      std::ranges::copy(
          descriptor.subspan(cold_manifest_format::kDescriptorContentSha256Offset, digest.size()),
          digest.begin());
      const common::ByteView key_view = generation_bytes.subspan(
          static_cast<std::size_t>(layout->keys_offset + key_offset), key_length);
      const std::string key{reinterpret_cast<const char*>(key_view.data()), key_view.size()};
      locations.push_back({*part_id, file_length, ingest::Sha256Digest{digest}, key});
      expected_key_offset += key_length;
    }
    if (expected_key_offset != layout->keys_length || !validate_locations(locations).is_ok())
      return std::unexpected(corruption("cold manifest descriptor model is invalid"));
    return DecodedColdLocationManifest{generation,
                                       previous,
                                       base_generation,
                                       *database_id,
                                       object_store_id,
                                       std::move(locations),
                                       static_cast<std::size_t>(layout->total_length)};
  } catch (const std::bad_alloc&) {
    return std::unexpected(resource_limit("cold manifest decode allocation failed"));
  } catch (const std::length_error&) {
    return std::unexpected(resource_limit("cold manifest decode exceeds container limits"));
  }
}

ColdLocationManifestDecodeResult
decode_cold_location_manifest_v1_exact(const common::ByteView bytes,
                                       const ColdLocationManifestDecodeLimits limits) {
  auto decoded = decode_cold_location_manifest_v1_prefix(bytes, limits);
  if (!decoded.has_value())
    return decoded;
  if (bytes.size() != decoded->encoded_size())
    return std::unexpected(corruption("cold manifest exact decoder rejects trailing bytes"));
  return decoded;
}

common::Status validate_cold_location_manifest_binding(
    const DecodedColdLocationManifest& cold,
    const manifest::DecodedTemporalManifestView& base_manifest) {
  if (cold.database_id() != base_manifest.database_id())
    return invalid("cold manifest database differs from Manifest v2");
  if (cold.base_manifest_generation() != base_manifest.generation()) {
    return {common::StatusCode::kUnavailable,
            "cold manifest is not bound to the pinned Manifest v2 generation"};
  }
  try {
    std::map<cseg::PartId, const manifest::TemporalPartDescriptor*> parts;
    for (const manifest::TemporalPartDescriptor& part : base_manifest.parts())
      parts.emplace(part.part_id, &part);
    for (const ColdPartLocationDescriptor& location : cold.locations()) {
      const auto found = parts.find(location.part_id);
      if (found == parts.end() || found->second->file_length != location.file_length ||
          found->second->content_sha256 != location.content_sha256) {
        return {common::StatusCode::kUnavailable,
                "cold location differs from its pinned Manifest v2 part"};
      }
    }
    return common::Status::ok();
  } catch (const std::bad_alloc&) {
    return exhausted("cold manifest binding allocation failed");
  } catch (const std::length_error&) {
    return exhausted("cold manifest binding exceeds container limits");
  }
}

common::Status
validate_cold_location_manifest_transition(const DecodedColdLocationManifest& predecessor,
                                           const DecodedColdLocationManifest& successor) {
  if (predecessor.generation() == std::numeric_limits<std::uint64_t>::max() ||
      successor.generation() != predecessor.generation() + 1U ||
      successor.previous_generation() != predecessor.generation()) {
    return invalid("cold manifest successor generation is not exact");
  }
  if (successor.database_id() != predecessor.database_id() ||
      successor.object_store_id() != predecessor.object_store_id()) {
    return invalid("cold manifest successor changes durable identity");
  }
  if (successor.base_manifest_generation() < predecessor.base_manifest_generation())
    return invalid("cold manifest successor moves Manifest v2 authority backward");
  const auto successor_locations = successor.locations();
  for (const ColdPartLocationDescriptor& location : predecessor.locations()) {
    const auto found = std::ranges::lower_bound(successor_locations, location.part_id, {},
                                                &ColdPartLocationDescriptor::part_id);
    if (found == successor_locations.end() || *found != location)
      return invalid("cold manifest successor removes or changes an existing location");
  }
  return common::Status::ok();
}

} // namespace chronos::tiering
