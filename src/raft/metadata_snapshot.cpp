#include "chronos/raft/metadata_snapshot.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/schema_definition_codec.hpp"
#include "chronos/raft/tablet_group_binding_codec.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                           std::byte{'M'}, std::byte{'A'}, std::byte{'S'},
                                           std::byte{'N'}, std::byte{0U}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor0 = 0U;
constexpr std::uint16_t kMinor1 = 1U;
constexpr std::size_t kAlignment = 8U;
constexpr std::size_t kTotalSizeOffset = 16U;
constexpr std::size_t kEntryCountOffset = 24U;
constexpr std::size_t kVoterCountOffset = 28U;
constexpr std::size_t kGroupIdOffset = 32U;
constexpr std::size_t kLastIncludedIndexOffset = 48U;
constexpr std::size_t kLastIncludedTermOffset = 56U;
constexpr std::size_t kApplicationGenerationOffset = 64U;
constexpr std::size_t kPartSetChecksumOffset = 72U;
constexpr std::size_t kConfigurationIndexOffset = 104U;
constexpr std::size_t kEntriesOffsetOffset = 112U;
constexpr std::size_t kHeaderCrcOffset = 120U;
constexpr std::size_t kReservedOffset = 124U;

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}
[[nodiscard]] common::Status corruption(std::string message) {
  return {common::StatusCode::kCorruption, std::move(message)};
}
[[nodiscard]] common::Status unsupported(std::string message) {
  return {common::StatusCode::kNotSupported, std::move(message)};
}
[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] bool valid_limits(const MetadataSnapshotCodecLimits& limits) noexcept {
  return limits.maximum_snapshot_bytes >=
             kMetadataSnapshotHeaderSize + sizeof(NodeId) + kMetadataSnapshotTrailerSize &&
         limits.maximum_snapshot_bytes <= kMaximumMetadataSnapshotSize &&
         limits.maximum_entries > 0U && limits.maximum_entry_payload_bytes > 0U &&
         limits.maximum_entry_payload_bytes <= kMaximumSchemaDefinitionSize &&
         limits.maximum_voters > 0U && limits.maximum_voters <= 1024U;
}

[[nodiscard]] bool valid_metadata(const MetadataApplicationSnapshot& snapshot,
                                  const MetadataSnapshotCodecLimits& limits) noexcept {
  const SnapshotMetadata& metadata = snapshot.raft_snapshot;
  return !snapshot.group_id.is_nil() && metadata.last_included_index != 0U &&
         metadata.last_included_term != 0U &&
         metadata.manifest_generation == metadata.last_included_index &&
         metadata.configuration_index <= metadata.last_included_index && !metadata.voters.empty() &&
         metadata.voters.size() <= limits.maximum_voters && metadata.voters.front() != 0U &&
         std::ranges::is_sorted(metadata.voters) &&
         std::adjacent_find(metadata.voters.begin(), metadata.voters.end()) ==
             metadata.voters.end();
}

[[nodiscard]] bool valid_entry_type(const std::uint8_t type, const std::uint16_t minor) noexcept {
  return type == kRaftMetadataCommandEntryType || type == kRaftSchemaDefinitionEntryType ||
         (minor >= kMinor1 && type == kRaftTabletGroupBindingEntryType);
}

void store_u16(const std::span<std::byte> bytes, const std::size_t offset,
               const std::uint16_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}
void store_u32(const std::span<std::byte> bytes, const std::size_t offset,
               const std::uint32_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}
void store_u64(const std::span<std::byte> bytes, const std::size_t offset,
               const std::uint64_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}
[[nodiscard]] std::uint16_t load_u16(const common::ByteView bytes,
                                     const std::size_t offset) noexcept {
  std::uint16_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    const auto byte =
        static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset + index]));
    value = static_cast<std::uint16_t>(value | static_cast<std::uint16_t>(byte << (index * 8U)));
  }
  return value;
}
[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes,
                                     const std::size_t offset) noexcept {
  std::uint32_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[offset + index]))
             << (index * 8U);
  return value;
}
[[nodiscard]] std::uint64_t load_u64(const common::ByteView bytes,
                                     const std::size_t offset) noexcept {
  std::uint64_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    value |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[offset + index]))
             << (index * 8U);
  return value;
}

[[nodiscard]] common::Result<std::size_t> encoded_size(const MetadataApplicationSnapshot& snapshot,
                                                       const MetadataSnapshotCodecLimits& limits,
                                                       const std::uint16_t minor) {
  if (snapshot.entries.size() > limits.maximum_entries ||
      snapshot.entries.size() > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(exhausted("metadata snapshot entry count exceeds limit"));
  }
  const auto voter_bytes =
      common::checked_multiply(snapshot.raft_snapshot.voters.size(), sizeof(NodeId));
  if (!voter_bytes.has_value())
    return common::make_unexpected(exhausted("metadata snapshot voter bytes overflow"));
  auto size = common::checked_add(kMetadataSnapshotHeaderSize, *voter_bytes);
  if (!size.has_value())
    return common::make_unexpected(exhausted("metadata snapshot size overflow"));
  LogIndex previous_index{};
  Term previous_term{};
  for (const MetadataSnapshotEntry& entry : snapshot.entries) {
    if (entry.index == 0U || entry.index <= previous_index ||
        entry.index > snapshot.raft_snapshot.last_included_index || entry.term == 0U ||
        entry.term < previous_term || entry.term > snapshot.raft_snapshot.last_included_term ||
        (entry.index == snapshot.raft_snapshot.last_included_index &&
         entry.term != snapshot.raft_snapshot.last_included_term) ||
        !valid_entry_type(entry.type, minor) || entry.payload.empty()) {
      return common::make_unexpected(invalid("metadata snapshot entries are noncanonical"));
    }
    if (entry.payload.size() > limits.maximum_entry_payload_bytes ||
        entry.payload.size() > std::numeric_limits<std::uint32_t>::max()) {
      return common::make_unexpected(exhausted("metadata snapshot entry payload exceeds limit"));
    }
    auto entry_size = common::checked_add(kMetadataSnapshotEntryHeaderSize, entry.payload.size());
    if (!entry_size.has_value())
      return common::make_unexpected(exhausted("metadata snapshot entry size overflow"));
    auto aligned = common::checked_align_up(*entry_size, kAlignment);
    if (!aligned.has_value())
      return common::make_unexpected(exhausted("metadata snapshot entry alignment overflow"));
    size = common::checked_add(*size, *aligned);
    if (!size.has_value())
      return common::make_unexpected(exhausted("metadata snapshot size overflow"));
    previous_index = entry.index;
    previous_term = entry.term;
  }
  size = common::checked_add(*size, kMetadataSnapshotTrailerSize);
  if (!size.has_value() || *size > limits.maximum_snapshot_bytes)
    return common::make_unexpected(exhausted("metadata snapshot exceeds size limit"));
  return *size;
}

} // namespace

common::Result<std::vector<std::byte>>
encode_metadata_application_snapshot_v1(const MetadataApplicationSnapshot& snapshot,
                                        const MetadataSnapshotCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("metadata snapshot codec limits are invalid"));
  if (!valid_metadata(snapshot, limits))
    return common::make_unexpected(invalid("metadata snapshot identity or metadata is invalid"));
  const std::uint16_t minor =
      std::ranges::any_of(snapshot.entries,
                          [](const MetadataSnapshotEntry& entry) {
                            return entry.type == kRaftTabletGroupBindingEntryType;
                          })
          ? kMinor1
          : kMinor0;
  auto total_size = encoded_size(snapshot, limits, minor);
  if (!total_size.has_value())
    return common::make_unexpected(total_size.error());
  try {
    std::vector<std::byte> bytes(*total_size, std::byte{0U});
    std::ranges::copy(kMagic, bytes.begin());
    store_u16(bytes, 8U, kMajor);
    store_u16(bytes, 10U, minor);
    store_u32(bytes, 12U, kMetadataSnapshotHeaderSize);
    store_u64(bytes, kTotalSizeOffset, *total_size);
    store_u32(bytes, kEntryCountOffset, static_cast<std::uint32_t>(snapshot.entries.size()));
    store_u32(bytes, kVoterCountOffset,
              static_cast<std::uint32_t>(snapshot.raft_snapshot.voters.size()));
    std::ranges::copy(snapshot.group_id.bytes(), bytes.begin() + kGroupIdOffset);
    store_u64(bytes, kLastIncludedIndexOffset, snapshot.raft_snapshot.last_included_index);
    store_u64(bytes, kLastIncludedTermOffset, snapshot.raft_snapshot.last_included_term);
    store_u64(bytes, kApplicationGenerationOffset, snapshot.raft_snapshot.manifest_generation);
    std::ranges::copy(snapshot.raft_snapshot.part_set_checksum,
                      bytes.begin() + kPartSetChecksumOffset);
    store_u64(bytes, kConfigurationIndexOffset, snapshot.raft_snapshot.configuration_index);
    const std::size_t entries_offset =
        kMetadataSnapshotHeaderSize + snapshot.raft_snapshot.voters.size() * sizeof(NodeId);
    store_u64(bytes, kEntriesOffsetOffset, entries_offset);

    std::size_t cursor = kMetadataSnapshotHeaderSize;
    for (const NodeId voter : snapshot.raft_snapshot.voters) {
      store_u64(bytes, cursor, voter);
      cursor += sizeof(voter);
    }
    for (const MetadataSnapshotEntry& entry : snapshot.entries) {
      store_u64(bytes, cursor, entry.index);
      store_u64(bytes, cursor + 8U, entry.term);
      bytes[cursor + 16U] = static_cast<std::byte>(entry.type);
      store_u32(bytes, cursor + 20U, static_cast<std::uint32_t>(entry.payload.size()));
      store_u32(bytes, cursor + 24U, common::crc32c(entry.payload));
      std::ranges::copy(entry.payload,
                        bytes.begin() +
                            static_cast<std::ptrdiff_t>(cursor + kMetadataSnapshotEntryHeaderSize));
      cursor = *common::checked_align_up(
          cursor + kMetadataSnapshotEntryHeaderSize + entry.payload.size(), kAlignment);
    }
    store_u32(bytes, kHeaderCrcOffset,
              common::crc32c(common::ByteView{bytes}.first(kMetadataSnapshotHeaderSize)));
    store_u32(
        bytes, bytes.size() - kMetadataSnapshotTrailerSize,
        common::crc32c(common::ByteView{bytes}.first(bytes.size() - kMetadataSnapshotTrailerSize)));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("metadata snapshot allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("metadata snapshot exceeded container limits"));
  }
}

common::Result<MetadataApplicationSnapshot>
decode_metadata_application_snapshot_v1(const common::ByteView bytes,
                                        const MetadataSnapshotCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("metadata snapshot codec limits are invalid"));
  if (bytes.size() < kMetadataSnapshotHeaderSize + sizeof(NodeId) + kMetadataSnapshotTrailerSize) {
    return common::make_unexpected(corruption("metadata snapshot is shorter than framing"));
  }
  if (bytes.size() > limits.maximum_snapshot_bytes)
    return common::make_unexpected(exhausted("metadata snapshot exceeds decode size limit"));
  std::array<std::byte, kMetadataSnapshotHeaderSize> header{};
  std::ranges::copy(bytes.first(header.size()), header.begin());
  const std::uint32_t header_crc = load_u32(bytes, kHeaderCrcOffset);
  store_u32(header, kHeaderCrcOffset, 0U);
  if (common::crc32c(header) != header_crc)
    return common::make_unexpected(corruption("metadata snapshot header checksum mismatch"));
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic) || load_u16(bytes, 8U) != kMajor)
    return common::make_unexpected(unsupported("metadata snapshot magic or major is unknown"));
  const std::uint16_t minor = load_u16(bytes, 10U);
  if (minor > kMinor1 || load_u32(bytes, 12U) != kMetadataSnapshotHeaderSize)
    return common::make_unexpected(unsupported("metadata snapshot minor or header is unknown"));

  const std::uint64_t total_size = load_u64(bytes, kTotalSizeOffset);
  const std::uint32_t entry_count = load_u32(bytes, kEntryCountOffset);
  const std::uint32_t voter_count = load_u32(bytes, kVoterCountOffset);
  const std::uint64_t entries_offset = load_u64(bytes, kEntriesOffsetOffset);
  const auto voter_bytes =
      common::checked_multiply(static_cast<std::size_t>(voter_count), sizeof(NodeId));
  if (entry_count > limits.maximum_entries || voter_count > limits.maximum_voters)
    return common::make_unexpected(exhausted("metadata snapshot counts exceed decode limits"));
  if (total_size != bytes.size() || voter_count == 0U || !voter_bytes.has_value() ||
      entries_offset != kMetadataSnapshotHeaderSize + *voter_bytes ||
      entries_offset > bytes.size() - kMetadataSnapshotTrailerSize ||
      std::ranges::any_of(bytes.subspan(kReservedOffset, 4U),
                          [](const std::byte value) { return value != std::byte{0U}; }) ||
      common::crc32c(bytes.first(bytes.size() - kMetadataSnapshotTrailerSize)) !=
          load_u32(bytes, bytes.size() - kMetadataSnapshotTrailerSize)) {
    return common::make_unexpected(corruption("metadata snapshot framing is invalid"));
  }

  common::Uuid::Bytes group_bytes{};
  std::ranges::copy(bytes.subspan(kGroupIdOffset, group_bytes.size()), group_bytes.begin());
  MetadataApplicationSnapshot snapshot{
      .group_id = GroupId{group_bytes},
      .raft_snapshot = {.last_included_index = load_u64(bytes, kLastIncludedIndexOffset),
                        .last_included_term = load_u64(bytes, kLastIncludedTermOffset),
                        .manifest_generation = load_u64(bytes, kApplicationGenerationOffset),
                        .part_set_checksum = {},
                        .configuration_index = load_u64(bytes, kConfigurationIndexOffset),
                        .voters = {}},
      .entries = {}};
  std::ranges::copy(
      bytes.subspan(kPartSetChecksumOffset, snapshot.raft_snapshot.part_set_checksum.size()),
      snapshot.raft_snapshot.part_set_checksum.begin());
  try {
    snapshot.raft_snapshot.voters.reserve(voter_count);
    std::size_t cursor = kMetadataSnapshotHeaderSize;
    for (std::uint32_t ordinal = 0U; ordinal < voter_count; ++ordinal) {
      snapshot.raft_snapshot.voters.push_back(load_u64(bytes, cursor));
      cursor += sizeof(NodeId);
    }
    if (!valid_metadata(snapshot, limits))
      return common::make_unexpected(corruption("metadata snapshot metadata is noncanonical"));
    snapshot.entries.reserve(entry_count);
    LogIndex previous_index{};
    Term previous_term{};
    cursor = static_cast<std::size_t>(entries_offset);
    const std::size_t trailer_offset = bytes.size() - kMetadataSnapshotTrailerSize;
    for (std::uint32_t ordinal = 0U; ordinal < entry_count; ++ordinal) {
      if (cursor > trailer_offset || kMetadataSnapshotEntryHeaderSize > trailer_offset - cursor)
        return common::make_unexpected(corruption("metadata snapshot entry header is truncated"));
      const LogIndex index = load_u64(bytes, cursor);
      const Term term = load_u64(bytes, cursor + 8U);
      const std::uint8_t type = std::to_integer<std::uint8_t>(bytes[cursor + 16U]);
      const std::uint32_t payload_size = load_u32(bytes, cursor + 20U);
      const std::uint32_t payload_crc = load_u32(bytes, cursor + 24U);
      const std::size_t payload_offset = cursor + kMetadataSnapshotEntryHeaderSize;
      if (index == 0U || index <= previous_index ||
          index > snapshot.raft_snapshot.last_included_index || term == 0U ||
          term < previous_term || term > snapshot.raft_snapshot.last_included_term ||
          (index == snapshot.raft_snapshot.last_included_index &&
           term != snapshot.raft_snapshot.last_included_term) ||
          !valid_entry_type(type, minor) || payload_size == 0U ||
          std::ranges::any_of(bytes.subspan(cursor + 17U, 3U),
                              [](const std::byte value) { return value != std::byte{0U}; }) ||
          load_u32(bytes, cursor + 28U) != 0U || payload_offset > trailer_offset ||
          payload_size > trailer_offset - payload_offset) {
        return common::make_unexpected(corruption("metadata snapshot entry is invalid"));
      }
      if (payload_size > limits.maximum_entry_payload_bytes)
        return common::make_unexpected(exhausted("metadata snapshot entry exceeds decode limit"));
      const common::ByteView payload = bytes.subspan(payload_offset, payload_size);
      if (common::crc32c(payload) != payload_crc)
        return common::make_unexpected(corruption("metadata snapshot entry checksum mismatch"));
      auto next = common::checked_align_up(payload_offset + payload_size, kAlignment);
      if (!next.has_value() || *next > trailer_offset ||
          std::ranges::any_of(
              bytes.subspan(payload_offset + payload_size, *next - payload_offset - payload_size),
              [](const std::byte value) { return value != std::byte{0U}; })) {
        return common::make_unexpected(corruption("metadata snapshot entry padding is invalid"));
      }
      snapshot.entries.push_back({.index = index,
                                  .term = term,
                                  .type = type,
                                  .payload = {payload.begin(), payload.end()}});
      cursor = *next;
      previous_index = index;
      previous_term = term;
    }
    if (cursor != trailer_offset)
      return common::make_unexpected(corruption("metadata snapshot has trailing entry bytes"));
    return snapshot;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("metadata snapshot decode allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("metadata snapshot decode exceeded limits"));
  }
}

} // namespace chronos::raft
