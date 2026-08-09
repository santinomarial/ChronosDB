#include "chronos/ingest/raft_tablet_snapshot.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/ingest/columnar_append_format.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                           std::byte{'R'}, std::byte{'T'}, std::byte{'A'},
                                           std::byte{'S'}, std::byte{0U}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;
constexpr std::size_t kAlignment = 8U;
constexpr std::size_t kTotalSizeOffset = 16U;
constexpr std::size_t kEntryCountOffset = 24U;
constexpr std::size_t kVoterCountOffset = 28U;
constexpr std::size_t kGroupIdOffset = 32U;
constexpr std::size_t kTableIdOffset = 48U;
constexpr std::size_t kTabletIdOffset = 64U;
constexpr std::size_t kLastIncludedIndexOffset = 80U;
constexpr std::size_t kLastIncludedTermOffset = 88U;
constexpr std::size_t kManifestGenerationOffset = 96U;
constexpr std::size_t kPartSetChecksumOffset = 104U;
constexpr std::size_t kConfigurationIndexOffset = 136U;
constexpr std::size_t kEntriesOffsetOffset = 144U;
constexpr std::size_t kHeaderCrcOffset = 152U;
constexpr std::size_t kReservedOffset = 156U;

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return common::Status{common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status unsupported(std::string message) {
  return common::Status{common::StatusCode::kNotSupported, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] bool valid_limits(const RaftTabletSnapshotCodecLimits limits) noexcept {
  return limits.maximum_snapshot_bytes >= kRaftTabletSnapshotHeaderSize + sizeof(raft::NodeId) +
                                              kRaftTabletSnapshotTrailerSize &&
         limits.maximum_snapshot_bytes <= kMaximumRaftTabletSnapshotSize &&
         limits.maximum_entries > 0U && limits.maximum_entry_payload_bytes > 0U &&
         limits.maximum_entry_payload_bytes <=
             columnar_append_v1::kMaximumApplicationPayloadLength &&
         limits.maximum_voters > 0U && limits.maximum_voters <= 1024U;
}

[[nodiscard]] bool valid_metadata(const RaftTabletApplicationSnapshot& snapshot,
                                  const RaftTabletSnapshotCodecLimits limits) noexcept {
  const raft::SnapshotMetadata& metadata = snapshot.raft_snapshot;
  return !snapshot.group_id.is_nil() && !snapshot.table_id.uuid().is_nil() &&
         !snapshot.tablet_id.uuid().is_nil() && metadata.last_included_index != 0U &&
         metadata.last_included_term != 0U && metadata.manifest_generation != 0U &&
         metadata.configuration_index <= metadata.last_included_index && !metadata.voters.empty() &&
         metadata.voters.size() <= limits.maximum_voters && metadata.voters.front() != 0U &&
         std::ranges::is_sorted(metadata.voters) &&
         std::adjacent_find(metadata.voters.begin(), metadata.voters.end()) ==
             metadata.voters.end();
}

void store_u16(std::span<std::byte> bytes, const std::size_t offset, const std::uint16_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
  }
}

void store_u32(std::span<std::byte> bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
  }
}

void store_u64(std::span<std::byte> bytes, const std::size_t offset, const std::uint64_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
  }
}

[[nodiscard]] std::uint16_t load_u16(const common::ByteView bytes, const std::size_t offset) {
  std::uint16_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset + index]))
             << (index * 8U);
  }
  return value;
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

template <typename Identifier>
[[nodiscard]] common::Result<Identifier> load_identifier(const common::ByteView bytes,
                                                         const std::size_t offset) {
  common::Uuid::Bytes value{};
  std::ranges::copy(bytes.subspan(offset, value.size()), value.begin());
  return Identifier::from_bytes(value);
}

[[nodiscard]] common::Result<std::size_t>
encoded_size(const RaftTabletApplicationSnapshot& snapshot,
             const RaftTabletSnapshotCodecLimits limits) {
  if (snapshot.entries.size() > limits.maximum_entries ||
      snapshot.entries.size() > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(exhausted("Raft tablet snapshot entry count exceeds limit"));
  }
  const auto voter_bytes =
      common::checked_multiply(snapshot.raft_snapshot.voters.size(), sizeof(raft::NodeId));
  if (!voter_bytes.has_value()) {
    return common::make_unexpected(exhausted("Raft tablet snapshot voter bytes overflow"));
  }
  auto size = common::checked_add(kRaftTabletSnapshotHeaderSize, *voter_bytes);
  if (!size.has_value()) {
    return common::make_unexpected(exhausted("Raft tablet snapshot size overflow"));
  }
  raft::LogIndex previous{};
  raft::Term previous_term{};
  for (const RaftTabletSnapshotEntry& entry : snapshot.entries) {
    if (entry.index == 0U || entry.index <= previous ||
        entry.index > snapshot.raft_snapshot.last_included_index || entry.term == 0U ||
        entry.term < previous_term || entry.term > snapshot.raft_snapshot.last_included_term ||
        (entry.index == snapshot.raft_snapshot.last_included_index &&
         entry.term != snapshot.raft_snapshot.last_included_term) ||
        entry.payload.empty()) {
      return common::make_unexpected(invalid("Raft tablet snapshot entries are noncanonical"));
    }
    if (entry.payload.size() > limits.maximum_entry_payload_bytes ||
        entry.payload.size() > std::numeric_limits<std::uint32_t>::max()) {
      return common::make_unexpected(exhausted("Raft tablet snapshot entry payload exceeds limit"));
    }
    auto entry_size = common::checked_add(kRaftTabletSnapshotEntryHeaderSize, entry.payload.size());
    if (!entry_size.has_value()) {
      return common::make_unexpected(exhausted("Raft tablet snapshot entry size overflow"));
    }
    auto aligned = common::checked_align_up(*entry_size, kAlignment);
    if (!aligned.has_value()) {
      return common::make_unexpected(exhausted("Raft tablet snapshot entry alignment overflow"));
    }
    size = common::checked_add(*size, *aligned);
    if (!size.has_value()) {
      return common::make_unexpected(exhausted("Raft tablet snapshot size overflow"));
    }
    previous = entry.index;
    previous_term = entry.term;
  }
  size = common::checked_add(*size, kRaftTabletSnapshotTrailerSize);
  if (!size.has_value() || *size > limits.maximum_snapshot_bytes) {
    return common::make_unexpected(exhausted("Raft tablet snapshot exceeds size limit"));
  }
  return *size;
}

} // namespace

common::Result<std::vector<std::byte>>
encode_raft_tablet_application_snapshot_v1(const RaftTabletApplicationSnapshot& snapshot,
                                           const RaftTabletSnapshotCodecLimits limits) {
  if (!valid_limits(limits)) {
    return common::make_unexpected(invalid("Raft tablet snapshot codec limits are invalid"));
  }
  if (!valid_metadata(snapshot, limits)) {
    return common::make_unexpected(invalid("Raft tablet snapshot identity or metadata is invalid"));
  }
  const auto total_size = encoded_size(snapshot, limits);
  if (!total_size.has_value()) {
    return common::make_unexpected(total_size.error());
  }
  try {
    std::vector<std::byte> bytes(*total_size, std::byte{0U});
    std::ranges::copy(kMagic, bytes.begin());
    store_u16(bytes, 8U, kMajor);
    store_u16(bytes, 10U, kMinor);
    store_u32(bytes, 12U, kRaftTabletSnapshotHeaderSize);
    store_u64(bytes, kTotalSizeOffset, *total_size);
    store_u32(bytes, kEntryCountOffset, static_cast<std::uint32_t>(snapshot.entries.size()));
    store_u32(bytes, kVoterCountOffset,
              static_cast<std::uint32_t>(snapshot.raft_snapshot.voters.size()));
    std::ranges::copy(snapshot.group_id.bytes(), bytes.begin() + kGroupIdOffset);
    std::ranges::copy(snapshot.table_id.bytes(), bytes.begin() + kTableIdOffset);
    std::ranges::copy(snapshot.tablet_id.bytes(), bytes.begin() + kTabletIdOffset);
    store_u64(bytes, kLastIncludedIndexOffset, snapshot.raft_snapshot.last_included_index);
    store_u64(bytes, kLastIncludedTermOffset, snapshot.raft_snapshot.last_included_term);
    store_u64(bytes, kManifestGenerationOffset, snapshot.raft_snapshot.manifest_generation);
    std::ranges::copy(snapshot.raft_snapshot.part_set_checksum,
                      bytes.begin() + kPartSetChecksumOffset);
    store_u64(bytes, kConfigurationIndexOffset, snapshot.raft_snapshot.configuration_index);
    const std::size_t entries_offset =
        kRaftTabletSnapshotHeaderSize + snapshot.raft_snapshot.voters.size() * sizeof(raft::NodeId);
    store_u64(bytes, kEntriesOffsetOffset, entries_offset);

    std::size_t cursor = kRaftTabletSnapshotHeaderSize;
    for (const raft::NodeId voter : snapshot.raft_snapshot.voters) {
      store_u64(bytes, cursor, voter);
      cursor += sizeof(voter);
    }
    for (const RaftTabletSnapshotEntry& entry : snapshot.entries) {
      store_u64(bytes, cursor, entry.index);
      store_u64(bytes, cursor + 8U, entry.term);
      store_u32(bytes, cursor + 16U, static_cast<std::uint32_t>(entry.payload.size()));
      store_u32(bytes, cursor + 20U, common::crc32c(entry.payload));
      std::ranges::copy(
          entry.payload,
          bytes.begin() + static_cast<std::ptrdiff_t>(cursor + kRaftTabletSnapshotEntryHeaderSize));
      const auto next = common::checked_align_up(
          cursor + kRaftTabletSnapshotEntryHeaderSize + entry.payload.size(), kAlignment);
      cursor = *next;
    }
    store_u32(bytes, kHeaderCrcOffset,
              common::crc32c(common::ByteView{bytes}.first(kRaftTabletSnapshotHeaderSize)));
    store_u32(bytes, bytes.size() - kRaftTabletSnapshotTrailerSize,
              common::crc32c(
                  common::ByteView{bytes}.first(bytes.size() - kRaftTabletSnapshotTrailerSize)));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft tablet snapshot allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Raft tablet snapshot exceeded container limits"));
  }
}

common::Result<RaftTabletApplicationSnapshot>
decode_raft_tablet_application_snapshot_v1(const common::ByteView bytes,
                                           const RaftTabletSnapshotCodecLimits limits) {
  if (!valid_limits(limits)) {
    return common::make_unexpected(invalid("Raft tablet snapshot codec limits are invalid"));
  }
  if (bytes.size() <
      kRaftTabletSnapshotHeaderSize + sizeof(raft::NodeId) + kRaftTabletSnapshotTrailerSize) {
    return common::make_unexpected(corruption("Raft tablet snapshot is shorter than framing"));
  }
  if (bytes.size() > limits.maximum_snapshot_bytes) {
    return common::make_unexpected(exhausted("Raft tablet snapshot exceeds decode size limit"));
  }
  std::array<std::byte, kRaftTabletSnapshotHeaderSize> header{};
  std::ranges::copy(bytes.first(header.size()), header.begin());
  const std::uint32_t header_crc = load_u32(bytes, kHeaderCrcOffset);
  store_u32(header, kHeaderCrcOffset, 0U);
  if (common::crc32c(header) != header_crc) {
    return common::make_unexpected(corruption("Raft tablet snapshot header checksum mismatch"));
  }
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic) || load_u16(bytes, 8U) != kMajor) {
    return common::make_unexpected(unsupported("Raft tablet snapshot magic or major is unknown"));
  }
  if (load_u16(bytes, 10U) != kMinor || load_u32(bytes, 12U) != kRaftTabletSnapshotHeaderSize) {
    return common::make_unexpected(unsupported("Raft tablet snapshot minor or header is unknown"));
  }
  const std::uint64_t total_size = load_u64(bytes, kTotalSizeOffset);
  const std::uint32_t entry_count = load_u32(bytes, kEntryCountOffset);
  const std::uint32_t voter_count = load_u32(bytes, kVoterCountOffset);
  const std::uint64_t entries_offset = load_u64(bytes, kEntriesOffsetOffset);
  const auto expected_voter_bytes =
      common::checked_multiply(static_cast<std::size_t>(voter_count), sizeof(raft::NodeId));
  if (entry_count > limits.maximum_entries || voter_count > limits.maximum_voters) {
    return common::make_unexpected(exhausted("Raft tablet snapshot counts exceed decode limits"));
  }
  if (total_size != bytes.size() || voter_count == 0U || !expected_voter_bytes.has_value() ||
      entries_offset != kRaftTabletSnapshotHeaderSize + *expected_voter_bytes ||
      entries_offset > bytes.size() - kRaftTabletSnapshotTrailerSize ||
      std::ranges::any_of(bytes.subspan(kReservedOffset, 4U),
                          [](const std::byte value) { return value != std::byte{0U}; }) ||
      common::crc32c(bytes.first(bytes.size() - kRaftTabletSnapshotTrailerSize)) !=
          load_u32(bytes, bytes.size() - kRaftTabletSnapshotTrailerSize)) {
    return common::make_unexpected(corruption("Raft tablet snapshot framing is invalid"));
  }

  common::Uuid::Bytes group_bytes{};
  std::ranges::copy(bytes.subspan(kGroupIdOffset, group_bytes.size()), group_bytes.begin());
  const raft::GroupId group_id{group_bytes};
  auto table_id = load_identifier<schema::TableId>(bytes, kTableIdOffset);
  auto tablet_id = load_identifier<schema::TabletId>(bytes, kTabletIdOffset);
  if (group_id.is_nil() || !table_id.has_value() || !tablet_id.has_value()) {
    return common::make_unexpected(corruption("Raft tablet snapshot identities are invalid"));
  }

  raft::SnapshotMetadata metadata{.last_included_index = load_u64(bytes, kLastIncludedIndexOffset),
                                  .last_included_term = load_u64(bytes, kLastIncludedTermOffset),
                                  .manifest_generation = load_u64(bytes, kManifestGenerationOffset),
                                  .part_set_checksum = {},
                                  .configuration_index = load_u64(bytes, kConfigurationIndexOffset),
                                  .voters = {}};
  std::ranges::copy(bytes.subspan(kPartSetChecksumOffset, metadata.part_set_checksum.size()),
                    metadata.part_set_checksum.begin());
  try {
    metadata.voters.reserve(voter_count);
    std::size_t cursor = kRaftTabletSnapshotHeaderSize;
    for (std::uint32_t index = 0U; index < voter_count; ++index) {
      metadata.voters.push_back(load_u64(bytes, cursor));
      cursor += sizeof(raft::NodeId);
    }
    RaftTabletApplicationSnapshot snapshot{.group_id = group_id,
                                           .table_id = *table_id,
                                           .tablet_id = *tablet_id,
                                           .raft_snapshot = std::move(metadata),
                                           .entries = {}};
    if (!valid_metadata(snapshot, limits)) {
      return common::make_unexpected(corruption("Raft tablet snapshot metadata is noncanonical"));
    }
    snapshot.entries.reserve(entry_count);
    raft::LogIndex previous{};
    raft::Term previous_term{};
    cursor = static_cast<std::size_t>(entries_offset);
    const std::size_t trailer_offset = bytes.size() - kRaftTabletSnapshotTrailerSize;
    for (std::uint32_t ordinal = 0U; ordinal < entry_count; ++ordinal) {
      if (cursor > trailer_offset || kRaftTabletSnapshotEntryHeaderSize > trailer_offset - cursor) {
        return common::make_unexpected(
            corruption("Raft tablet snapshot entry header is truncated"));
      }
      const raft::LogIndex index = load_u64(bytes, cursor);
      const raft::Term term = load_u64(bytes, cursor + 8U);
      const std::uint32_t payload_size = load_u32(bytes, cursor + 16U);
      const std::uint32_t payload_crc = load_u32(bytes, cursor + 20U);
      const std::size_t payload_offset = cursor + kRaftTabletSnapshotEntryHeaderSize;
      if (index == 0U || index <= previous || index > snapshot.raft_snapshot.last_included_index ||
          term == 0U || term < previous_term || term > snapshot.raft_snapshot.last_included_term ||
          (index == snapshot.raft_snapshot.last_included_index &&
           term != snapshot.raft_snapshot.last_included_term) ||
          payload_size == 0U || payload_offset > trailer_offset ||
          payload_size > trailer_offset - payload_offset) {
        return common::make_unexpected(corruption("Raft tablet snapshot entry is invalid"));
      }
      if (payload_size > limits.maximum_entry_payload_bytes) {
        return common::make_unexpected(
            exhausted("Raft tablet snapshot entry exceeds decode payload limit"));
      }
      const common::ByteView payload = bytes.subspan(payload_offset, payload_size);
      if (common::crc32c(payload) != payload_crc) {
        return common::make_unexpected(corruption("Raft tablet snapshot entry checksum mismatch"));
      }
      auto next = common::checked_align_up(payload_offset + payload_size, kAlignment);
      if (!next.has_value() || *next > trailer_offset ||
          std::ranges::any_of(
              bytes.subspan(payload_offset + payload_size, *next - payload_offset - payload_size),
              [](const std::byte value) { return value != std::byte{0U}; })) {
        return common::make_unexpected(corruption("Raft tablet snapshot entry padding is invalid"));
      }
      snapshot.entries.push_back(RaftTabletSnapshotEntry{
          .index = index,
          .term = term,
          .payload = std::vector<std::byte>{payload.begin(), payload.end()}});
      cursor = *next;
      previous = index;
      previous_term = term;
    }
    if (cursor != trailer_offset) {
      return common::make_unexpected(corruption("Raft tablet snapshot has trailing entry bytes"));
    }
    return snapshot;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft tablet snapshot decode allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Raft tablet snapshot decode exceeded limits"));
  }
}

} // namespace chronos::ingest
