#include "chronos/common/bytes.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/common/status.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/metadata_snapshot.hpp"
#include "chronos/raft/tablet_group_binding_codec.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ranges>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace {

constexpr std::size_t kHeaderCrcOffset = 120U;
constexpr std::size_t kEntryPayloadCrcOffset = 24U;
constexpr std::size_t kFuzzSnapshotLimit = 64U * 1024U;
constexpr std::size_t kFuzzPayloadLimit = 4U * 1024U;

[[nodiscard]] chronos::raft::MetadataSnapshotCodecLimits limits() {
  return {.maximum_snapshot_bytes = kFuzzSnapshotLimit,
          .maximum_entries = 16U,
          .maximum_entry_payload_bytes = kFuzzPayloadLimit,
          .maximum_voters = 8U};
}

[[nodiscard]] std::uint8_t input_byte(const chronos::common::ByteView input,
                                      const std::size_t offset, const std::uint8_t fallback = 0U) {
  return offset < input.size() ? std::to_integer<std::uint8_t>(input[offset]) : fallback;
}

[[nodiscard]] chronos::raft::GroupId group_id(const chronos::common::ByteView input) {
  chronos::common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{1U};
  bytes[1U] = static_cast<std::byte>(input_byte(input, 0U));
  bytes.back() = static_cast<std::byte>(input_byte(input, 1U));
  return chronos::raft::GroupId{bytes};
}

[[nodiscard]] chronos::raft::TabletGroupBindingMetadata
binding(const chronos::common::ByteView input) {
  chronos::common::Uuid::Bytes tablet_bytes{};
  tablet_bytes.front() = std::byte{1U};
  tablet_bytes.back() = static_cast<std::byte>(input_byte(input, 2U));
  chronos::common::Uuid::Bytes group_bytes{};
  group_bytes.front() = std::byte{2U};
  group_bytes.back() = static_cast<std::byte>(input_byte(input, 3U));
  return {chronos::schema::TabletId::from_bytes(tablet_bytes).value(),
          chronos::raft::GroupId{group_bytes}};
}

[[nodiscard]] std::vector<std::byte> node_payload(const chronos::common::ByteView input,
                                                  const std::size_t selector_offset,
                                                  const chronos::raft::NodeId node_id) {
  const std::size_t endpoint_size =
      1U + static_cast<std::size_t>(input_byte(input, selector_offset)) % 65U;
  const char endpoint_byte =
      static_cast<char>('a' + static_cast<char>(input_byte(input, selector_offset + 1U) % 26U));
  auto encoded = chronos::raft::encode_metadata_command_v1(
      chronos::raft::ClusterNodeMetadata{node_id, std::string(endpoint_size, endpoint_byte)});
  if (!encoded.has_value())
    std::abort();
  return std::move(*encoded);
}

[[nodiscard]] chronos::raft::MetadataApplicationSnapshot
snapshot(const chronos::common::ByteView input, const bool minor_one) {
  chronos::raft::SnapshotMetadata metadata{.last_included_index = 8U,
                                           .last_included_term = 3U,
                                           .manifest_generation = 8U,
                                           .part_set_checksum = {},
                                           .configuration_index = 6U,
                                           .voters = {}};
  for (std::size_t offset = 0U; offset < metadata.part_set_checksum.size(); ++offset)
    metadata.part_set_checksum[offset] =
        static_cast<std::byte>(input_byte(input, 8U + offset, 0x5aU));
  const std::size_t voter_count = 2U + static_cast<std::size_t>(input_byte(input, 4U)) % 7U;
  metadata.voters.reserve(voter_count);
  for (std::size_t ordinal = 0U; ordinal < voter_count; ++ordinal)
    metadata.voters.push_back(ordinal + 1U);

  chronos::raft::MetadataApplicationSnapshot value{
      .group_id = group_id(input),
      .raft_snapshot = std::move(metadata),
      .entries = {{.index = 2U,
                   .term = 1U,
                   .type = chronos::raft::kRaftMetadataCommandEntryType,
                   .payload = node_payload(input, 5U, 7U)},
                  {.index = 7U,
                   .term = 2U,
                   .type = chronos::raft::kRaftMetadataCommandEntryType,
                   .payload = node_payload(input, 6U, 8U)}}};
  if (minor_one) {
    auto encoded_binding = chronos::raft::encode_tablet_group_binding_v1(binding(input));
    if (!encoded_binding.has_value())
      std::abort();
    value.entries.push_back({.index = 8U,
                             .term = 3U,
                             .type = chronos::raft::kRaftTabletGroupBindingEntryType,
                             .payload = std::move(*encoded_binding)});
  }
  return value;
}

void store_u32(const chronos::common::MutableByteView bytes, const std::size_t offset,
               const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

void repair_header_crc(std::vector<std::byte>& bytes) {
  if (bytes.size() < chronos::raft::kMetadataSnapshotHeaderSize)
    return;
  store_u32(bytes, kHeaderCrcOffset, 0U);
  store_u32(bytes, kHeaderCrcOffset,
            chronos::common::crc32c(chronos::common::ByteView{bytes}.first(
                chronos::raft::kMetadataSnapshotHeaderSize)));
}

void repair_file_crc(std::vector<std::byte>& bytes) {
  if (bytes.size() < chronos::raft::kMetadataSnapshotTrailerSize)
    return;
  const std::size_t trailer = bytes.size() - chronos::raft::kMetadataSnapshotTrailerSize;
  store_u32(bytes, trailer,
            chronos::common::crc32c(chronos::common::ByteView{bytes}.first(trailer)));
}

void repair_entry_crcs(std::vector<std::byte>& bytes,
                       const chronos::raft::MetadataApplicationSnapshot& expected) {
  std::size_t cursor = chronos::raft::kMetadataSnapshotHeaderSize +
                       expected.raft_snapshot.voters.size() * sizeof(chronos::raft::NodeId);
  const std::size_t trailer = bytes.size() - chronos::raft::kMetadataSnapshotTrailerSize;
  for (const chronos::raft::MetadataSnapshotEntry& entry : expected.entries) {
    const std::size_t payload_offset = cursor + chronos::raft::kMetadataSnapshotEntryHeaderSize;
    if (payload_offset > trailer || entry.payload.size() > trailer - payload_offset)
      std::abort();
    store_u32(bytes, cursor + kEntryPayloadCrcOffset,
              chronos::common::crc32c(
                  chronos::common::ByteView{bytes}.subspan(payload_offset, entry.payload.size())));
    cursor = (payload_offset + entry.payload.size() + 7U) & ~std::size_t{7U};
  }
}

template <typename T> void require_rejected(const chronos::common::Result<T>& result) {
  if (result.has_value())
    std::abort();
}

template <typename T> void require_resource_exhausted(const chronos::common::Result<T>& result) {
  if (result.has_value() ||
      result.error().code() != chronos::common::StatusCode::kResourceExhausted)
    std::abort();
}

void exercise(const chronos::common::ByteView bytes) {
  const auto decoded = chronos::raft::decode_metadata_application_snapshot_v1(bytes, limits());
  if (!decoded.has_value())
    return;
  if (decoded->entries.size() > limits().maximum_entries ||
      decoded->raft_snapshot.voters.size() > limits().maximum_voters)
    std::abort();

  // A reader may accept minor 1 without a binding; the encoder intentionally emits the lowest
  // minor required by the decoded semantics.
  const auto canonical = chronos::raft::encode_metadata_application_snapshot_v1(*decoded, limits());
  if (!canonical.has_value())
    std::abort();
  const auto canonical_decoded =
      chronos::raft::decode_metadata_application_snapshot_v1(*canonical, limits());
  if (!canonical_decoded.has_value() || *canonical_decoded != *decoded)
    std::abort();
  const auto stable =
      chronos::raft::encode_metadata_application_snapshot_v1(*canonical_decoded, limits());
  if (!stable.has_value() || *stable != *canonical)
    std::abort();
}

void verify_nested_payloads(const chronos::raft::MetadataApplicationSnapshot& expected) {
  for (const chronos::raft::MetadataSnapshotEntry& entry : expected.entries) {
    if (entry.type == chronos::raft::kRaftMetadataCommandEntryType) {
      if (!chronos::raft::decode_metadata_command_v1(entry.payload).has_value())
        std::abort();
    } else if (entry.type == chronos::raft::kRaftTabletGroupBindingEntryType) {
      if (!chronos::raft::decode_tablet_group_binding_v1(entry.payload).has_value())
        std::abort();
    } else {
      std::abort();
    }
  }
}

void exercise_lower_limits(const std::vector<std::byte>& encoded,
                           const chronos::raft::MetadataApplicationSnapshot& expected) {
  auto lower = limits();
  lower.maximum_snapshot_bytes = encoded.size() - 1U;
  require_rejected(chronos::raft::encode_metadata_application_snapshot_v1(expected, lower));
  require_resource_exhausted(
      chronos::raft::decode_metadata_application_snapshot_v1(encoded, lower));

  lower = limits();
  lower.maximum_entries = expected.entries.size() - 1U;
  require_rejected(chronos::raft::encode_metadata_application_snapshot_v1(expected, lower));
  require_resource_exhausted(
      chronos::raft::decode_metadata_application_snapshot_v1(encoded, lower));

  lower = limits();
  lower.maximum_voters = expected.raft_snapshot.voters.size() - 1U;
  require_rejected(chronos::raft::encode_metadata_application_snapshot_v1(expected, lower));
  require_resource_exhausted(
      chronos::raft::decode_metadata_application_snapshot_v1(encoded, lower));

  const auto largest_payload = std::ranges::max_element(
      expected.entries, {},
      [](const chronos::raft::MetadataSnapshotEntry& entry) { return entry.payload.size(); });
  lower = limits();
  lower.maximum_entry_payload_bytes = largest_payload->payload.size() - 1U;
  require_rejected(chronos::raft::encode_metadata_application_snapshot_v1(expected, lower));
  require_resource_exhausted(
      chronos::raft::decode_metadata_application_snapshot_v1(encoded, lower));
}

void mutate(std::vector<std::byte>& candidate,
            const chronos::raft::MetadataApplicationSnapshot& expected,
            const chronos::common::ByteView input) {
  const std::size_t selector = static_cast<std::size_t>(input_byte(input, 40U)) |
                               (static_cast<std::size_t>(input_byte(input, 41U)) << 8U);
  const std::size_t offset = selector % candidate.size();
  const std::uint8_t supplied_mask = input_byte(input, 42U, 1U);
  candidate[offset] ^= static_cast<std::byte>(supplied_mask == 0U ? 1U : supplied_mask);

  switch (input_byte(input, 43U) & 3U) {
  case 0U:
    break;
  case 1U:
    repair_file_crc(candidate);
    break;
  case 2U:
    repair_header_crc(candidate);
    repair_file_crc(candidate);
    break;
  case 3U:
    repair_entry_crcs(candidate, expected);
    repair_header_crc(candidate);
    repair_file_crc(candidate);
    break;
  }

  if ((input_byte(input, 44U) & 1U) != 0U) {
    const std::size_t new_size =
        static_cast<std::size_t>(input_byte(input, 45U)) * candidate.size() / 255U;
    candidate.resize(new_size);
  }
}

void exercise_structured(const chronos::common::ByteView input, const bool minor_one) {
  const chronos::raft::MetadataApplicationSnapshot expected = snapshot(input, minor_one);
  verify_nested_payloads(expected);
  const auto encoded = chronos::raft::encode_metadata_application_snapshot_v1(expected, limits());
  if (!encoded.has_value() || encoded->size() <= chronos::raft::kMetadataSnapshotHeaderSize ||
      std::to_integer<std::uint8_t>((*encoded)[10U]) != (minor_one ? 1U : 0U))
    std::abort();
  const auto decoded = chronos::raft::decode_metadata_application_snapshot_v1(*encoded, limits());
  if (!decoded.has_value() || *decoded != expected)
    std::abort();
  const auto reencoded = chronos::raft::encode_metadata_application_snapshot_v1(*decoded, limits());
  if (!reencoded.has_value() || *reencoded != *encoded)
    std::abort();
  exercise_lower_limits(*encoded, expected);
  exercise(*encoded);

  std::vector<std::byte> candidate = *encoded;
  mutate(candidate, expected, input);
  exercise(candidate);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const chronos::common::ByteView input =
      chronos::common::byte_view(std::span<const std::uint8_t>{data, size});
  exercise(input);
  exercise_structured(input, false);
  exercise_structured(input, true);
  return 0;
}
