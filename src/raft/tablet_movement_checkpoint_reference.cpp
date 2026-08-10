#include "chronos/raft/tablet_movement_checkpoint_reference.hpp"

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
                                           std::byte{'M'}, std::byte{'O'}, std::byte{'V'},
                                           std::byte{'R'}, std::byte{0U}};
constexpr std::array<std::byte, 8U> kGenerationMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                                     std::byte{'M'}, std::byte{'V'}, std::byte{'R'},
                                                     std::byte{'G'}, std::byte{0U}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;
constexpr std::size_t kFixedPayloadSize = 112U;
constexpr std::size_t kHeaderCrcOffset = 32U;
constexpr std::size_t kGenerationHeaderCrcOffset = 44U;

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}
[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}
[[nodiscard]] common::Status unsupported(const char* message) {
  return {common::StatusCode::kNotSupported, message};
}

[[nodiscard]] bool valid_limits(const TabletMovementCheckpointReferenceCodecLimits& limits) {
  return limits.maximum_checkpoint_bytes >= kTabletMovementCheckpointReferenceHeaderSize +
                                                kFixedPayloadSize +
                                                kTabletMovementCheckpointReferenceTrailerSize &&
         limits.maximum_checkpoint_bytes <= kMaximumTabletMovementCheckpointReferenceSize &&
         limits.movement.maximum_snapshot_bytes > 0U && limits.movement.maximum_chunk_bytes > 0U &&
         limits.movement.maximum_chunk_bytes <= limits.movement.maximum_snapshot_bytes &&
         limits.movement.maximum_replicas >= 2U &&
         limits.movement.maximum_replicas <= std::numeric_limits<std::uint32_t>::max();
}

[[nodiscard]] common::Status
validate_reference(const TabletMovementCheckpointReference& reference,
                   const TabletMovementCheckpointReferenceCodecLimits& limits) {
  common::Status structural = validate_tablet_movement_record(reference.record, limits.movement);
  if (!structural.is_ok())
    return structural;
  if (reference.record.phase == TabletMovementPhase::kAddingTarget ||
      reference.snapshot_session_placement_epoch == 0U) {
    return invalid("movement checkpoint reference requires an active snapshot session");
  }
  std::uint64_t expected_epoch = reference.snapshot_session_placement_epoch;
  if (reference.record.phase == TabletMovementPhase::kTargetPromoted) {
    if (expected_epoch == std::numeric_limits<std::uint64_t>::max())
      return invalid("movement checkpoint reference epoch is exhausted");
    ++expected_epoch;
  } else if (reference.record.phase == TabletMovementPhase::kComplete) {
    if (expected_epoch > std::numeric_limits<std::uint64_t>::max() - 2U)
      return invalid("movement checkpoint reference epoch is exhausted");
    expected_epoch += 2U;
  }
  return reference.record.placement_epoch == expected_epoch
             ? common::Status::ok()
             : invalid("movement checkpoint reference session epoch disagrees with phase");
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
  std::array<std::byte, kTabletMovementCheckpointReferenceHeaderSize> copy{};
  std::ranges::copy(header, copy.begin());
  std::fill_n(copy.begin() + static_cast<std::ptrdiff_t>(kHeaderCrcOffset), sizeof(std::uint32_t),
              std::byte{0U});
  return common::crc32c(copy);
}

[[nodiscard]] std::uint32_t generation_header_crc(const common::ByteView header) {
  std::array<std::byte, kTabletMovementCheckpointReferenceGenerationHeaderSize> copy{};
  std::ranges::copy(header, copy.begin());
  std::fill_n(copy.begin() + static_cast<std::ptrdiff_t>(kGenerationHeaderCrcOffset),
              sizeof(std::uint32_t), std::byte{0U});
  return common::crc32c(copy);
}

[[nodiscard]] common::Result<schema::TabletId> read_tablet_id(common::ByteReader& reader) {
  auto bytes = reader.read_exact(common::Uuid::kSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::Uuid::Bytes owned{};
  std::ranges::copy(*bytes, owned.begin());
  return schema::TabletId::from_bytes(owned);
}

} // namespace

common::Result<TabletMovementSnapshotSession>
tablet_movement_snapshot_session(const TabletMovementCheckpointReference& reference,
                                 const TabletMovementLimits limits) {
  const TabletMovementCheckpointReferenceCodecLimits codec_limits{
      kMaximumTabletMovementCheckpointReferenceSize, limits};
  common::Status validated = validate_reference(reference, codec_limits);
  if (!validated.is_ok())
    return common::make_unexpected(std::move(validated));
  return TabletMovementSnapshotSession{
      reference.record.tablet_id, reference.snapshot_session_placement_epoch,
      reference.record.source_node, reference.record.target_node, reference.record.snapshot};
}

common::Result<std::vector<std::byte>> encode_tablet_movement_checkpoint_reference_v1(
    const TabletMovementCheckpointReference& reference,
    const TabletMovementCheckpointReferenceCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("movement checkpoint reference limits are invalid"));
  common::Status validated = validate_reference(reference, limits);
  if (!validated.is_ok())
    return common::make_unexpected(std::move(validated));
  const std::size_t maximum_payload = limits.maximum_checkpoint_bytes -
                                      kTabletMovementCheckpointReferenceHeaderSize -
                                      kTabletMovementCheckpointReferenceTrailerSize;
  const std::size_t maximum_identity_bytes = maximum_payload - kFixedPayloadSize;
  if (reference.record.voting_replicas.size() > maximum_identity_bytes / sizeof(NodeId) ||
      reference.record.learners.size() > maximum_identity_bytes / sizeof(NodeId)) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "movement checkpoint reference exceeds size limit"});
  }
  const std::size_t voter_bytes = reference.record.voting_replicas.size() * sizeof(NodeId);
  const std::size_t learner_bytes = reference.record.learners.size() * sizeof(NodeId);
  if (voter_bytes > maximum_identity_bytes - learner_bytes) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "movement checkpoint reference exceeds size limit"});
  }
  const std::size_t payload_size = kFixedPayloadSize + voter_bytes + learner_bytes;
  const std::size_t total_size = kTabletMovementCheckpointReferenceHeaderSize + payload_size +
                                 kTabletMovementCheckpointReferenceTrailerSize;
  std::vector<std::byte> output(total_size, std::byte{0U});
  common::ByteWriter header{
      std::span<std::byte>{output}.first(kTabletMovementCheckpointReferenceHeaderSize)};
  for (const common::Status& status :
       {header.write_exact(kMagic), header.write_u16_le(kMajor), header.write_u16_le(kMinor),
        header.write_u32_le(kTabletMovementCheckpointReferenceHeaderSize),
        header.write_u64_le(total_size), header.write_u64_le(payload_size), header.write_u32_le(0U),
        header.write_u32_le(0U), header.zero_fill(24U)}) {
    if (!status.is_ok())
      return common::make_unexpected(status);
  }
  auto payload_span = std::span<std::byte>{output}.subspan(
      kTabletMovementCheckpointReferenceHeaderSize, payload_size);
  common::ByteWriter payload{payload_span};
  const TabletMovementRecord& record = reference.record;
  for (const common::Status& status :
       {payload.write_exact(record.tablet_id.bytes()), payload.write_u64_le(record.placement_epoch),
        payload.write_u64_le(record.source_node), payload.write_u64_le(record.target_node),
        payload.write_u8(static_cast<std::uint8_t>(record.phase)), payload.zero_fill(7U),
        payload.write_u32_le(static_cast<std::uint32_t>(record.voting_replicas.size())),
        payload.write_u32_le(static_cast<std::uint32_t>(record.learners.size())),
        payload.write_u64_le(record.snapshot.manifest_generation),
        payload.write_u64_le(record.snapshot.applied_index),
        payload.write_u64_le(record.snapshot.applied_term),
        payload.write_u64_le(record.snapshot.total_bytes),
        payload.write_u32_le(record.snapshot.content_crc32c), payload.zero_fill(4U),
        payload.write_u64_le(record.received_bytes),
        payload.write_u64_le(reference.snapshot_session_placement_epoch)}) {
    if (!status.is_ok())
      return common::make_unexpected(status);
  }
  for (const NodeId voter : record.voting_replicas) {
    common::Status status = payload.write_u64_le(voter);
    if (!status.is_ok())
      return common::make_unexpected(status);
  }
  for (const NodeId learner : record.learners) {
    common::Status status = payload.write_u64_le(learner);
    if (!status.is_ok())
      return common::make_unexpected(status);
  }
  if (!payload.full())
    return common::make_unexpected(corruption("movement reference payload size mismatch"));
  store_u32(output, 36U, common::crc32c(payload_span));
  store_u32(
      output, kHeaderCrcOffset,
      header_crc(common::ByteView{output}.first(kTabletMovementCheckpointReferenceHeaderSize)));
  store_u32(output, total_size - kTabletMovementCheckpointReferenceTrailerSize,
            common::crc32c(common::ByteView{output}.first(
                total_size - kTabletMovementCheckpointReferenceTrailerSize)));
  return output;
}

common::Result<TabletMovementCheckpointReference> decode_tablet_movement_checkpoint_reference_v1(
    const common::ByteView bytes, const TabletMovementCheckpointReferenceCodecLimits limits) {
  constexpr std::size_t kMinimumSize = kTabletMovementCheckpointReferenceHeaderSize +
                                       kFixedPayloadSize +
                                       kTabletMovementCheckpointReferenceTrailerSize;
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("movement checkpoint reference limits are invalid"));
  if (bytes.size() < kMinimumSize || bytes.size() > limits.maximum_checkpoint_bytes)
    return common::make_unexpected(corruption("movement checkpoint reference size is invalid"));
  const common::ByteView header = bytes.first(kTabletMovementCheckpointReferenceHeaderSize);
  if (header_crc(header) != load_u32(header, kHeaderCrcOffset))
    return common::make_unexpected(corruption("movement reference header checksum mismatch"));
  if (!std::ranges::equal(header.first(kMagic.size()), kMagic) || load_u16(header, 8U) != kMajor ||
      load_u16(header, 10U) != kMinor) {
    return common::make_unexpected(unsupported("movement reference version is unsupported"));
  }
  const std::uint64_t payload_size = load_u64(header, 24U);
  if (load_u32(header, 12U) != kTabletMovementCheckpointReferenceHeaderSize ||
      load_u64(header, 16U) != bytes.size() ||
      payload_size != bytes.size() - kTabletMovementCheckpointReferenceHeaderSize -
                          kTabletMovementCheckpointReferenceTrailerSize ||
      std::ranges::any_of(header.subspan(40U, 24U),
                          [](const std::byte value) { return value != std::byte{0U}; })) {
    return common::make_unexpected(corruption("movement checkpoint reference header is invalid"));
  }
  const common::ByteView payload = bytes.subspan(kTabletMovementCheckpointReferenceHeaderSize,
                                                 static_cast<std::size_t>(payload_size));
  if (common::crc32c(payload) != load_u32(header, 36U) ||
      common::crc32c(bytes.first(bytes.size() - kTabletMovementCheckpointReferenceTrailerSize)) !=
          load_u32(bytes, bytes.size() - kTabletMovementCheckpointReferenceTrailerSize)) {
    return common::make_unexpected(corruption("movement checkpoint reference checksum mismatch"));
  }

  common::ByteReader reader{payload};
  auto tablet_id = read_tablet_id(reader);
  auto epoch = reader.read_u64_le();
  auto source = reader.read_u64_le();
  auto target = reader.read_u64_le();
  auto phase = reader.read_u8();
  auto phase_reserved = reader.read_exact(7U);
  auto voter_count = reader.read_u32_le();
  auto learner_count = reader.read_u32_le();
  auto manifest_generation = reader.read_u64_le();
  auto applied_index = reader.read_u64_le();
  auto applied_term = reader.read_u64_le();
  auto total_snapshot_bytes = reader.read_u64_le();
  auto content_crc = reader.read_u32_le();
  auto snapshot_reserved = reader.read_u32_le();
  auto received_bytes = reader.read_u64_le();
  auto session_epoch = reader.read_u64_le();
  if (!tablet_id.has_value() || !epoch.has_value() || !source.has_value() || !target.has_value() ||
      !phase.has_value() || !phase_reserved.has_value() || !voter_count.has_value() ||
      !learner_count.has_value() || !manifest_generation.has_value() ||
      !applied_index.has_value() || !applied_term.has_value() ||
      !total_snapshot_bytes.has_value() || !content_crc.has_value() ||
      !snapshot_reserved.has_value() || !received_bytes.has_value() || !session_epoch.has_value() ||
      std::ranges::any_of(*phase_reserved,
                          [](const std::byte value) { return value != std::byte{0U}; }) ||
      *snapshot_reserved != 0U || *voter_count > limits.movement.maximum_replicas ||
      *learner_count > limits.movement.maximum_replicas ||
      *received_bytes > limits.movement.maximum_snapshot_bytes ||
      *total_snapshot_bytes > limits.movement.maximum_snapshot_bytes ||
      *received_bytes > std::numeric_limits<std::size_t>::max() ||
      *total_snapshot_bytes > std::numeric_limits<std::size_t>::max()) {
    return common::make_unexpected(corruption("movement checkpoint reference fields are invalid"));
  }
  const std::uint64_t identity_count = static_cast<std::uint64_t>(*voter_count) + *learner_count;
  if (identity_count != reader.remaining() / sizeof(NodeId) ||
      reader.remaining() % sizeof(NodeId) != 0U) {
    return common::make_unexpected(corruption("movement reference replica lengths are invalid"));
  }
  std::vector<NodeId> voters;
  std::vector<NodeId> learners;
  voters.reserve(*voter_count);
  learners.reserve(*learner_count);
  for (std::uint32_t ordinal = 0U; ordinal < *voter_count; ++ordinal) {
    auto value = reader.read_u64_le();
    if (!value.has_value())
      return common::make_unexpected(value.error());
    voters.push_back(*value);
  }
  for (std::uint32_t ordinal = 0U; ordinal < *learner_count; ++ordinal) {
    auto value = reader.read_u64_le();
    if (!value.has_value())
      return common::make_unexpected(value.error());
    learners.push_back(*value);
  }
  if (!reader.empty())
    return common::make_unexpected(corruption("movement checkpoint reference has trailing bytes"));
  TabletMovementCheckpointReference reference{
      TabletMovementRecord{
          *tablet_id, *epoch, *source, *target, std::move(voters), std::move(learners),
          static_cast<TabletMovementPhase>(*phase),
          SnapshotTransferMetadata{*manifest_generation, *applied_index, *applied_term,
                                   static_cast<std::size_t>(*total_snapshot_bytes), *content_crc},
          static_cast<std::size_t>(*received_bytes)},
      *session_epoch};
  common::Status validated = validate_reference(reference, limits);
  if (!validated.is_ok())
    return common::make_unexpected(
        corruption("movement checkpoint reference semantic state is invalid"));
  return reference;
}

common::Result<std::vector<std::byte>> encode_tablet_movement_checkpoint_reference_generation_v1(
    const TabletMovementCheckpointReferenceGeneration& generation,
    const TabletMovementCheckpointReferenceCodecLimits limits) {
  if (generation.checkpoint_generation == 0U)
    return common::make_unexpected(invalid("movement reference generation must be nonzero"));
  auto nested = encode_tablet_movement_checkpoint_reference_v1(generation.reference, limits);
  if (!nested.has_value())
    return common::make_unexpected(nested.error());
  constexpr std::size_t kFramingSize = kTabletMovementCheckpointReferenceGenerationHeaderSize +
                                       kTabletMovementCheckpointReferenceGenerationTrailerSize;
  if (nested->size() > limits.maximum_checkpoint_bytes - kFramingSize) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "movement reference generation exceeds configured size limit"});
  }
  const std::size_t total_size = kFramingSize + nested->size();
  std::vector<std::byte> output(total_size, std::byte{0U});
  common::ByteWriter writer{
      std::span<std::byte>{output}.first(kTabletMovementCheckpointReferenceGenerationHeaderSize)};
  for (const common::Status& status :
       {writer.write_exact(kGenerationMagic), writer.write_u16_le(kMajor),
        writer.write_u16_le(kMinor),
        writer.write_u32_le(kTabletMovementCheckpointReferenceGenerationHeaderSize),
        writer.write_u64_le(total_size), writer.write_u64_le(generation.checkpoint_generation),
        writer.write_u64_le(nested->size()), writer.write_u32_le(common::crc32c(*nested)),
        writer.write_u32_le(0U), writer.zero_fill(16U)}) {
    if (!status.is_ok())
      return common::make_unexpected(status);
  }
  std::ranges::copy(*nested,
                    output.begin() + static_cast<std::ptrdiff_t>(
                                         kTabletMovementCheckpointReferenceGenerationHeaderSize));
  store_u32(output, kGenerationHeaderCrcOffset,
            generation_header_crc(common::ByteView{output}.first(
                kTabletMovementCheckpointReferenceGenerationHeaderSize)));
  store_u32(output, total_size - kTabletMovementCheckpointReferenceGenerationTrailerSize,
            common::crc32c(common::ByteView{output}.first(
                total_size - kTabletMovementCheckpointReferenceGenerationTrailerSize)));
  return output;
}

common::Result<TabletMovementCheckpointReferenceGeneration>
decode_tablet_movement_checkpoint_reference_generation_v1(
    const common::ByteView bytes, const TabletMovementCheckpointReferenceCodecLimits limits) {
  constexpr std::size_t kMinimumNestedSize = kTabletMovementCheckpointReferenceHeaderSize +
                                             kFixedPayloadSize +
                                             kTabletMovementCheckpointReferenceTrailerSize;
  constexpr std::size_t kMinimumSize = kTabletMovementCheckpointReferenceGenerationHeaderSize +
                                       kMinimumNestedSize +
                                       kTabletMovementCheckpointReferenceGenerationTrailerSize;
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("movement checkpoint reference limits are invalid"));
  if (bytes.size() < kMinimumSize || bytes.size() > limits.maximum_checkpoint_bytes)
    return common::make_unexpected(corruption("movement reference generation size is invalid"));
  const common::ByteView header =
      bytes.first(kTabletMovementCheckpointReferenceGenerationHeaderSize);
  if (generation_header_crc(header) != load_u32(header, kGenerationHeaderCrcOffset)) {
    return common::make_unexpected(
        corruption("movement reference generation header checksum mismatch"));
  }
  if (!std::ranges::equal(header.first(kGenerationMagic.size()), kGenerationMagic) ||
      load_u16(header, 8U) != kMajor || load_u16(header, 10U) != kMinor) {
    return common::make_unexpected(
        unsupported("movement reference generation version is unsupported"));
  }
  const std::uint64_t nested_size = load_u64(header, 32U);
  if (load_u32(header, 12U) != kTabletMovementCheckpointReferenceGenerationHeaderSize ||
      load_u64(header, 16U) != bytes.size() || load_u64(header, 24U) == 0U ||
      nested_size != bytes.size() - kTabletMovementCheckpointReferenceGenerationHeaderSize -
                         kTabletMovementCheckpointReferenceGenerationTrailerSize ||
      std::ranges::any_of(header.subspan(48U, 16U),
                          [](const std::byte value) { return value != std::byte{0U}; })) {
    return common::make_unexpected(corruption("movement reference generation header is invalid"));
  }
  const common::ByteView nested =
      bytes.subspan(kTabletMovementCheckpointReferenceGenerationHeaderSize,
                    static_cast<std::size_t>(nested_size));
  if (common::crc32c(nested) != load_u32(header, 40U) ||
      common::crc32c(
          bytes.first(bytes.size() - kTabletMovementCheckpointReferenceGenerationTrailerSize)) !=
          load_u32(bytes, bytes.size() - kTabletMovementCheckpointReferenceGenerationTrailerSize)) {
    return common::make_unexpected(corruption("movement reference generation checksum mismatch"));
  }
  auto reference = decode_tablet_movement_checkpoint_reference_v1(nested, limits);
  if (!reference.has_value())
    return common::make_unexpected(reference.error());
  return TabletMovementCheckpointReferenceGeneration{load_u64(header, 24U), std::move(*reference)};
}

} // namespace chronos::raft
