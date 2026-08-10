#include "chronos/raft/tablet_movement_checkpoint.hpp"

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
                                           std::byte{'E'}, std::byte{0U}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;
constexpr std::size_t kFixedPayloadSize = 104U;
constexpr std::size_t kHeaderCrcOffset = 32U;

[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return common::Status{common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status unsupported(const char* message) {
  return common::Status{common::StatusCode::kNotSupported, message};
}

[[nodiscard]] bool valid_limits(const TabletMovementCheckpointCodecLimits& limits) {
  return limits.maximum_checkpoint_bytes >= kTabletMovementCheckpointHeaderSize +
                                                kFixedPayloadSize +
                                                kTabletMovementCheckpointTrailerSize &&
         limits.maximum_checkpoint_bytes <= kMaximumTabletMovementCheckpointSize &&
         limits.movement.maximum_snapshot_bytes > 0U && limits.movement.maximum_chunk_bytes > 0U &&
         limits.movement.maximum_chunk_bytes <= limits.movement.maximum_snapshot_bytes &&
         limits.movement.maximum_replicas >= 2U;
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
  for (std::size_t ordinal = 0U; ordinal < sizeof(value); ++ordinal) {
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[offset + ordinal]))
             << (ordinal * 8U);
  }
  return value;
}

[[nodiscard]] std::uint64_t load_u64(const common::ByteView bytes, const std::size_t offset) {
  std::uint64_t value{};
  for (std::size_t ordinal = 0U; ordinal < sizeof(value); ++ordinal) {
    value |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[offset + ordinal]))
             << (ordinal * 8U);
  }
  return value;
}

[[nodiscard]] std::uint32_t header_crc(const common::ByteView header) {
  std::array<std::byte, kTabletMovementCheckpointHeaderSize> copy{};
  std::ranges::copy(header, copy.begin());
  std::fill_n(copy.begin() + static_cast<std::ptrdiff_t>(kHeaderCrcOffset), sizeof(std::uint32_t),
              std::byte{0U});
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

common::Result<std::vector<std::byte>>
encode_tablet_movement_checkpoint_v1(const TabletMovementCheckpoint& checkpoint,
                                     const TabletMovementCheckpointCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("tablet movement checkpoint codec limits are invalid"));
  const common::Status validated = validate_tablet_movement_state(
      checkpoint.record, checkpoint.received_snapshot, limits.movement);
  if (!validated.is_ok())
    return common::make_unexpected(validated);
  const std::size_t voter_bytes = checkpoint.record.voting_replicas.size() * sizeof(NodeId);
  const std::size_t learner_bytes = checkpoint.record.learners.size() * sizeof(NodeId);
  if (voter_bytes > std::numeric_limits<std::size_t>::max() - learner_bytes ||
      kFixedPayloadSize + voter_bytes + learner_bytes >
          std::numeric_limits<std::size_t>::max() - checkpoint.received_snapshot.size()) {
    return common::make_unexpected(invalid("tablet movement checkpoint size overflows"));
  }
  const std::size_t payload_size =
      kFixedPayloadSize + voter_bytes + learner_bytes + checkpoint.received_snapshot.size();
  if (payload_size > limits.maximum_checkpoint_bytes - kTabletMovementCheckpointHeaderSize -
                         kTabletMovementCheckpointTrailerSize) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "tablet movement checkpoint exceeds size limit"});
  }
  const std::size_t total_size =
      kTabletMovementCheckpointHeaderSize + payload_size + kTabletMovementCheckpointTrailerSize;
  std::vector<std::byte> output(total_size, std::byte{0U});
  common::ByteWriter header{
      std::span<std::byte>{output}.first(kTabletMovementCheckpointHeaderSize)};
  if (!header.write_exact(kMagic).is_ok() || !header.write_u16_le(kMajor).is_ok() ||
      !header.write_u16_le(kMinor).is_ok() ||
      !header.write_u32_le(kTabletMovementCheckpointHeaderSize).is_ok() ||
      !header.write_u64_le(total_size).is_ok() || !header.write_u64_le(payload_size).is_ok() ||
      !header.zero_fill(kTabletMovementCheckpointHeaderSize - 32U).is_ok()) {
    return common::make_unexpected(corruption("tablet movement checkpoint header size mismatch"));
  }
  auto payload_span =
      std::span<std::byte>{output}.subspan(kTabletMovementCheckpointHeaderSize, payload_size);
  common::ByteWriter payload{payload_span};
  const TabletMovementRecord& record = checkpoint.record;
  common::Status status = payload.write_exact(record.tablet_id.bytes());
  if (status.is_ok())
    status = payload.write_u64_le(record.placement_epoch);
  if (status.is_ok())
    status = payload.write_u64_le(record.source_node);
  if (status.is_ok())
    status = payload.write_u64_le(record.target_node);
  if (status.is_ok())
    status = payload.write_u8(static_cast<std::uint8_t>(record.phase));
  if (status.is_ok())
    status = payload.zero_fill(7U);
  if (status.is_ok())
    status = payload.write_u32_le(static_cast<std::uint32_t>(record.voting_replicas.size()));
  if (status.is_ok())
    status = payload.write_u32_le(static_cast<std::uint32_t>(record.learners.size()));
  if (status.is_ok())
    status = payload.write_u64_le(record.snapshot.manifest_generation);
  if (status.is_ok())
    status = payload.write_u64_le(record.snapshot.applied_index);
  if (status.is_ok())
    status = payload.write_u64_le(record.snapshot.applied_term);
  if (status.is_ok())
    status = payload.write_u64_le(record.snapshot.total_bytes);
  if (status.is_ok())
    status = payload.write_u32_le(record.snapshot.content_crc32c);
  if (status.is_ok())
    status = payload.zero_fill(4U);
  if (status.is_ok())
    status = payload.write_u64_le(record.received_bytes);
  for (const NodeId voter : record.voting_replicas) {
    if (status.is_ok())
      status = payload.write_u64_le(voter);
  }
  for (const NodeId learner : record.learners) {
    if (status.is_ok())
      status = payload.write_u64_le(learner);
  }
  if (status.is_ok())
    status = payload.write_exact(checkpoint.received_snapshot);
  if (!status.is_ok() || !payload.full())
    return common::make_unexpected(status.is_ok() ? corruption("movement payload size mismatch")
                                                  : status);

  store_u32(output, 36U, common::crc32c(payload_span));
  store_u32(output, kHeaderCrcOffset,
            header_crc(common::ByteView{output}.first(kTabletMovementCheckpointHeaderSize)));
  store_u32(output, total_size - kTabletMovementCheckpointTrailerSize,
            common::crc32c(
                common::ByteView{output}.first(total_size - kTabletMovementCheckpointTrailerSize)));
  return output;
}

common::Result<TabletMovementCheckpoint>
decode_tablet_movement_checkpoint_v1(const common::ByteView bytes,
                                     const TabletMovementCheckpointCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("tablet movement checkpoint codec limits are invalid"));
  if (bytes.size() < kTabletMovementCheckpointHeaderSize + kFixedPayloadSize +
                         kTabletMovementCheckpointTrailerSize ||
      bytes.size() > limits.maximum_checkpoint_bytes)
    return common::make_unexpected(corruption("tablet movement checkpoint size is invalid"));
  if (header_crc(bytes.first(kTabletMovementCheckpointHeaderSize)) !=
      load_u32(bytes, kHeaderCrcOffset)) {
    return common::make_unexpected(
        corruption("tablet movement checkpoint header checksum mismatch"));
  }
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic) || load_u16(bytes, 8U) != kMajor ||
      load_u16(bytes, 10U) != kMinor) {
    return common::make_unexpected(
        unsupported("tablet movement checkpoint version is unsupported"));
  }
  const std::uint64_t total_size = load_u64(bytes, 16U);
  const std::uint64_t payload_size = load_u64(bytes, 24U);
  if (load_u32(bytes, 12U) != kTabletMovementCheckpointHeaderSize || total_size != bytes.size() ||
      payload_size != bytes.size() - kTabletMovementCheckpointHeaderSize -
                          kTabletMovementCheckpointTrailerSize ||
      std::ranges::any_of(bytes.subspan(40U, 24U),
                          [](const std::byte value) { return value != std::byte{0U}; })) {
    return common::make_unexpected(corruption("tablet movement checkpoint header is invalid"));
  }
  const common::ByteView payload =
      bytes.subspan(kTabletMovementCheckpointHeaderSize, static_cast<std::size_t>(payload_size));
  if (common::crc32c(payload) != load_u32(bytes, 36U) ||
      common::crc32c(bytes.first(bytes.size() - kTabletMovementCheckpointTrailerSize)) !=
          load_u32(bytes, bytes.size() - kTabletMovementCheckpointTrailerSize)) {
    return common::make_unexpected(corruption("tablet movement checkpoint checksum mismatch"));
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
  if (!tablet_id.has_value() || !epoch.has_value() || !source.has_value() || !target.has_value() ||
      !phase.has_value() || !phase_reserved.has_value() || !voter_count.has_value() ||
      !learner_count.has_value() || !manifest_generation.has_value() ||
      !applied_index.has_value() || !applied_term.has_value() ||
      !total_snapshot_bytes.has_value() || !content_crc.has_value() ||
      !snapshot_reserved.has_value() || !received_bytes.has_value() ||
      std::ranges::any_of(*phase_reserved,
                          [](const std::byte value) { return value != std::byte{0U}; }) ||
      *snapshot_reserved != 0U || *voter_count > limits.movement.maximum_replicas ||
      *learner_count > limits.movement.maximum_replicas ||
      *received_bytes > limits.movement.maximum_snapshot_bytes ||
      *total_snapshot_bytes > limits.movement.maximum_snapshot_bytes ||
      *received_bytes > std::numeric_limits<std::size_t>::max() ||
      *total_snapshot_bytes > std::numeric_limits<std::size_t>::max()) {
    return common::make_unexpected(corruption("tablet movement checkpoint payload fields invalid"));
  }
  const std::uint64_t identity_count = static_cast<std::uint64_t>(*voter_count) + *learner_count;
  if (identity_count > reader.remaining() / sizeof(NodeId) ||
      *received_bytes > reader.remaining() - identity_count * sizeof(NodeId)) {
    return common::make_unexpected(
        corruption("tablet movement checkpoint payload lengths invalid"));
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
  auto snapshot = reader.read_exact(static_cast<std::size_t>(*received_bytes));
  if (!snapshot.has_value() || !reader.empty())
    return common::make_unexpected(corruption("tablet movement checkpoint has trailing bytes"));
  std::vector<std::byte> owned_snapshot(snapshot->begin(), snapshot->end());
  TabletMovementCheckpoint checkpoint{
      TabletMovementRecord{
          *tablet_id, *epoch, *source, *target, std::move(voters), std::move(learners),
          static_cast<TabletMovementPhase>(*phase),
          SnapshotTransferMetadata{*manifest_generation, *applied_index, *applied_term,
                                   static_cast<std::size_t>(*total_snapshot_bytes), *content_crc},
          static_cast<std::size_t>(*received_bytes)},
      std::move(owned_snapshot)};
  const common::Status validated = validate_tablet_movement_state(
      checkpoint.record, checkpoint.received_snapshot, limits.movement);
  if (!validated.is_ok())
    return common::make_unexpected(
        validated.code() == common::StatusCode::kCorruption
            ? validated
            : corruption("tablet movement checkpoint semantic state is invalid"));
  return checkpoint;
}

} // namespace chronos::raft
