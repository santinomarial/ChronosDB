#include "chronos/manifest/temporal_codec.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/manifest/format.hpp"
#include "chronos/manifest/temporal_layout.hpp"

#include <algorithm>
#include <bit>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace chronos::manifest {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const std::string_view message) {
  return common::Status{code, std::string{message}};
}
[[nodiscard]] common::Status invalid(const std::string_view message) {
  return status(common::StatusCode::kInvalidArgument, message);
}
[[nodiscard]] ManifestDecodeError incomplete(const std::string_view message,
                                             const std::uint64_t required) {
  return {ManifestDecodeErrorKind::kIncomplete, status(common::StatusCode::kOutOfRange, message),
          required};
}
[[nodiscard]] ManifestDecodeError corruption(const std::string_view message) {
  return {ManifestDecodeErrorKind::kCorruption, status(common::StatusCode::kCorruption, message)};
}
[[nodiscard]] ManifestDecodeError unsupported(const std::string_view message) {
  return {ManifestDecodeErrorKind::kUnsupported,
          status(common::StatusCode::kNotSupported, message)};
}
[[nodiscard]] ManifestDecodeError resource_limit(const std::string_view message) {
  return {ManifestDecodeErrorKind::kResourceLimit,
          status(common::StatusCode::kResourceExhausted, message)};
}

[[nodiscard]] std::uint16_t load_u16(const common::ByteView bytes,
                                     const std::size_t offset) noexcept {
  return static_cast<std::uint16_t>(std::to_integer<std::uint16_t>(bytes[offset]) |
                                    (std::to_integer<std::uint16_t>(bytes[offset + 1U]) << 8U));
}
[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes,
                                     const std::size_t offset) noexcept {
  std::uint32_t value = 0U;
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= std::to_integer<std::uint32_t>(bytes[offset + index]) << (index * 8U);
  }
  return value;
}
[[nodiscard]] std::uint64_t load_u64(const common::ByteView bytes,
                                     const std::size_t offset) noexcept {
  std::uint64_t value = 0U;
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= std::to_integer<std::uint64_t>(bytes[offset + index]) << (index * 8U);
  }
  return value;
}
[[nodiscard]] std::int64_t load_i64(const common::ByteView bytes,
                                    const std::size_t offset) noexcept {
  return std::bit_cast<std::int64_t>(load_u64(bytes, offset));
}
void store_u16(const common::MutableByteView bytes, const std::size_t offset,
               const std::uint16_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
  }
}
void store_u32(const common::MutableByteView bytes, const std::size_t offset,
               const std::uint32_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
  }
}
void store_u64(const common::MutableByteView bytes, const std::size_t offset,
               const std::uint64_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
  }
}
void store_i64(const common::MutableByteView bytes, const std::size_t offset,
               const std::int64_t value) noexcept {
  store_u64(bytes, offset, std::bit_cast<std::uint64_t>(value));
}
[[nodiscard]] bool is_zero(const common::ByteView bytes) noexcept {
  return std::ranges::all_of(bytes, [](const std::byte value) { return value == std::byte{0}; });
}
[[nodiscard]] bool valid_source(const ManifestCommitSource source) noexcept {
  return source == ManifestCommitSource::kWal || source == ManifestCommitSource::kRaft;
}
[[nodiscard]] bool valid_wal_id(const wal::WalId& id) noexcept {
  return std::ranges::any_of(id.bytes, [](const std::byte value) { return value != std::byte{0}; });
}

template <typename Identifier>
[[nodiscard]] common::Result<Identifier> parse_id(const common::ByteView bytes,
                                                  const std::size_t offset) {
  common::Uuid::Bytes encoded{};
  std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(offset), encoded.size(), encoded.begin());
  return Identifier::from_bytes(encoded);
}
[[nodiscard]] common::Uuid parse_uuid(const common::ByteView bytes, const std::size_t offset) {
  common::Uuid::Bytes encoded{};
  std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(offset), encoded.size(), encoded.begin());
  return common::Uuid{encoded};
}
void copy_bytes(const common::MutableByteView output, const std::size_t offset,
                const common::ByteView value) {
  std::ranges::copy(value, output.begin() + static_cast<std::ptrdiff_t>(offset));
}
void copy_id(const common::MutableByteView output, const std::size_t offset, const auto& id) {
  copy_bytes(output, offset, id.bytes());
}
void copy_uuid(const common::MutableByteView output, const std::size_t offset,
               const common::Uuid& id) {
  copy_bytes(output, offset, id.bytes());
}

[[nodiscard]] common::Status validate_checkpoint(const TemporalWalReclaimCheckpoint& checkpoint) {
  if (!valid_wal_id(checkpoint.wal_id)) {
    return invalid("Manifest v2 WAL reclaim identity is zero");
  }
  const WalCheckpoint& value = checkpoint.coordinate;
  if (value.record_sequence == 0U) {
    return value.segment_number == wal::kFirstSegmentNumber &&
                   value.byte_offset == wal::kSegmentHeaderSize
               ? common::Status::ok()
               : invalid("Manifest v2 empty WAL checkpoint is not segment 1 offset 64");
  }
  if (value.segment_number == 0U || value.byte_offset < wal::kSegmentHeaderSize ||
      value.byte_offset > wal::kSegmentSizeLimit || value.byte_offset % format::kAlignment != 0U) {
    return invalid("Manifest v2 WAL checkpoint is outside WAL v1 bounds");
  }
  return common::Status::ok();
}

[[nodiscard]] const TemporalTabletDescriptor*
find_tablet(const std::span<const TemporalTabletDescriptor> tablets,
            const schema::TabletId& tablet_id) {
  const auto found =
      std::ranges::lower_bound(tablets, tablet_id, {}, [](const TemporalTabletDescriptor& tablet) {
        return tablet.tablet_id;
      });
  return found != tablets.end() && found->tablet_id == tablet_id ? &*found : nullptr;
}
[[nodiscard]] bool retry_less(const TemporalRetryDescriptor& left,
                              const TemporalRetryDescriptor& right) {
  return left.client_id != right.client_id ? left.client_id < right.client_id
                                           : left.client_batch_id < right.client_batch_id;
}

[[nodiscard]] common::Status
validate_model(const std::optional<TemporalWalReclaimCheckpoint>& wal_checkpoint,
               const std::span<const TemporalTabletDescriptor> tablets,
               const std::span<const TemporalPartDescriptor> parts,
               const std::span<const TemporalRetryDescriptor> retries) {
  if (wal_checkpoint.has_value()) {
    const common::Status checkpoint = validate_checkpoint(*wal_checkpoint);
    if (!checkpoint.is_ok()) {
      return checkpoint;
    }
  }
  if (tablets.empty() && (!parts.empty() || !retries.empty())) {
    return invalid("Manifest v2 without tablets cannot contain parts or retries");
  }
  std::uint64_t next_part = 0U;
  std::uint64_t maximum_wal_position = 0U;
  std::vector<cseg::PartId> part_ids;
  part_ids.reserve(parts.size());
  for (std::size_t tablet_index = 0U; tablet_index < tablets.size(); ++tablet_index) {
    const TemporalTabletDescriptor& tablet = tablets[tablet_index];
    if (tablet_index != 0U && !(tablets[tablet_index - 1U].tablet_id < tablet.tablet_id)) {
      return invalid("Manifest v2 tablets are not strictly sorted");
    }
    if (!valid_source(tablet.commit_source) || tablet.source_id.is_nil() ||
        tablet.reclaim_position > tablet.durable_position ||
        (tablet.commit_source == ManifestCommitSource::kWal && tablet.reclaim_position != 0U) ||
        tablet.first_part_index != next_part || tablet.part_count > parts.size() ||
        tablet.first_part_index > parts.size() - tablet.part_count) {
      return invalid("Manifest v2 tablet source, boundary, or part range is invalid");
    }
    if (tablet.commit_source == ManifestCommitSource::kWal && wal_checkpoint.has_value()) {
      if (tablet.source_id != common::Uuid{wal_checkpoint->wal_id.bytes}) {
        return invalid("Manifest v2 WAL tablet disagrees with the global WAL identity");
      }
      maximum_wal_position = std::max(maximum_wal_position, tablet.durable_position);
    }
    std::uint64_t rows = 0U;
    for (std::uint64_t local = 0U; local < tablet.part_count; ++local) {
      const std::size_t index = static_cast<std::size_t>(tablet.first_part_index + local);
      const TemporalPartDescriptor& part = parts[index];
      if (part.table_id != tablet.table_id || part.tablet_id != tablet.tablet_id ||
          part.commit_source != tablet.commit_source || part.source_id != tablet.source_id ||
          (local != 0U && !(parts[index - 1U].part_id < part.part_id)) || part.file_length == 0U ||
          part.file_length > cseg::format::kMaximumFileLength ||
          part.file_length % cseg::format::kAlignment != 0U || part.row_count == 0U ||
          part.row_count > cseg::format::kMaximumRowCount || part.minimum_commit_position == 0U ||
          part.minimum_commit_position > part.maximum_commit_position ||
          part.maximum_commit_position > tablet.durable_position ||
          part.minimum_event_time > part.maximum_event_time ||
          part.minimum_system_time > part.maximum_system_time ||
          part.cseg_format_major != cseg::temporal_format::kFormatMajor ||
          part.cseg_format_minor != cseg::temporal_format::kFormatMinor) {
        return invalid("Manifest v2 part identity, source, version, length, or extrema is invalid");
      }
      const std::optional<std::uint64_t> next_rows = common::checked_add(rows, part.row_count);
      if (!next_rows.has_value()) {
        return invalid("Manifest v2 tablet version count overflowed");
      }
      rows = *next_rows;
      part_ids.push_back(part.part_id);
    }
    if (rows != tablet.durable_version_count) {
      return invalid("Manifest v2 tablet version count disagrees with its parts");
    }
    next_part += tablet.part_count;
  }
  if (next_part != parts.size()) {
    return invalid("Manifest v2 tablet ranges do not cover all parts");
  }
  if (wal_checkpoint.has_value() &&
      wal_checkpoint->coordinate.record_sequence > maximum_wal_position) {
    return invalid("Manifest v2 WAL reclaim checkpoint exceeds represented WAL tablets");
  }
  std::ranges::sort(part_ids);
  if (std::ranges::adjacent_find(part_ids) != part_ids.end()) {
    return invalid("Manifest v2 contains a duplicate part identity");
  }
  for (std::size_t index = 0U; index < retries.size(); ++index) {
    const TemporalRetryDescriptor& retry = retries[index];
    const TemporalTabletDescriptor* tablet = find_tablet(tablets, retry.tablet_id);
    if ((index != 0U && !retry_less(retries[index - 1U], retry)) || tablet == nullptr ||
        retry.table_id != tablet->table_id || retry.commit_source != tablet->commit_source ||
        retry.source_id != tablet->source_id || retry.commit_position == 0U ||
        retry.commit_position > tablet->durable_position || retry.applied_row_count == 0U) {
      return invalid("Manifest v2 retry does not bind to its tablet source and outcome");
    }
  }
  return common::Status::ok();
}

[[nodiscard]] bool valid_limits(const ManifestDecodeLimits limits) noexcept {
  return limits.max_file_length >= format::kFileHeaderLength + format::kTrailerLength &&
         limits.max_file_length <= format::kMaximumFileLength &&
         limits.max_tablets <= format::kMaximumDescriptorCount &&
         limits.max_parts <= format::kMaximumDescriptorCount &&
         limits.max_retries <= format::kMaximumDescriptorCount;
}

[[nodiscard]] std::expected<ManifestCommitSource, ManifestDecodeError>
parse_source(const common::ByteView descriptor, const std::size_t offset) {
  const std::uint8_t source = std::to_integer<std::uint8_t>(descriptor[offset]);
  if (source == 0U) {
    return std::unexpected(corruption("Manifest v2 commit source zero is invalid"));
  }
  if (source > static_cast<std::uint8_t>(ManifestCommitSource::kRaft)) {
    return std::unexpected(unsupported("Manifest v2 commit source is unsupported"));
  }
  return static_cast<ManifestCommitSource>(source);
}

} // namespace

EncodedTemporalManifest::EncodedTemporalManifest(std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}
common::ByteView EncodedTemporalManifest::bytes() const noexcept {
  return bytes_;
}
std::size_t EncodedTemporalManifest::size() const noexcept {
  return bytes_.size();
}

DecodedTemporalManifestView::DecodedTemporalManifestView(
    const GenerationLineage lineage, const DatabaseId database_id,
    std::optional<TemporalWalReclaimCheckpoint> wal_reclaim_checkpoint,
    std::vector<TemporalTabletDescriptor> tablets, std::vector<TemporalPartDescriptor> parts,
    std::vector<TemporalRetryDescriptor> retries, const common::ByteView encoded_bytes) noexcept
    : generation_(lineage.generation), previous_generation_(lineage.previous_generation),
      database_id_(database_id), wal_reclaim_checkpoint_(wal_reclaim_checkpoint),
      tablets_(std::move(tablets)), parts_(std::move(parts)), retries_(std::move(retries)),
      encoded_bytes_(encoded_bytes) {}
std::span<const TemporalTabletDescriptor> DecodedTemporalManifestView::tablets() const noexcept {
  return tablets_;
}
std::span<const TemporalPartDescriptor> DecodedTemporalManifestView::parts() const noexcept {
  return parts_;
}
std::span<const TemporalRetryDescriptor> DecodedTemporalManifestView::retries() const noexcept {
  return retries_;
}
common::ByteView DecodedTemporalManifestView::encoded_bytes() const noexcept {
  return encoded_bytes_;
}
std::size_t DecodedTemporalManifestView::retained_buffer_bytes() const noexcept {
  const auto retained = [](const auto& values) {
    return common::checked_multiply(values.capacity(),
                                    sizeof(typename std::decay_t<decltype(values)>::value_type))
        .value_or(std::numeric_limits<std::size_t>::max());
  };
  const std::optional<std::size_t> first =
      common::checked_add(retained(tablets_), retained(parts_));
  return first.has_value() ? common::checked_add(*first, retained(retries_))
                                 .value_or(std::numeric_limits<std::size_t>::max())
                           : std::numeric_limits<std::size_t>::max();
}

common::Result<EncodedTemporalManifest>
encode_manifest_v2_temporal(const TemporalManifestEncodeInput& input) {
  if (input.generation == 0U) {
    return common::make_unexpected(invalid("Manifest v2 generation must be nonzero"));
  }
  const common::Status model =
      validate_model(input.wal_reclaim_checkpoint, input.tablets, input.parts, input.retries);
  if (!model.is_ok()) {
    return common::make_unexpected(model);
  }
  const common::Result<ManifestLayout> layout = plan_manifest_v2_temporal_layout(
      {input.tablets.size(), input.parts.size(), input.retries.size()});
  if (!layout.has_value()) {
    return common::make_unexpected(layout.error());
  }
  if (layout->total_length > std::numeric_limits<std::size_t>::max()) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Manifest v2 length does not fit this platform"));
  }
  std::vector<std::byte> storage(static_cast<std::size_t>(layout->total_length), std::byte{0});
  const common::MutableByteView bytes{storage};
  std::ranges::copy(format::kMagic, storage.begin());
  store_u16(bytes, format::kFormatMajorOffset, temporal_format::kFormatMajor);
  store_u16(bytes, format::kFormatMinorOffset, temporal_format::kFormatMinor);
  store_u32(bytes, format::kHeaderLengthOffset, format::kFileHeaderLength);
  if (input.wal_reclaim_checkpoint.has_value()) {
    store_u32(bytes, format::kFileFlagsOffset, temporal_format::kHasWalReclaimCheckpointFlag);
  }
  store_u64(bytes, format::kTotalLengthOffset, layout->total_length);
  store_u64(bytes, format::kGenerationOffset, input.generation);
  store_u64(bytes, format::kPreviousGenerationOffset,
            input.generation == 1U ? 0U : input.generation - 1U);
  store_u64(bytes, format::kTabletCountOffset, input.tablets.size());
  store_u64(bytes, format::kPartCountOffset, input.parts.size());
  store_u64(bytes, format::kRetryCountOffset, input.retries.size());
  copy_id(bytes, format::kDatabaseIdOffset, input.database_id);
  if (input.wal_reclaim_checkpoint.has_value()) {
    copy_bytes(bytes, format::kWalIdOffset, input.wal_reclaim_checkpoint->wal_id.bytes);
    store_u64(bytes, format::kReclaimRecordSequenceOffset,
              input.wal_reclaim_checkpoint->coordinate.record_sequence);
    store_u64(bytes, format::kReclaimSegmentNumberOffset,
              input.wal_reclaim_checkpoint->coordinate.segment_number);
    store_u64(bytes, format::kReclaimByteOffsetOffset,
              input.wal_reclaim_checkpoint->coordinate.byte_offset);
  }
  store_u64(bytes, format::kTabletsOffsetFieldOffset, layout->tablets_offset);
  store_u64(bytes, format::kPartsOffsetFieldOffset, layout->parts_offset);
  store_u64(bytes, format::kRetriesOffsetFieldOffset, layout->retries_offset);
  store_u64(bytes, format::kTrailerOffsetFieldOffset, layout->trailer_offset);

  for (std::size_t index = 0U; index < input.tablets.size(); ++index) {
    const TemporalTabletDescriptor& tablet = input.tablets[index];
    const std::size_t offset = static_cast<std::size_t>(layout->tablets_offset) +
                               index * temporal_format::kTabletDescriptorLength;
    copy_id(bytes, offset + temporal_format::kTabletTableIdOffset, tablet.table_id);
    copy_id(bytes, offset + temporal_format::kTabletIdOffset, tablet.tablet_id);
    copy_id(bytes, offset + temporal_format::kTabletRecoverySchemaIdOffset,
            tablet.recovery_schema_id);
    store_u64(bytes, offset + temporal_format::kTabletRecoverySchemaVersionOffset,
              tablet.recovery_schema_version.value());
    copy_uuid(bytes, offset + temporal_format::kTabletSourceIdOffset, tablet.source_id);
    store_u64(bytes, offset + temporal_format::kTabletDurablePositionOffset,
              tablet.durable_position);
    store_u64(bytes, offset + temporal_format::kTabletReclaimPositionOffset,
              tablet.reclaim_position);
    store_u64(bytes, offset + temporal_format::kTabletFirstPartIndexOffset,
              tablet.first_part_index);
    store_u64(bytes, offset + temporal_format::kTabletPartCountOffset, tablet.part_count);
    store_u64(bytes, offset + temporal_format::kTabletDurableVersionCountOffset,
              tablet.durable_version_count);
    bytes[offset + temporal_format::kTabletCommitSourceOffset] =
        std::byte{static_cast<std::uint8_t>(tablet.commit_source)};
  }
  for (std::size_t index = 0U; index < input.parts.size(); ++index) {
    const TemporalPartDescriptor& part = input.parts[index];
    const std::size_t offset = static_cast<std::size_t>(layout->parts_offset) +
                               index * temporal_format::kPartDescriptorLength;
    copy_id(bytes, offset + temporal_format::kPartIdOffset, part.part_id);
    copy_id(bytes, offset + temporal_format::kPartTableIdOffset, part.table_id);
    copy_id(bytes, offset + temporal_format::kPartTabletIdOffset, part.tablet_id);
    copy_id(bytes, offset + temporal_format::kPartSchemaIdOffset, part.schema_id);
    store_u64(bytes, offset + temporal_format::kPartSchemaVersionOffset,
              part.schema_version.value());
    store_u64(bytes, offset + temporal_format::kPartFileLengthOffset, part.file_length);
    store_u64(bytes, offset + temporal_format::kPartRowCountOffset, part.row_count);
    store_u64(bytes, offset + temporal_format::kPartMinimumCommitPositionOffset,
              part.minimum_commit_position);
    store_u64(bytes, offset + temporal_format::kPartMaximumCommitPositionOffset,
              part.maximum_commit_position);
    store_i64(bytes, offset + temporal_format::kPartMinimumEventTimeOffset,
              part.minimum_event_time);
    store_i64(bytes, offset + temporal_format::kPartMaximumEventTimeOffset,
              part.maximum_event_time);
    store_i64(bytes, offset + temporal_format::kPartMinimumSystemTimeOffset,
              part.minimum_system_time);
    store_i64(bytes, offset + temporal_format::kPartMaximumSystemTimeOffset,
              part.maximum_system_time);
    copy_uuid(bytes, offset + temporal_format::kPartSourceIdOffset, part.source_id);
    copy_bytes(bytes, offset + temporal_format::kPartContentSha256Offset,
               part.content_sha256.bytes());
    store_u16(bytes, offset + temporal_format::kPartCsegFormatMajorOffset, part.cseg_format_major);
    store_u16(bytes, offset + temporal_format::kPartCsegFormatMinorOffset, part.cseg_format_minor);
    bytes[offset + temporal_format::kPartCommitSourceOffset] =
        std::byte{static_cast<std::uint8_t>(part.commit_source)};
  }
  for (std::size_t index = 0U; index < input.retries.size(); ++index) {
    const TemporalRetryDescriptor& retry = input.retries[index];
    const std::size_t offset = static_cast<std::size_t>(layout->retries_offset) +
                               index * temporal_format::kRetryDescriptorLength;
    copy_id(bytes, offset + temporal_format::kRetryClientIdOffset, retry.client_id);
    copy_id(bytes, offset + temporal_format::kRetryClientBatchIdOffset, retry.client_batch_id);
    copy_id(bytes, offset + temporal_format::kRetryTableIdOffset, retry.table_id);
    copy_id(bytes, offset + temporal_format::kRetryTabletIdOffset, retry.tablet_id);
    copy_bytes(bytes, offset + temporal_format::kRetryRequestDigestOffset,
               retry.request_digest.bytes());
    copy_uuid(bytes, offset + temporal_format::kRetrySourceIdOffset, retry.source_id);
    store_u64(bytes, offset + temporal_format::kRetryCommitPositionOffset, retry.commit_position);
    store_u32(bytes, offset + temporal_format::kRetryAppliedRowCountOffset,
              retry.applied_row_count);
    bytes[offset + temporal_format::kRetryCommitSourceOffset] =
        std::byte{static_cast<std::uint8_t>(retry.commit_source)};
  }
  store_u32(bytes, format::kHeaderCrc32cOffset,
            common::crc32c(common::ByteView{storage}.first(format::kHeaderCrc32cOffset)));
  const std::size_t file_crc_offset = storage.size() - format::kFileCrc32cLength;
  store_u32(bytes, file_crc_offset,
            common::crc32c(common::ByteView{storage}.first(file_crc_offset)));
  return EncodedTemporalManifest{std::move(storage)};
}

TemporalManifestDecodeResult decode_manifest_v2_temporal_prefix(const common::ByteView bytes,
                                                                const ManifestDecodeLimits limits) {
  if (!valid_limits(limits)) {
    return std::unexpected(resource_limit("Manifest v2 decode limits are outside format bounds"));
  }
  if (bytes.size() < format::kMagic.size()) {
    return std::unexpected(
        incomplete("Manifest requires the complete magic", format::kMagic.size()));
  }
  if (!std::ranges::equal(format::kMagic, bytes.first(format::kMagic.size()))) {
    return std::unexpected(corruption("Manifest magic mismatch"));
  }
  if (bytes.size() < format::kFileHeaderLength) {
    return std::unexpected(
        incomplete("Manifest requires the complete header", format::kFileHeaderLength));
  }
  const common::ByteView header = bytes.first(format::kFileHeaderLength);
  if (common::crc32c(header.first(format::kHeaderCrc32cOffset)) !=
      load_u32(header, format::kHeaderCrc32cOffset)) {
    return std::unexpected(corruption("Manifest header CRC32C mismatch"));
  }
  const std::uint16_t major = load_u16(header, format::kFormatMajorOffset);
  const std::uint16_t minor = load_u16(header, format::kFormatMinorOffset);
  if (major == 0U) {
    return std::unexpected(corruption("Manifest format major zero is invalid"));
  }
  if (major != temporal_format::kFormatMajor || minor != temporal_format::kFormatMinor) {
    return std::unexpected(unsupported("Manifest format version is unsupported"));
  }
  const std::uint32_t file_flags = load_u32(header, format::kFileFlagsOffset);
  if ((file_flags & ~temporal_format::kHasWalReclaimCheckpointFlag) != 0U) {
    return std::unexpected(unsupported("Manifest v2 required file flags are unsupported"));
  }
  if (load_u32(header, format::kHeaderLengthOffset) != format::kFileHeaderLength ||
      load_u32(header, format::kHeaderReserved0Offset) != 0U ||
      !is_zero(header.subspan(format::kHeaderReserved1Offset,
                              format::kHeaderCrc32cOffset - format::kHeaderReserved1Offset)) ||
      load_u32(header, format::kHeaderReserved2Offset) != 0U) {
    return std::unexpected(corruption("Manifest v2 fixed header or reserved bytes are invalid"));
  }
  if (file_flags == 0U &&
      !is_zero(header.subspan(format::kWalIdOffset,
                              format::kTabletsOffsetFieldOffset - format::kWalIdOffset))) {
    return std::unexpected(corruption("Manifest v2 absent WAL checkpoint fields are nonzero"));
  }

  const std::uint64_t total_length = load_u64(header, format::kTotalLengthOffset);
  const std::uint64_t tablet_count = load_u64(header, format::kTabletCountOffset);
  const std::uint64_t part_count = load_u64(header, format::kPartCountOffset);
  const std::uint64_t retry_count = load_u64(header, format::kRetryCountOffset);
  if (tablet_count > format::kMaximumDescriptorCount ||
      part_count > format::kMaximumDescriptorCount ||
      retry_count > format::kMaximumDescriptorCount) {
    return std::unexpected(corruption("Manifest v2 descriptor count exceeds format bounds"));
  }
  const common::Result<ManifestLayout> layout =
      plan_manifest_v2_temporal_layout({tablet_count, part_count, retry_count});
  if (!layout.has_value() || total_length != layout->total_length ||
      load_u64(header, format::kTabletsOffsetFieldOffset) != layout->tablets_offset ||
      load_u64(header, format::kPartsOffsetFieldOffset) != layout->parts_offset ||
      load_u64(header, format::kRetriesOffsetFieldOffset) != layout->retries_offset ||
      load_u64(header, format::kTrailerOffsetFieldOffset) != layout->trailer_offset) {
    return std::unexpected(corruption("Manifest v2 counts, length, or offsets disagree"));
  }
  if (total_length > limits.max_file_length || tablet_count > limits.max_tablets ||
      part_count > limits.max_parts || retry_count > limits.max_retries) {
    return std::unexpected(resource_limit("Manifest v2 exceeds configured decode limits"));
  }
  if (total_length > std::numeric_limits<std::size_t>::max()) {
    return std::unexpected(resource_limit("Manifest v2 length does not fit this platform"));
  }
  if (bytes.size() < static_cast<std::size_t>(total_length)) {
    return std::unexpected(incomplete("Manifest v2 generation is incomplete", total_length));
  }
  const common::ByteView generation_bytes = bytes.first(static_cast<std::size_t>(total_length));
  const std::size_t file_crc_offset = generation_bytes.size() - format::kFileCrc32cLength;
  if (!is_zero(generation_bytes.subspan(file_crc_offset - format::kTrailerPaddingLength,
                                        format::kTrailerPaddingLength)) ||
      common::crc32c(generation_bytes.first(file_crc_offset)) !=
          load_u32(generation_bytes, file_crc_offset)) {
    return std::unexpected(corruption("Manifest v2 trailer or file CRC32C is invalid"));
  }
  const std::uint64_t generation = load_u64(header, format::kGenerationOffset);
  const std::uint64_t previous = load_u64(header, format::kPreviousGenerationOffset);
  if (generation == 0U || previous != (generation == 1U ? 0U : generation - 1U)) {
    return std::unexpected(corruption("Manifest v2 generation lineage is invalid"));
  }
  const common::Result<DatabaseId> database_id =
      parse_id<DatabaseId>(header, format::kDatabaseIdOffset);
  if (!database_id.has_value()) {
    return std::unexpected(corruption("Manifest v2 database identity is zero"));
  }
  std::optional<TemporalWalReclaimCheckpoint> wal_checkpoint;
  if (file_flags == temporal_format::kHasWalReclaimCheckpointFlag) {
    wal::WalId id{};
    std::copy_n(header.begin() + static_cast<std::ptrdiff_t>(format::kWalIdOffset), id.bytes.size(),
                id.bytes.begin());
    wal_checkpoint = TemporalWalReclaimCheckpoint{
        id,
        {.record_sequence = load_u64(header, format::kReclaimRecordSequenceOffset),
         .segment_number = load_u64(header, format::kReclaimSegmentNumberOffset),
         .byte_offset = load_u64(header, format::kReclaimByteOffsetOffset)}};
    if (!validate_checkpoint(*wal_checkpoint).is_ok()) {
      return std::unexpected(corruption("Manifest v2 WAL reclaim checkpoint is invalid"));
    }
  }

  std::vector<TemporalTabletDescriptor> tablets;
  tablets.reserve(static_cast<std::size_t>(tablet_count));
  for (std::uint64_t index = 0U; index < tablet_count; ++index) {
    const std::size_t offset = static_cast<std::size_t>(
        layout->tablets_offset + index * temporal_format::kTabletDescriptorLength);
    const common::ByteView descriptor =
        generation_bytes.subspan(offset, temporal_format::kTabletDescriptorLength);
    if (load_u32(descriptor, temporal_format::kTabletFlagsOffset) != 0U) {
      return std::unexpected(unsupported("Manifest v2 tablet flags are unsupported"));
    }
    if (!is_zero(descriptor.subspan(temporal_format::kTabletReserved0Offset, 3U)) ||
        !is_zero(descriptor.subspan(temporal_format::kTabletReserved1Offset, 8U))) {
      return std::unexpected(corruption("Manifest v2 tablet reserved bytes are nonzero"));
    }
    const auto table = parse_id<schema::TableId>(descriptor, temporal_format::kTabletTableIdOffset);
    const auto tablet = parse_id<schema::TabletId>(descriptor, temporal_format::kTabletIdOffset);
    const auto schema_id =
        parse_id<schema::SchemaId>(descriptor, temporal_format::kTabletRecoverySchemaIdOffset);
    const auto schema_version = schema::SchemaVersion::from_value(
        load_u64(descriptor, temporal_format::kTabletRecoverySchemaVersionOffset));
    const auto source = parse_source(descriptor, temporal_format::kTabletCommitSourceOffset);
    if (!table.has_value() || !tablet.has_value() || !schema_id.has_value() ||
        !schema_version.has_value()) {
      return std::unexpected(corruption("Manifest v2 tablet identity or schema is zero"));
    }
    if (!source.has_value()) {
      return std::unexpected(source.error());
    }
    tablets.push_back(
        {.table_id = *table,
         .tablet_id = *tablet,
         .recovery_schema_id = *schema_id,
         .recovery_schema_version = *schema_version,
         .source_id = parse_uuid(descriptor, temporal_format::kTabletSourceIdOffset),
         .durable_position = load_u64(descriptor, temporal_format::kTabletDurablePositionOffset),
         .reclaim_position = load_u64(descriptor, temporal_format::kTabletReclaimPositionOffset),
         .first_part_index = load_u64(descriptor, temporal_format::kTabletFirstPartIndexOffset),
         .part_count = load_u64(descriptor, temporal_format::kTabletPartCountOffset),
         .durable_version_count =
             load_u64(descriptor, temporal_format::kTabletDurableVersionCountOffset),
         .commit_source = *source});
  }

  std::vector<TemporalPartDescriptor> parts;
  parts.reserve(static_cast<std::size_t>(part_count));
  for (std::uint64_t index = 0U; index < part_count; ++index) {
    const std::size_t offset = static_cast<std::size_t>(
        layout->parts_offset + index * temporal_format::kPartDescriptorLength);
    const common::ByteView descriptor =
        generation_bytes.subspan(offset, temporal_format::kPartDescriptorLength);
    if (load_u32(descriptor, temporal_format::kPartFlagsOffset) != 0U) {
      return std::unexpected(unsupported("Manifest v2 part flags are unsupported"));
    }
    if (!is_zero(descriptor.subspan(temporal_format::kPartReserved0Offset, 3U)) ||
        !is_zero(descriptor.subspan(temporal_format::kPartReserved1Offset, 28U))) {
      return std::unexpected(corruption("Manifest v2 part reserved bytes are nonzero"));
    }
    const auto part_id = parse_id<cseg::PartId>(descriptor, temporal_format::kPartIdOffset);
    const auto table = parse_id<schema::TableId>(descriptor, temporal_format::kPartTableIdOffset);
    const auto tablet =
        parse_id<schema::TabletId>(descriptor, temporal_format::kPartTabletIdOffset);
    const auto schema_id =
        parse_id<schema::SchemaId>(descriptor, temporal_format::kPartSchemaIdOffset);
    const auto schema_version = schema::SchemaVersion::from_value(
        load_u64(descriptor, temporal_format::kPartSchemaVersionOffset));
    const auto source = parse_source(descriptor, temporal_format::kPartCommitSourceOffset);
    const std::uint16_t cseg_major =
        load_u16(descriptor, temporal_format::kPartCsegFormatMajorOffset);
    const std::uint16_t cseg_minor =
        load_u16(descriptor, temporal_format::kPartCsegFormatMinorOffset);
    if (cseg_major == 0U) {
      return std::unexpected(corruption("Manifest v2 part CSEG major zero is invalid"));
    }
    if (cseg_major != cseg::temporal_format::kFormatMajor ||
        cseg_minor != cseg::temporal_format::kFormatMinor) {
      return std::unexpected(unsupported("Manifest v2 part CSEG version is unsupported"));
    }
    if (!part_id.has_value() || !table.has_value() || !tablet.has_value() ||
        !schema_id.has_value() || !schema_version.has_value()) {
      return std::unexpected(corruption("Manifest v2 part identity or schema is zero"));
    }
    if (!source.has_value()) {
      return std::unexpected(source.error());
    }
    ingest::Sha256Digest::Bytes digest{};
    std::copy_n(descriptor.begin() +
                    static_cast<std::ptrdiff_t>(temporal_format::kPartContentSha256Offset),
                digest.size(), digest.begin());
    parts.push_back(
        {.part_id = *part_id,
         .table_id = *table,
         .tablet_id = *tablet,
         .schema_id = *schema_id,
         .schema_version = *schema_version,
         .file_length = load_u64(descriptor, temporal_format::kPartFileLengthOffset),
         .row_count = load_u64(descriptor, temporal_format::kPartRowCountOffset),
         .minimum_commit_position =
             load_u64(descriptor, temporal_format::kPartMinimumCommitPositionOffset),
         .maximum_commit_position =
             load_u64(descriptor, temporal_format::kPartMaximumCommitPositionOffset),
         .minimum_event_time = load_i64(descriptor, temporal_format::kPartMinimumEventTimeOffset),
         .maximum_event_time = load_i64(descriptor, temporal_format::kPartMaximumEventTimeOffset),
         .minimum_system_time = load_i64(descriptor, temporal_format::kPartMinimumSystemTimeOffset),
         .maximum_system_time = load_i64(descriptor, temporal_format::kPartMaximumSystemTimeOffset),
         .source_id = parse_uuid(descriptor, temporal_format::kPartSourceIdOffset),
         .content_sha256 = ingest::Sha256Digest{digest},
         .cseg_format_major = cseg_major,
         .cseg_format_minor = cseg_minor,
         .commit_source = *source});
  }

  std::vector<TemporalRetryDescriptor> retries;
  retries.reserve(static_cast<std::size_t>(retry_count));
  for (std::uint64_t index = 0U; index < retry_count; ++index) {
    const std::size_t offset = static_cast<std::size_t>(
        layout->retries_offset + index * temporal_format::kRetryDescriptorLength);
    const common::ByteView descriptor =
        generation_bytes.subspan(offset, temporal_format::kRetryDescriptorLength);
    if (load_u32(descriptor, temporal_format::kRetryFlagsOffset) != 0U) {
      return std::unexpected(unsupported("Manifest v2 retry flags are unsupported"));
    }
    if (!is_zero(descriptor.subspan(temporal_format::kRetryReserved0Offset, 3U)) ||
        !is_zero(descriptor.subspan(temporal_format::kRetryReserved1Offset, 12U))) {
      return std::unexpected(corruption("Manifest v2 retry reserved bytes are nonzero"));
    }
    const auto client =
        parse_id<ingest::ClientId>(descriptor, temporal_format::kRetryClientIdOffset);
    const auto batch =
        parse_id<ingest::ClientBatchId>(descriptor, temporal_format::kRetryClientBatchIdOffset);
    const auto table = parse_id<schema::TableId>(descriptor, temporal_format::kRetryTableIdOffset);
    const auto tablet =
        parse_id<schema::TabletId>(descriptor, temporal_format::kRetryTabletIdOffset);
    const auto source = parse_source(descriptor, temporal_format::kRetryCommitSourceOffset);
    if (!client.has_value() || !batch.has_value() || !table.has_value() || !tablet.has_value()) {
      return std::unexpected(corruption("Manifest v2 retry identity is zero"));
    }
    if (!source.has_value()) {
      return std::unexpected(source.error());
    }
    ingest::Sha256Digest::Bytes digest{};
    std::copy_n(descriptor.begin() +
                    static_cast<std::ptrdiff_t>(temporal_format::kRetryRequestDigestOffset),
                digest.size(), digest.begin());
    retries.push_back(
        {.client_id = *client,
         .client_batch_id = *batch,
         .table_id = *table,
         .tablet_id = *tablet,
         .request_digest = ingest::Sha256Digest{digest},
         .source_id = parse_uuid(descriptor, temporal_format::kRetrySourceIdOffset),
         .commit_position = load_u64(descriptor, temporal_format::kRetryCommitPositionOffset),
         .applied_row_count = load_u32(descriptor, temporal_format::kRetryAppliedRowCountOffset),
         .commit_source = *source});
  }
  const common::Status model = validate_model(wal_checkpoint, tablets, parts, retries);
  if (!model.is_ok()) {
    return std::unexpected(corruption(model.message()));
  }
  return DecodedTemporalManifestView{DecodedTemporalManifestView::GenerationLineage{
                                         .generation = generation, .previous_generation = previous},
                                     *database_id,
                                     wal_checkpoint,
                                     std::move(tablets),
                                     std::move(parts),
                                     std::move(retries),
                                     generation_bytes};
}

TemporalManifestDecodeResult decode_manifest_v2_temporal_exact(const common::ByteView bytes,
                                                               const ManifestDecodeLimits limits) {
  TemporalManifestDecodeResult decoded = decode_manifest_v2_temporal_prefix(bytes, limits);
  if (!decoded.has_value()) {
    return decoded;
  }
  if (bytes.size() != decoded->encoded_bytes().size()) {
    return std::unexpected(corruption("Manifest v2 exact decoder rejects trailing bytes"));
  }
  return decoded;
}

} // namespace chronos::manifest
