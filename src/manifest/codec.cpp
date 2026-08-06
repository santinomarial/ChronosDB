#include "chronos/manifest/codec.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/manifest/layout.hpp"

#include <algorithm>
#include <array>
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
                                             const std::uint64_t required_size) {
  return {ManifestDecodeErrorKind::kIncomplete, status(common::StatusCode::kOutOfRange, message),
          required_size};
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

[[nodiscard]] std::uint16_t load_u16_le(const common::ByteView bytes,
                                        const std::size_t offset) noexcept {
  return static_cast<std::uint16_t>(std::to_integer<std::uint16_t>(bytes[offset]) |
                                    (std::to_integer<std::uint16_t>(bytes[offset + 1U]) << 8U));
}

[[nodiscard]] std::uint32_t load_u32_le(const common::ByteView bytes,
                                        const std::size_t offset) noexcept {
  std::uint32_t value = 0U;
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= std::to_integer<std::uint32_t>(bytes[offset + index]) << (index * 8U);
  }
  return value;
}

[[nodiscard]] std::uint64_t load_u64_le(const common::ByteView bytes,
                                        const std::size_t offset) noexcept {
  std::uint64_t value = 0U;
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= std::to_integer<std::uint64_t>(bytes[offset + index]) << (index * 8U);
  }
  return value;
}

[[nodiscard]] std::int64_t load_i64_le(const common::ByteView bytes,
                                       const std::size_t offset) noexcept {
  return std::bit_cast<std::int64_t>(load_u64_le(bytes, offset));
}

void store_u16_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint16_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
  }
}

void store_u32_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint32_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
  }
}

void store_u64_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint64_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
  }
}

void store_i64_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::int64_t value) noexcept {
  store_u64_le(bytes, offset, std::bit_cast<std::uint64_t>(value));
}

[[nodiscard]] bool is_zero(const common::ByteView bytes) noexcept {
  return std::ranges::all_of(bytes, [](const std::byte value) { return value == std::byte{0}; });
}

[[nodiscard]] bool valid_wal_id(const wal::WalId& wal_id) noexcept {
  return std::ranges::any_of(wal_id.bytes,
                             [](const std::byte value) { return value != std::byte{0}; });
}

template <typename Identifier>
[[nodiscard]] common::Result<Identifier> parse_identifier(const common::ByteView bytes,
                                                          const std::size_t offset) {
  common::Uuid::Bytes encoded{};
  std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(offset), encoded.size(), encoded.begin());
  return Identifier::from_bytes(encoded);
}

void copy_identifier(const common::MutableByteView output, const std::size_t offset,
                     const auto& identifier) {
  std::copy(identifier.bytes().begin(), identifier.bytes().end(),
            output.begin() + static_cast<std::ptrdiff_t>(offset));
}

[[nodiscard]] bool retry_less(const RetryDescriptor& left, const RetryDescriptor& right) {
  if (left.client_id != right.client_id) {
    return left.client_id < right.client_id;
  }
  return left.client_batch_id < right.client_batch_id;
}

[[nodiscard]] common::Status validate_checkpoint(const WalCheckpoint& checkpoint) {
  if (checkpoint.record_sequence == 0U) {
    if (checkpoint.segment_number != wal::kFirstSegmentNumber ||
        checkpoint.byte_offset != wal::kSegmentHeaderSize) {
      return invalid("Manifest empty reclaim checkpoint is not segment 1 offset 64");
    }
    return common::Status::ok();
  }
  if (checkpoint.segment_number == 0U || checkpoint.byte_offset < wal::kSegmentHeaderSize ||
      checkpoint.byte_offset > wal::kSegmentSizeLimit ||
      (checkpoint.byte_offset % format::kAlignment) != 0U) {
    return invalid("Manifest nonempty reclaim checkpoint is outside WAL v1 boundaries");
  }
  return common::Status::ok();
}

[[nodiscard]] const TabletDescriptor* find_tablet(const std::span<const TabletDescriptor> tablets,
                                                  const schema::TabletId& tablet_id) noexcept {
  const auto found = std::ranges::lower_bound(
      tablets, tablet_id, {}, [](const TabletDescriptor& tablet) { return tablet.tablet_id; });
  return found != tablets.end() && found->tablet_id == tablet_id ? &*found : nullptr;
}

[[nodiscard]] common::Status validate_model(const wal::WalId& wal_id,
                                            const WalCheckpoint& checkpoint,
                                            const std::span<const TabletDescriptor> tablets,
                                            const std::span<const PartDescriptor> parts,
                                            const std::span<const RetryDescriptor> retries) {
  if (!valid_wal_id(wal_id)) {
    return invalid("Manifest WAL identity must be nonzero");
  }
  common::Status checkpoint_status = validate_checkpoint(checkpoint);
  if (!checkpoint_status.is_ok()) {
    return checkpoint_status;
  }
  if (tablets.empty() && (!parts.empty() || !retries.empty() || checkpoint.record_sequence != 0U)) {
    return invalid("Manifest without tablets cannot contain durable state or WAL coverage");
  }

  std::uint64_t next_part_index = 0U;
  std::uint64_t maximum_tablet_boundary = 0U;
  std::vector<cseg::PartId> unique_part_ids;
  unique_part_ids.reserve(parts.size());
  for (std::size_t tablet_index = 0U; tablet_index < tablets.size(); ++tablet_index) {
    const TabletDescriptor& tablet = tablets[tablet_index];
    if (tablet_index != 0U && !(tablets[tablet_index - 1U].tablet_id < tablet.tablet_id)) {
      return invalid("Manifest tablet descriptors are not strictly sorted by tablet identity");
    }
    if (tablet.first_part_index != next_part_index || tablet.part_count > parts.size() ||
        tablet.first_part_index > parts.size() - tablet.part_count) {
      return invalid("Manifest tablet part range is not canonical or is out of bounds");
    }

    std::uint64_t observed_rows = 0U;
    for (std::uint64_t local_index = 0U; local_index < tablet.part_count; ++local_index) {
      const std::size_t part_index =
          static_cast<std::size_t>(tablet.first_part_index + local_index);
      const PartDescriptor& part = parts[part_index];
      if (part.table_id != tablet.table_id || part.tablet_id != tablet.tablet_id) {
        return invalid("Manifest part table or tablet identity disagrees with its owner");
      }
      if (local_index != 0U && !(parts[part_index - 1U].part_id < part.part_id)) {
        return invalid("Manifest parts are not strictly sorted within their tablet range");
      }
      if (part.file_length == 0U || part.file_length > cseg::format::kMaximumFileLength ||
          (part.file_length % cseg::format::kAlignment) != 0U || part.row_count == 0U ||
          part.row_count > cseg::format::kMaximumRowCount || part.minimum_record_sequence == 0U ||
          part.minimum_record_sequence > part.maximum_record_sequence ||
          part.maximum_record_sequence > tablet.durable_record_sequence ||
          part.minimum_event_time > part.maximum_event_time) {
        return invalid("Manifest part descriptor contains invalid lengths or extrema");
      }
      const std::optional<std::uint64_t> new_rows =
          common::checked_add(observed_rows, part.row_count);
      if (!new_rows.has_value()) {
        return invalid("Manifest tablet durable row count overflowed");
      }
      observed_rows = *new_rows;
      unique_part_ids.push_back(part.part_id);
    }
    if (observed_rows != tablet.durable_row_count) {
      return invalid("Manifest tablet durable row count does not equal its part rows");
    }
    const std::optional<std::uint64_t> range_end =
        common::checked_add(tablet.first_part_index, tablet.part_count);
    if (!range_end.has_value()) {
      return invalid("Manifest tablet part range overflowed");
    }
    next_part_index = *range_end;
    maximum_tablet_boundary = std::max(maximum_tablet_boundary, tablet.durable_record_sequence);
  }
  if (next_part_index != parts.size()) {
    return invalid("Manifest tablet part ranges do not cover the global part array exactly");
  }
  if (checkpoint.record_sequence > maximum_tablet_boundary) {
    return invalid("Manifest reclaim checkpoint exceeds every tablet durable boundary");
  }
  std::ranges::sort(unique_part_ids);
  if (std::ranges::adjacent_find(unique_part_ids) != unique_part_ids.end()) {
    return invalid("Manifest contains a duplicate part identity");
  }

  for (std::size_t index = 0U; index < retries.size(); ++index) {
    const RetryDescriptor& retry = retries[index];
    if (index != 0U && !retry_less(retries[index - 1U], retry)) {
      return invalid("Manifest retry descriptors are not strictly sorted by retry identity");
    }
    const TabletDescriptor* tablet = find_tablet(tablets, retry.tablet_id);
    if (tablet == nullptr || retry.table_id != tablet->table_id || retry.wal_id != wal_id ||
        retry.record_sequence == 0U || retry.record_sequence > tablet->durable_record_sequence ||
        retry.applied_row_count == 0U) {
      return invalid("Manifest retry descriptor does not bind to its tablet, WAL, or outcome");
    }
  }
  return common::Status::ok();
}

[[nodiscard]] bool valid_limits(const ManifestDecodeLimits& limits) noexcept {
  return limits.max_file_length >= format::kFileHeaderLength + format::kTrailerLength &&
         limits.max_file_length <= format::kMaximumFileLength &&
         limits.max_tablets <= format::kMaximumDescriptorCount &&
         limits.max_parts <= format::kMaximumDescriptorCount &&
         limits.max_retries <= format::kMaximumDescriptorCount;
}

} // namespace

EncodedManifest::EncodedManifest(std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedManifest::bytes() const noexcept {
  return bytes_;
}

std::size_t EncodedManifest::size() const noexcept {
  return bytes_.size();
}

ManifestDecodeError::ManifestDecodeError(const ManifestDecodeErrorKind kind,
                                         common::Status status_value,
                                         const std::uint64_t required_size) noexcept
    : kind_(kind), status_(std::move(status_value)), required_size_(required_size) {}

DecodedManifestView::DecodedManifestView(
    // Current and previous generations are one inseparable durable lineage tuple.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    const std::uint64_t generation, const std::uint64_t previous_generation, DatabaseId database_id,
    wal::WalId wal_id, const WalCheckpoint reclaim_checkpoint,
    std::vector<TabletDescriptor> tablets, std::vector<PartDescriptor> parts,
    std::vector<RetryDescriptor> retries, const common::ByteView encoded_bytes) noexcept
    : generation_(generation), previous_generation_(previous_generation), database_id_(database_id),
      wal_id_(wal_id), reclaim_checkpoint_(reclaim_checkpoint), tablets_(std::move(tablets)),
      parts_(std::move(parts)), retries_(std::move(retries)), encoded_bytes_(encoded_bytes) {}

std::span<const TabletDescriptor> DecodedManifestView::tablets() const noexcept {
  return tablets_;
}

std::span<const PartDescriptor> DecodedManifestView::parts() const noexcept {
  return parts_;
}

std::span<const RetryDescriptor> DecodedManifestView::retries() const noexcept {
  return retries_;
}

common::ByteView DecodedManifestView::encoded_bytes() const noexcept {
  return encoded_bytes_;
}

common::Result<EncodedManifest> encode_manifest_v1(const ManifestEncodeInput& input) {
  if (input.generation == 0U) {
    return common::make_unexpected(invalid("Manifest generation must be nonzero"));
  }
  const common::Status model_status = validate_model(input.wal_id, input.reclaim_checkpoint,
                                                     input.tablets, input.parts, input.retries);
  if (!model_status.is_ok()) {
    return common::make_unexpected(model_status);
  }
  const common::Result<ManifestLayout> layout =
      plan_manifest_v1_layout({.tablet_count = input.tablets.size(),
                               .part_count = input.parts.size(),
                               .retry_count = input.retries.size()});
  if (!layout.has_value()) {
    return common::make_unexpected(layout.error());
  }
  if (layout->total_length > std::numeric_limits<std::size_t>::max()) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Manifest length does not fit this platform"));
  }

  std::vector<std::byte> storage(static_cast<std::size_t>(layout->total_length), std::byte{0});
  const common::MutableByteView bytes{storage};
  std::copy(format::kMagic.begin(), format::kMagic.end(), storage.begin());
  store_u16_le(bytes, format::kFormatMajorOffset, format::kFormatMajor);
  store_u16_le(bytes, format::kFormatMinorOffset, format::kFormatMinor);
  store_u32_le(bytes, format::kHeaderLengthOffset, format::kFileHeaderLength);
  store_u64_le(bytes, format::kTotalLengthOffset, layout->total_length);
  store_u64_le(bytes, format::kGenerationOffset, input.generation);
  store_u64_le(bytes, format::kPreviousGenerationOffset,
               input.generation == 1U ? 0U : input.generation - 1U);
  store_u64_le(bytes, format::kTabletCountOffset, input.tablets.size());
  store_u64_le(bytes, format::kPartCountOffset, input.parts.size());
  store_u64_le(bytes, format::kRetryCountOffset, input.retries.size());
  copy_identifier(bytes, format::kDatabaseIdOffset, input.database_id);
  std::copy(input.wal_id.bytes.begin(), input.wal_id.bytes.end(),
            storage.begin() + static_cast<std::ptrdiff_t>(format::kWalIdOffset));
  store_u64_le(bytes, format::kReclaimRecordSequenceOffset,
               input.reclaim_checkpoint.record_sequence);
  store_u64_le(bytes, format::kReclaimSegmentNumberOffset, input.reclaim_checkpoint.segment_number);
  store_u64_le(bytes, format::kReclaimByteOffsetOffset, input.reclaim_checkpoint.byte_offset);
  store_u64_le(bytes, format::kTabletsOffsetFieldOffset, layout->tablets_offset);
  store_u64_le(bytes, format::kPartsOffsetFieldOffset, layout->parts_offset);
  store_u64_le(bytes, format::kRetriesOffsetFieldOffset, layout->retries_offset);
  store_u64_le(bytes, format::kTrailerOffsetFieldOffset, layout->trailer_offset);

  for (std::size_t index = 0U; index < input.tablets.size(); ++index) {
    const TabletDescriptor& tablet = input.tablets[index];
    const std::size_t offset =
        static_cast<std::size_t>(layout->tablets_offset) + index * format::kTabletDescriptorLength;
    copy_identifier(bytes, offset + format::kTabletTableIdOffset, tablet.table_id);
    copy_identifier(bytes, offset + format::kTabletIdOffset, tablet.tablet_id);
    copy_identifier(bytes, offset + format::kTabletRecoverySchemaIdOffset,
                    tablet.recovery_schema_id);
    store_u64_le(bytes, offset + format::kTabletRecoverySchemaVersionOffset,
                 tablet.recovery_schema_version.value());
    store_u64_le(bytes, offset + format::kTabletDurableRecordSequenceOffset,
                 tablet.durable_record_sequence);
    store_u64_le(bytes, offset + format::kTabletFirstPartIndexOffset, tablet.first_part_index);
    store_u64_le(bytes, offset + format::kTabletPartCountOffset, tablet.part_count);
    store_u64_le(bytes, offset + format::kTabletDurableRowCountOffset, tablet.durable_row_count);
  }

  for (std::size_t index = 0U; index < input.parts.size(); ++index) {
    const PartDescriptor& part = input.parts[index];
    const std::size_t offset =
        static_cast<std::size_t>(layout->parts_offset) + index * format::kPartDescriptorLength;
    copy_identifier(bytes, offset + format::kPartIdOffset, part.part_id);
    copy_identifier(bytes, offset + format::kPartTableIdOffset, part.table_id);
    copy_identifier(bytes, offset + format::kPartTabletIdOffset, part.tablet_id);
    copy_identifier(bytes, offset + format::kPartSchemaIdOffset, part.schema_id);
    store_u64_le(bytes, offset + format::kPartSchemaVersionOffset, part.schema_version.value());
    store_u64_le(bytes, offset + format::kPartFileLengthOffset, part.file_length);
    store_u64_le(bytes, offset + format::kPartRowCountOffset, part.row_count);
    store_u64_le(bytes, offset + format::kPartMinimumRecordSequenceOffset,
                 part.minimum_record_sequence);
    store_u64_le(bytes, offset + format::kPartMaximumRecordSequenceOffset,
                 part.maximum_record_sequence);
    store_i64_le(bytes, offset + format::kPartMinimumEventTimeOffset, part.minimum_event_time);
    store_i64_le(bytes, offset + format::kPartMaximumEventTimeOffset, part.maximum_event_time);
  }

  for (std::size_t index = 0U; index < input.retries.size(); ++index) {
    const RetryDescriptor& retry = input.retries[index];
    const std::size_t offset =
        static_cast<std::size_t>(layout->retries_offset) + index * format::kRetryDescriptorLength;
    copy_identifier(bytes, offset + format::kRetryClientIdOffset, retry.client_id);
    copy_identifier(bytes, offset + format::kRetryClientBatchIdOffset, retry.client_batch_id);
    copy_identifier(bytes, offset + format::kRetryTableIdOffset, retry.table_id);
    copy_identifier(bytes, offset + format::kRetryTabletIdOffset, retry.tablet_id);
    std::copy(retry.request_digest.bytes().begin(), retry.request_digest.bytes().end(),
              storage.begin() +
                  static_cast<std::ptrdiff_t>(offset + format::kRetryRequestDigestOffset));
    std::copy(retry.wal_id.bytes.begin(), retry.wal_id.bytes.end(),
              storage.begin() + static_cast<std::ptrdiff_t>(offset + format::kRetryWalIdOffset));
    store_u64_le(bytes, offset + format::kRetryRecordSequenceOffset, retry.record_sequence);
    store_u32_le(bytes, offset + format::kRetryAppliedRowCountOffset, retry.applied_row_count);
  }

  store_u32_le(bytes, format::kHeaderCrc32cOffset,
               common::crc32c(common::ByteView{storage}.first(format::kHeaderCrc32cOffset)));
  const std::size_t file_crc_offset = storage.size() - format::kFileCrc32cLength;
  store_u32_le(bytes, file_crc_offset,
               common::crc32c(common::ByteView{storage}.first(file_crc_offset)));
  return EncodedManifest{std::move(storage)};
}

ManifestDecodeResult decode_manifest_v1_prefix(const common::ByteView bytes,
                                               const ManifestDecodeLimits limits) {
  if (!valid_limits(limits)) {
    return std::unexpected(resource_limit("Manifest decode limits are outside v1 bounds"));
  }
  if (bytes.size() < format::kMagic.size()) {
    return std::unexpected(
        incomplete("Manifest requires the complete magic", format::kMagic.size()));
  }
  if (!std::equal(format::kMagic.begin(), format::kMagic.end(), bytes.begin())) {
    return std::unexpected(corruption("Manifest magic mismatch"));
  }
  if (bytes.size() < format::kFileHeaderLength) {
    return std::unexpected(
        incomplete("Manifest requires the complete 256-byte header", format::kFileHeaderLength));
  }
  const common::ByteView header = bytes.first(format::kFileHeaderLength);
  if (common::crc32c(header.first(format::kHeaderCrc32cOffset)) !=
      load_u32_le(header, format::kHeaderCrc32cOffset)) {
    return std::unexpected(corruption("Manifest header CRC32C mismatch"));
  }
  const std::uint16_t major = load_u16_le(header, format::kFormatMajorOffset);
  const std::uint16_t minor = load_u16_le(header, format::kFormatMinorOffset);
  if (major == 0U) {
    return std::unexpected(corruption("Manifest format major version zero is invalid"));
  }
  if (major != format::kFormatMajor || minor != format::kFormatMinor) {
    return std::unexpected(unsupported("Manifest format version is unsupported"));
  }
  if (load_u32_le(header, format::kFileFlagsOffset) != 0U) {
    return std::unexpected(unsupported("Manifest required file flags are unsupported"));
  }
  if (load_u32_le(header, format::kHeaderLengthOffset) != format::kFileHeaderLength ||
      load_u32_le(header, format::kHeaderReserved0Offset) != 0U ||
      !is_zero(header.subspan(format::kHeaderReserved1Offset,
                              format::kHeaderCrc32cOffset - format::kHeaderReserved1Offset)) ||
      load_u32_le(header, format::kHeaderReserved2Offset) != 0U) {
    return std::unexpected(
        corruption("Manifest fixed header layout or reserved bytes are invalid"));
  }

  const std::uint64_t total_length = load_u64_le(header, format::kTotalLengthOffset);
  const std::uint64_t tablet_count = load_u64_le(header, format::kTabletCountOffset);
  const std::uint64_t part_count = load_u64_le(header, format::kPartCountOffset);
  const std::uint64_t retry_count = load_u64_le(header, format::kRetryCountOffset);
  if (tablet_count > format::kMaximumDescriptorCount ||
      part_count > format::kMaximumDescriptorCount ||
      retry_count > format::kMaximumDescriptorCount) {
    return std::unexpected(corruption("Manifest descriptor count exceeds the v1 registry limit"));
  }
  const common::Result<ManifestLayout> layout =
      plan_manifest_v1_layout({tablet_count, part_count, retry_count});
  if (!layout.has_value() || total_length != layout->total_length ||
      load_u64_le(header, format::kTabletsOffsetFieldOffset) != layout->tablets_offset ||
      load_u64_le(header, format::kPartsOffsetFieldOffset) != layout->parts_offset ||
      load_u64_le(header, format::kRetriesOffsetFieldOffset) != layout->retries_offset ||
      load_u64_le(header, format::kTrailerOffsetFieldOffset) != layout->trailer_offset) {
    return std::unexpected(corruption("Manifest counts, length, and canonical offsets disagree"));
  }
  if (total_length > limits.max_file_length || tablet_count > limits.max_tablets ||
      part_count > limits.max_parts || retry_count > limits.max_retries) {
    return std::unexpected(resource_limit("Manifest exceeds configured decode limits"));
  }
  if (total_length > std::numeric_limits<std::size_t>::max()) {
    return std::unexpected(resource_limit("Manifest length does not fit this platform"));
  }
  if (bytes.size() < static_cast<std::size_t>(total_length)) {
    return std::unexpected(incomplete("Manifest generation is incomplete", total_length));
  }
  const common::ByteView generation_bytes = bytes.first(static_cast<std::size_t>(total_length));
  const std::size_t file_crc_offset = generation_bytes.size() - format::kFileCrc32cLength;
  if (!is_zero(generation_bytes.subspan(file_crc_offset - format::kTrailerPaddingLength,
                                        format::kTrailerPaddingLength)) ||
      common::crc32c(generation_bytes.first(file_crc_offset)) !=
          load_u32_le(generation_bytes, file_crc_offset)) {
    return std::unexpected(corruption("Manifest trailer padding or file CRC32C is invalid"));
  }

  const std::uint64_t generation = load_u64_le(header, format::kGenerationOffset);
  const std::uint64_t previous_generation = load_u64_le(header, format::kPreviousGenerationOffset);
  if (generation == 0U || previous_generation != (generation == 1U ? 0U : generation - 1U)) {
    return std::unexpected(corruption("Manifest generation lineage fields are invalid"));
  }
  const common::Result<DatabaseId> database_id =
      parse_identifier<DatabaseId>(header, format::kDatabaseIdOffset);
  wal::WalId wal_id{};
  std::copy_n(header.begin() + static_cast<std::ptrdiff_t>(format::kWalIdOffset),
              wal_id.bytes.size(), wal_id.bytes.begin());
  if (!database_id.has_value() || !valid_wal_id(wal_id)) {
    return std::unexpected(corruption("Manifest database or WAL identity is zero"));
  }
  const WalCheckpoint checkpoint{
      .record_sequence = load_u64_le(header, format::kReclaimRecordSequenceOffset),
      .segment_number = load_u64_le(header, format::kReclaimSegmentNumberOffset),
      .byte_offset = load_u64_le(header, format::kReclaimByteOffsetOffset),
  };

  std::vector<TabletDescriptor> tablets;
  tablets.reserve(static_cast<std::size_t>(tablet_count));
  for (std::uint64_t index = 0U; index < tablet_count; ++index) {
    const std::size_t offset =
        static_cast<std::size_t>(layout->tablets_offset + index * format::kTabletDescriptorLength);
    const common::ByteView descriptor =
        generation_bytes.subspan(offset, format::kTabletDescriptorLength);
    if (load_u32_le(descriptor, format::kTabletFlagsOffset) != 0U) {
      return std::unexpected(unsupported("Manifest tablet required flags are unsupported"));
    }
    if (load_u32_le(descriptor, format::kTabletReservedOffset) != 0U) {
      return std::unexpected(corruption("Manifest tablet reserved bytes are nonzero"));
    }
    const auto table_id =
        parse_identifier<schema::TableId>(descriptor, format::kTabletTableIdOffset);
    const auto tablet_id = parse_identifier<schema::TabletId>(descriptor, format::kTabletIdOffset);
    const auto schema_id =
        parse_identifier<schema::SchemaId>(descriptor, format::kTabletRecoverySchemaIdOffset);
    const auto schema_version = schema::SchemaVersion::from_value(
        load_u64_le(descriptor, format::kTabletRecoverySchemaVersionOffset));
    if (!table_id.has_value() || !tablet_id.has_value() || !schema_id.has_value() ||
        !schema_version.has_value()) {
      return std::unexpected(
          corruption("Manifest tablet contains a zero identity or schema version"));
    }
    tablets.push_back(TabletDescriptor{
        .table_id = *table_id,
        .tablet_id = *tablet_id,
        .recovery_schema_id = *schema_id,
        .recovery_schema_version = *schema_version,
        .durable_record_sequence =
            load_u64_le(descriptor, format::kTabletDurableRecordSequenceOffset),
        .first_part_index = load_u64_le(descriptor, format::kTabletFirstPartIndexOffset),
        .part_count = load_u64_le(descriptor, format::kTabletPartCountOffset),
        .durable_row_count = load_u64_le(descriptor, format::kTabletDurableRowCountOffset),
    });
  }

  std::vector<PartDescriptor> parts;
  parts.reserve(static_cast<std::size_t>(part_count));
  for (std::uint64_t index = 0U; index < part_count; ++index) {
    const std::size_t offset =
        static_cast<std::size_t>(layout->parts_offset + index * format::kPartDescriptorLength);
    const common::ByteView descriptor =
        generation_bytes.subspan(offset, format::kPartDescriptorLength);
    if (load_u32_le(descriptor, format::kPartFlagsOffset) != 0U) {
      return std::unexpected(unsupported("Manifest part required flags are unsupported"));
    }
    if (load_u32_le(descriptor, format::kPartReservedOffset) != 0U) {
      return std::unexpected(corruption("Manifest part reserved bytes are nonzero"));
    }
    const auto part_id = parse_identifier<cseg::PartId>(descriptor, format::kPartIdOffset);
    const auto table_id = parse_identifier<schema::TableId>(descriptor, format::kPartTableIdOffset);
    const auto tablet_id =
        parse_identifier<schema::TabletId>(descriptor, format::kPartTabletIdOffset);
    const auto schema_id =
        parse_identifier<schema::SchemaId>(descriptor, format::kPartSchemaIdOffset);
    const auto schema_version = schema::SchemaVersion::from_value(
        load_u64_le(descriptor, format::kPartSchemaVersionOffset));
    if (!part_id.has_value() || !table_id.has_value() || !tablet_id.has_value() ||
        !schema_id.has_value() || !schema_version.has_value()) {
      return std::unexpected(
          corruption("Manifest part contains a zero identity or schema version"));
    }
    parts.push_back(PartDescriptor{
        .part_id = *part_id,
        .table_id = *table_id,
        .tablet_id = *tablet_id,
        .schema_id = *schema_id,
        .schema_version = *schema_version,
        .file_length = load_u64_le(descriptor, format::kPartFileLengthOffset),
        .row_count = load_u64_le(descriptor, format::kPartRowCountOffset),
        .minimum_record_sequence =
            load_u64_le(descriptor, format::kPartMinimumRecordSequenceOffset),
        .maximum_record_sequence =
            load_u64_le(descriptor, format::kPartMaximumRecordSequenceOffset),
        .minimum_event_time = load_i64_le(descriptor, format::kPartMinimumEventTimeOffset),
        .maximum_event_time = load_i64_le(descriptor, format::kPartMaximumEventTimeOffset),
    });
  }

  std::vector<RetryDescriptor> retries;
  retries.reserve(static_cast<std::size_t>(retry_count));
  for (std::uint64_t index = 0U; index < retry_count; ++index) {
    const std::size_t offset =
        static_cast<std::size_t>(layout->retries_offset + index * format::kRetryDescriptorLength);
    const common::ByteView descriptor =
        generation_bytes.subspan(offset, format::kRetryDescriptorLength);
    if (load_u32_le(descriptor, format::kRetryFlagsOffset) != 0U) {
      return std::unexpected(unsupported("Manifest retry required flags are unsupported"));
    }
    const auto client_id =
        parse_identifier<ingest::ClientId>(descriptor, format::kRetryClientIdOffset);
    const auto client_batch_id =
        parse_identifier<ingest::ClientBatchId>(descriptor, format::kRetryClientBatchIdOffset);
    const auto table_id =
        parse_identifier<schema::TableId>(descriptor, format::kRetryTableIdOffset);
    const auto tablet_id =
        parse_identifier<schema::TabletId>(descriptor, format::kRetryTabletIdOffset);
    if (!client_id.has_value() || !client_batch_id.has_value() || !table_id.has_value() ||
        !tablet_id.has_value()) {
      return std::unexpected(corruption("Manifest retry contains a zero identity"));
    }
    ingest::Sha256Digest::Bytes digest_bytes{};
    std::copy_n(descriptor.begin() + static_cast<std::ptrdiff_t>(format::kRetryRequestDigestOffset),
                digest_bytes.size(), digest_bytes.begin());
    wal::WalId retry_wal_id{};
    std::copy_n(descriptor.begin() + static_cast<std::ptrdiff_t>(format::kRetryWalIdOffset),
                retry_wal_id.bytes.size(), retry_wal_id.bytes.begin());
    retries.push_back(RetryDescriptor{
        .client_id = *client_id,
        .client_batch_id = *client_batch_id,
        .table_id = *table_id,
        .tablet_id = *tablet_id,
        .request_digest = ingest::Sha256Digest{digest_bytes},
        .wal_id = retry_wal_id,
        .record_sequence = load_u64_le(descriptor, format::kRetryRecordSequenceOffset),
        .applied_row_count = load_u32_le(descriptor, format::kRetryAppliedRowCountOffset),
    });
  }

  const common::Status model_status = validate_model(wal_id, checkpoint, tablets, parts, retries);
  if (!model_status.is_ok()) {
    return std::unexpected(corruption(model_status.message()));
  }
  return DecodedManifestView{generation,       previous_generation, *database_id,
                             wal_id,           checkpoint,          std::move(tablets),
                             std::move(parts), std::move(retries),  generation_bytes};
}

ManifestDecodeResult decode_manifest_v1_exact(const common::ByteView bytes,
                                              const ManifestDecodeLimits limits) {
  ManifestDecodeResult decoded = decode_manifest_v1_prefix(bytes, limits);
  if (!decoded.has_value()) {
    return decoded;
  }
  if (bytes.size() != decoded->encoded_bytes().size()) {
    return std::unexpected(corruption("Manifest exact decoder rejects trailing bytes"));
  }
  return decoded;
}

} // namespace chronos::manifest
