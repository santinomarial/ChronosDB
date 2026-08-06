#include "chronos/manifest/part_validation.hpp"

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/result.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/page_codec.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/manifest/naming.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <utility>

namespace chronos::manifest {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const std::string_view message) {
  return common::Status{code, std::string{message}};
}

[[nodiscard]] common::Status corruption(const std::string_view message) {
  return status(common::StatusCode::kCorruption, message);
}

[[nodiscard]] const TabletSchemaBinding*
find_binding(const std::span<const TabletSchemaBinding> bindings,
             const schema::TabletId& tablet_id) noexcept {
  const auto found =
      std::ranges::lower_bound(bindings, tablet_id, {}, [](const TabletSchemaBinding& binding) {
        return binding.tablet_id;
      });
  return found != bindings.end() && found->tablet_id == tablet_id ? &*found : nullptr;
}

[[nodiscard]] common::Status decode_failure(const cseg::CsegPartDecodeError& error) {
  using cseg::CsegPartDecodeErrorKind;
  switch (error.kind()) {
  case CsegPartDecodeErrorKind::kIncomplete:
    return corruption("Manifest referenced CSEG file is incomplete");
  case CsegPartDecodeErrorKind::kCorruption:
  case CsegPartDecodeErrorKind::kUnsupported:
  case CsegPartDecodeErrorKind::kResourceLimit:
    return error.status();
  }
  return status(common::StatusCode::kInternal, "Unknown CSEG decode error classification");
}

[[nodiscard]] bool metadata_agrees(const PartDescriptor& descriptor,
                                   const cseg::DecodedCsegMetadataView& metadata) noexcept {
  return descriptor.part_id == metadata.part_id() && descriptor.table_id == metadata.table_id() &&
         descriptor.tablet_id == metadata.tablet_id() &&
         descriptor.schema_id == metadata.schema_id() &&
         descriptor.schema_version == metadata.schema_version() &&
         descriptor.file_length == metadata.total_length() &&
         descriptor.row_count == metadata.row_count() &&
         descriptor.minimum_event_time == metadata.minimum_event_time() &&
         descriptor.maximum_event_time == metadata.maximum_event_time();
}

[[nodiscard]] common::Result<common::ByteView>
cell_bytes(const columnar::PhysicalColumnView& column, const std::uint32_t row) {
  const common::Result<columnar::ColumnCellView> cell = column.cell(row);
  if (!cell.has_value() || cell->is_null()) {
    return common::make_unexpected(
        corruption("Validated CSEG system cell became inaccessible or null"));
  }
  const common::Result<common::ByteView> bytes = cell->bytes();
  if (!bytes.has_value()) {
    return common::make_unexpected(corruption("Validated CSEG system cell is not byte-valued"));
  }
  return *bytes;
}

[[nodiscard]] common::Result<std::uint64_t> load_u64(const common::ByteView bytes) {
  if (bytes.size() != sizeof(std::uint64_t)) {
    return common::make_unexpected(
        corruption("Validated CSEG record-sequence cell has an invalid width"));
  }
  std::uint64_t value = 0U;
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    value |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[index]))
             << (index * 8U);
  }
  return value;
}

struct RecordSummary {
  std::uint64_t minimum{std::numeric_limits<std::uint64_t>::max()};
  std::uint64_t maximum{};
};

[[nodiscard]] common::Result<RecordSummary> summarize_records(const cseg::DecodedCsegPartView& part,
                                                              const wal::WalId& expected_wal) {
  const cseg::DecodedCsegMetadataView& metadata = part.metadata();
  const std::size_t user_count = metadata.columns().size() - cseg::format::kSystemColumnCount;
  RecordSummary summary;
  for (const cseg::CsegGranuleDescriptor& granule : metadata.granules()) {
    const std::size_t first_page = static_cast<std::size_t>(granule.first_page_index);
    common::Result<cseg::DecodedCsegPage> wal_page = part.decode_page(first_page + user_count);
    common::Result<cseg::DecodedCsegPage> sequence_page =
        part.decode_page(first_page + user_count + 1U);
    if (!wal_page.has_value() || !sequence_page.has_value()) {
      return common::make_unexpected(corruption("Validated CSEG system page no longer decodes"));
    }
    for (std::uint32_t row = 0U; row < granule.row_count; ++row) {
      const common::Result<common::ByteView> wal_bytes = cell_bytes(wal_page->physical(), row);
      const common::Result<common::ByteView> sequence_bytes =
          cell_bytes(sequence_page->physical(), row);
      if (!wal_bytes.has_value() || !sequence_bytes.has_value()) {
        return common::make_unexpected(corruption("Validated CSEG system cell no longer decodes"));
      }
      if (!std::ranges::equal(*wal_bytes, expected_wal.bytes)) {
        return common::make_unexpected(
            corruption("CSEG row WAL identity disagrees with its manifest"));
      }
      const common::Result<std::uint64_t> sequence = load_u64(*sequence_bytes);
      if (!sequence.has_value()) {
        return common::make_unexpected(sequence.error());
      }
      summary.minimum = std::min(summary.minimum, *sequence);
      summary.maximum = std::max(summary.maximum, *sequence);
    }
  }
  return summary;
}

} // namespace

common::Status validate_manifest_v1_part_image(const PartDescriptor& descriptor,
                                               const wal::WalId& wal_id,
                                               const schema::TableSchema& schema_value,
                                               const ReferencedPartImage& image,
                                               const ReferencedPartValidationLimits limits) {
  const common::Result<cseg::PartId> named_part = parse_part_file_name(image.file_name);
  if (!named_part.has_value() || *named_part != descriptor.part_id) {
    return corruption("Installed CSEG filename disagrees with its manifest descriptor");
  }
  if (image.bytes.size() != descriptor.file_length) {
    return corruption("Installed CSEG length disagrees with its manifest descriptor");
  }

  cseg::CsegPartDecodeResult decoded = cseg::decode_cseg_v1_part_exact(image.bytes, limits.decode);
  if (!decoded.has_value()) {
    return decode_failure(decoded.error());
  }
  if (!metadata_agrees(descriptor, decoded->metadata())) {
    return corruption("Installed CSEG header disagrees with its manifest descriptor");
  }
  common::Status part_status =
      cseg::validate_cseg_v1_part(*decoded, schema_value, descriptor.tablet_id, limits.contents);
  if (!part_status.is_ok()) {
    return part_status;
  }
  const common::Result<RecordSummary> records = summarize_records(*decoded, wal_id);
  if (!records.has_value()) {
    return records.error();
  }
  if (records->minimum != descriptor.minimum_record_sequence ||
      records->maximum != descriptor.maximum_record_sequence) {
    return corruption("CSEG record-sequence extrema disagree with its manifest descriptor");
  }
  return common::Status::ok();
}

common::Status
validate_manifest_v1_referenced_parts(const DecodedManifestView& manifest,
                                      const std::span<const TabletSchemaBinding> bindings,
                                      const std::span<const ReferencedPartImage> images,
                                      const ReferencedPartValidationLimits limits) {
  common::Status binding = validate_manifest_v1_schema_binding(manifest, bindings);
  if (!binding.is_ok()) {
    return binding;
  }
  if (images.size() != manifest.parts().size()) {
    return corruption("Installed CSEG images do not exactly cover manifest part descriptors");
  }

  for (std::size_t index = 0U; index < manifest.parts().size(); ++index) {
    const PartDescriptor& descriptor = manifest.parts()[index];
    const ReferencedPartImage& image = images[index];
    const TabletSchemaBinding* schema_binding = find_binding(bindings, descriptor.tablet_id);
    if (schema_binding == nullptr) {
      return status(common::StatusCode::kInternal,
                    "Validated manifest schema binding became inaccessible");
    }
    const std::shared_ptr<const schema::TableSchema> schema_value =
        schema_binding->lineage.get().find(descriptor.schema_id);
    if (!schema_value) {
      return status(common::StatusCode::kInternal,
                    "Validated manifest part schema became inaccessible");
    }
    common::Status part_status = validate_manifest_v1_part_image(descriptor, manifest.wal_id(),
                                                                 *schema_value, image, limits);
    if (!part_status.is_ok()) {
      return part_status;
    }
  }
  return common::Status::ok();
}

} // namespace chronos::manifest
