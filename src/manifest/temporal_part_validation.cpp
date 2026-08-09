#include "chronos/manifest/temporal_part_validation.hpp"

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/result.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/cseg/temporal_format.hpp"
#include "chronos/ingest/sha256.hpp"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <type_traits>

namespace chronos::manifest {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const std::string_view message) {
  return common::Status{code, std::string{message}};
}

[[nodiscard]] common::Status corruption(const std::string_view message) {
  return status(common::StatusCode::kCorruption, message);
}

[[nodiscard]] common::Status decode_failure(const cseg::CsegPartDecodeError& error) {
  using cseg::CsegPartDecodeErrorKind;
  switch (error.kind()) {
  case CsegPartDecodeErrorKind::kIncomplete:
    return corruption("Manifest v2 referenced CSEG image is incomplete");
  case CsegPartDecodeErrorKind::kCorruption:
  case CsegPartDecodeErrorKind::kUnsupported:
  case CsegPartDecodeErrorKind::kResourceLimit:
    return error.status();
  }
  return status(common::StatusCode::kInternal, "Unknown CSEG decode error classification");
}

[[nodiscard]] common::Result<common::ByteView>
cell_bytes(const columnar::PhysicalColumnView& column, const std::uint32_t row) {
  const common::Result<columnar::ColumnCellView> cell = column.cell(row);
  if (!cell.has_value() || cell->is_null()) {
    return common::make_unexpected(
        corruption("Validated temporal CSEG system cell became inaccessible or null"));
  }
  const common::Result<common::ByteView> bytes = cell->bytes();
  if (!bytes.has_value()) {
    return common::make_unexpected(
        corruption("Validated temporal CSEG system cell is not byte-valued"));
  }
  return *bytes;
}

template <typename Integer>
  requires std::is_integral_v<Integer>
[[nodiscard]] common::Result<Integer> load_le(const common::ByteView bytes,
                                              const std::string_view field) {
  if (bytes.size() != sizeof(Integer)) {
    return common::make_unexpected(corruption(std::string{"Validated temporal CSEG "} +
                                              std::string{field} + " cell has an invalid width"));
  }
  using Unsigned = std::make_unsigned_t<Integer>;
  Unsigned value = 0U;
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    value |= static_cast<Unsigned>(std::to_integer<std::uint8_t>(bytes[index])) << (index * 8U);
  }
  return std::bit_cast<Integer>(value);
}

struct TemporalSummary {
  std::uint64_t minimum_commit_position{std::numeric_limits<std::uint64_t>::max()};
  std::uint64_t maximum_commit_position{};
  std::int64_t minimum_system_time{std::numeric_limits<std::int64_t>::max()};
  std::int64_t maximum_system_time{std::numeric_limits<std::int64_t>::min()};
};

[[nodiscard]] common::Result<TemporalSummary>
summarize_temporal_rows(const cseg::DecodedCsegPartView& part,
                        const ManifestCommitSource expected_source,
                        const common::Uuid& expected_source_id) {
  const cseg::DecodedCsegMetadataView& metadata = part.metadata();
  const std::size_t user_count =
      metadata.columns().size() - cseg::temporal_format::kSystemColumnCount;
  TemporalSummary summary;

  for (const cseg::CsegGranuleDescriptor& granule : metadata.granules()) {
    const std::size_t first_page = static_cast<std::size_t>(granule.first_page_index);
    common::Result<cseg::DecodedCsegPage> source_page = part.decode_page(first_page + user_count);
    common::Result<cseg::DecodedCsegPage> source_id_page =
        part.decode_page(first_page + user_count + 1U);
    common::Result<cseg::DecodedCsegPage> position_page =
        part.decode_page(first_page + user_count + 2U);
    common::Result<cseg::DecodedCsegPage> system_time_page =
        part.decode_page(first_page + user_count + 7U);
    if (!source_page.has_value() || !source_id_page.has_value() || !position_page.has_value() ||
        !system_time_page.has_value()) {
      return common::make_unexpected(
          corruption("Validated temporal CSEG system page no longer decodes"));
    }

    for (std::uint32_t row = 0U; row < granule.row_count; ++row) {
      const common::Result<common::ByteView> source = cell_bytes(source_page->physical(), row);
      const common::Result<common::ByteView> source_id =
          cell_bytes(source_id_page->physical(), row);
      const common::Result<common::ByteView> position = cell_bytes(position_page->physical(), row);
      const common::Result<common::ByteView> system_time =
          cell_bytes(system_time_page->physical(), row);
      if (!source.has_value() || !source_id.has_value() || !position.has_value() ||
          !system_time.has_value()) {
        return common::make_unexpected(
            corruption("Validated temporal CSEG system cell no longer decodes"));
      }
      const common::Result<std::uint8_t> decoded_source = load_le<std::uint8_t>(*source, "source");
      const common::Result<std::uint64_t> decoded_position =
          load_le<std::uint64_t>(*position, "commit-position");
      const common::Result<std::int64_t> decoded_system_time =
          load_le<std::int64_t>(*system_time, "system-time");
      if (!decoded_source.has_value() || !decoded_position.has_value() ||
          !decoded_system_time.has_value()) {
        return common::make_unexpected(corruption("Temporal CSEG summary decoding failed"));
      }
      if (*decoded_source != static_cast<std::uint8_t>(expected_source)) {
        return common::make_unexpected(
            corruption("CSEG row commit source disagrees with its Manifest v2 owner"));
      }
      if (source_id->size() != common::Uuid::kSize ||
          !std::ranges::equal(*source_id, expected_source_id.bytes())) {
        return common::make_unexpected(
            corruption("CSEG row source identity disagrees with its Manifest v2 owner"));
      }
      summary.minimum_commit_position =
          std::min(summary.minimum_commit_position, *decoded_position);
      summary.maximum_commit_position =
          std::max(summary.maximum_commit_position, *decoded_position);
      summary.minimum_system_time = std::min(summary.minimum_system_time, *decoded_system_time);
      summary.maximum_system_time = std::max(summary.maximum_system_time, *decoded_system_time);
    }
  }
  return summary;
}

} // namespace

common::Result<TemporalPartDescriptor> describe_manifest_v2_temporal_part_image(
    const common::ByteView image, const schema::TableSchema& schema_value,
    const schema::TabletId& target_tablet, const ManifestCommitSource commit_source,
    const common::Uuid& source_id, const TemporalPartValidationLimits limits) {
  cseg::CsegPartDecodeResult decoded =
      cseg::decode_cseg_v2_temporal_part_exact(image, limits.decode);
  if (!decoded.has_value()) {
    return common::make_unexpected(decode_failure(decoded.error()));
  }
  common::Status validation =
      cseg::validate_cseg_v2_temporal_part(*decoded, schema_value, target_tablet, limits.contents);
  if (!validation.is_ok()) {
    return common::make_unexpected(std::move(validation));
  }

  const common::Result<TemporalSummary> summary =
      summarize_temporal_rows(*decoded, commit_source, source_id);
  if (!summary.has_value()) {
    return common::make_unexpected(summary.error());
  }
  const common::Result<ingest::Sha256Digest> digest = ingest::sha256(image);
  if (!digest.has_value()) {
    return common::make_unexpected(digest.error());
  }

  const cseg::DecodedCsegMetadataView& metadata = decoded->metadata();
  return TemporalPartDescriptor{
      .part_id = metadata.part_id(),
      .table_id = metadata.table_id(),
      .tablet_id = metadata.tablet_id(),
      .schema_id = metadata.schema_id(),
      .schema_version = metadata.schema_version(),
      .file_length = metadata.total_length(),
      .row_count = metadata.row_count(),
      .minimum_commit_position = summary->minimum_commit_position,
      .maximum_commit_position = summary->maximum_commit_position,
      .minimum_event_time = metadata.minimum_event_time(),
      .maximum_event_time = metadata.maximum_event_time(),
      .minimum_system_time = summary->minimum_system_time,
      .maximum_system_time = summary->maximum_system_time,
      .source_id = source_id,
      .content_sha256 = *digest,
      .cseg_format_major = cseg::temporal_format::kFormatMajor,
      .cseg_format_minor = cseg::temporal_format::kFormatMinor,
      .commit_source = commit_source,
  };
}

common::Status validate_manifest_v2_temporal_part_image(const TemporalPartDescriptor& descriptor,
                                                        const TemporalTabletDescriptor& owner,
                                                        const common::ByteView image,
                                                        const schema::TableSchema& schema_value,
                                                        const TemporalPartValidationLimits limits) {
  if (descriptor.table_id != owner.table_id || descriptor.tablet_id != owner.tablet_id ||
      descriptor.commit_source != owner.commit_source || descriptor.source_id != owner.source_id ||
      descriptor.maximum_commit_position > owner.durable_position) {
    return corruption("Manifest v2 part descriptor disagrees with its tablet owner");
  }
  const common::Result<TemporalPartDescriptor> actual = describe_manifest_v2_temporal_part_image(
      image, schema_value, owner.tablet_id, owner.commit_source, owner.source_id, limits);
  if (!actual.has_value()) {
    return actual.error();
  }
  if (*actual != descriptor) {
    return corruption("Manifest v2 part descriptor disagrees with its exact CSEG image");
  }
  return common::Status::ok();
}

} // namespace chronos::manifest
