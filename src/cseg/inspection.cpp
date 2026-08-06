#include "chronos/cseg/inspection.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/cseg/part_codec.hpp"

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

namespace chronos::cseg {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const std::string_view message) {
  return common::Status{code, std::string{message}};
}

[[nodiscard]] CsegInspectionError decode_error(const CsegPartDecodeError& error) {
  CsegInspectionErrorKind kind = CsegInspectionErrorKind::kCorruption;
  switch (error.kind()) {
  case CsegPartDecodeErrorKind::kIncomplete:
    kind = CsegInspectionErrorKind::kIncomplete;
    break;
  case CsegPartDecodeErrorKind::kCorruption:
    kind = CsegInspectionErrorKind::kCorruption;
    break;
  case CsegPartDecodeErrorKind::kUnsupported:
    kind = CsegInspectionErrorKind::kUnsupported;
    break;
  case CsegPartDecodeErrorKind::kResourceLimit:
    kind = CsegInspectionErrorKind::kResourceLimit;
    break;
  }
  return {kind, error.status(), error.required_size()};
}

[[nodiscard]] CsegInspectionError validation_error(const common::Status& error) {
  CsegInspectionErrorKind kind = CsegInspectionErrorKind::kCorruption;
  if (error.code() == common::StatusCode::kNotSupported) {
    kind = CsegInspectionErrorKind::kUnsupported;
  } else if (error.code() == common::StatusCode::kResourceExhausted) {
    kind = CsegInspectionErrorKind::kResourceLimit;
  } else if (error.code() == common::StatusCode::kInvalidArgument) {
    kind = CsegInspectionErrorKind::kInvalidArgument;
  }
  return {kind, error};
}

} // namespace

CsegInspectionError::CsegInspectionError(const CsegInspectionErrorKind kind,
                                         common::Status status_value,
                                         const std::uint64_t required_size) noexcept
    : kind_(kind), status_(std::move(status_value)), required_size_(required_size) {}

CsegInspectionResult inspect_cseg_v1_part(const common::ByteView bytes,
                                          const CsegInspectionLimits limits) {
  CsegPartDecodeResult decoded = decode_cseg_v1_part_exact(bytes, limits.decode);
  if (!decoded.has_value()) {
    return std::unexpected(decode_error(decoded.error()));
  }
  common::Status validated = validate_cseg_v1_part_contents(*decoded, limits.validation);
  if (!validated.is_ok()) {
    return std::unexpected(validation_error(validated));
  }

  const DecodedCsegMetadataView& metadata = decoded->metadata();
  std::uint64_t stored_page_bytes = 0U;
  std::uint64_t uncompressed_page_bytes = 0U;
  std::uint64_t raw_page_count = 0U;
  std::uint64_t zstd_page_count = 0U;
  for (const CsegPageDescriptor& page : metadata.pages()) {
    const auto stored = common::checked_add(stored_page_bytes, page.stored_length);
    const auto uncompressed =
        common::checked_add(uncompressed_page_bytes, page.uncompressed_length);
    if (!stored.has_value() || !uncompressed.has_value()) {
      return std::unexpected(
          CsegInspectionError{CsegInspectionErrorKind::kResourceLimit,
                              status(common::StatusCode::kResourceExhausted,
                                     "CSEG inspection page-byte accounting overflows")});
    }
    stored_page_bytes = *stored;
    uncompressed_page_bytes = *uncompressed;
    if (page.compression == PageCompression::kNone) {
      ++raw_page_count;
    } else {
      ++zstd_page_count;
    }
  }
  return CsegInspectionReport{
      .part_id = metadata.part_id(),
      .table_id = metadata.table_id(),
      .tablet_id = metadata.tablet_id(),
      .schema_id = metadata.schema_id(),
      .schema_version = metadata.schema_version(),
      .total_length = metadata.total_length(),
      .row_count = metadata.row_count(),
      .event_time_column_ordinal = metadata.event_time_column_ordinal(),
      .ordering_column_count = metadata.ordering_column_count(),
      .minimum_event_time = metadata.minimum_event_time(),
      .maximum_event_time = metadata.maximum_event_time(),
      .stored_page_bytes = stored_page_bytes,
      .uncompressed_page_bytes = uncompressed_page_bytes,
      .raw_page_count = raw_page_count,
      .zstd_page_count = zstd_page_count,
      .columns = {metadata.columns().begin(), metadata.columns().end()},
      .granules = {metadata.granules().begin(), metadata.granules().end()},
      .pages = {metadata.pages().begin(), metadata.pages().end()},
  };
}

} // namespace chronos::cseg
