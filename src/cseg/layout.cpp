#include "chronos/cseg/layout.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/cseg/format.hpp"

#include <optional>
#include <string>
#include <string_view>

namespace chronos::cseg {
namespace {

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

[[nodiscard]] common::Status exhausted(const std::string_view message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::string{message}};
}

// Offset, count, and element length are the conventional table-layout inputs.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] common::Result<std::uint64_t> checked_table_end(const std::uint64_t offset,
                                                              const std::uint64_t count,
                                                              const std::uint64_t element_length,
                                                              const std::string_view label) {
  const std::optional<std::uint64_t> length = common::checked_multiply(count, element_length);
  const std::optional<std::uint64_t> end =
      length.has_value() ? common::checked_add(offset, *length) : std::nullopt;
  if (!end.has_value()) {
    return common::make_unexpected(exhausted(label));
  }
  return *end;
}

} // namespace

common::Result<CsegMetadataLayout>
plan_cseg_v1_metadata_layout(const CsegMetadataLayoutInput input) {
  if (input.user_column_count == 0U || input.user_column_count > format::kMaximumUserColumnCount) {
    return common::make_unexpected(invalid("CSEG user column count is outside the v1 limit"));
  }
  if (input.granule_count == 0U || input.granule_count > format::kMaximumGranuleCount) {
    return common::make_unexpected(invalid("CSEG granule count is outside the v1 limit"));
  }

  const std::optional<std::uint32_t> stored_columns =
      common::checked_add(input.user_column_count, format::kSystemColumnCount);
  if (!stored_columns.has_value()) {
    return common::make_unexpected(exhausted("CSEG stored column count overflowed"));
  }
  const std::optional<std::uint64_t> page_count = common::checked_multiply(
      static_cast<std::uint64_t>(input.granule_count), static_cast<std::uint64_t>(*stored_columns));
  if (!page_count.has_value() || *page_count > format::kMaximumPageCount) {
    return common::make_unexpected(exhausted("CSEG page count exceeds the v1 field"));
  }

  const std::uint64_t columns_offset = format::kColumnsOffset;
  const common::Result<std::uint64_t> granules_offset =
      checked_table_end(columns_offset, *stored_columns, format::kColumnDescriptorLength,
                        "CSEG column descriptor table overflowed");
  if (!granules_offset.has_value()) {
    return common::make_unexpected(granules_offset.error());
  }
  const common::Result<std::uint64_t> pages_offset =
      checked_table_end(*granules_offset, input.granule_count, format::kGranuleDescriptorLength,
                        "CSEG granule descriptor table overflowed");
  if (!pages_offset.has_value()) {
    return common::make_unexpected(pages_offset.error());
  }
  const common::Result<std::uint64_t> trailer_offset =
      checked_table_end(*pages_offset, *page_count, format::kPageDescriptorLength,
                        "CSEG page descriptor table overflowed");
  if (!trailer_offset.has_value()) {
    return common::make_unexpected(trailer_offset.error());
  }
  const std::optional<std::uint64_t> metadata_length = common::checked_add(
      *trailer_offset, static_cast<std::uint64_t>(format::kMetadataTrailerLength));
  if (!metadata_length.has_value() || *metadata_length > format::kMaximumFileLength) {
    return common::make_unexpected(exhausted("CSEG metadata exceeds the v1 file limit"));
  }
  if ((*metadata_length % format::kAlignment) != 0U) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "CSEG metadata layout is not aligned"});
  }

  return CsegMetadataLayout{
      .stored_column_count = *stored_columns,
      .page_count = static_cast<std::uint32_t>(*page_count),
      .columns_offset = columns_offset,
      .granules_offset = *granules_offset,
      .pages_offset = *pages_offset,
      .metadata_trailer_offset = *trailer_offset,
      .metadata_length = *metadata_length,
  };
}

common::Result<CsegPageLayout> plan_cseg_v1_page_layout(const std::uint64_t current_offset,
                                                        const std::uint64_t stored_length) {
  if ((current_offset % format::kAlignment) != 0U) {
    return common::make_unexpected(invalid("CSEG page offset must be canonically aligned"));
  }
  if (current_offset > format::kMaximumFileLength) {
    return common::make_unexpected(exhausted("CSEG page offset exceeds the v1 file limit"));
  }
  if (stored_length == 0U) {
    return common::make_unexpected(invalid("CSEG stored page length must be nonzero"));
  }
  if (stored_length > format::kMaximumStoredPageLength) {
    return common::make_unexpected(exhausted("CSEG stored page length exceeds the v1 limit"));
  }
  const std::optional<std::uint64_t> end = common::checked_add(current_offset, stored_length);
  if (!end.has_value()) {
    return common::make_unexpected(exhausted("CSEG page end overflowed"));
  }
  const common::Result<std::uint64_t> aligned = common::checked_align_up(*end, format::kAlignment);
  if (!aligned.has_value() || *aligned > format::kMaximumFileLength) {
    return common::make_unexpected(exhausted("CSEG total length exceeds the v1 limit"));
  }
  return CsegPageLayout{.offset = current_offset,
                        .stored_length = stored_length,
                        .padding_length = *aligned - *end,
                        .next_offset = *aligned};
}

common::Result<CsegFileLayout>
plan_cseg_v1_layout(const CsegMetadataLayoutInput input,
                    const std::span<const std::uint64_t> stored_page_lengths) {
  const common::Result<CsegMetadataLayout> metadata = plan_cseg_v1_metadata_layout(input);
  if (!metadata.has_value()) {
    return common::make_unexpected(metadata.error());
  }
  if (stored_page_lengths.size() != metadata->page_count) {
    return common::make_unexpected(
        invalid("CSEG stored page length count does not match the canonical page count"));
  }

  std::uint64_t cursor = metadata->metadata_length;
  for (const std::uint64_t stored_length : stored_page_lengths) {
    const common::Result<CsegPageLayout> page = plan_cseg_v1_page_layout(cursor, stored_length);
    if (!page.has_value()) {
      return common::make_unexpected(page.error());
    }
    cursor = page->next_offset;
  }

  return CsegFileLayout{.metadata = *metadata, .total_length = cursor};
}

} // namespace chronos::cseg
