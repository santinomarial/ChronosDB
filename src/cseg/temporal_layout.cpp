#include "chronos/cseg/temporal_layout.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/temporal_format.hpp"

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

struct DescriptorTableLayout {
  std::uint64_t offset{};
  std::uint64_t entry_count{};
  std::uint64_t entry_length{};
};

[[nodiscard]] common::Result<std::uint64_t> checked_table_end(const DescriptorTableLayout table,
                                                              const std::string_view label) {
  const auto length = common::checked_multiply(table.entry_count, table.entry_length);
  const auto end = length.has_value() ? common::checked_add(table.offset, *length) : std::nullopt;
  return end.has_value() ? common::Result<std::uint64_t>{*end}
                         : common::make_unexpected(exhausted(label));
}

} // namespace

common::Result<CsegMetadataLayout>
plan_cseg_v2_temporal_metadata_layout(const CsegMetadataLayoutInput input) {
  if (input.user_column_count == 0U || input.user_column_count > format::kMaximumUserColumnCount) {
    return common::make_unexpected(invalid("CSEG v2 user column count is outside the limit"));
  }
  if (input.granule_count == 0U || input.granule_count > format::kMaximumGranuleCount) {
    return common::make_unexpected(invalid("CSEG v2 granule count is outside the limit"));
  }
  const auto stored_columns =
      common::checked_add(input.user_column_count, temporal_format::kSystemColumnCount);
  if (!stored_columns.has_value() || *stored_columns > temporal_format::kMaximumStoredColumnCount) {
    return common::make_unexpected(exhausted("CSEG v2 stored column count overflowed"));
  }
  const auto page_count = common::checked_multiply(static_cast<std::uint64_t>(input.granule_count),
                                                   static_cast<std::uint64_t>(*stored_columns));
  if (!page_count.has_value() || *page_count > format::kMaximumPageCount) {
    return common::make_unexpected(exhausted("CSEG v2 page count exceeds its field"));
  }
  const std::uint64_t columns_offset = format::kColumnsOffset;
  auto granules_offset = checked_table_end({.offset = columns_offset,
                                            .entry_count = *stored_columns,
                                            .entry_length = format::kColumnDescriptorLength},
                                           "CSEG v2 column table overflowed");
  if (!granules_offset.has_value())
    return common::make_unexpected(granules_offset.error());
  auto pages_offset = checked_table_end({.offset = *granules_offset,
                                         .entry_count = input.granule_count,
                                         .entry_length = format::kGranuleDescriptorLength},
                                        "CSEG v2 granule table overflowed");
  if (!pages_offset.has_value())
    return common::make_unexpected(pages_offset.error());
  auto trailer_offset = checked_table_end({.offset = *pages_offset,
                                           .entry_count = *page_count,
                                           .entry_length = format::kPageDescriptorLength},
                                          "CSEG v2 page table overflowed");
  if (!trailer_offset.has_value())
    return common::make_unexpected(trailer_offset.error());
  const auto metadata_length = common::checked_add(
      *trailer_offset, static_cast<std::uint64_t>(format::kMetadataTrailerLength));
  if (!metadata_length.has_value() || *metadata_length > format::kMaximumFileLength) {
    return common::make_unexpected(exhausted("CSEG v2 metadata exceeds the file limit"));
  }
  if ((*metadata_length % format::kAlignment) != 0U) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "CSEG v2 metadata is not aligned"});
  }
  return CsegMetadataLayout{.stored_column_count = *stored_columns,
                            .page_count = static_cast<std::uint32_t>(*page_count),
                            .columns_offset = columns_offset,
                            .granules_offset = *granules_offset,
                            .pages_offset = *pages_offset,
                            .metadata_trailer_offset = *trailer_offset,
                            .metadata_length = *metadata_length};
}

common::Result<CsegPageLayout>
plan_cseg_v2_temporal_page_layout(const std::uint64_t current_offset,
                                  const std::uint64_t stored_length) {
  if ((current_offset % format::kAlignment) != 0U) {
    return common::make_unexpected(invalid("CSEG v2 page offset must be canonically aligned"));
  }
  if (current_offset > format::kMaximumFileLength) {
    return common::make_unexpected(exhausted("CSEG v2 page offset exceeds the file limit"));
  }
  if (stored_length == 0U) {
    return common::make_unexpected(invalid("CSEG v2 stored page length must be nonzero"));
  }
  if (stored_length > format::kMaximumStoredPageLength) {
    return common::make_unexpected(exhausted("CSEG v2 stored page length exceeds the limit"));
  }
  const auto end = common::checked_add(current_offset, stored_length);
  if (!end.has_value()) {
    return common::make_unexpected(exhausted("CSEG v2 page end overflowed"));
  }
  const auto aligned = common::checked_align_up(*end, format::kAlignment);
  if (!aligned.has_value() || *aligned > format::kMaximumFileLength) {
    return common::make_unexpected(exhausted("CSEG v2 total length exceeds the file limit"));
  }
  return CsegPageLayout{.offset = current_offset,
                        .stored_length = stored_length,
                        .padding_length = *aligned - *end,
                        .next_offset = *aligned};
}

common::Result<CsegFileLayout>
plan_cseg_v2_temporal_layout(const CsegMetadataLayoutInput input,
                             const std::span<const std::uint64_t> stored_page_lengths) {
  auto metadata = plan_cseg_v2_temporal_metadata_layout(input);
  if (!metadata.has_value())
    return common::make_unexpected(metadata.error());
  if (stored_page_lengths.size() != metadata->page_count) {
    return common::make_unexpected(
        invalid("CSEG v2 stored page length count does not match the canonical page count"));
  }
  std::uint64_t cursor = metadata->metadata_length;
  for (const std::uint64_t stored_length : stored_page_lengths) {
    auto page = plan_cseg_v2_temporal_page_layout(cursor, stored_length);
    if (!page.has_value())
      return common::make_unexpected(page.error());
    cursor = page->next_offset;
  }
  return CsegFileLayout{.metadata = *metadata, .total_length = cursor};
}

} // namespace chronos::cseg
