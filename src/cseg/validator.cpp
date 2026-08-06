#include "chronos/cseg/validator.hpp"

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/result.hpp"
#include "chronos/schema/logical_type.hpp"
#include "sort_order_internal.hpp"
#include "system_rows_internal.hpp"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::cseg {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const std::string_view message) {
  return common::Status{code, std::string{message}};
}

[[nodiscard]] common::Status corruption(const std::string_view message) {
  return status(common::StatusCode::kCorruption, message);
}

using SortCell = detail::SortCellView;

struct BoundaryCaptureInput {
  std::uint32_t row_count;
  std::uint64_t max_bytes;
};

struct OwnedSortCell {
  bool is_null{};
  bool is_boolean{};
  bool boolean{};
  std::vector<std::byte> bytes;

  [[nodiscard]] SortCell view() const noexcept {
    return {.is_null = is_null, .is_boolean = is_boolean, .boolean = boolean, .bytes = bytes};
  }
};

[[nodiscard]] common::Result<SortCell> sort_cell(const columnar::PhysicalColumnView& column,
                                                 const std::uint32_t row) {
  const common::Result<columnar::ColumnCellView> cell = column.cell(row);
  if (!cell.has_value()) {
    return common::make_unexpected(corruption("validated CSEG sort cell is inaccessible"));
  }
  if (cell->is_null()) {
    return SortCell{.is_null = true, .is_boolean = false, .boolean = false, .bytes = {}};
  }
  if (column.type().kind() == schema::LogicalTypeKind::kBool) {
    const common::Result<bool> value = cell->boolean();
    if (!value.has_value()) {
      return common::make_unexpected(corruption("validated CSEG BOOL sort cell is malformed"));
    }
    return SortCell{.is_null = false, .is_boolean = true, .boolean = *value, .bytes = {}};
  }
  const common::Result<common::ByteView> value = cell->bytes();
  if (!value.has_value()) {
    return common::make_unexpected(corruption("validated CSEG byte sort cell is malformed"));
  }
  return SortCell{.is_null = false, .is_boolean = false, .boolean = false, .bytes = *value};
}

template <typename Unsigned>
[[nodiscard]] common::Result<Unsigned> load_little_endian(const common::ByteView bytes) {
  if (bytes.size() != sizeof(Unsigned)) {
    return common::make_unexpected(corruption("CSEG sort cell has an invalid fixed width"));
  }
  Unsigned value = 0U;
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    value |= static_cast<Unsigned>(std::to_integer<std::uint8_t>(bytes[index])) << (index * 8U);
  }
  return value;
}

[[nodiscard]] common::Result<int>
compare_rows(const std::span<const DecodedCsegPage> key_pages,
             const std::span<const CsegColumnDescriptor> key_columns, const std::uint32_t left_row,
             const std::uint32_t right_row) {
  for (std::size_t index = 0U; index < key_pages.size(); ++index) {
    const common::Result<SortCell> left = sort_cell(key_pages[index].physical(), left_row);
    const common::Result<SortCell> right = sort_cell(key_pages[index].physical(), right_row);
    if (!left.has_value()) {
      return common::make_unexpected(left.error());
    }
    if (!right.has_value()) {
      return common::make_unexpected(right.error());
    }
    const common::Result<int> compared =
        detail::compare_sort_cells(key_columns[index].logical_type.kind(), *left, *right);
    if (!compared.has_value() || *compared != 0) {
      return compared;
    }
  }
  return 0;
}

[[nodiscard]] common::Result<int>
compare_boundary(const std::span<const OwnedSortCell> left,
                 const std::span<const DecodedCsegPage> right_pages,
                 const std::span<const CsegColumnDescriptor> key_columns) {
  for (std::size_t index = 0U; index < left.size(); ++index) {
    const common::Result<SortCell> right = sort_cell(right_pages[index].physical(), 0U);
    if (!right.has_value()) {
      return common::make_unexpected(right.error());
    }
    const common::Result<int> compared = detail::compare_sort_cells(
        key_columns[index].logical_type.kind(), left[index].view(), *right);
    if (!compared.has_value() || *compared != 0) {
      return compared;
    }
  }
  return 0;
}

[[nodiscard]] common::Result<std::vector<OwnedSortCell>>
capture_last_row(const std::span<const DecodedCsegPage> key_pages,
                 const BoundaryCaptureInput input) {
  std::uint64_t boundary_bytes = 0U;
  for (const DecodedCsegPage& page : key_pages) {
    const common::Result<SortCell> cell = sort_cell(page.physical(), input.row_count - 1U);
    if (!cell.has_value()) {
      return common::make_unexpected(cell.error());
    }
    const std::optional<std::uint64_t> next =
        common::checked_add(boundary_bytes, static_cast<std::uint64_t>(cell->bytes.size()));
    if (!next.has_value() || *next > input.max_bytes) {
      return common::make_unexpected(
          status(common::StatusCode::kResourceExhausted,
                 "CSEG validation boundary row exceeds its configured working-memory limit"));
    }
    boundary_bytes = *next;
  }

  std::vector<OwnedSortCell> captured;
  captured.reserve(key_pages.size());
  for (const DecodedCsegPage& page : key_pages) {
    const common::Result<SortCell> cell = sort_cell(page.physical(), input.row_count - 1U);
    if (!cell.has_value()) {
      return common::make_unexpected(cell.error());
    }
    captured.push_back({.is_null = cell->is_null,
                        .is_boolean = cell->is_boolean,
                        .boolean = cell->boolean,
                        .bytes = {cell->bytes.begin(), cell->bytes.end()}});
  }
  return captured;
}

[[nodiscard]] std::uint64_t captured_bytes(const std::span<const OwnedSortCell> cells) noexcept {
  std::uint64_t bytes = 0U;
  for (const OwnedSortCell& cell : cells) {
    bytes += cell.bytes.size();
  }
  return bytes;
}

[[nodiscard]] common::Result<std::int64_t>
event_time_value(const columnar::PhysicalColumnView& event_time, const std::uint32_t row) {
  const common::Result<SortCell> cell = sort_cell(event_time, row);
  if (!cell.has_value() || cell->is_null || cell->is_boolean) {
    return common::make_unexpected(corruption("CSEG event-time cell is invalid"));
  }
  const common::Result<std::uint64_t> bits = load_little_endian<std::uint64_t>(cell->bytes);
  if (!bits.has_value()) {
    return common::make_unexpected(bits.error());
  }
  return std::bit_cast<std::int64_t>(*bits);
}

} // namespace

common::Status validate_cseg_v1_part_contents(const DecodedCsegPartView& part,
                                              const CsegValidationLimits limits) {
  if (limits.max_working_bytes == 0U) {
    return status(common::StatusCode::kInvalidArgument,
                  "CSEG validation working-memory limit must be nonzero");
  }
  const DecodedCsegMetadataView& metadata = part.metadata();
  const std::uint32_t stored_count = static_cast<std::uint32_t>(metadata.columns().size());
  const std::uint32_t user_count = stored_count - format::kSystemColumnCount;
  std::vector<std::uint32_t> key_ordinals(metadata.ordering_column_count());
  for (std::uint32_t column = 0U; column < user_count; ++column) {
    const std::optional<std::uint32_t> ordering = metadata.columns()[column].ordering_ordinal;
    if (ordering.has_value()) {
      key_ordinals[ordering.value()] = column;
    }
  }
  key_ordinals.push_back(user_count);
  key_ordinals.push_back(user_count + 1U);
  key_ordinals.push_back(user_count + 2U);

  std::vector<CsegColumnDescriptor> key_columns;
  key_columns.reserve(key_ordinals.size());
  for (const std::uint32_t ordinal : key_ordinals) {
    key_columns.push_back(metadata.columns()[ordinal]);
  }

  std::vector<OwnedSortCell> previous_boundary;
  std::int64_t part_minimum = std::numeric_limits<std::int64_t>::max();
  std::int64_t part_maximum = std::numeric_limits<std::int64_t>::min();
  for (std::size_t granule_index = 0U; granule_index < metadata.granules().size();
       ++granule_index) {
    const CsegGranuleDescriptor& granule = metadata.granules()[granule_index];
    const std::uint64_t first_page = granule.first_page_index;
    std::uint64_t page_bytes = 0U;
    for (const std::uint32_t ordinal : key_ordinals) {
      const CsegPageDescriptor& page = metadata.pages()[first_page + ordinal];
      const std::optional<std::uint64_t> next =
          common::checked_add(page_bytes, page.uncompressed_length);
      if (!next.has_value()) {
        return status(common::StatusCode::kResourceExhausted,
                      "CSEG validation working-byte accounting overflows");
      }
      page_bytes = *next;
    }
    const CsegPageDescriptor& operation_descriptor = metadata.pages()[first_page + user_count + 3U];
    const std::optional<std::uint64_t> with_operation =
        common::checked_add(page_bytes, operation_descriptor.uncompressed_length);
    if (!with_operation.has_value()) {
      return status(common::StatusCode::kResourceExhausted,
                    "CSEG validation working-byte accounting overflows");
    }
    const std::optional<std::uint64_t> required =
        common::checked_add(*with_operation, captured_bytes(previous_boundary));
    if (!required.has_value() || *required > limits.max_working_bytes) {
      return status(common::StatusCode::kResourceExhausted,
                    "CSEG validation exceeds its configured working-memory limit");
    }

    std::vector<DecodedCsegPage> key_pages;
    key_pages.reserve(key_ordinals.size());
    for (const std::uint32_t ordinal : key_ordinals) {
      common::Result<DecodedCsegPage> page =
          part.decode_page(static_cast<std::size_t>(first_page + ordinal));
      if (!page.has_value()) {
        return corruption("structurally validated CSEG key page no longer decodes");
      }
      key_pages.push_back(std::move(*page));
    }
    common::Result<DecodedCsegPage> operation =
        part.decode_page(static_cast<std::size_t>(first_page + user_count + 3U));
    if (!operation.has_value()) {
      return corruption("structurally validated CSEG operation page no longer decodes");
    }

    const std::size_t system_start = metadata.ordering_column_count();
    common::Status system = detail::validate_cseg_v1_system_rows(
        {.wal_id = key_pages[system_start].physical(),
         .record_sequence = key_pages[system_start + 1U].physical(),
         .row_ordinal = key_pages[system_start + 2U].physical(),
         .operation = operation->physical()},
        granule.row_count);
    if (!system.is_ok()) {
      return system;
    }

    const std::optional<std::uint32_t> event_key =
        metadata.columns()[metadata.event_time_column_ordinal()].ordering_ordinal;
    if (!event_key.has_value()) {
      return corruption("CSEG event-time ordering ordinal disappeared after metadata validation");
    }
    const columnar::PhysicalColumnView& event_time = key_pages[event_key.value()].physical();
    std::int64_t granule_minimum = std::numeric_limits<std::int64_t>::max();
    std::int64_t granule_maximum = std::numeric_limits<std::int64_t>::min();
    for (std::uint32_t row = 0U; row < granule.row_count; ++row) {
      const common::Result<std::int64_t> value = event_time_value(event_time, row);
      if (!value.has_value()) {
        return value.error();
      }
      granule_minimum = std::min(granule_minimum, *value);
      granule_maximum = std::max(granule_maximum, *value);
    }
    if (granule_minimum != granule.minimum_event_time ||
        granule_maximum != granule.maximum_event_time) {
      return corruption("CSEG decoded event-time values disagree with granule extrema");
    }
    part_minimum = std::min(part_minimum, granule_minimum);
    part_maximum = std::max(part_maximum, granule_maximum);

    if (!previous_boundary.empty()) {
      const common::Result<int> boundary =
          compare_boundary(previous_boundary, key_pages, key_columns);
      if (!boundary.has_value()) {
        return boundary.error();
      }
      if (*boundary >= 0) {
        return corruption("CSEG physical row order is not strictly increasing across granules");
      }
    }
    std::vector<OwnedSortCell>{}.swap(previous_boundary);
    for (std::uint32_t row = 1U; row < granule.row_count; ++row) {
      const common::Result<int> compared = compare_rows(key_pages, key_columns, row - 1U, row);
      if (!compared.has_value()) {
        return compared.error();
      }
      if (*compared >= 0) {
        return corruption("CSEG physical row order is not strictly increasing");
      }
    }
    common::Result<std::vector<OwnedSortCell>> captured =
        capture_last_row(key_pages, {.row_count = granule.row_count,
                                     .max_bytes = limits.max_working_bytes - *with_operation});
    if (!captured.has_value()) {
      return captured.error();
    }
    previous_boundary = std::move(*captured);
  }
  if (part_minimum != metadata.minimum_event_time() ||
      part_maximum != metadata.maximum_event_time()) {
    return corruption("CSEG decoded event-time values disagree with header extrema");
  }
  return common::Status::ok();
}

common::Status validate_cseg_v1_part(const DecodedCsegPartView& part,
                                     const schema::TableSchema& schema_value,
                                     const schema::TabletId& target_tablet,
                                     const CsegValidationLimits limits) {
  common::Status binding =
      validate_cseg_v1_metadata_schema(part.metadata(), schema_value, target_tablet);
  if (!binding.is_ok()) {
    return binding;
  }
  return validate_cseg_v1_part_contents(part, limits);
}

} // namespace chronos::cseg
