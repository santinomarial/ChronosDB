#include "chronos/manifest/compaction.hpp"

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"
#include "chronos/cseg/page_codec.hpp"
#include "chronos/cseg/validator.hpp"
#include "chronos/manifest/naming.hpp"
#include "cseg/sort_order_internal.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const std::string_view message) {
  return common::Status{code, std::string{message}};
}

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return status(common::StatusCode::kInvalidArgument, message);
}

[[nodiscard]] common::Status corruption(const std::string_view message) {
  return status(common::StatusCode::kCorruption, message);
}

[[nodiscard]] common::Status exhausted(const std::string_view message) {
  return status(common::StatusCode::kResourceExhausted, message);
}

[[nodiscard]] std::size_t fixed_width(const schema::LogicalTypeKind kind) noexcept {
  using schema::LogicalTypeKind;
  switch (kind) {
  case LogicalTypeKind::kInt8:
  case LogicalTypeKind::kUInt8:
    return 1U;
  case LogicalTypeKind::kInt16:
  case LogicalTypeKind::kUInt16:
    return 2U;
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kUInt32:
  case LogicalTypeKind::kFloat32:
  case LogicalTypeKind::kDate:
    return 4U;
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kUInt64:
  case LogicalTypeKind::kFloat64:
  case LogicalTypeKind::kTimestampNs:
    return 8U;
  case LogicalTypeKind::kDecimal:
  case LogicalTypeKind::kUuid:
    return 16U;
  case LogicalTypeKind::kBool:
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
  case LogicalTypeKind::kBinary:
    return 0U;
  }
  return 0U;
}

template <typename Unsigned>
[[nodiscard]] common::Result<Unsigned> load_little_endian(const common::ByteView bytes) {
  if (bytes.size() != sizeof(Unsigned)) {
    return common::make_unexpected(corruption("CSEG compaction fixed cell width is invalid"));
  }
  Unsigned value = 0U;
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    const Unsigned byte = static_cast<Unsigned>(std::to_integer<std::uint8_t>(bytes[index]));
    const Unsigned shifted = static_cast<Unsigned>(byte << (index * 8U));
    value = static_cast<Unsigned>(value | shifted);
  }
  return value;
}

void store_little_endian(std::vector<std::byte>& bytes, const std::size_t offset,
                         const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
  }
}

void set_bit(std::vector<std::byte>& bytes, const std::uint32_t row) {
  bytes[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
}

struct LoadedPart {
  cseg::DecodedCsegPartView part;
  std::vector<std::vector<cseg::DecodedCsegPage>> granules;
};

struct RowRef {
  std::size_t part{};
  std::size_t granule{};
  std::uint32_t row{};
};

[[nodiscard]] const columnar::PhysicalColumnView&
column(const std::span<const LoadedPart> parts, const RowRef row, const std::size_t ordinal) {
  return parts[row.part].granules[row.granule][ordinal].physical();
}

[[nodiscard]] common::Result<cseg::detail::SortCellView>
sort_cell(const columnar::PhysicalColumnView& column_value, const std::uint32_t row) {
  const common::Result<columnar::ColumnCellView> cell = column_value.cell(row);
  if (!cell.has_value()) {
    return common::make_unexpected(cell.error());
  }
  if (cell->is_null()) {
    return cseg::detail::SortCellView{
        .is_null = true, .is_boolean = false, .boolean = false, .bytes = {}};
  }
  if (column_value.type().kind() == schema::LogicalTypeKind::kBool) {
    const common::Result<bool> value = cell->boolean();
    if (!value.has_value()) {
      return common::make_unexpected(value.error());
    }
    return cseg::detail::SortCellView{
        .is_null = false, .is_boolean = true, .boolean = *value, .bytes = {}};
  }
  const common::Result<common::ByteView> bytes = cell->bytes();
  if (!bytes.has_value()) {
    return common::make_unexpected(bytes.error());
  }
  return cseg::detail::SortCellView{
      .is_null = false, .is_boolean = false, .boolean = false, .bytes = *bytes};
}

struct Ordering {
  std::span<const std::uint32_t> key_ordinals;
  std::size_t system_start{};
};

// Left/right row pairs and their explicit column metadata are intentionally adjacent: keeping one
// comparison call-shaped value avoids duplicating the frozen tuple-ordering procedure.
[[nodiscard]] common::Result<int>
compare_column(const std::span<const LoadedPart> parts,
               const RowRef left, // NOLINT(bugprone-easily-swappable-parameters)
               const RowRef right, const std::size_t ordinal, const schema::LogicalTypeKind kind) {
  const common::Result<cseg::detail::SortCellView> lhs =
      sort_cell(column(parts, left, ordinal), left.row);
  const common::Result<cseg::detail::SortCellView> rhs =
      sort_cell(column(parts, right, ordinal), right.row);
  if (!lhs.has_value()) {
    return common::make_unexpected(lhs.error());
  }
  if (!rhs.has_value()) {
    return common::make_unexpected(rhs.error());
  }
  return cseg::detail::compare_sort_cells(kind, *lhs, *rhs);
}

[[nodiscard]] common::Result<int>
compare_rows(const std::span<const LoadedPart> parts,
             const RowRef left, // NOLINT(bugprone-easily-swappable-parameters)
             const RowRef right, const Ordering ordering) {
  for (const std::uint32_t ordinal : ordering.key_ordinals) {
    const common::Result<int> compared =
        compare_column(parts, left, right, ordinal, column(parts, left, ordinal).type().kind());
    if (!compared.has_value() || *compared != 0) {
      return compared;
    }
  }
  constexpr std::array kSystemKinds{schema::LogicalTypeKind::kUuid,
                                    schema::LogicalTypeKind::kUInt64,
                                    schema::LogicalTypeKind::kUInt32};
  for (std::size_t system = 0U; system < std::size(kSystemKinds); ++system) {
    const common::Result<int> compared =
        compare_column(parts, left, right, ordering.system_start + system, kSystemKinds[system]);
    if (!compared.has_value() || *compared != 0) {
      return compared;
    }
  }
  return 0;
}

[[nodiscard]] common::Result<std::vector<RowRef>>
stable_sorted_rows(const std::span<const LoadedPart> parts, std::vector<RowRef> current,
                   const Ordering ordering) {
  std::vector<RowRef> output(current.size());
  for (std::size_t width = 1U; width < current.size();) {
    for (std::size_t begin = 0U; begin < current.size(); begin += width * 2U) {
      const std::size_t middle = std::min(begin + width, current.size());
      const std::size_t end = std::min(begin + width * 2U, current.size());
      std::size_t left = begin;
      std::size_t right = middle;
      for (std::size_t out = begin; out < end; ++out) {
        if (right == end) {
          output[out] = current[left++];
        } else if (left == middle) {
          output[out] = current[right++];
        } else {
          const common::Result<int> compared =
              compare_rows(parts, current[left], current[right], ordering);
          if (!compared.has_value()) {
            return common::make_unexpected(compared.error());
          }
          output[out] = *compared <= 0 ? current[left++] : current[right++];
        }
      }
    }
    current.swap(output);
    if (width > current.size() / 2U) {
      break;
    }
    width *= 2U;
  }
  return current;
}

[[nodiscard]] common::Result<columnar::ColumnCellView>
cell(const std::span<const LoadedPart> parts, const RowRef row, const std::size_t ordinal) {
  return column(parts, row, ordinal).cell(row.row);
}

[[nodiscard]] common::Result<std::size_t>
variable_cell_size(const std::span<const LoadedPart> parts, const RowRef row,
                   const std::size_t ordinal) {
  const common::Result<columnar::ColumnCellView> value = cell(parts, row, ordinal);
  if (!value.has_value()) {
    return common::make_unexpected(value.error());
  }
  if (value->is_null()) {
    return 0U;
  }
  const common::Result<common::ByteView> bytes = value->bytes();
  if (!bytes.has_value()) {
    return common::make_unexpected(bytes.error());
  }
  return bytes->size();
}

[[nodiscard]] common::Result<std::int64_t> event_time(const std::span<const LoadedPart> parts,
                                                      const RowRef row,
                                                      const std::size_t event_ordinal) {
  const common::Result<columnar::ColumnCellView> value = cell(parts, row, event_ordinal);
  if (!value.has_value() || value->is_null()) {
    return common::make_unexpected(corruption("CSEG compaction event-time cell is invalid"));
  }
  const common::Result<common::ByteView> bytes = value->bytes();
  if (!bytes.has_value()) {
    return common::make_unexpected(bytes.error());
  }
  const common::Result<std::uint64_t> bits = load_little_endian<std::uint64_t>(*bytes);
  if (!bits.has_value()) {
    return common::make_unexpected(bits.error());
  }
  return std::bit_cast<std::int64_t>(*bits);
}

struct GranulePlan {
  std::size_t first{};
  std::uint32_t count{};
  std::int64_t minimum_event{};
  std::int64_t maximum_event{};
};

[[nodiscard]] common::Result<std::vector<GranulePlan>>
plan_granules(const std::span<const LoadedPart> parts, const std::span<const RowRef> rows,
              const std::span<const schema::ColumnDefinition> columns,
              const std::size_t event_ordinal) {
  std::vector<GranulePlan> plans;
  std::size_t first = 0U;
  while (first < rows.size()) {
    std::vector<std::uint64_t> variable_bytes(columns.size(), 0U);
    std::uint32_t count = 0U;
    std::int64_t minimum = std::numeric_limits<std::int64_t>::max();
    std::int64_t maximum = std::numeric_limits<std::int64_t>::min();
    while (first + count < rows.size() && count < cseg::format::kMaximumGranuleRowCount) {
      bool fits = true;
      for (std::size_t ordinal = 0U; ordinal < columns.size(); ++ordinal) {
        const schema::ColumnDefinition& definition = columns[ordinal];
        std::uint64_t next_variable = variable_bytes[ordinal];
        if (definition.type().is_variable_width()) {
          const common::Result<std::size_t> bytes =
              variable_cell_size(parts, rows[first + count], ordinal);
          if (!bytes.has_value() ||
              *bytes > std::numeric_limits<std::uint64_t>::max() - next_variable) {
            return common::make_unexpected(
                bytes.has_value() ? exhausted("CSEG compaction variable page length overflows")
                                  : bytes.error());
          }
          next_variable += *bytes;
        }
        const std::uint64_t next_count = static_cast<std::uint64_t>(count) + 1U;
        std::uint64_t length = definition.nullable() ? columnar::bitmap_size(count + 1U) : 0U;
        if (definition.type().is_variable_width()) {
          length += (next_count + 1U) * sizeof(std::uint32_t) + next_variable;
        } else if (definition.type().kind() == schema::LogicalTypeKind::kBool) {
          length += columnar::bitmap_size(count + 1U);
        } else {
          length += next_count * fixed_width(definition.type().kind());
        }
        if (length > cseg::format::kMaximumUncompressedPageLength) {
          fits = false;
          break;
        }
      }
      if (!fits) {
        break;
      }
      for (std::size_t ordinal = 0U; ordinal < columns.size(); ++ordinal) {
        if (columns[ordinal].type().is_variable_width()) {
          const common::Result<std::size_t> bytes =
              variable_cell_size(parts, rows[first + count], ordinal);
          if (!bytes.has_value()) {
            return common::make_unexpected(bytes.error());
          }
          variable_bytes[ordinal] += *bytes;
        }
      }
      const common::Result<std::int64_t> event =
          event_time(parts, rows[first + count], event_ordinal);
      if (!event.has_value()) {
        return common::make_unexpected(event.error());
      }
      minimum = std::min(minimum, *event);
      maximum = std::max(maximum, *event);
      ++count;
    }
    if (count == 0U) {
      return common::make_unexpected(exhausted("one compacted row exceeds the CSEG page limit"));
    }
    plans.push_back(
        {.first = first, .count = count, .minimum_event = minimum, .maximum_event = maximum});
    first += count;
  }
  return plans;
}

// These parameters describe one physical column and are kept explicit at the sole call site.
[[nodiscard]] common::Result<columnar::PhysicalColumnView> materialize_column(
    const std::span<const LoadedPart> parts, // NOLINT(bugprone-easily-swappable-parameters)
    const std::span<const RowRef> rows, const std::size_t ordinal, const schema::LogicalType type,
    const bool nullable, columnar::ColumnVectorBuffers& buffers) {
  const std::uint32_t count = static_cast<std::uint32_t>(rows.size());
  if (nullable) {
    buffers.validity.assign(columnar::bitmap_size(count), std::byte{0});
  }
  if (type.is_variable_width()) {
    buffers.offsets.assign((rows.size() + 1U) * sizeof(std::uint32_t), std::byte{0});
  } else if (type.kind() == schema::LogicalTypeKind::kBool) {
    buffers.values.assign(columnar::bitmap_size(count), std::byte{0});
  } else {
    buffers.values.assign(rows.size() * fixed_width(type.kind()), std::byte{0});
  }
  std::uint32_t null_count = 0U;
  for (std::uint32_t row = 0U; row < count; ++row) {
    const common::Result<columnar::ColumnCellView> value = cell(parts, rows[row], ordinal);
    if (!value.has_value()) {
      return common::make_unexpected(value.error());
    }
    if (value->is_null()) {
      ++null_count;
    } else {
      if (nullable) {
        set_bit(buffers.validity, row);
      }
      if (type.kind() == schema::LogicalTypeKind::kBool) {
        const common::Result<bool> boolean = value->boolean();
        if (!boolean.has_value()) {
          return common::make_unexpected(boolean.error());
        }
        if (*boolean) {
          set_bit(buffers.values, row);
        }
      } else {
        const common::Result<common::ByteView> bytes = value->bytes();
        if (!bytes.has_value()) {
          return common::make_unexpected(bytes.error());
        }
        if (type.is_variable_width()) {
          buffers.values.insert(buffers.values.end(), bytes->begin(), bytes->end());
        } else {
          const std::size_t width = fixed_width(type.kind());
          std::ranges::copy(*bytes,
                            buffers.values.begin() + static_cast<std::ptrdiff_t>(row * width));
        }
      }
    }
    if (type.is_variable_width()) {
      if (buffers.values.size() > std::numeric_limits<std::uint32_t>::max()) {
        return common::make_unexpected(exhausted("CSEG compaction offsets exceed uint32"));
      }
      store_little_endian(buffers.offsets,
                          (static_cast<std::size_t>(row) + 1U) * sizeof(std::uint32_t),
                          static_cast<std::uint32_t>(buffers.values.size()));
    }
  }
  return columnar::PhysicalColumnView::create(
      {.type = type, .nullable = nullable, .row_count = count, .null_count = null_count},
      {.validity = buffers.validity, .offsets = buffers.offsets, .values = buffers.values});
}

[[nodiscard]] common::Result<common::ByteView>
required_bytes(const std::span<const LoadedPart> parts, const RowRef row,
               const std::size_t ordinal) {
  const common::Result<columnar::ColumnCellView> value = cell(parts, row, ordinal);
  if (!value.has_value() || value->is_null()) {
    return common::make_unexpected(corruption("CSEG compaction system cell is invalid"));
  }
  return value->bytes();
}

[[nodiscard]] const TabletDescriptor* find_tablet(const DecodedManifestView& manifest,
                                                  const schema::TabletId& tablet_id) noexcept {
  const auto found =
      std::ranges::lower_bound(manifest.tablets(), tablet_id, {}, &TabletDescriptor::tablet_id);
  return found != manifest.tablets().end() && found->tablet_id == tablet_id ? &*found : nullptr;
}

[[nodiscard]] const PartDescriptor* find_part(const DecodedManifestView& manifest,
                                              const cseg::PartId& part_id) noexcept {
  const auto found = std::ranges::find(manifest.parts(), part_id, &PartDescriptor::part_id);
  return found == manifest.parts().end() ? nullptr : &*found;
}

[[nodiscard]] bool contains_input(const std::span<const CompactionPartImage> inputs,
                                  const cseg::PartId& part_id) noexcept {
  const auto found = std::ranges::lower_bound(inputs, part_id, {}, &CompactionPartImage::part_id);
  return found != inputs.end() && found->part_id == part_id;
}

} // namespace

common::Result<EncodedCompactionPart>
merge_append_only_cseg_v1(const AppendOnlyCompactionRequest& request) {
  if (request.inputs.empty() || request.limits.max_rows == 0U ||
      request.limits.max_materialized_page_bytes == 0U || !request.wal_id.is_valid()) {
    return common::make_unexpected(invalid("CSEG compaction request or limits are invalid"));
  }
  if (request.limits.equivalence.max_parts_per_side == 0U) {
    return common::make_unexpected(invalid("CSEG compaction input part limit is invalid"));
  }
  if (request.inputs.size() > request.limits.equivalence.max_parts_per_side) {
    return common::make_unexpected(exhausted("CSEG compaction input part limit exceeded"));
  }
  for (std::size_t index = 0U; index < request.inputs.size(); ++index) {
    if (request.inputs[index].bytes.empty() ||
        (index != 0U && !(request.inputs[index - 1U].part_id < request.inputs[index].part_id)) ||
        request.inputs[index].part_id == request.output_part_id) {
      return common::make_unexpected(
          invalid("CSEG compaction inputs must be sorted and output identity fresh"));
    }
  }
  try {
    const schema::TableSchema& schema = request.schema.get();
    std::vector<std::uint32_t> key_ordinals;
    key_ordinals.reserve(schema.physical_ordering_key().size());
    for (const schema::ColumnId& id : schema.physical_ordering_key()) {
      const std::optional<std::size_t> ordinal = schema.column_ordinal(id);
      if (!ordinal.has_value()) {
        return common::make_unexpected(invalid("CSEG compaction schema ordering key is invalid"));
      }
      key_ordinals.push_back(static_cast<std::uint32_t>(*ordinal));
    }
    const std::optional<std::size_t> event_ordinal =
        schema.column_ordinal(schema.event_time_column());
    if (!event_ordinal.has_value()) {
      return common::make_unexpected(invalid("CSEG compaction schema event time is invalid"));
    }

    std::vector<LoadedPart> parts;
    std::vector<RowRef> rows;
    parts.reserve(request.inputs.size());
    std::uint64_t total_rows = 0U;
    std::uint64_t materialized_bytes = 0U;
    for (const CompactionPartImage& image : request.inputs) {
      cseg::CsegPartDecodeResult decoded =
          cseg::decode_cseg_v1_part_exact(image.bytes, request.limits.equivalence.decode);
      if (!decoded.has_value()) {
        return common::make_unexpected(decoded.error().status());
      }
      if (decoded->metadata().part_id() != image.part_id) {
        return common::make_unexpected(
            invalid("CSEG compaction input identity disagrees with bytes"));
      }
      common::Status validated = cseg::validate_cseg_v1_part(*decoded, schema, request.tablet_id,
                                                             request.limits.equivalence.validation);
      if (!validated.is_ok()) {
        return common::make_unexpected(std::move(validated));
      }
      const std::optional<std::uint64_t> with_rows =
          common::checked_add(total_rows, decoded->metadata().row_count());
      if (!with_rows.has_value() || *with_rows > request.limits.max_rows ||
          *with_rows > request.limits.equivalence.max_rows_per_side) {
        return common::make_unexpected(exhausted("CSEG compaction row limit exceeded"));
      }
      total_rows = *with_rows;
      std::vector<std::vector<cseg::DecodedCsegPage>> granules;
      granules.reserve(decoded->metadata().granules().size());
      for (const cseg::CsegGranuleDescriptor& granule : decoded->metadata().granules()) {
        std::vector<cseg::DecodedCsegPage> pages;
        pages.reserve(decoded->metadata().columns().size());
        for (std::size_t ordinal = 0U; ordinal < decoded->metadata().columns().size(); ++ordinal) {
          const cseg::CsegPageDescriptor& descriptor =
              decoded->metadata()
                  .pages()[static_cast<std::size_t>(granule.first_page_index) + ordinal];
          const std::optional<std::uint64_t> with_page =
              common::checked_add(materialized_bytes, descriptor.uncompressed_length);
          if (!with_page.has_value() || *with_page > request.limits.max_materialized_page_bytes) {
            return common::make_unexpected(
                exhausted("CSEG compaction materialized-page limit exceeded"));
          }
          materialized_bytes = *with_page;
          common::Result<cseg::DecodedCsegPage> page =
              decoded->decode_page(static_cast<std::size_t>(granule.first_page_index) + ordinal);
          if (!page.has_value()) {
            return common::make_unexpected(page.error());
          }
          pages.push_back(std::move(*page));
        }
        granules.push_back(std::move(pages));
      }
      const std::size_t part_index = parts.size();
      for (std::size_t granule = 0U; granule < decoded->metadata().granules().size(); ++granule) {
        for (std::uint32_t row = 0U; row < decoded->metadata().granules()[granule].row_count;
             ++row) {
          rows.push_back({.part = part_index, .granule = granule, .row = row});
        }
      }
      parts.push_back({.part = std::move(*decoded), .granules = std::move(granules)});
    }
    if (rows.empty()) {
      return common::make_unexpected(invalid("CSEG compaction inputs contain no rows"));
    }
    const Ordering ordering{.key_ordinals = key_ordinals, .system_start = schema.columns().size()};
    common::Result<std::vector<RowRef>> sorted =
        stable_sorted_rows(parts, std::move(rows), ordering);
    if (!sorted.has_value()) {
      return common::make_unexpected(sorted.error());
    }
    for (std::size_t index = 1U; index < sorted->size(); ++index) {
      const common::Result<int> compared =
          compare_rows(parts, (*sorted)[index - 1U], (*sorted)[index], ordering);
      if (!compared.has_value()) {
        return common::make_unexpected(compared.error());
      }
      if (*compared == 0) {
        return common::make_unexpected(
            corruption("CSEG compaction inputs contain a duplicate physical row tuple"));
      }
    }
    common::Result<std::vector<GranulePlan>> plans =
        plan_granules(parts, *sorted, schema.columns(), *event_ordinal);
    if (!plans.has_value()) {
      return common::make_unexpected(plans.error());
    }

    const std::span<const cseg::CsegColumnDescriptor> descriptors =
        parts.front().part.metadata().columns();
    std::vector<cseg::CsegGranuleDescriptor> granules;
    std::vector<cseg::EncodedCsegPage> pages;
    granules.reserve(plans->size());
    const std::optional<std::size_t> page_count =
        common::checked_multiply(plans->size(), descriptors.size());
    if (!page_count.has_value() || *page_count > cseg::format::kMaximumPageCount) {
      return common::make_unexpected(exhausted("CSEG compaction output page count overflows"));
    }
    pages.reserve(*page_count);
    std::uint64_t first_row = 0U;
    for (const GranulePlan& plan : *plans) {
      granules.push_back({.first_row = first_row,
                          .row_count = plan.count,
                          .first_page_index = pages.size(),
                          .minimum_event_time = plan.minimum_event,
                          .maximum_event_time = plan.maximum_event});
      first_row += plan.count;
      const std::span<const RowRef> granule_rows =
          std::span<const RowRef>{*sorted}.subspan(plan.first, plan.count);
      for (std::size_t ordinal = 0U; ordinal < descriptors.size(); ++ordinal) {
        columnar::ColumnVectorBuffers buffers;
        common::Result<columnar::PhysicalColumnView> physical =
            materialize_column(parts, granule_rows, ordinal, descriptors[ordinal].logical_type,
                               descriptors[ordinal].nullable, buffers);
        if (!physical.has_value()) {
          return common::make_unexpected(physical.error());
        }
        common::Result<cseg::EncodedCsegPage> page =
            cseg::encode_cseg_v1_page(*physical, request.compression);
        if (!page.has_value()) {
          return common::make_unexpected(page.error());
        }
        pages.push_back(std::move(*page));
      }
    }
    const std::size_t system_start = schema.columns().size();
    std::uint64_t minimum_sequence = std::numeric_limits<std::uint64_t>::max();
    std::uint64_t maximum_sequence = 0U;
    std::int64_t minimum_event = std::numeric_limits<std::int64_t>::max();
    std::int64_t maximum_event = std::numeric_limits<std::int64_t>::min();
    for (const GranulePlan& plan : *plans) {
      minimum_event = std::min(minimum_event, plan.minimum_event);
      maximum_event = std::max(maximum_event, plan.maximum_event);
    }
    for (const RowRef row : *sorted) {
      const common::Result<common::ByteView> wal = required_bytes(parts, row, system_start);
      const common::Result<common::ByteView> sequence =
          required_bytes(parts, row, system_start + 1U);
      if (!wal.has_value() || !sequence.has_value()) {
        return common::make_unexpected(wal.has_value() ? sequence.error() : wal.error());
      }
      if (!std::ranges::equal(*wal, request.wal_id.bytes)) {
        return common::make_unexpected(invalid("CSEG compaction input uses an unexpected WAL"));
      }
      const common::Result<std::uint64_t> value = load_little_endian<std::uint64_t>(*sequence);
      if (!value.has_value()) {
        return common::make_unexpected(value.error());
      }
      minimum_sequence = std::min(minimum_sequence, *value);
      maximum_sequence = std::max(maximum_sequence, *value);
    }
    common::Result<cseg::EncodedCsegPart> encoded = cseg::encode_cseg_v1_part(
        {.part_id = request.output_part_id,
         .table_id = schema.table_id(),
         .tablet_id = request.tablet_id,
         .schema_id = schema.schema_id(),
         .schema_version = schema.version(),
         .row_count = total_rows,
         .event_time_column_ordinal = static_cast<std::uint32_t>(*event_ordinal),
         .ordering_column_count = static_cast<std::uint32_t>(key_ordinals.size()),
         .minimum_event_time = minimum_event,
         .maximum_event_time = maximum_event,
         .columns = descriptors,
         .granules = granules,
         .pages = pages});
    if (!encoded.has_value()) {
      return common::make_unexpected(encoded.error());
    }
    const std::array output_image{
        CompactionPartImage{.part_id = request.output_part_id, .bytes = encoded->bytes()}};
    common::Status equivalent = validate_append_only_cseg_v1_equivalence(
        request.inputs, output_image, schema, request.tablet_id, request.wal_id,
        request.limits.equivalence);
    if (!equivalent.is_ok()) {
      return common::make_unexpected(std::move(equivalent));
    }
    return EncodedCompactionPart{{.part_id = request.output_part_id,
                                  .table_id = schema.table_id(),
                                  .tablet_id = request.tablet_id,
                                  .schema_id = schema.schema_id(),
                                  .schema_version = schema.version(),
                                  .file_length = encoded->size(),
                                  .row_count = total_rows,
                                  .minimum_record_sequence = minimum_sequence,
                                  .maximum_record_sequence = maximum_sequence,
                                  .minimum_event_time = minimum_event,
                                  .maximum_event_time = maximum_event},
                                 request.wal_id,
                                 std::move(*encoded)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("CSEG compaction allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("CSEG compaction allocation length is unsupported"));
  }
}

common::Result<EncodedManifest>
build_manifest_v1_for_append_only_compaction(const AppendOnlyCompactionManifestBuildInput& input) {
  try {
    const DecodedManifestView& predecessor = input.predecessor.get();
    const EncodedCompactionPart& output = input.output.get();
    const schema::TableSchema& schema = input.schema.get();
    if (input.inputs.empty() || output.wal_id != predecessor.wal_id() ||
        output.descriptor.table_id != schema.table_id() ||
        output.descriptor.schema_id != schema.schema_id() ||
        output.descriptor.schema_version != schema.version()) {
      return common::make_unexpected(
          invalid("Manifest compaction input, WAL, or schema context is invalid"));
    }

    const std::array output_image{CompactionPartImage{.part_id = output.descriptor.part_id,
                                                      .bytes = output.encoded_part.bytes()}};
    common::Status validation = validate_append_only_cseg_v1_equivalence(
        input.inputs, output_image, schema, output.descriptor.tablet_id, predecessor.wal_id(),
        input.equivalence_limits);
    if (!validation.is_ok()) {
      return common::make_unexpected(std::move(validation));
    }
    const std::string output_name = part_file_name(output.descriptor.part_id);
    validation = validate_manifest_v1_part_image(
        output.descriptor, predecessor.wal_id(), schema,
        {.file_name = output_name, .bytes = output.encoded_part.bytes()},
        input.part_validation_limits);
    if (!validation.is_ok()) {
      return common::make_unexpected(std::move(validation));
    }

    const TabletDescriptor* target = find_tablet(predecessor, output.descriptor.tablet_id);
    if (target == nullptr || target->table_id != output.descriptor.table_id ||
        find_part(predecessor, output.descriptor.part_id) != nullptr) {
      return common::make_unexpected(
          invalid("Manifest compaction target is absent or output identity is not fresh"));
    }
    std::uint64_t input_rows = 0U;
    for (std::size_t index = 0U; index < input.inputs.size(); ++index) {
      if ((index != 0U && !(input.inputs[index - 1U].part_id < input.inputs[index].part_id)) ||
          !contains_input(input.inputs, input.inputs[index].part_id)) {
        return common::make_unexpected(
            invalid("Manifest compaction inputs are not strictly identity sorted"));
      }
      const PartDescriptor* descriptor = find_part(predecessor, input.inputs[index].part_id);
      if (descriptor == nullptr || descriptor->tablet_id != target->tablet_id ||
          descriptor->schema_id != output.descriptor.schema_id ||
          descriptor->schema_version != output.descriptor.schema_version) {
        return common::make_unexpected(
            invalid("Manifest compaction input is absent from the exact target schema"));
      }
      const std::optional<std::uint64_t> next_rows =
          common::checked_add(input_rows, descriptor->row_count);
      if (!next_rows.has_value()) {
        return common::make_unexpected(exhausted("Manifest compaction input row count overflows"));
      }
      input_rows = *next_rows;
    }
    if (input_rows != output.descriptor.row_count) {
      return common::make_unexpected(
          invalid("Manifest compaction output row count disagrees with its inputs"));
    }

    const std::optional<std::uint64_t> generation =
        common::checked_add(predecessor.generation(), std::uint64_t{1U});
    if (!generation.has_value()) {
      return common::make_unexpected(exhausted("Manifest compaction generation overflows"));
    }
    const std::uint64_t successor_generation = generation.value_or(0U);
    std::vector<TabletDescriptor> tablets(predecessor.tablets().begin(),
                                          predecessor.tablets().end());
    std::vector<PartDescriptor> parts;
    const std::optional<std::size_t> retained_count =
        common::checked_add(predecessor.parts().size() - input.inputs.size(), std::size_t{1U});
    if (!retained_count.has_value()) {
      return common::make_unexpected(exhausted("Manifest compaction part count overflows"));
    }
    parts.reserve(*retained_count);
    for (TabletDescriptor& tablet : tablets) {
      const std::size_t first = static_cast<std::size_t>(tablet.first_part_index);
      const std::span<const PartDescriptor> old_parts =
          predecessor.parts().subspan(first, static_cast<std::size_t>(tablet.part_count));
      tablet.first_part_index = parts.size();
      if (tablet.tablet_id == target->tablet_id) {
        for (const PartDescriptor& descriptor : old_parts) {
          if (!contains_input(input.inputs, descriptor.part_id)) {
            parts.push_back(descriptor);
          }
        }
        parts.push_back(output.descriptor);
        std::ranges::sort(
            std::span{parts}.subspan(static_cast<std::size_t>(tablet.first_part_index)), {},
            &PartDescriptor::part_id);
      } else {
        parts.insert(parts.end(), old_parts.begin(), old_parts.end());
      }
      tablet.part_count = parts.size() - static_cast<std::size_t>(tablet.first_part_index);
    }

    common::Result<EncodedManifest> encoded = encode_manifest_v1({
        .generation = successor_generation,
        .database_id = predecessor.database_id(),
        .wal_id = predecessor.wal_id(),
        .reclaim_checkpoint = predecessor.reclaim_checkpoint(),
        .tablets = tablets,
        .parts = parts,
        .retries = predecessor.retries(),
    });
    if (!encoded.has_value()) {
      return common::make_unexpected(encoded.error());
    }
    ManifestDecodeResult decoded = decode_manifest_v1_exact(encoded->bytes());
    if (!decoded.has_value()) {
      return common::make_unexpected(
          corruption("Generated compaction Manifest no longer decodes exactly"));
    }
    std::vector<cseg::PartId> input_ids;
    input_ids.reserve(input.inputs.size());
    for (const CompactionPartImage& image : input.inputs) {
      input_ids.push_back(image.part_id);
    }
    const std::array output_ids{output.descriptor.part_id};
    validation =
        validate_manifest_v1_compaction_transition(predecessor, *decoded, input.schema_bindings,
                                                   {.tablet_id = target->tablet_id,
                                                    .input_part_ids = input_ids,
                                                    .output_part_ids = output_ids});
    if (!validation.is_ok()) {
      return common::make_unexpected(std::move(validation));
    }
    return encoded;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Manifest compaction generation allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("Manifest compaction generation exceeds container limits"));
  }
}

} // namespace chronos::manifest
