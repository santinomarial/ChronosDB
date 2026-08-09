#include "chronos/manifest/sealed_head_flush.hpp"

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/validator.hpp"
#include "cseg/sort_order_internal.hpp"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <numeric>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
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
[[nodiscard]] common::Result<Unsigned> load_le(const common::ByteView bytes) {
  if (bytes.size() != sizeof(Unsigned)) {
    return common::make_unexpected(invalid("sealed-head cell has an invalid fixed width"));
  }
  Unsigned value = 0U;
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    const Unsigned byte = static_cast<Unsigned>(std::to_integer<std::uint8_t>(bytes[index]));
    const Unsigned shifted = static_cast<Unsigned>(byte << (index * 8U));
    value = static_cast<Unsigned>(value | shifted);
  }
  return value;
}

template <typename Unsigned>
void store_le(std::vector<std::byte>& bytes, const std::size_t offset, const Unsigned value) {
  for (std::size_t index = 0U; index < sizeof(Unsigned); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xFFU);
  }
}

void set_bit(std::vector<std::byte>& bytes, const std::uint32_t row) {
  bytes[static_cast<std::size_t>(row) / 8U] |= static_cast<std::byte>(1U << (row % 8U));
}

using SortCell = cseg::detail::SortCellView;

[[nodiscard]] common::Result<SortCell> cell(const head::HeadColumnView& column,
                                            const std::uint32_t row) {
  const common::Result<head::HeadCellView> value = column.cell(row);
  if (!value.has_value()) {
    return common::make_unexpected(value.error());
  }
  if (value->is_null()) {
    return SortCell{.is_null = true, .is_boolean = false, .boolean = false, .bytes = {}};
  }
  if (column.type().kind() == schema::LogicalTypeKind::kBool) {
    const common::Result<bool> boolean = value->boolean();
    if (!boolean.has_value()) {
      return common::make_unexpected(boolean.error());
    }
    return SortCell{.is_null = false, .is_boolean = true, .boolean = *boolean, .bytes = {}};
  }
  const common::Result<common::ByteView> bytes = value->bytes();
  if (!bytes.has_value()) {
    return common::make_unexpected(bytes.error());
  }
  return SortCell{.bytes = *bytes};
}

[[nodiscard]] int compare_bytes(const common::ByteView left,
                                const common::ByteView right) noexcept {
  for (std::size_t index = 0U; index < std::min(left.size(), right.size()); ++index) {
    const auto lhs = std::to_integer<std::uint8_t>(left[index]);
    const auto rhs = std::to_integer<std::uint8_t>(right[index]);
    if (lhs != rhs) {
      return lhs < rhs ? -1 : 1;
    }
  }
  return left.size() == right.size() ? 0 : (left.size() < right.size() ? -1 : 1);
}

struct RowOrderingContext {
  std::span<const head::HeadColumnView> columns;
  std::span<const std::uint32_t> key_ordinals;
  std::reference_wrapper<const head::HeadSnapshot> snapshot;
};

[[nodiscard]] common::Result<int> compare_rows(const RowOrderingContext& context,
                                               const std::uint32_t left,
                                               const std::uint32_t right) {
  for (const std::uint32_t ordinal : context.key_ordinals) {
    const common::Result<SortCell> lhs = cell(context.columns[ordinal], left);
    const common::Result<SortCell> rhs = cell(context.columns[ordinal], right);
    if (!lhs.has_value()) {
      return common::make_unexpected(lhs.error());
    }
    if (!rhs.has_value()) {
      return common::make_unexpected(rhs.error());
    }
    const common::Result<int> compared =
        cseg::detail::compare_sort_cells(context.columns[ordinal].type().kind(), *lhs, *rhs);
    if (!compared.has_value() || *compared != 0) {
      return compared;
    }
  }
  const common::Result<head::HeadRowMetadata> lhs = context.snapshot.get().row_metadata(left);
  const common::Result<head::HeadRowMetadata> rhs = context.snapshot.get().row_metadata(right);
  if (!lhs.has_value()) {
    return common::make_unexpected(lhs.error());
  }
  if (!rhs.has_value()) {
    return common::make_unexpected(rhs.error());
  }
  const int wal =
      compare_bytes(lhs->commit_position.wal_id.bytes, rhs->commit_position.wal_id.bytes);
  if (wal != 0) {
    return wal;
  }
  if (lhs->commit_position.record_sequence != rhs->commit_position.record_sequence) {
    return lhs->commit_position.record_sequence < rhs->commit_position.record_sequence ? -1 : 1;
  }
  return lhs->row_ordinal == rhs->row_ordinal ? 0 : (lhs->row_ordinal < rhs->row_ordinal ? -1 : 1);
}

[[nodiscard]] common::Result<std::vector<std::uint32_t>>
sorted_rows(const RowOrderingContext& context, const std::uint32_t row_count) {
  std::vector<std::uint32_t> current(row_count);
  std::vector<std::uint32_t> output(row_count);
  std::iota(current.begin(), current.end(), 0U);
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
          const common::Result<int> compared = compare_rows(context, current[left], current[right]);
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

[[nodiscard]] common::Result<std::int64_t> event_time(const head::HeadColumnView& column,
                                                      const std::uint32_t row) {
  const common::Result<SortCell> value = cell(column, row);
  if (!value.has_value()) {
    return common::make_unexpected(value.error());
  }
  const common::Result<std::uint64_t> bits = load_le<std::uint64_t>(value->bytes);
  if (!bits.has_value()) {
    return common::make_unexpected(bits.error());
  }
  return std::bit_cast<std::int64_t>(*bits);
}

struct GranulePlan {
  std::uint32_t first_sorted_row{};
  std::uint32_t row_count{};
  std::int64_t minimum_event_time{};
  std::int64_t maximum_event_time{};
};

struct WalBounds {
  wal::WalId wal_id;
  std::uint64_t minimum_sequence{};
  std::uint64_t maximum_sequence{};
};

[[nodiscard]] common::Result<WalBounds> wal_bounds(const head::HeadSnapshot& snapshot) {
  WalBounds result{.wal_id = {},
                   .minimum_sequence = std::numeric_limits<std::uint64_t>::max(),
                   .maximum_sequence = 0U};
  for (std::uint32_t row = 0U; row < snapshot.row_count(); ++row) {
    const common::Result<head::HeadRowMetadata> metadata = snapshot.row_metadata(row);
    if (!metadata.has_value()) {
      return common::make_unexpected(metadata.error());
    }
    if (metadata->commit_position.source != head::CommitSource::kWal) {
      return common::make_unexpected(invalid("CSEG v1 cannot encode a non-WAL commit position"));
    }
    if (row == 0U) {
      result.wal_id = metadata->commit_position.wal_id;
    } else if (metadata->commit_position.wal_id != result.wal_id) {
      return common::make_unexpected(invalid("sealed head contains mixed WAL identities"));
    }
    result.minimum_sequence =
        std::min(result.minimum_sequence, metadata->commit_position.record_sequence);
    result.maximum_sequence =
        std::max(result.maximum_sequence, metadata->commit_position.record_sequence);
  }
  return result;
}

[[nodiscard]] common::Result<std::vector<GranulePlan>>
plan_granules(const std::span<const head::HeadColumnView> columns,
              const std::span<const std::uint32_t> rows, const std::uint32_t event_ordinal) {
  std::vector<GranulePlan> plans;
  std::uint32_t first = 0U;
  while (first < rows.size()) {
    std::vector<std::uint64_t> variable_bytes(columns.size(), 0U);
    std::uint32_t count = 0U;
    std::int64_t minimum = std::numeric_limits<std::int64_t>::max();
    std::int64_t maximum = std::numeric_limits<std::int64_t>::min();
    while (count < rows.size() - static_cast<std::size_t>(first) &&
           count < cseg::format::kMaximumGranuleRowCount) {
      const std::size_t sorted_position = static_cast<std::size_t>(first) + count;
      const std::uint32_t source_row = rows[sorted_position];
      bool fits = true;
      for (std::size_t ordinal = 0U; ordinal < columns.size(); ++ordinal) {
        const head::HeadColumnView& column = columns[ordinal];
        std::uint64_t next_variable = variable_bytes[ordinal];
        if (column.type().is_variable_width()) {
          const common::Result<SortCell> value = cell(column, source_row);
          if (!value.has_value()) {
            return common::make_unexpected(value.error());
          }
          if (!value->is_null) {
            if (value->bytes.size() > std::numeric_limits<std::uint64_t>::max() - next_variable) {
              return common::make_unexpected(exhausted("CSEG variable page length overflows"));
            }
            next_variable += value->bytes.size();
          }
        }
        const std::uint64_t next_count = static_cast<std::uint64_t>(count) + 1U;
        std::uint64_t length = column.nullable() ? columnar::bitmap_size(count + 1U) : 0U;
        if (column.type().is_variable_width()) {
          length += (next_count + 1U) * sizeof(std::uint32_t) + next_variable;
        } else if (column.type().kind() == schema::LogicalTypeKind::kBool) {
          length += columnar::bitmap_size(count + 1U);
        } else {
          length += next_count * fixed_width(column.type().kind());
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
          const common::Result<SortCell> value = cell(columns[ordinal], source_row);
          if (!value.has_value()) {
            return common::make_unexpected(value.error());
          }
          if (!value->is_null) {
            variable_bytes[ordinal] += value->bytes.size();
          }
        }
      }
      const common::Result<std::int64_t> event = event_time(columns[event_ordinal], source_row);
      if (!event.has_value()) {
        return common::make_unexpected(event.error());
      }
      minimum = std::min(minimum, *event);
      maximum = std::max(maximum, *event);
      ++count;
    }
    if (count == 0U) {
      return common::make_unexpected(
          exhausted("one sealed-head row exceeds the CSEG v1 page limit"));
    }
    plans.push_back({.first_sorted_row = first,
                     .row_count = count,
                     .minimum_event_time = minimum,
                     .maximum_event_time = maximum});
    first += count;
  }
  return plans;
}

[[nodiscard]] common::Result<columnar::PhysicalColumnView>
materialize_user_page(const head::HeadColumnView& column,
                      const std::span<const std::uint32_t> source_rows,
                      columnar::ColumnVectorBuffers& buffers) {
  const std::uint32_t count = static_cast<std::uint32_t>(source_rows.size());
  if (column.nullable()) {
    buffers.validity.assign(columnar::bitmap_size(count), std::byte{0});
  }
  if (column.type().is_variable_width()) {
    buffers.offsets.assign((static_cast<std::size_t>(count) + 1U) * sizeof(std::uint32_t),
                           std::byte{0});
  } else if (column.type().kind() == schema::LogicalTypeKind::kBool) {
    buffers.values.assign(columnar::bitmap_size(count), std::byte{0});
  } else {
    buffers.values.assign(static_cast<std::size_t>(count) * fixed_width(column.type().kind()),
                          std::byte{0});
  }
  std::uint32_t null_count = 0U;
  for (std::uint32_t row = 0U; row < count; ++row) {
    const common::Result<SortCell> value = cell(column, source_rows[row]);
    if (!value.has_value()) {
      return common::make_unexpected(value.error());
    }
    if (value->is_null) {
      ++null_count;
    } else {
      if (column.nullable()) {
        set_bit(buffers.validity, row);
      }
      if (column.type().is_variable_width()) {
        buffers.values.insert(buffers.values.end(), value->bytes.begin(), value->bytes.end());
      } else if (column.type().kind() == schema::LogicalTypeKind::kBool) {
        if (value->boolean) {
          set_bit(buffers.values, row);
        }
      } else {
        const std::size_t width = fixed_width(column.type().kind());
        std::copy(value->bytes.begin(), value->bytes.end(),
                  buffers.values.begin() + static_cast<std::ptrdiff_t>(row * width));
      }
    }
    if (column.type().is_variable_width()) {
      if (buffers.values.size() > std::numeric_limits<std::uint32_t>::max()) {
        return common::make_unexpected(exhausted("CSEG variable page exceeds uint32 offsets"));
      }
      store_le(buffers.offsets, (static_cast<std::size_t>(row) + 1U) * sizeof(std::uint32_t),
               static_cast<std::uint32_t>(buffers.values.size()));
    }
  }
  return columnar::PhysicalColumnView::create(
      {.type = column.type(),
       .nullable = column.nullable(),
       .row_count = count,
       .null_count = null_count},
      {.validity = buffers.validity, .offsets = buffers.offsets, .values = buffers.values});
}

enum class SystemPage : std::uint8_t { kWalId, kSequence, kOrdinal, kOperation };

[[nodiscard]] common::Result<cseg::EncodedCsegPage>
encode_system_page(const head::HeadSnapshot& snapshot,
                   const std::span<const std::uint32_t> source_rows, const SystemPage system,
                   const cseg::PageCompression compression) {
  schema::LogicalTypeKind kind = schema::LogicalTypeKind::kUuid;
  std::size_t width = 16U;
  if (system == SystemPage::kSequence) {
    kind = schema::LogicalTypeKind::kUInt64;
    width = 8U;
  } else if (system == SystemPage::kOrdinal) {
    kind = schema::LogicalTypeKind::kUInt32;
    width = 4U;
  } else if (system == SystemPage::kOperation) {
    kind = schema::LogicalTypeKind::kUInt8;
    width = 1U;
  }
  std::vector<std::byte> values(source_rows.size() * width, std::byte{0});
  for (std::size_t row = 0U; row < source_rows.size(); ++row) {
    const common::Result<head::HeadRowMetadata> metadata = snapshot.row_metadata(source_rows[row]);
    if (!metadata.has_value()) {
      return common::make_unexpected(metadata.error());
    }
    const std::size_t offset = row * width;
    switch (system) {
    case SystemPage::kWalId:
      std::copy(metadata->commit_position.wal_id.bytes.begin(),
                metadata->commit_position.wal_id.bytes.end(),
                values.begin() + static_cast<std::ptrdiff_t>(offset));
      break;
    case SystemPage::kSequence:
      store_le(values, offset, metadata->commit_position.record_sequence);
      break;
    case SystemPage::kOrdinal:
      store_le(values, offset, metadata->row_ordinal);
      break;
    case SystemPage::kOperation:
      values[offset] = static_cast<std::byte>(cseg::format::kAppendRowsOperation);
      break;
    }
  }
  const schema::LogicalType type = schema::LogicalType::create(kind).value();
  const common::Result<columnar::PhysicalColumnView> physical =
      columnar::PhysicalColumnView::create(
          {.type = type,
           .nullable = false,
           .row_count = static_cast<std::uint32_t>(source_rows.size()),
           .null_count = 0U},
          {.validity = {}, .offsets = {}, .values = values});
  if (!physical.has_value()) {
    return common::make_unexpected(physical.error());
  }
  return cseg::encode_cseg_v1_page(*physical, compression);
}

[[nodiscard]] cseg::CsegColumnDescriptor system_descriptor(const cseg::StorageKind storage,
                                                           const schema::LogicalTypeKind kind) {
  return {.column_id = std::nullopt,
          .storage_kind = storage,
          .logical_type = schema::LogicalType::create(kind).value(),
          .nullable = false,
          .event_time = false,
          .schema_ordinal = std::nullopt,
          .ordering_ordinal = std::nullopt};
}

[[nodiscard]] common::Result<std::uint64_t>
validation_working_limit(const cseg::DecodedCsegMetadataView& metadata) {
  std::uint64_t bytes = 0U;
  for (const cseg::CsegPageDescriptor& page : metadata.pages()) {
    const std::optional<std::uint64_t> next = common::checked_add(bytes, page.uncompressed_length);
    if (!next.has_value()) {
      return common::make_unexpected(exhausted("CSEG validation working-byte limit overflows"));
    }
    bytes = *next;
  }
  const std::optional<std::uint64_t> with_boundary = common::checked_add(bytes, bytes);
  if (!with_boundary.has_value()) {
    return common::make_unexpected(exhausted("CSEG validation working-byte limit overflows"));
  }
  return *with_boundary;
}

[[nodiscard]] common::Status verify_user_cell(const head::HeadCellView& source,
                                              const columnar::ColumnCellView& encoded,
                                              const schema::LogicalTypeKind kind) {
  if (source.is_null() != encoded.is_null()) {
    return invalid("encoded CSEG changed a sealed-head NULL value");
  }
  if (source.is_null()) {
    return common::Status::ok();
  }
  if (kind == schema::LogicalTypeKind::kBool) {
    const common::Result<bool> source_value = source.boolean();
    const common::Result<bool> encoded_value = encoded.boolean();
    if (!source_value.has_value() || !encoded_value.has_value() ||
        *source_value != *encoded_value) {
      return invalid("encoded CSEG changed a sealed-head BOOL value");
    }
    return common::Status::ok();
  }
  const common::Result<common::ByteView> source_value = source.bytes();
  const common::Result<common::ByteView> encoded_value = encoded.bytes();
  if (!source_value.has_value() || !encoded_value.has_value() ||
      !std::ranges::equal(*source_value, *encoded_value)) {
    return invalid("encoded CSEG changed a sealed-head byte value");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<common::ByteView> system_cell_bytes(const cseg::DecodedCsegPage& page,
                                                                 const std::uint32_t row) {
  const common::Result<columnar::ColumnCellView> cell = page.physical().cell(row);
  if (!cell.has_value()) {
    return common::make_unexpected(cell.error());
  }
  return cell->bytes();
}

[[nodiscard]] common::Status
verify_encoded_rows(const cseg::DecodedCsegPartView& part, const head::HeadSnapshot& snapshot,
                    const std::span<const head::HeadColumnView> columns,
                    const std::span<const std::uint32_t> sorted_rows) {
  std::size_t sorted_position = 0U;
  for (const cseg::CsegGranuleDescriptor& granule : part.metadata().granules()) {
    if (sorted_position > sorted_rows.size() ||
        granule.row_count > sorted_rows.size() - sorted_position) {
      return invalid("encoded CSEG row range exceeds the sealed-head row multiset");
    }
    for (std::size_t column = 0U; column < columns.size(); ++column) {
      const std::size_t page_index = static_cast<std::size_t>(granule.first_page_index) + column;
      common::Result<cseg::DecodedCsegPage> page = part.decode_page(page_index);
      if (!page.has_value()) {
        return page.error();
      }
      for (std::uint32_t row = 0U; row < granule.row_count; ++row) {
        const std::uint32_t source_row = sorted_rows[sorted_position + row];
        const common::Result<head::HeadCellView> source = columns[column].cell(source_row);
        const common::Result<columnar::ColumnCellView> encoded = page->physical().cell(row);
        if (!source.has_value()) {
          return source.error();
        }
        if (!encoded.has_value()) {
          return encoded.error();
        }
        common::Status same = verify_user_cell(*source, *encoded, columns[column].type().kind());
        if (!same.is_ok()) {
          return same;
        }
      }
    }

    std::vector<cseg::DecodedCsegPage> system_pages;
    system_pages.reserve(cseg::format::kSystemColumnCount);
    for (std::size_t system = 0U; system < cseg::format::kSystemColumnCount; ++system) {
      common::Result<cseg::DecodedCsegPage> page = part.decode_page(
          static_cast<std::size_t>(granule.first_page_index) + columns.size() + system);
      if (!page.has_value()) {
        return page.error();
      }
      system_pages.push_back(std::move(*page));
    }
    for (std::uint32_t row = 0U; row < granule.row_count; ++row) {
      const std::uint32_t source_row = sorted_rows[sorted_position + row];
      const common::Result<head::HeadRowMetadata> source = snapshot.row_metadata(source_row);
      if (!source.has_value()) {
        return source.error();
      }
      const common::Result<common::ByteView> wal = system_cell_bytes(system_pages[0], row);
      const common::Result<common::ByteView> sequence = system_cell_bytes(system_pages[1], row);
      const common::Result<common::ByteView> ordinal = system_cell_bytes(system_pages[2], row);
      const common::Result<common::ByteView> operation = system_cell_bytes(system_pages[3], row);
      if (!wal.has_value() || !sequence.has_value() || !ordinal.has_value() ||
          !operation.has_value()) {
        return invalid("encoded CSEG system row is inaccessible");
      }
      const common::Result<std::uint64_t> sequence_value = load_le<std::uint64_t>(*sequence);
      const common::Result<std::uint32_t> ordinal_value = load_le<std::uint32_t>(*ordinal);
      if (!sequence_value.has_value() || !ordinal_value.has_value() ||
          !std::ranges::equal(*wal, source->commit_position.wal_id.bytes) ||
          *sequence_value != source->commit_position.record_sequence ||
          *ordinal_value != source->row_ordinal ||
          source->operation != head::HeadOperationKind::kAppendRows || operation->size() != 1U ||
          std::to_integer<std::uint8_t>(operation->front()) != cseg::format::kAppendRowsOperation) {
        return invalid("encoded CSEG changed sealed-head system row identity");
      }
    }
    sorted_position += granule.row_count;
  }
  if (sorted_position != sorted_rows.size()) {
    return invalid("encoded CSEG did not preserve the complete sealed-head row multiset");
  }
  return common::Status::ok();
}

} // namespace

common::Result<EncodedSealedHeadPart> encode_sealed_head_v1(const SealedHeadFlushRequest& request) {
  const head::HeadSnapshot& snapshot = request.snapshot.get();
  if (!snapshot.is_sealed() || snapshot.row_count() == 0U) {
    return common::make_unexpected(invalid("CSEG flush requires a nonempty sealed head snapshot"));
  }
  try {
    const common::Result<WalBounds> wal = wal_bounds(snapshot);
    if (!wal.has_value()) {
      return common::make_unexpected(wal.error());
    }
    const std::shared_ptr<const schema::TableSchema>& schema = snapshot.schema_ptr();
    std::vector<head::HeadColumnView> columns;
    columns.reserve(snapshot.column_count());
    for (std::size_t ordinal = 0U; ordinal < snapshot.column_count(); ++ordinal) {
      common::Result<head::HeadColumnView> column = snapshot.column(ordinal);
      if (!column.has_value()) {
        return common::make_unexpected(column.error());
      }
      columns.push_back(*column);
    }
    const std::optional<std::size_t> event = schema->column_ordinal(schema->event_time_column());
    if (!event.has_value() || *event > std::numeric_limits<std::uint32_t>::max()) {
      return common::make_unexpected(invalid("sealed-head schema has no valid event-time ordinal"));
    }
    std::vector<std::uint32_t> key_ordinals;
    for (const schema::ColumnId& id : schema->physical_ordering_key()) {
      const std::optional<std::size_t> ordinal = schema->column_ordinal(id);
      if (!ordinal.has_value() || *ordinal > std::numeric_limits<std::uint32_t>::max()) {
        return common::make_unexpected(invalid("sealed-head schema has an invalid ordering key"));
      }
      key_ordinals.push_back(static_cast<std::uint32_t>(*ordinal));
    }
    common::Result<std::vector<std::uint32_t>> rows =
        sorted_rows({.columns = columns, .key_ordinals = key_ordinals, .snapshot = snapshot},
                    snapshot.row_count());
    if (!rows.has_value()) {
      return common::make_unexpected(rows.error());
    }
    common::Result<std::vector<GranulePlan>> plans =
        plan_granules(columns, *rows, static_cast<std::uint32_t>(*event));
    if (!plans.has_value()) {
      return common::make_unexpected(plans.error());
    }

    std::vector<cseg::CsegColumnDescriptor> descriptors;
    descriptors.reserve(columns.size() + cseg::format::kSystemColumnCount);
    for (std::size_t ordinal = 0U; ordinal < columns.size(); ++ordinal) {
      const auto key = std::find(key_ordinals.begin(), key_ordinals.end(), ordinal);
      descriptors.push_back(
          {.column_id = columns[ordinal].column_id(),
           .storage_kind = cseg::StorageKind::kUser,
           .logical_type = columns[ordinal].type(),
           .nullable = columns[ordinal].nullable(),
           .event_time = ordinal == *event,
           .schema_ordinal = static_cast<std::uint32_t>(ordinal),
           .ordering_ordinal = key == key_ordinals.end()
                                   ? std::optional<std::uint32_t>{}
                                   : static_cast<std::uint32_t>(key - key_ordinals.begin())});
    }
    descriptors.push_back(
        system_descriptor(cseg::StorageKind::kWalId, schema::LogicalTypeKind::kUuid));
    descriptors.push_back(
        system_descriptor(cseg::StorageKind::kRecordSequence, schema::LogicalTypeKind::kUInt64));
    descriptors.push_back(
        system_descriptor(cseg::StorageKind::kRowOrdinal, schema::LogicalTypeKind::kUInt32));
    descriptors.push_back(
        system_descriptor(cseg::StorageKind::kOperation, schema::LogicalTypeKind::kUInt8));

    std::vector<cseg::CsegGranuleDescriptor> granules;
    std::vector<cseg::EncodedCsegPage> pages;
    granules.reserve(plans->size());
    const std::optional<std::size_t> page_count =
        common::checked_multiply(plans->size(), descriptors.size());
    if (!page_count.has_value()) {
      return common::make_unexpected(exhausted("CSEG page count overflows container limits"));
    }
    pages.reserve(*page_count);
    std::uint64_t first_row = 0U;
    for (const GranulePlan& plan : *plans) {
      const std::span<const std::uint32_t> source_rows =
          std::span<const std::uint32_t>{*rows}.subspan(plan.first_sorted_row, plan.row_count);
      granules.push_back({.first_row = first_row,
                          .row_count = plan.row_count,
                          .first_page_index = pages.size(),
                          .minimum_event_time = plan.minimum_event_time,
                          .maximum_event_time = plan.maximum_event_time});
      first_row += plan.row_count;
      for (const head::HeadColumnView& column : columns) {
        columnar::ColumnVectorBuffers buffers;
        common::Result<columnar::PhysicalColumnView> physical =
            materialize_user_page(column, source_rows, buffers);
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
      for (const SystemPage system : {SystemPage::kWalId, SystemPage::kSequence,
                                      SystemPage::kOrdinal, SystemPage::kOperation}) {
        common::Result<cseg::EncodedCsegPage> page =
            encode_system_page(snapshot, source_rows, system, request.compression);
        if (!page.has_value()) {
          return common::make_unexpected(page.error());
        }
        pages.push_back(std::move(*page));
      }
    }

    std::int64_t minimum_event = std::numeric_limits<std::int64_t>::max();
    std::int64_t maximum_event = std::numeric_limits<std::int64_t>::min();
    for (const GranulePlan& plan : *plans) {
      minimum_event = std::min(minimum_event, plan.minimum_event_time);
      maximum_event = std::max(maximum_event, plan.maximum_event_time);
    }
    common::Result<cseg::EncodedCsegPart> encoded = cseg::encode_cseg_v1_part(
        {.part_id = request.part_id,
         .table_id = snapshot.table_id(),
         .tablet_id = snapshot.tablet_id(),
         .schema_id = schema->schema_id(),
         .schema_version = schema->version(),
         .row_count = snapshot.row_count(),
         .event_time_column_ordinal = static_cast<std::uint32_t>(*event),
         .ordering_column_count = static_cast<std::uint32_t>(key_ordinals.size()),
         .minimum_event_time = minimum_event,
         .maximum_event_time = maximum_event,
         .columns = descriptors,
         .granules = granules,
         .pages = pages});
    if (!encoded.has_value()) {
      return common::make_unexpected(encoded.error());
    }
    cseg::CsegPartDecodeResult decoded = cseg::decode_cseg_v1_part_exact(encoded->bytes());
    if (!decoded.has_value()) {
      return common::make_unexpected(decoded.error().status());
    }
    common::Status rows_verified = verify_encoded_rows(*decoded, snapshot, columns, *rows);
    if (!rows_verified.is_ok()) {
      return common::make_unexpected(std::move(rows_verified));
    }
    const common::Result<std::uint64_t> working_limit =
        validation_working_limit(decoded->metadata());
    if (!working_limit.has_value()) {
      return common::make_unexpected(working_limit.error());
    }
    common::Status validated = cseg::validate_cseg_v1_part(*decoded, *schema, snapshot.tablet_id(),
                                                           {.max_working_bytes = *working_limit});
    if (!validated.is_ok()) {
      return common::make_unexpected(std::move(validated));
    }
    return EncodedSealedHeadPart{{.part_id = request.part_id,
                                  .table_id = snapshot.table_id(),
                                  .tablet_id = snapshot.tablet_id(),
                                  .schema_id = schema->schema_id(),
                                  .schema_version = schema->version(),
                                  .file_length = static_cast<std::uint64_t>(encoded->size()),
                                  .row_count = snapshot.row_count(),
                                  .minimum_record_sequence = wal->minimum_sequence,
                                  .maximum_record_sequence = wal->maximum_sequence,
                                  .minimum_event_time = minimum_event,
                                  .maximum_event_time = maximum_event},
                                 wal->wal_id,
                                 std::move(*encoded)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("sealed-head flush allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("sealed-head flush exceeds container limits"));
  }
}

} // namespace chronos::manifest
