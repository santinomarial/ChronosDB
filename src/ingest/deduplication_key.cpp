#include "deduplication_key.hpp"

#include "chronos/common/result.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::ingest::detail {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status internal(std::string message) {
  return common::Status{common::StatusCode::kInternal, std::move(message)};
}

struct KeyCell {
  bool is_boolean{};
  bool boolean{};
  common::ByteView bytes;
};

struct BatchKeyColumn {
  schema::LogicalType type;
  columnar::ColumnVectorView view;
};

struct HeadKeyColumn {
  schema::LogicalType type;
  head::HeadColumnView view;
};

struct BytePair {
  common::ByteView left;
  common::ByteView right;
};

struct RowPair {
  std::uint32_t left;
  std::uint32_t right;
};

struct CrossSourceRows {
  std::uint32_t batch;
  std::uint32_t head;
};

[[nodiscard]] common::Result<KeyCell> batch_cell(const BatchKeyColumn& column,
                                                 const std::uint32_t row) {
  common::Result<columnar::ColumnCellView> cell = column.view.cell(row);
  if (!cell.has_value()) {
    return common::make_unexpected(internal(
        "validated batch deduplication cell became inaccessible: " + cell.error().message()));
  }
  if (cell->is_null()) {
    return common::make_unexpected(
        internal("validated non-null batch deduplication key contains NULL"));
  }
  if (column.type.kind() == schema::LogicalTypeKind::kBool) {
    common::Result<bool> value = cell->boolean();
    if (!value.has_value()) {
      return common::make_unexpected(internal("batch deduplication BOOL cell is malformed"));
    }
    return KeyCell{.is_boolean = true, .boolean = *value, .bytes = {}};
  }
  common::Result<common::ByteView> value = cell->bytes();
  if (!value.has_value()) {
    return common::make_unexpected(internal("batch deduplication byte cell is malformed"));
  }
  return KeyCell{.is_boolean = false, .boolean = false, .bytes = *value};
}

[[nodiscard]] common::Result<KeyCell> head_cell(const HeadKeyColumn& column,
                                                const std::uint32_t row) {
  common::Result<head::HeadCellView> cell = column.view.cell(row);
  if (!cell.has_value()) {
    return common::make_unexpected(internal(
        "published head deduplication cell became inaccessible: " + cell.error().message()));
  }
  if (cell->is_null()) {
    return common::make_unexpected(
        internal("published non-null head deduplication key contains NULL"));
  }
  if (column.type.kind() == schema::LogicalTypeKind::kBool) {
    common::Result<bool> value = cell->boolean();
    if (!value.has_value()) {
      return common::make_unexpected(internal("head deduplication BOOL cell is malformed"));
    }
    return KeyCell{.is_boolean = true, .boolean = *value, .bytes = {}};
  }
  common::Result<common::ByteView> value = cell->bytes();
  if (!value.has_value()) {
    return common::make_unexpected(internal("head deduplication byte cell is malformed"));
  }
  return KeyCell{.is_boolean = false, .boolean = false, .bytes = *value};
}

template <typename Unsigned>
[[nodiscard]] common::Result<Unsigned> load_little_endian(const common::ByteView bytes) {
  if (bytes.size() != sizeof(Unsigned)) {
    return common::make_unexpected(internal("floating deduplication cell has the wrong width"));
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
[[nodiscard]] constexpr bool floating_nan(const Unsigned bits) noexcept {
  if constexpr (sizeof(Unsigned) == sizeof(std::uint32_t)) {
    return (bits & 0x7f800000U) == 0x7f800000U && (bits & 0x007fffffU) != 0U;
  } else {
    return (bits & 0x7ff0000000000000ULL) == 0x7ff0000000000000ULL &&
           (bits & 0x000fffffffffffffULL) != 0U;
  }
}

template <typename Unsigned>
[[nodiscard]] constexpr Unsigned normalize_floating_zero(const Unsigned bits) noexcept {
  constexpr Unsigned magnitude_mask = sizeof(Unsigned) == sizeof(std::uint32_t)
                                          ? static_cast<Unsigned>(0x7fffffffU)
                                          : static_cast<Unsigned>(0x7fffffffffffffffULL);
  return (bits & magnitude_mask) == 0U ? 0U : bits;
}

[[nodiscard]] int compare_bytes(const common::ByteView left,
                                const common::ByteView right) noexcept {
  const std::size_t common_size = std::min(left.size(), right.size());
  for (std::size_t index = 0U; index < common_size; ++index) {
    const std::uint8_t left_byte = std::to_integer<std::uint8_t>(left[index]);
    const std::uint8_t right_byte = std::to_integer<std::uint8_t>(right[index]);
    if (left_byte != right_byte) {
      return left_byte < right_byte ? -1 : 1;
    }
  }
  if (left.size() == right.size()) {
    return 0;
  }
  return left.size() < right.size() ? -1 : 1;
}

template <typename Unsigned>
[[nodiscard]] common::Result<int> compare_floating_cells(const BytePair values,
                                                         const RowPair rows) {
  common::Result<Unsigned> left_bits = load_little_endian<Unsigned>(values.left);
  common::Result<Unsigned> right_bits = load_little_endian<Unsigned>(values.right);
  if (!left_bits.has_value()) {
    return common::make_unexpected(left_bits.error());
  }
  if (!right_bits.has_value()) {
    return common::make_unexpected(right_bits.error());
  }
  const bool left_nan = floating_nan(*left_bits);
  const bool right_nan = floating_nan(*right_bits);
  const Unsigned normalized_left = normalize_floating_zero(*left_bits);
  const Unsigned normalized_right = normalize_floating_zero(*right_bits);
  if (normalized_left != normalized_right) {
    return normalized_left < normalized_right ? -1 : 1;
  }
  if (left_nan || right_nan) {
    if (rows.left == rows.right) {
      return 0;
    }
    return rows.left < rows.right ? -1 : 1;
  }
  return 0;
}

[[nodiscard]] common::Result<int> compare_batch_rows(const std::vector<BatchKeyColumn>& columns,
                                                     const std::uint32_t left_row,
                                                     const std::uint32_t right_row) {
  for (const BatchKeyColumn& column : columns) {
    common::Result<KeyCell> left = batch_cell(column, left_row);
    common::Result<KeyCell> right = batch_cell(column, right_row);
    if (!left.has_value()) {
      return common::make_unexpected(left.error());
    }
    if (!right.has_value()) {
      return common::make_unexpected(right.error());
    }
    if (left->is_boolean) {
      if (left->boolean != right->boolean) {
        return left->boolean ? 1 : -1;
      }
      continue;
    }
    common::Result<int> compared = 0;
    switch (column.type.kind()) {
    case schema::LogicalTypeKind::kFloat32:
      compared = compare_floating_cells<std::uint32_t>(
          BytePair{.left = left->bytes, .right = right->bytes},
          RowPair{.left = left_row, .right = right_row});
      break;
    case schema::LogicalTypeKind::kFloat64:
      compared = compare_floating_cells<std::uint64_t>(
          BytePair{.left = left->bytes, .right = right->bytes},
          RowPair{.left = left_row, .right = right_row});
      break;
    default:
      compared = compare_bytes(left->bytes, right->bytes);
      break;
    }
    if (!compared.has_value()) {
      return common::make_unexpected(compared.error());
    }
    if (*compared != 0) {
      return *compared;
    }
  }
  return 0;
}

template <typename Unsigned>
[[nodiscard]] common::Result<bool> floating_cells_equal(const BytePair values) {
  common::Result<Unsigned> left_bits = load_little_endian<Unsigned>(values.left);
  common::Result<Unsigned> right_bits = load_little_endian<Unsigned>(values.right);
  if (!left_bits.has_value()) {
    return common::make_unexpected(left_bits.error());
  }
  if (!right_bits.has_value()) {
    return common::make_unexpected(right_bits.error());
  }
  if (floating_nan(*left_bits) || floating_nan(*right_bits)) {
    return false;
  }
  return normalize_floating_zero(*left_bits) == normalize_floating_zero(*right_bits);
}

[[nodiscard]] common::Result<bool>
batch_head_rows_equal(const std::vector<BatchKeyColumn>& batch_columns,
                      const std::vector<HeadKeyColumn>& head_columns, const CrossSourceRows rows) {
  if (batch_columns.size() != head_columns.size()) {
    return common::make_unexpected(internal("deduplication key column counts diverged"));
  }
  for (std::size_t index = 0U; index < batch_columns.size(); ++index) {
    const BatchKeyColumn& batch_column = batch_columns[index];
    const HeadKeyColumn& head_column = head_columns[index];
    common::Result<KeyCell> batch_value = batch_cell(batch_column, rows.batch);
    common::Result<KeyCell> head_value = head_cell(head_column, rows.head);
    if (!batch_value.has_value()) {
      return common::make_unexpected(batch_value.error());
    }
    if (!head_value.has_value()) {
      return common::make_unexpected(head_value.error());
    }
    if (batch_value->is_boolean) {
      if (batch_value->boolean != head_value->boolean) {
        return false;
      }
      continue;
    }
    common::Result<bool> equal = false;
    switch (batch_column.type.kind()) {
    case schema::LogicalTypeKind::kFloat32:
      equal = floating_cells_equal<std::uint32_t>(
          BytePair{.left = batch_value->bytes, .right = head_value->bytes});
      break;
    case schema::LogicalTypeKind::kFloat64:
      equal = floating_cells_equal<std::uint64_t>(
          BytePair{.left = batch_value->bytes, .right = head_value->bytes});
      break;
    default:
      equal = compare_bytes(batch_value->bytes, head_value->bytes) == 0;
      break;
    }
    if (!equal.has_value()) {
      return common::make_unexpected(equal.error());
    }
    if (!*equal) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] common::Result<std::vector<BatchKeyColumn>>
batch_key_columns(const columnar::OwnedColumnarBatch& batch) {
  std::vector<BatchKeyColumn> columns;
  columns.reserve(batch.schema().deduplication_key().size());
  for (const schema::ColumnId& column_id : batch.schema().deduplication_key()) {
    const std::optional<std::size_t> ordinal = batch.schema().column_ordinal(column_id);
    if (!ordinal.has_value()) {
      return common::make_unexpected(internal("batch schema lost a deduplication key column"));
    }
    const columnar::OwnedColumnVector* const column = batch.column(*ordinal);
    if (column == nullptr || column->nullable()) {
      return common::make_unexpected(
          internal("batch deduplication key column violates schema invariants"));
    }
    columns.push_back(BatchKeyColumn{.type = column->type(), .view = column->view()});
  }
  return columns;
}

[[nodiscard]] common::Status
validate_unique_batch_keys(const columnar::OwnedColumnarBatch& batch,
                           const std::vector<BatchKeyColumn>& columns) {
  std::vector<std::uint32_t> order(batch.row_count());
  std::vector<std::uint32_t> scratch(batch.row_count());
  std::iota(order.begin(), order.end(), 0U);

  const std::size_t row_count = order.size();
  for (std::size_t width = 1U; width < row_count;) {
    for (std::size_t begin = 0U; begin < row_count;) {
      const std::size_t middle = begin + std::min(width, row_count - begin);
      const std::size_t end = middle + std::min(width, row_count - middle);
      std::size_t left = begin;
      std::size_t right = middle;
      std::size_t output = begin;
      while (left < middle && right < end) {
        common::Result<int> compared = compare_batch_rows(columns, order[left], order[right]);
        if (!compared.has_value()) {
          return compared.error();
        }
        scratch[output++] = *compared <= 0 ? order[left++] : order[right++];
      }
      while (left < middle) {
        scratch[output++] = order[left++];
      }
      while (right < end) {
        scratch[output++] = order[right++];
      }
      begin = end;
    }
    order.swap(scratch);
    if (width >= row_count - width) {
      break;
    }
    width *= 2U;
  }

  for (std::size_t index = 1U; index < order.size(); ++index) {
    common::Result<int> compared = compare_batch_rows(columns, order[index - 1U], order[index]);
    if (!compared.has_value()) {
      return compared.error();
    }
    if (*compared == 0) {
      return invalid("APPEND_ROWS batch contains a duplicate logical deduplication key");
    }
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status
validate_generation_conflicts(const columnar::OwnedColumnarBatch& batch,
                              const std::vector<BatchKeyColumn>& batch_columns,
                              const head::HeadSnapshot& generation) {
  if (generation.row_count() == 0U) {
    return common::Status::ok();
  }
  std::vector<HeadKeyColumn> head_columns;
  head_columns.reserve(batch.schema().deduplication_key().size());
  for (const schema::ColumnId& column_id : batch.schema().deduplication_key()) {
    const std::optional<std::size_t> ordinal = batch.schema().column_ordinal(column_id);
    if (!ordinal.has_value()) {
      return internal("batch schema lost a deduplication key column");
    }
    common::Result<head::HeadColumnView> column = generation.column(*ordinal);
    if (!column.has_value()) {
      return internal("published generation lost a deduplication key column");
    }
    if (column->column_id() != column_id || column->type() != batch.column(*ordinal)->type() ||
        column->nullable()) {
      return internal("published generation deduplication key diverged from the schema lineage");
    }
    head_columns.push_back(HeadKeyColumn{.type = column->type(), .view = *column});
  }

  for (std::uint32_t batch_row = 0U; batch_row < batch.row_count(); ++batch_row) {
    for (std::uint32_t head_row = 0U; head_row < generation.row_count(); ++head_row) {
      common::Result<bool> equal = batch_head_rows_equal(
          batch_columns, head_columns, CrossSourceRows{.batch = batch_row, .head = head_row});
      if (!equal.has_value()) {
        return equal.error();
      }
      if (*equal) {
        return invalid("APPEND_ROWS logical deduplication key conflicts with a visible tablet row");
      }
    }
  }
  return common::Status::ok();
}

} // namespace

common::Status
validate_append_deduplication(const columnar::OwnedColumnarBatch& batch,
                              const std::span<const head::HeadSnapshot> sealed_generations,
                              const head::HeadSnapshot& active_generation) {
  if (batch.schema().deduplication_key().empty()) {
    return common::Status::ok();
  }
  common::Result<std::vector<BatchKeyColumn>> columns = batch_key_columns(batch);
  if (!columns.has_value()) {
    return columns.error();
  }
  common::Status unique = validate_unique_batch_keys(batch, *columns);
  if (!unique.is_ok()) {
    return unique;
  }
  for (const head::HeadSnapshot& sealed : sealed_generations) {
    common::Status conflict = validate_generation_conflicts(batch, *columns, sealed);
    if (!conflict.is_ok()) {
      return conflict;
    }
  }
  return validate_generation_conflicts(batch, *columns, active_generation);
}

} // namespace chronos::ingest::detail
