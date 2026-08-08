#include "chronos/query/asof_join.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"
#include "chronos/query/row_version.hpp"
#include "chronos/query/value.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

inline constexpr std::size_t kConservativeAllocationOverheadBytes = 64U;

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

[[nodiscard]] common::Status exhausted(const std::string_view message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::string{message}};
}

[[nodiscard]] common::Status internal(const std::string_view message) {
  return common::Status{common::StatusCode::kInternal, std::string{message}};
}

template <typename Value>
[[nodiscard]] const Value* optional_pointer(const std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
}

[[nodiscard]] common::Result<std::size_t> add_bytes(const std::size_t left, const std::size_t right,
                                                    const std::string_view message) {
  const std::optional<std::size_t> sum = common::checked_add(left, right);
  if (!sum.has_value())
    return common::make_unexpected(exhausted(message));
  return *sum;
}

[[nodiscard]] common::Result<std::size_t>
multiply_bytes(const std::size_t count, const std::size_t width, const std::string_view message) {
  const std::optional<std::size_t> product = common::checked_multiply(count, width);
  if (!product.has_value())
    return common::make_unexpected(exhausted(message));
  return *product;
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

void set_bit(std::vector<std::byte>& bytes, const std::uint32_t row) {
  bytes[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
}

void store_u32_le(std::vector<std::byte>& bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t byte = 0U; byte < sizeof(value); ++byte)
    bytes[offset + byte] = static_cast<std::byte>((value >> (byte * 8U)) & 0xffU);
}

struct BufferedRow {
  std::size_t chunk_ordinal{};
  std::size_t selected_row{};
};

struct JoinedRow {
  BufferedRow left;
  BufferedRow right;
  bool matched{};
};

[[nodiscard]] common::Result<std::size_t> state_charge(const AsofJoinLimits& limits) {
  common::Result<std::size_t> left_chunks =
      multiply_bytes(limits.maximum_left_rows, sizeof(AccountedVectorChunk),
                     "ASOF left retained-chunk state size overflowed");
  if (!left_chunks.has_value())
    return left_chunks;
  common::Result<std::size_t> right_chunks =
      multiply_bytes(limits.maximum_right_rows, sizeof(AccountedVectorChunk),
                     "ASOF right retained-chunk state size overflowed");
  if (!right_chunks.has_value())
    return right_chunks;
  common::Result<std::size_t> left_rows =
      multiply_bytes(limits.maximum_left_rows, sizeof(BufferedRow),
                     "ASOF left row-reference state size overflowed");
  if (!left_rows.has_value())
    return left_rows;
  common::Result<std::size_t> right_rows =
      multiply_bytes(limits.maximum_right_rows, sizeof(BufferedRow),
                     "ASOF right row-reference state size overflowed");
  if (!right_rows.has_value())
    return right_rows;
  common::Result<std::size_t> joined =
      multiply_bytes(limits.maximum_left_rows, sizeof(JoinedRow),
                     "ASOF joined row-reference state size overflowed");
  if (!joined.has_value())
    return joined;
  std::size_t total = 0U;
  for (const std::size_t bytes : {*left_chunks, *right_chunks, *left_rows, *right_rows, *joined}) {
    common::Result<std::size_t> next =
        add_bytes(total, bytes, "ASOF operator state size overflowed");
    if (!next.has_value())
      return next;
    total = *next;
  }
  common::Result<std::size_t> overhead = multiply_bytes(5U, kConservativeAllocationOverheadBytes,
                                                        "ASOF operator state overhead overflowed");
  if (!overhead.has_value())
    return overhead;
  return add_bytes(total, *overhead, "ASOF operator state size overflowed");
}

[[nodiscard]] common::Result<void> validate_limits(const AsofJoinLimits& limits) {
  if (limits.maximum_left_rows == 0U || limits.maximum_right_rows == 0U ||
      limits.maximum_equality_keys == 0U || limits.maximum_physical_ordering_keys == 0U ||
      limits.maximum_state_bytes == 0U || limits.output_limits.maximum_rows == 0U ||
      limits.output_limits.maximum_columns == 0U ||
      limits.output_limits.maximum_buffer_bytes == 0U ||
      limits.output_limits.maximum_retained_buffer_bytes == 0U) {
    return common::make_unexpected(invalid("ASOF join limits must be nonzero"));
  }
  common::Result<std::size_t> charge = state_charge(limits);
  if (!charge.has_value())
    return common::make_unexpected(charge.error());
  if (*charge > limits.maximum_state_bytes)
    return common::make_unexpected(exhausted("ASOF join state exceeds its byte limit"));
  return {};
}

[[nodiscard]] bool valid_ordinal(const std::size_t ordinal,
                                 const std::vector<VectorAsofColumnShape>& columns) noexcept {
  return ordinal < columns.size();
}

[[nodiscard]] common::Result<void> validate_definition(const VectorAsofJoinDefinition& definition,
                                                       const AsofJoinLimits& limits) {
  common::Result<void> valid = validate_limits(limits);
  if (!valid.has_value())
    return valid;
  if (definition.equality_keys.empty())
    return common::make_unexpected(invalid("ASOF join requires an equality key"));
  if (definition.right_physical_ordering_key_ordinals.empty()) {
    return common::make_unexpected(invalid("ASOF join requires a right physical ordering key"));
  }
  if (definition.equality_keys.size() > limits.maximum_equality_keys ||
      definition.equality_keys.capacity() > limits.maximum_equality_keys ||
      definition.right_physical_ordering_key_ordinals.size() >
          limits.maximum_physical_ordering_keys ||
      definition.right_physical_ordering_key_ordinals.capacity() >
          limits.maximum_physical_ordering_keys) {
    return common::make_unexpected(exhausted("ASOF join key configuration exceeds its limit"));
  }
  const std::optional<std::size_t> outputs =
      common::checked_add(definition.left_output_column_ordinals.size(),
                          definition.right_output_column_ordinals.size());
  const std::optional<std::size_t> outputs_with_presence =
      outputs.has_value() ? common::checked_add(*outputs, std::size_t{1U}) : std::nullopt;
  if (!outputs_with_presence.has_value() ||
      *outputs_with_presence > limits.output_limits.maximum_columns) {
    return common::make_unexpected(exhausted("ASOF join output width exceeds its limit"));
  }
  if (!valid_ordinal(definition.left_timestamp_column_ordinal, definition.left_input_columns) ||
      definition.left_input_columns[definition.left_timestamp_column_ordinal].type.kind() !=
          schema::LogicalTypeKind::kTimestampNs ||
      !valid_ordinal(definition.right_timestamp_column_ordinal, definition.right_input_columns) ||
      definition.right_input_columns[definition.right_timestamp_column_ordinal].type.kind() !=
          schema::LogicalTypeKind::kTimestampNs) {
    return common::make_unexpected(
        invalid("ASOF join timestamps must be in-range TIMESTAMP_NS columns"));
  }
  for (const VectorAsofEqualityKey& key : definition.equality_keys) {
    if (!valid_ordinal(key.left_column_ordinal, definition.left_input_columns) ||
        !valid_ordinal(key.right_column_ordinal, definition.right_input_columns) ||
        definition.left_input_columns[key.left_column_ordinal].type !=
            definition.right_input_columns[key.right_column_ordinal].type) {
      return common::make_unexpected(invalid("ASOF join equality key shapes must match exactly"));
    }
  }
  for (const std::size_t ordinal : definition.right_physical_ordering_key_ordinals) {
    if (!valid_ordinal(ordinal, definition.right_input_columns))
      return common::make_unexpected(invalid("ASOF join right physical key is out of range"));
  }
  for (const std::size_t ordinal : definition.left_output_column_ordinals) {
    if (!valid_ordinal(ordinal, definition.left_input_columns))
      return common::make_unexpected(invalid("ASOF join left output is out of range"));
  }
  for (const std::size_t ordinal : definition.right_output_column_ordinals) {
    if (!valid_ordinal(ordinal, definition.right_input_columns))
      return common::make_unexpected(invalid("ASOF join right output is out of range"));
  }
  common::Result<VectorRowVersionLayout> suffix =
      vector_row_version_layout(definition.right_row_version_first_column_ordinal);
  if (!suffix.has_value())
    return common::make_unexpected(suffix.error());
  const std::array<std::pair<std::size_t, VectorRowVersionColumnKind>, 4U> suffix_columns{{
      {suffix->wal_id_column_ordinal(), VectorRowVersionColumnKind::kWalId},
      {suffix->record_sequence_column_ordinal(), VectorRowVersionColumnKind::kRecordSequence},
      {suffix->row_ordinal_column_ordinal(), VectorRowVersionColumnKind::kRowOrdinal},
      {suffix->operation_column_ordinal(), VectorRowVersionColumnKind::kOperation},
  }};
  for (const auto& [ordinal, kind] : suffix_columns) {
    common::Result<schema::LogicalType> expected = vector_row_version_column_type(kind);
    if (!expected.has_value())
      return common::make_unexpected(expected.error());
    if (!valid_ordinal(ordinal, definition.right_input_columns) ||
        definition.right_input_columns[ordinal].type != *expected ||
        definition.right_input_columns[ordinal].nullable) {
      return common::make_unexpected(invalid("ASOF join right row-version suffix is invalid"));
    }
  }
  return {};
}

[[nodiscard]] common::Result<void>
validate_chunk_shape(const VectorChunk& chunk, const std::vector<VectorAsofColumnShape>& expected) {
  if (chunk.column_count() != expected.size())
    return common::make_unexpected(invalid("ASOF join input column count changed"));
  for (std::size_t ordinal = 0U; ordinal < expected.size(); ++ordinal) {
    const columnar::PhysicalColumnView* column = chunk.column(ordinal);
    if (column == nullptr || column->type() != expected[ordinal].type ||
        column->nullable() != expected[ordinal].nullable) {
      return common::make_unexpected(invalid("ASOF join input shape changed"));
    }
  }
  return {};
}

[[nodiscard]] common::Result<columnar::ColumnCellView>
row_cell(const std::vector<AccountedVectorChunk>& chunks, const BufferedRow row,
         const std::size_t ordinal) {
  if (row.chunk_ordinal >= chunks.size())
    return common::make_unexpected(internal("ASOF join row references an absent chunk"));
  return chunks[row.chunk_ordinal].chunk().cell(
      {.column_ordinal = ordinal, .selected_row = row.selected_row});
}

[[nodiscard]] common::Result<bool> is_nan(const schema::LogicalType type,
                                          const columnar::ColumnCellView& cell) {
  if (cell.is_null())
    return false;
  if (type.kind() != schema::LogicalTypeKind::kFloat32 &&
      type.kind() != schema::LogicalTypeKind::kFloat64) {
    return false;
  }
  common::Result<ScalarValue> value = ScalarValue::from_column_cell(type, cell);
  if (!value.has_value())
    return common::make_unexpected(value.error());
  if (const auto* single = std::get_if<float>(&value->storage()); single != nullptr)
    return std::isnan(*single);
  const auto* wide = std::get_if<double>(&value->storage());
  if (wide == nullptr)
    return common::make_unexpected(internal("ASOF floating key has invalid storage"));
  return std::isnan(*wide);
}

// Left and right rows are the conventional operands of one join comparison.
// NOLINTBEGIN(bugprone-easily-swappable-parameters)
[[nodiscard]] common::Result<bool> keys_match(const std::vector<AccountedVectorChunk>& left_chunks,
                                              const std::vector<AccountedVectorChunk>& right_chunks,
                                              const BufferedRow left, const BufferedRow right,
                                              const VectorAsofJoinDefinition& definition) {
  for (const VectorAsofEqualityKey& key : definition.equality_keys) {
    common::Result<columnar::ColumnCellView> left_cell =
        row_cell(left_chunks, left, key.left_column_ordinal);
    if (!left_cell.has_value())
      return common::make_unexpected(left_cell.error());
    common::Result<columnar::ColumnCellView> right_cell =
        row_cell(right_chunks, right, key.right_column_ordinal);
    if (!right_cell.has_value())
      return common::make_unexpected(right_cell.error());
    if (left_cell->is_null() || right_cell->is_null())
      return false;
    const schema::LogicalType type = definition.left_input_columns[key.left_column_ordinal].type;
    common::Result<bool> left_nan = is_nan(type, *left_cell);
    if (!left_nan.has_value())
      return common::make_unexpected(left_nan.error());
    common::Result<bool> right_nan = is_nan(type, *right_cell);
    if (!right_nan.has_value())
      return common::make_unexpected(right_nan.error());
    if (*left_nan || *right_nan)
      return false;
    common::Result<int> comparison =
        compare_physical_cells(type, *left_cell, *right_cell, ScalarNullPlacement::kLast);
    if (!comparison.has_value())
      return common::make_unexpected(comparison.error());
    if (*comparison != 0)
      return false;
  }
  return true;
}

[[nodiscard]] common::Result<bool>
eligible_time(const std::vector<AccountedVectorChunk>& left_chunks,
              const std::vector<AccountedVectorChunk>& right_chunks, const BufferedRow left,
              const BufferedRow right, const VectorAsofJoinDefinition& definition) {
  common::Result<columnar::ColumnCellView> left_time =
      row_cell(left_chunks, left, definition.left_timestamp_column_ordinal);
  if (!left_time.has_value())
    return common::make_unexpected(left_time.error());
  common::Result<columnar::ColumnCellView> right_time =
      row_cell(right_chunks, right, definition.right_timestamp_column_ordinal);
  if (!right_time.has_value())
    return common::make_unexpected(right_time.error());
  if (left_time->is_null() || right_time->is_null())
    return false;
  common::Result<int> comparison = compare_physical_cells(
      definition.right_input_columns[definition.right_timestamp_column_ordinal].type, *right_time,
      *left_time, ScalarNullPlacement::kLast);
  if (!comparison.has_value())
    return common::make_unexpected(comparison.error());
  return *comparison <= 0;
}

[[nodiscard]] common::Result<int>
compare_right_winners(const std::vector<AccountedVectorChunk>& right_chunks,
                      const BufferedRow candidate, const BufferedRow current,
                      const VectorAsofJoinDefinition& definition) {
  const auto compare_ordinal = [&](const std::size_t ordinal,
                                   const ScalarNullPlacement nulls) -> common::Result<int> {
    common::Result<columnar::ColumnCellView> candidate_cell =
        row_cell(right_chunks, candidate, ordinal);
    if (!candidate_cell.has_value())
      return common::make_unexpected(candidate_cell.error());
    common::Result<columnar::ColumnCellView> current_cell =
        row_cell(right_chunks, current, ordinal);
    if (!current_cell.has_value())
      return common::make_unexpected(current_cell.error());
    return compare_physical_cells(definition.right_input_columns[ordinal].type, *candidate_cell,
                                  *current_cell, nulls);
  };
  common::Result<int> time =
      compare_ordinal(definition.right_timestamp_column_ordinal, ScalarNullPlacement::kFirst);
  if (!time.has_value() || *time != 0)
    return time;
  for (const std::size_t ordinal : definition.right_physical_ordering_key_ordinals) {
    common::Result<int> physical = compare_ordinal(ordinal, ScalarNullPlacement::kLast);
    if (!physical.has_value() || *physical != 0)
      return physical;
  }
  const VectorRowVersionLayout suffix =
      vector_row_version_layout(definition.right_row_version_first_column_ordinal).value();
  for (const std::size_t ordinal :
       {suffix.wal_id_column_ordinal(), suffix.record_sequence_column_ordinal(),
        suffix.row_ordinal_column_ordinal()}) {
    common::Result<int> version = compare_ordinal(ordinal, ScalarNullPlacement::kLast);
    if (!version.has_value() || *version != 0)
      return version;
  }
  return 0;
}
// NOLINTEND(bugprone-easily-swappable-parameters)

struct OutputColumn {
  bool right{};
  bool presence{};
  std::size_t ordinal{};
  VectorAsofColumnShape shape;
};

[[nodiscard]] common::Result<std::vector<OutputColumn>>
output_columns(const VectorAsofJoinDefinition& definition) {
  try {
    std::vector<OutputColumn> columns;
    columns.reserve(definition.left_output_column_ordinals.size() +
                    definition.right_output_column_ordinals.size() + 1U);
    for (const std::size_t ordinal : definition.left_output_column_ordinals) {
      columns.push_back({.right = false,
                         .presence = false,
                         .ordinal = ordinal,
                         .shape = definition.left_input_columns[ordinal]});
    }
    for (const std::size_t ordinal : definition.right_output_column_ordinals) {
      VectorAsofColumnShape shape = definition.right_input_columns[ordinal];
      shape.nullable = shape.nullable || definition.left_outer;
      columns.push_back({.right = true, .presence = false, .ordinal = ordinal, .shape = shape});
    }
    columns.push_back(
        {.right = false,
         .presence = true,
         .ordinal = 0U,
         .shape = {.type = schema::LogicalType::create(schema::LogicalTypeKind::kBool).value(),
                   .nullable = false}});
    return columns;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("ASOF output-column allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("ASOF output columns exceed container limits"));
  }
}

[[nodiscard]] common::Result<std::optional<columnar::ColumnCellView>>
output_cell(const std::vector<AccountedVectorChunk>& left_chunks,
            const std::vector<AccountedVectorChunk>& right_chunks, const JoinedRow& row,
            const OutputColumn& column) {
  if (column.presence)
    return std::optional<columnar::ColumnCellView>{};
  if (column.right && !row.matched)
    return std::optional<columnar::ColumnCellView>{};
  common::Result<columnar::ColumnCellView> cell =
      column.right ? row_cell(right_chunks, row.right, column.ordinal)
                   : row_cell(left_chunks, row.left, column.ordinal);
  if (!cell.has_value())
    return common::make_unexpected(cell.error());
  return std::optional<columnar::ColumnCellView>{*cell};
}

struct OutputPlan {
  std::uint32_t row_count;
  std::size_t retained_charge;
};

[[nodiscard]] common::Result<OutputPlan>
plan_output(const std::vector<AccountedVectorChunk>& left_chunks,
            const std::vector<AccountedVectorChunk>& right_chunks,
            const std::vector<JoinedRow>& rows, const std::vector<OutputColumn>& columns,
            const VectorChunkLimits& limits) {
  if (rows.empty())
    return common::make_unexpected(internal("ASOF output requires joined rows"));
  if (rows.size() > limits.maximum_rows || columns.size() > limits.maximum_columns)
    return common::make_unexpected(exhausted("ASOF output exceeds its row or column limit"));
  common::Result<std::size_t> total =
      multiply_bytes(rows.size(), sizeof(std::uint32_t), "ASOF selection size overflowed");
  if (!total.has_value())
    return common::make_unexpected(total.error());
  for (const OutputColumn& column : columns) {
    if (column.shape.nullable) {
      total = add_bytes(*total, columnar::bitmap_size(static_cast<std::uint32_t>(rows.size())),
                        "ASOF validity size overflowed");
    }
    if (!total.has_value())
      return common::make_unexpected(total.error());
    if (column.shape.type.kind() == schema::LogicalTypeKind::kBool) {
      total = add_bytes(*total, columnar::bitmap_size(static_cast<std::uint32_t>(rows.size())),
                        "ASOF Boolean size overflowed");
    } else if (column.shape.type.is_variable_width()) {
      common::Result<std::size_t> offsets =
          multiply_bytes(rows.size() + 1U, sizeof(std::uint32_t), "ASOF offset size overflowed");
      if (!offsets.has_value())
        return common::make_unexpected(offsets.error());
      total = add_bytes(*total, *offsets, "ASOF output size overflowed");
      if (!total.has_value())
        return common::make_unexpected(total.error());
      std::size_t values = 0U;
      for (const JoinedRow& row : rows) {
        common::Result<std::optional<columnar::ColumnCellView>> cell =
            output_cell(left_chunks, right_chunks, row, column);
        if (!cell.has_value())
          return common::make_unexpected(cell.error());
        const columnar::ColumnCellView* present = optional_pointer(*cell);
        if (present == nullptr || present->is_null())
          continue;
        common::Result<common::ByteView> bytes = present->bytes();
        if (!bytes.has_value())
          return common::make_unexpected(bytes.error());
        common::Result<std::size_t> next =
            add_bytes(values, bytes->size(), "ASOF variable value size overflowed");
        if (!next.has_value() || *next > std::numeric_limits<std::uint32_t>::max()) {
          return common::make_unexpected(
              exhausted("ASOF variable output exceeds the canonical offset domain"));
        }
        values = *next;
      }
      total = add_bytes(*total, values, "ASOF output size overflowed");
    } else {
      common::Result<std::size_t> values = multiply_bytes(
          rows.size(), fixed_width(column.shape.type.kind()), "ASOF fixed output size overflowed");
      if (!values.has_value())
        return common::make_unexpected(values.error());
      total = add_bytes(*total, *values, "ASOF output size overflowed");
    }
    if (!total.has_value())
      return common::make_unexpected(total.error());
    if (*total > limits.maximum_buffer_bytes)
      return common::make_unexpected(exhausted("ASOF output exceeds its buffer-byte limit"));
  }
  common::Result<std::size_t> objects =
      multiply_bytes(columns.size(), sizeof(columnar::OwnedPhysicalColumn),
                     "ASOF output column-container size overflowed");
  if (!objects.has_value())
    return common::make_unexpected(objects.error());
  common::Result<std::size_t> charge =
      add_bytes(*total, *objects, "ASOF retained output size overflowed");
  if (!charge.has_value())
    return common::make_unexpected(charge.error());
  const std::optional<std::size_t> buffer_allocations =
      common::checked_multiply(columns.size(), std::size_t{3U});
  const std::optional<std::size_t> allocations =
      buffer_allocations.has_value() ? common::checked_add(*buffer_allocations, std::size_t{2U})
                                     : std::nullopt;
  if (!allocations.has_value())
    return common::make_unexpected(exhausted("ASOF allocation count overflowed"));
  common::Result<std::size_t> overhead = multiply_bytes(
      *allocations, kConservativeAllocationOverheadBytes, "ASOF allocation overhead overflowed");
  if (!overhead.has_value())
    return common::make_unexpected(overhead.error());
  charge = add_bytes(*charge, *overhead, "ASOF retained output size overflowed");
  if (!charge.has_value())
    return common::make_unexpected(charge.error());
  if (*charge > limits.maximum_retained_buffer_bytes)
    return common::make_unexpected(exhausted("ASOF output exceeds its retained-byte limit"));
  return OutputPlan{.row_count = static_cast<std::uint32_t>(rows.size()),
                    .retained_charge = *charge};
}

[[nodiscard]] common::Result<columnar::OwnedPhysicalColumn>
materialize_column(const std::vector<AccountedVectorChunk>& left_chunks,
                   const std::vector<AccountedVectorChunk>& right_chunks,
                   const std::vector<JoinedRow>& rows, const OutputColumn& column,
                   const std::uint32_t row_count) {
  columnar::ColumnVectorBuffers buffers;
  if (column.shape.nullable)
    buffers.validity.resize(columnar::bitmap_size(row_count));
  if (column.shape.type.kind() == schema::LogicalTypeKind::kBool) {
    buffers.values.resize(columnar::bitmap_size(row_count));
  } else if (column.shape.type.is_variable_width()) {
    buffers.offsets.resize((static_cast<std::size_t>(row_count) + 1U) * sizeof(std::uint32_t));
    std::size_t value_bytes = 0U;
    for (const JoinedRow& row : rows) {
      common::Result<std::optional<columnar::ColumnCellView>> cell =
          output_cell(left_chunks, right_chunks, row, column);
      if (!cell.has_value())
        return common::make_unexpected(cell.error());
      const columnar::ColumnCellView* present = optional_pointer(*cell);
      if (present == nullptr || present->is_null())
        continue;
      common::Result<common::ByteView> bytes = present->bytes();
      if (!bytes.has_value())
        return common::make_unexpected(bytes.error());
      common::Result<std::size_t> next =
          add_bytes(value_bytes, bytes->size(), "ASOF variable materialization overflowed");
      if (!next.has_value())
        return common::make_unexpected(next.error());
      value_bytes = *next;
    }
    buffers.values.reserve(value_bytes);
  } else {
    buffers.values.resize(static_cast<std::size_t>(row_count) *
                          fixed_width(column.shape.type.kind()));
  }

  std::uint32_t null_count = 0U;
  const std::size_t width = fixed_width(column.shape.type.kind());
  for (std::uint32_t output_row = 0U; output_row < row_count; ++output_row) {
    const JoinedRow& row = rows[output_row];
    if (column.presence) {
      if (row.matched)
        set_bit(buffers.values, output_row);
      continue;
    }
    common::Result<std::optional<columnar::ColumnCellView>> cell =
        output_cell(left_chunks, right_chunks, row, column);
    if (!cell.has_value())
      return common::make_unexpected(cell.error());
    const columnar::ColumnCellView* present = optional_pointer(*cell);
    if (present == nullptr || present->is_null()) {
      ++null_count;
    } else {
      if (column.shape.nullable)
        set_bit(buffers.validity, output_row);
      if (column.shape.type.kind() == schema::LogicalTypeKind::kBool) {
        common::Result<bool> value = present->boolean();
        if (!value.has_value())
          return common::make_unexpected(value.error());
        if (*value)
          set_bit(buffers.values, output_row);
      } else {
        common::Result<common::ByteView> bytes = present->bytes();
        if (!bytes.has_value())
          return common::make_unexpected(bytes.error());
        if (column.shape.type.is_variable_width()) {
          buffers.values.insert(buffers.values.end(), bytes->begin(), bytes->end());
        } else {
          if (bytes->size() != width)
            return common::make_unexpected(internal("ASOF fixed cell width changed"));
          std::ranges::copy(*bytes, buffers.values.begin() +
                                        static_cast<std::ptrdiff_t>(output_row * width));
        }
      }
    }
    if (column.shape.type.is_variable_width()) {
      store_u32_le(buffers.offsets,
                   (static_cast<std::size_t>(output_row) + 1U) * sizeof(std::uint32_t),
                   static_cast<std::uint32_t>(buffers.values.size()));
    }
  }
  return columnar::OwnedPhysicalColumn::create({.type = column.shape.type,
                                                .nullable = column.shape.nullable,
                                                .row_count = row_count,
                                                .null_count = null_count},
                                               std::move(buffers));
}

[[nodiscard]] common::Result<AccountedVectorChunk> materialize_output(
    const QueryResourceContext& resources, const std::vector<AccountedVectorChunk>& left_chunks,
    const std::vector<AccountedVectorChunk>& right_chunks, const std::vector<JoinedRow>& rows,
    const VectorAsofJoinDefinition& definition, const VectorChunkLimits& limits) {
  common::Result<std::vector<OutputColumn>> columns = output_columns(definition);
  if (!columns.has_value())
    return common::make_unexpected(columns.error());
  common::Result<OutputPlan> plan = plan_output(left_chunks, right_chunks, rows, *columns, limits);
  if (!plan.has_value())
    return common::make_unexpected(plan.error());
  common::Result<QueryMemoryReservation> reservation = resources.reserve(plan->retained_charge);
  if (!reservation.has_value())
    return common::make_unexpected(reservation.error());
  try {
    std::vector<columnar::OwnedPhysicalColumn> output_columns;
    output_columns.reserve(columns->size());
    for (const OutputColumn& column : *columns) {
      common::Result<void> active = resources.check_cancelled();
      if (!active.has_value())
        return common::make_unexpected(active.error());
      common::Result<columnar::OwnedPhysicalColumn> materialized =
          materialize_column(left_chunks, right_chunks, rows, column, plan->row_count);
      if (!materialized.has_value())
        return common::make_unexpected(materialized.error());
      output_columns.push_back(std::move(*materialized));
    }
    common::Result<VectorSelection> selection = VectorSelection::all(plan->row_count);
    if (!selection.has_value())
      return common::make_unexpected(selection.error());
    common::Result<VectorChunk> output =
        VectorChunk::create(std::move(output_columns), std::move(*selection), limits);
    if (!output.has_value())
      return common::make_unexpected(output.error());
    return AccountedVectorChunk::create(std::move(*output), std::move(*reservation), resources);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("ASOF output allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("ASOF output exceeds container limits"));
  }
}

} // namespace

common::Result<std::size_t> asof_join_state_reservation_bytes(const AsofJoinLimits limits) {
  common::Result<void> valid = validate_limits(limits);
  if (!valid.has_value())
    return common::make_unexpected(valid.error());
  return state_charge(limits);
}

class AsofJoinOperator::State {
public:
  std::vector<AccountedVectorChunk> left_chunks;
  std::vector<AccountedVectorChunk> right_chunks;
  std::vector<BufferedRow> left_rows;
  std::vector<BufferedRow> right_rows;
  std::vector<JoinedRow> joined_rows;
  std::optional<QueryMemoryReservation> reservation;
};

AsofJoinOperator::~AsofJoinOperator() = default;

AsofJoinOperator::AsofJoinOperator(std::unique_ptr<PhysicalOperator> left,
                                   std::unique_ptr<PhysicalOperator> right,
                                   VectorAsofJoinDefinition definition, const AsofJoinLimits limits,
                                   std::unique_ptr<State> state) noexcept
    : left_(std::move(left)), right_(std::move(right)), definition_(std::move(definition)),
      limits_(limits), state_(std::move(state)) {}

common::Result<std::unique_ptr<PhysicalOperator>>
AsofJoinOperator::create(std::unique_ptr<PhysicalOperator> left,
                         std::unique_ptr<PhysicalOperator> right,
                         VectorAsofJoinDefinition definition, const AsofJoinLimits limits) {
  if (left == nullptr || right == nullptr)
    return common::make_unexpected(invalid("ASOF join inputs must be non-null"));
  common::Result<void> valid = validate_definition(definition, limits);
  if (!valid.has_value())
    return common::make_unexpected(valid.error());
  try {
    return std::unique_ptr<PhysicalOperator>{new AsofJoinOperator{std::move(left), std::move(right),
                                                                  std::move(definition), limits,
                                                                  std::make_unique<State>()}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("ASOF join operator allocation failed"));
  }
}

common::Result<PhysicalOperatorStep> AsofJoinOperator::next(const QueryResourceContext& resources) {
  if (ended_)
    return PhysicalOperatorStep::end();
  common::Result<void> active = resources.check_cancelled();
  if (!active.has_value())
    return common::make_unexpected(active.error());
  const auto fail = [&](const common::Status& status) -> common::Result<PhysicalOperatorStep> {
    static_cast<void>(resources.request_cancel());
    left_.reset();
    right_.reset();
    state_.reset();
    ended_ = true;
    return common::make_unexpected(status);
  };

  try {
    common::Result<std::size_t> state_bytes = asof_join_state_reservation_bytes(limits_);
    if (!state_bytes.has_value())
      return fail(state_bytes.error());
    common::Result<QueryMemoryReservation> reservation = resources.reserve(*state_bytes);
    if (!reservation.has_value())
      return fail(reservation.error());
    state_->reservation.emplace(std::move(*reservation));
    state_->left_chunks.reserve(limits_.maximum_left_rows);
    state_->right_chunks.reserve(limits_.maximum_right_rows);
    state_->left_rows.reserve(limits_.maximum_left_rows);
    state_->right_rows.reserve(limits_.maximum_right_rows);
    state_->joined_rows.reserve(limits_.maximum_left_rows);
    std::size_t retained_state = 0U;
    for (const auto [capacity, width] : {
             std::pair{state_->left_chunks.capacity(), sizeof(AccountedVectorChunk)},
             std::pair{state_->right_chunks.capacity(), sizeof(AccountedVectorChunk)},
             std::pair{state_->left_rows.capacity(), sizeof(BufferedRow)},
             std::pair{state_->right_rows.capacity(), sizeof(BufferedRow)},
             std::pair{state_->joined_rows.capacity(), sizeof(JoinedRow)},
         }) {
      common::Result<std::size_t> bytes =
          multiply_bytes(capacity, width, "ASOF allocated state size overflowed");
      if (!bytes.has_value())
        return fail(bytes.error());
      common::Result<std::size_t> total =
          add_bytes(retained_state, *bytes, "ASOF allocated state size overflowed");
      if (!total.has_value())
        return fail(total.error());
      retained_state = *total;
    }
    common::Result<std::size_t> retained_with_overhead =
        add_bytes(retained_state, 5U * kConservativeAllocationOverheadBytes,
                  "ASOF allocated state size overflowed");
    if (!retained_with_overhead.has_value())
      return fail(retained_with_overhead.error());
    if (*retained_with_overhead > *state_bytes)
      return fail(exhausted("ASOF state allocation exceeded its charge"));

    const auto drain = [&](std::unique_ptr<PhysicalOperator>& input,
                           const std::vector<VectorAsofColumnShape>& expected,
                           const std::uint32_t maximum_rows,
                           std::vector<AccountedVectorChunk>& chunks,
                           std::vector<BufferedRow>& rows) -> common::Result<void> {
      for (;;) {
        common::Result<void> pulling = resources.check_cancelled();
        if (!pulling.has_value())
          return common::make_unexpected(pulling.error());
        common::Result<PhysicalOperatorStep> step = input->next(resources);
        if (!step.has_value())
          return common::make_unexpected(step.error());
        if (step->kind() == PhysicalOperatorStepKind::kEnd) {
          input.reset();
          return {};
        }
        common::Result<AccountedVectorChunk> chunk = std::move(*step).take_chunk();
        if (!chunk.has_value())
          return common::make_unexpected(chunk.error());
        if (!chunk->belongs_to(resources))
          return common::make_unexpected(invalid("ASOF input belongs to another query"));
        common::Result<void> shape = validate_chunk_shape(chunk->chunk(), expected);
        if (!shape.has_value())
          return common::make_unexpected(shape.error());
        const std::size_t selected = chunk->chunk().selected_row_count();
        if (selected > static_cast<std::size_t>(maximum_rows) - rows.size())
          return common::make_unexpected(exhausted("ASOF input exceeds its row limit"));
        if (selected == 0U)
          continue;
        const std::size_t chunk_ordinal = chunks.size();
        for (std::size_t row = 0U; row < selected; ++row)
          rows.push_back({.chunk_ordinal = chunk_ordinal, .selected_row = row});
        chunks.push_back(std::move(*chunk));
      }
    };

    common::Result<void> right =
        drain(right_, definition_.right_input_columns, limits_.maximum_right_rows,
              state_->right_chunks, state_->right_rows);
    if (!right.has_value())
      return fail(right.error());
    common::Result<void> left =
        drain(left_, definition_.left_input_columns, limits_.maximum_left_rows, state_->left_chunks,
              state_->left_rows);
    if (!left.has_value())
      return fail(left.error());

    std::size_t comparisons = 0U;
    for (const BufferedRow left_row : state_->left_rows) {
      std::optional<BufferedRow> winner;
      for (const BufferedRow right_row : state_->right_rows) {
        if ((comparisons++ & 255U) == 0U) {
          common::Result<void> comparing = resources.check_cancelled();
          if (!comparing.has_value())
            return fail(comparing.error());
        }
        common::Result<bool> equal =
            keys_match(state_->left_chunks, state_->right_chunks, left_row, right_row, definition_);
        if (!equal.has_value())
          return fail(equal.error());
        if (!*equal)
          continue;
        common::Result<bool> eligible = eligible_time(state_->left_chunks, state_->right_chunks,
                                                      left_row, right_row, definition_);
        if (!eligible.has_value())
          return fail(eligible.error());
        if (!*eligible)
          continue;
        const BufferedRow* current = optional_pointer(winner);
        if (current == nullptr) {
          winner = right_row;
          continue;
        }
        common::Result<int> later =
            compare_right_winners(state_->right_chunks, right_row, *current, definition_);
        if (!later.has_value())
          return fail(later.error());
        if (*later > 0)
          winner = right_row;
      }
      const BufferedRow* selected = optional_pointer(winner);
      if (selected != nullptr) {
        state_->joined_rows.push_back({.left = left_row, .right = *selected, .matched = true});
      } else if (definition_.left_outer) {
        state_->joined_rows.push_back({.left = left_row, .right = {}, .matched = false});
      }
    }

    if (state_->joined_rows.empty()) {
      state_.reset();
      ended_ = true;
      return PhysicalOperatorStep::end();
    }
    common::Result<AccountedVectorChunk> output =
        materialize_output(resources, state_->left_chunks, state_->right_chunks,
                           state_->joined_rows, definition_, limits_.output_limits);
    if (!output.has_value())
      return fail(output.error());
    state_.reset();
    ended_ = true;
    return PhysicalOperatorStep::chunk(std::move(*output));
  } catch (const std::bad_alloc&) {
    return fail(exhausted("ASOF join state allocation failed"));
  } catch (const std::length_error&) {
    return fail(exhausted("ASOF join state exceeds container limits"));
  }
}

} // namespace chronos::query
