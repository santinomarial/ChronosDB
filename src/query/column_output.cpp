#include "chronos/query/column_output.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"
#include "chronos/schema/logical_type.hpp"
#include "query/vector_expression_internal.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::query {
namespace {

inline constexpr std::size_t kConservativeAllocationOverheadBytes = 64U;

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status out_of_range(std::string message) {
  return common::Status{common::StatusCode::kOutOfRange, std::move(message)};
}

[[nodiscard]] common::Status internal(std::string message) {
  return common::Status{common::StatusCode::kInternal, std::move(message)};
}

[[nodiscard]] common::Result<std::size_t> add(const std::size_t left, const std::size_t right,
                                              const char* const message) {
  const std::optional<std::size_t> sum = common::checked_add(left, right);
  if (!sum.has_value())
    return common::make_unexpected(exhausted(message));
  return *sum;
}

[[nodiscard]] common::Result<std::size_t>
bytes_for(const std::size_t count, const std::size_t width, const char* const message) {
  const std::optional<std::size_t> bytes = common::checked_multiply(count, width);
  if (!bytes.has_value())
    return common::make_unexpected(exhausted(message));
  return *bytes;
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

[[nodiscard]] common::Result<void> validate_limits(const VectorChunkLimits limits) {
  if (limits.maximum_rows == 0U || limits.maximum_columns == 0U ||
      limits.maximum_buffer_bytes == 0U || limits.maximum_retained_buffer_bytes == 0U) {
    return common::make_unexpected(invalid("column output limits must be nonzero"));
  }
  return {};
}

[[nodiscard]] const schema::LogicalType* constant_type(const ScalarValue& value) noexcept {
  const std::optional<schema::LogicalType>& type = value.type();
  return type.has_value() ? std::addressof(*type) : nullptr;
}

[[nodiscard]] common::Result<void> validate_constant(const ScalarValue& value) {
  const schema::LogicalType* type = constant_type(value);
  if (type == nullptr)
    return common::make_unexpected(invalid("column output constant must have a logical type"));
  if (value.is_null())
    return {};

  using schema::LogicalTypeKind;
  bool valid_storage = false;
  switch (type->kind()) {
  case LogicalTypeKind::kBool:
    valid_storage = std::holds_alternative<bool>(value.storage());
    break;
  case LogicalTypeKind::kInt8:
  case LogicalTypeKind::kInt16:
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kTimestampNs:
  case LogicalTypeKind::kDate:
    valid_storage = std::holds_alternative<std::int64_t>(value.storage());
    break;
  case LogicalTypeKind::kUInt8:
  case LogicalTypeKind::kUInt16:
  case LogicalTypeKind::kUInt32:
  case LogicalTypeKind::kUInt64:
    valid_storage = std::holds_alternative<std::uint64_t>(value.storage());
    break;
  case LogicalTypeKind::kFloat32:
    valid_storage = std::holds_alternative<float>(value.storage());
    break;
  case LogicalTypeKind::kFloat64:
    valid_storage = std::holds_alternative<double>(value.storage());
    break;
  case LogicalTypeKind::kDecimal:
    valid_storage = std::holds_alternative<Decimal128Value>(value.storage());
    break;
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
    valid_storage = std::holds_alternative<std::string>(value.storage());
    break;
  case LogicalTypeKind::kBinary:
    valid_storage = std::holds_alternative<std::vector<std::byte>>(value.storage());
    break;
  case LogicalTypeKind::kUuid:
    valid_storage = std::holds_alternative<common::Uuid>(value.storage());
    break;
  }
  if (!valid_storage)
    return common::make_unexpected(internal("column output constant storage is inconsistent"));
  return {};
}

struct OutputPlan {
  std::uint32_t physical_row_count{};
  bool compact_selected_rows{};
  std::size_t retained_charge{};
};

struct ColumnOutputPlan {
  OutputPlan output;
  std::array<std::uint32_t, kMaximumColumnOutputWidth> computed_variable_value_bytes{};
};

[[nodiscard]] std::uint32_t source_row(const VectorChunk& input, const OutputPlan& plan,
                                       const std::uint32_t output_row) noexcept {
  return plan.compact_selected_rows ? input.selection().indices()[output_row] : output_row;
}

[[nodiscard]] common::Result<std::size_t>
variable_value_bytes(const VectorChunk& input, const columnar::PhysicalColumnView& column,
                     const OutputPlan& plan) {
  std::size_t total = 0U;
  for (std::uint32_t output_row = 0U; output_row < plan.physical_row_count; ++output_row) {
    const common::Result<columnar::ColumnCellView> cell =
        column.cell(source_row(input, plan, output_row));
    if (!cell.has_value())
      return common::make_unexpected(cell.error());
    if (cell->is_null())
      continue;
    const common::Result<common::ByteView> bytes = cell->bytes();
    if (!bytes.has_value())
      return common::make_unexpected(bytes.error());
    common::Result<std::size_t> next =
        add(total, bytes->size(), "source-column variable output size overflowed");
    if (!next.has_value())
      return next;
    total = *next;
  }
  if (total > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    return common::make_unexpected(
        exhausted("source-column variable output exceeds UINT32 offsets"));
  }
  return total;
}

[[nodiscard]] common::Result<std::size_t>
add_source_column_bytes(const VectorChunk& input, const std::size_t ordinal, const OutputPlan& plan,
                        const VectorChunkLimits limits, std::size_t total) {
  const columnar::PhysicalColumnView* column = input.column(ordinal);
  if (column == nullptr) {
    return common::make_unexpected(
        out_of_range("source-column output ordinal is outside the input chunk"));
  }
  if (column->row_count() != input.physical_row_count()) {
    return common::make_unexpected(
        internal("source-column output input has inconsistent physical row counts"));
  }
  if (column->nullable()) {
    common::Result<std::size_t> next = add(total, columnar::bitmap_size(plan.physical_row_count),
                                           "source-column output validity size overflowed");
    if (!next.has_value())
      return next;
    total = *next;
  }
  if (column->type().kind() == schema::LogicalTypeKind::kBool) {
    common::Result<std::size_t> next = add(total, columnar::bitmap_size(plan.physical_row_count),
                                           "source-column Boolean output size overflowed");
    if (!next.has_value())
      return next;
    total = *next;
  } else if (column->type().is_variable_width()) {
    common::Result<std::size_t> offset_count =
        add(plan.physical_row_count, 1U, "source-column offset count overflowed");
    if (!offset_count.has_value())
      return offset_count;
    common::Result<std::size_t> offsets = bytes_for(*offset_count, sizeof(std::uint32_t),
                                                    "source-column offset buffer size overflowed");
    if (!offsets.has_value())
      return offsets;
    common::Result<std::size_t> next = add(total, *offsets, "source-column output size overflowed");
    if (!next.has_value())
      return next;
    common::Result<std::size_t> values = variable_value_bytes(input, *column, plan);
    if (!values.has_value())
      return values;
    next = add(*next, *values, "source-column output size overflowed");
    if (!next.has_value())
      return next;
    total = *next;
  } else {
    common::Result<std::size_t> values =
        bytes_for(plan.physical_row_count, fixed_width(column->type().kind()),
                  "source-column fixed output size overflowed");
    if (!values.has_value())
      return values;
    common::Result<std::size_t> next = add(total, *values, "source-column output size overflowed");
    if (!next.has_value())
      return next;
    total = *next;
  }
  if (total > limits.maximum_buffer_bytes) {
    return common::make_unexpected(
        exhausted("source-column output exceeds the configured buffer-byte limit"));
  }
  return total;
}

[[nodiscard]] common::Result<std::size_t> constant_payload_size(const ScalarValue& value) {
  if (value.is_null())
    return 0U;
  if (const auto* text = std::get_if<std::string>(&value.storage()); text != nullptr)
    return text->size();
  if (const auto* binary = std::get_if<std::vector<std::byte>>(&value.storage());
      binary != nullptr) {
    return binary->size();
  }
  return common::make_unexpected(internal("variable column output constant storage is invalid"));
}

[[nodiscard]] common::Result<std::size_t> add_constant_column_bytes(const ScalarValue& value,
                                                                    const OutputPlan& plan,
                                                                    const VectorChunkLimits limits,
                                                                    std::size_t total) {
  const common::Result<void> valid = validate_constant(value);
  if (!valid.has_value())
    return common::make_unexpected(valid.error());
  const schema::LogicalType* type_ptr = constant_type(value);
  if (type_ptr == nullptr)
    return common::make_unexpected(internal("column output constant lost its logical type"));
  const schema::LogicalType& type = *type_ptr;
  if (value.is_null()) {
    common::Result<std::size_t> next = add(total, columnar::bitmap_size(plan.physical_row_count),
                                           "constant column output validity size overflowed");
    if (!next.has_value())
      return next;
    total = *next;
  }
  if (type.kind() == schema::LogicalTypeKind::kBool) {
    common::Result<std::size_t> next = add(total, columnar::bitmap_size(plan.physical_row_count),
                                           "constant Boolean output size overflowed");
    if (!next.has_value())
      return next;
    total = *next;
  } else if (type.is_variable_width()) {
    common::Result<std::size_t> offset_count =
        add(plan.physical_row_count, 1U, "constant column output offset count overflowed");
    if (!offset_count.has_value())
      return offset_count;
    common::Result<std::size_t> offsets = bytes_for(
        *offset_count, sizeof(std::uint32_t), "constant column output offset size overflowed");
    if (!offsets.has_value())
      return offsets;
    common::Result<std::size_t> next =
        add(total, *offsets, "constant column output size overflowed");
    if (!next.has_value())
      return next;
    common::Result<std::size_t> payload = constant_payload_size(value);
    if (!payload.has_value())
      return payload;
    common::Result<std::size_t> values = bytes_for(plan.physical_row_count, *payload,
                                                   "constant column output value size overflowed");
    if (!values.has_value())
      return values;
    if (*values > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
      return common::make_unexpected(exhausted("constant column output exceeds UINT32 offsets"));
    }
    next = add(*next, *values, "constant column output size overflowed");
    if (!next.has_value())
      return next;
    total = *next;
  } else {
    common::Result<std::size_t> values = bytes_for(
        plan.physical_row_count, fixed_width(type.kind()), "constant fixed output size overflowed");
    if (!values.has_value())
      return values;
    common::Result<std::size_t> next =
        add(total, *values, "constant column output size overflowed");
    if (!next.has_value())
      return next;
    total = *next;
  }
  if (total > limits.maximum_buffer_bytes) {
    return common::make_unexpected(
        exhausted("column output exceeds the configured buffer-byte limit"));
  }
  return total;
}

[[nodiscard]] common::Result<std::size_t>
add_expression_column_bytes(const VectorExpression& expression, const VectorChunk& input,
                            const OutputPlan& plan, const VectorChunkLimits limits,
                            std::size_t total, std::uint32_t& planned_variable_value_bytes) {
  planned_variable_value_bytes = 0U;
  const common::Result<void> valid = detail::validate_vector_expression_input(expression, input);
  if (!valid.has_value())
    return common::make_unexpected(valid.error());
  const VectorExpressionShape& shape = expression.result_shape();
  if (shape.nullable) {
    common::Result<std::size_t> next = add(total, columnar::bitmap_size(plan.physical_row_count),
                                           "computed column validity size overflowed");
    if (!next.has_value())
      return next;
    total = *next;
  }
  if (shape.type.is_variable_width()) {
    common::Result<std::size_t> offset_count =
        add(plan.physical_row_count, 1U, "computed column offset count overflowed");
    if (!offset_count.has_value())
      return offset_count;
    common::Result<std::size_t> offset_bytes = bytes_for(
        *offset_count, sizeof(std::uint32_t), "computed column offset buffer size overflowed");
    if (!offset_bytes.has_value())
      return offset_bytes;
    common::Result<std::size_t> next =
        add(total, *offset_bytes, "computed column output size overflowed");
    if (!next.has_value())
      return next;
    total = *next;
    std::size_t value_bytes = 0U;
    for (std::uint32_t output_row = 0U; output_row < plan.physical_row_count; ++output_row) {
      common::Result<detail::BorrowedVariableExpressionValue> value =
          detail::evaluate_variable_vector_expression_row(expression, input,
                                                          source_row(input, plan, output_row));
      if (!value.has_value())
        return common::make_unexpected(value.error());
      if (value->is_null)
        continue;
      common::Result<std::size_t> value_next =
          add(value_bytes, value->bytes.size(), "computed variable output size overflowed");
      if (!value_next.has_value())
        return value_next;
      value_bytes = *value_next;
    }
    if (value_bytes > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
      return common::make_unexpected(exhausted("computed variable output exceeds UINT32 offsets"));
    }
    planned_variable_value_bytes = static_cast<std::uint32_t>(value_bytes);
    next = add(total, value_bytes, "computed column output size overflowed");
    if (!next.has_value())
      return next;
    if (*next > limits.maximum_buffer_bytes)
      return common::make_unexpected(exhausted("column output exceeds the buffer-byte limit"));
    return *next;
  }
  common::Result<std::size_t> value_bytes =
      shape.type.kind() == schema::LogicalTypeKind::kBool
          ? common::Result<std::size_t>{columnar::bitmap_size(plan.physical_row_count)}
          : bytes_for(plan.physical_row_count, fixed_width(shape.type.kind()),
                      "computed column fixed size overflowed");
  if (!value_bytes.has_value())
    return common::make_unexpected(value_bytes.error());
  common::Result<std::size_t> next =
      add(total, *value_bytes, "computed column output size overflowed");
  if (!next.has_value())
    return next;
  if (*next > limits.maximum_buffer_bytes)
    return common::make_unexpected(exhausted("column output exceeds the buffer-byte limit"));
  return *next;
}

[[nodiscard]] common::Result<OutputPlan>
plan_output(const VectorChunk& input, const std::vector<std::size_t>& input_column_ordinals,
            const VectorChunkLimits limits) {
  const common::Result<void> valid_limits = validate_limits(limits);
  if (!valid_limits.has_value())
    return common::make_unexpected(valid_limits.error());
  if (input_column_ordinals.size() > limits.maximum_columns ||
      input_column_ordinals.size() > kMaximumSourceColumnOutputWidth) {
    return common::make_unexpected(
        exhausted("source-column output exceeds the configured column limit"));
  }

  const bool compact = input.selected_row_count() != 0U;
  const std::size_t output_rows =
      compact ? input.selected_row_count() : static_cast<std::size_t>(input.physical_row_count());
  if (output_rows > limits.maximum_rows) {
    return common::make_unexpected(
        exhausted("source-column output exceeds the configured row limit"));
  }
  const auto physical_rows = static_cast<std::uint32_t>(output_rows);
  common::Result<std::size_t> total = bytes_for(compact ? output_rows : 0U, sizeof(std::uint32_t),
                                                "source-column output selection size overflowed");
  if (!total.has_value())
    return common::make_unexpected(total.error());

  OutputPlan plan{
      .physical_row_count = physical_rows, .compact_selected_rows = compact, .retained_charge = 0U};
  if (*total > limits.maximum_buffer_bytes) {
    return common::make_unexpected(
        exhausted("source-column output exceeds the configured buffer-byte limit"));
  }
  for (const std::size_t ordinal : input_column_ordinals) {
    total = add_source_column_bytes(input, ordinal, plan, limits, *total);
    if (!total.has_value())
      return common::make_unexpected(total.error());
  }

  common::Result<std::size_t> column_objects =
      bytes_for(input_column_ordinals.size(), sizeof(columnar::OwnedPhysicalColumn),
                "source-column output container size overflowed");
  if (!column_objects.has_value())
    return common::make_unexpected(column_objects.error());
  common::Result<std::size_t> charge =
      add(*total, *column_objects, "source-column output retained size overflowed");
  if (!charge.has_value())
    return common::make_unexpected(charge.error());
  const std::optional<std::size_t> buffer_allocations =
      common::checked_multiply(input_column_ordinals.size(), std::size_t{3U});
  const std::optional<std::size_t> allocation_count =
      buffer_allocations.has_value() ? common::checked_add(*buffer_allocations, std::size_t{2U})
                                     : std::nullopt;
  if (!allocation_count.has_value()) {
    return common::make_unexpected(exhausted("source-column output allocation count overflowed"));
  }
  common::Result<std::size_t> overhead =
      bytes_for(*allocation_count, kConservativeAllocationOverheadBytes,
                "source-column output allocation overhead overflowed");
  if (!overhead.has_value())
    return common::make_unexpected(overhead.error());
  charge = add(*charge, *overhead, "source-column output retained size overflowed");
  if (!charge.has_value())
    return common::make_unexpected(charge.error());
  if (*charge > limits.maximum_retained_buffer_bytes) {
    return common::make_unexpected(
        exhausted("source-column output exceeds the configured retained-byte limit"));
  }
  plan.retained_charge = *charge;
  return plan;
}

[[nodiscard]] common::Result<ColumnOutputPlan>
plan_output(const VectorChunk& input, const std::vector<ColumnOutputPosition>& positions,
            const VectorChunkLimits limits) {
  const common::Result<void> valid_limits = validate_limits(limits);
  if (!valid_limits.has_value())
    return common::make_unexpected(valid_limits.error());
  if (positions.size() > limits.maximum_columns || positions.size() > kMaximumColumnOutputWidth) {
    return common::make_unexpected(exhausted("column output exceeds the configured column limit"));
  }

  const bool compact = input.selected_row_count() != 0U;
  const std::size_t output_rows =
      compact ? input.selected_row_count() : static_cast<std::size_t>(input.physical_row_count());
  if (output_rows > limits.maximum_rows)
    return common::make_unexpected(exhausted("column output exceeds the configured row limit"));
  ColumnOutputPlan plan{.output = {.physical_row_count = static_cast<std::uint32_t>(output_rows),
                                   .compact_selected_rows = compact,
                                   .retained_charge = 0U}};
  common::Result<std::size_t> total = bytes_for(compact ? output_rows : 0U, sizeof(std::uint32_t),
                                                "column output selection size overflowed");
  if (!total.has_value())
    return common::make_unexpected(total.error());
  if (*total > limits.maximum_buffer_bytes)
    return common::make_unexpected(exhausted("column output exceeds the buffer-byte limit"));

  for (std::size_t position_index = 0U; position_index < positions.size(); ++position_index) {
    const ColumnOutputPosition& position = positions[position_index];
    if (const auto* source = std::get_if<SourceColumnOutputPosition>(&position);
        source != nullptr) {
      total =
          add_source_column_bytes(input, source->input_column_ordinal, plan.output, limits, *total);
    } else if (const auto* constant = std::get_if<ConstantColumnOutputPosition>(&position);
               constant != nullptr) {
      total = add_constant_column_bytes(constant->value, plan.output, limits, *total);
    } else if (const auto* computed = std::get_if<ComputedColumnOutputPosition>(&position);
               computed != nullptr) {
      total = add_expression_column_bytes(computed->expression, input, plan.output, limits, *total,
                                          plan.computed_variable_value_bytes[position_index]);
    } else {
      return common::make_unexpected(internal("column output position is invalid"));
    }
    if (!total.has_value())
      return common::make_unexpected(total.error());
  }

  common::Result<std::size_t> column_objects =
      bytes_for(positions.size(), sizeof(columnar::OwnedPhysicalColumn),
                "column output container size overflowed");
  if (!column_objects.has_value())
    return common::make_unexpected(column_objects.error());
  common::Result<std::size_t> charge =
      add(*total, *column_objects, "column output retained size overflowed");
  if (!charge.has_value())
    return common::make_unexpected(charge.error());
  const std::optional<std::size_t> buffer_allocations =
      common::checked_multiply(positions.size(), std::size_t{3U});
  const std::optional<std::size_t> allocation_count =
      buffer_allocations.has_value() ? common::checked_add(*buffer_allocations, std::size_t{2U})
                                     : std::nullopt;
  if (!allocation_count.has_value())
    return common::make_unexpected(exhausted("column output allocation count overflowed"));
  common::Result<std::size_t> overhead =
      bytes_for(*allocation_count, kConservativeAllocationOverheadBytes,
                "column output allocation overhead overflowed");
  if (!overhead.has_value())
    return common::make_unexpected(overhead.error());
  charge = add(*charge, *overhead, "column output retained size overflowed");
  if (!charge.has_value())
    return common::make_unexpected(charge.error());
  if (*charge > limits.maximum_retained_buffer_bytes) {
    return common::make_unexpected(
        exhausted("column output exceeds the configured retained-byte limit"));
  }
  plan.output.retained_charge = *charge;
  return plan;
}

void set_bit(std::vector<std::byte>& bitmap, const std::uint32_t row) {
  bitmap[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
}

void store_u32_le(std::vector<std::byte>& bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

template <typename Unsigned>
void store_unsigned_le(std::vector<std::byte>& bytes, const std::size_t offset,
                       const Unsigned value) {
  static_assert(std::is_unsigned_v<Unsigned>);
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] =
        static_cast<std::byte>((value >> (index * 8U)) & static_cast<Unsigned>(0xffU));
  }
}

[[nodiscard]] common::Result<columnar::OwnedPhysicalColumn>
materialize_column(const VectorChunk& input, const std::size_t input_column_ordinal,
                   const OutputPlan& plan) {
  const columnar::PhysicalColumnView* source = input.column(input_column_ordinal);
  if (source == nullptr) {
    return common::make_unexpected(
        out_of_range("source-column output ordinal is outside the input chunk"));
  }

  columnar::ColumnVectorBuffers buffers;
  if (source->nullable())
    buffers.validity.resize(columnar::bitmap_size(plan.physical_row_count));
  if (source->type().kind() == schema::LogicalTypeKind::kBool) {
    buffers.values.resize(columnar::bitmap_size(plan.physical_row_count));
  } else if (source->type().is_variable_width()) {
    common::Result<std::size_t> offset_count =
        add(plan.physical_row_count, 1U, "source-column materialized offset count overflowed");
    if (!offset_count.has_value())
      return common::make_unexpected(offset_count.error());
    common::Result<std::size_t> offset_bytes = bytes_for(
        *offset_count, sizeof(std::uint32_t), "source-column materialized offset size overflowed");
    if (!offset_bytes.has_value())
      return common::make_unexpected(offset_bytes.error());
    buffers.offsets.resize(*offset_bytes);
    common::Result<std::size_t> values = variable_value_bytes(input, *source, plan);
    if (!values.has_value())
      return common::make_unexpected(values.error());
    buffers.values.reserve(*values);
  } else {
    common::Result<std::size_t> value_bytes =
        bytes_for(plan.physical_row_count, fixed_width(source->type().kind()),
                  "source-column materialized fixed size overflowed");
    if (!value_bytes.has_value())
      return common::make_unexpected(value_bytes.error());
    buffers.values.resize(*value_bytes);
  }

  std::uint32_t null_count = 0U;
  const std::size_t width = fixed_width(source->type().kind());
  for (std::uint32_t output_row = 0U; output_row < plan.physical_row_count; ++output_row) {
    const common::Result<columnar::ColumnCellView> cell =
        source->cell(source_row(input, plan, output_row));
    if (!cell.has_value())
      return common::make_unexpected(cell.error());
    if (cell->is_null()) {
      ++null_count;
    } else {
      if (source->nullable())
        set_bit(buffers.validity, output_row);
      if (source->type().kind() == schema::LogicalTypeKind::kBool) {
        const common::Result<bool> value = cell->boolean();
        if (!value.has_value())
          return common::make_unexpected(value.error());
        if (*value)
          set_bit(buffers.values, output_row);
      } else {
        const common::Result<common::ByteView> value = cell->bytes();
        if (!value.has_value())
          return common::make_unexpected(value.error());
        if (source->type().is_variable_width()) {
          buffers.values.insert(buffers.values.end(), value->begin(), value->end());
        } else {
          if (value->size() != width) {
            return common::make_unexpected(
                internal("source-column fixed cell has an inconsistent width"));
          }
          std::ranges::copy(*value, buffers.values.begin() +
                                        static_cast<std::ptrdiff_t>(output_row * width));
        }
      }
    }
    if (source->type().is_variable_width()) {
      store_u32_le(buffers.offsets,
                   (static_cast<std::size_t>(output_row) + 1U) * sizeof(std::uint32_t),
                   static_cast<std::uint32_t>(buffers.values.size()));
    }
  }

  return columnar::OwnedPhysicalColumn::create({.type = source->type(),
                                                .nullable = source->nullable(),
                                                .row_count = plan.physical_row_count,
                                                .null_count = null_count},
                                               std::move(buffers));
}

[[nodiscard]] common::Result<void> store_constant_fixed_cell(const ScalarValue& value,
                                                             std::vector<std::byte>& bytes,
                                                             const std::size_t offset) {
  using schema::LogicalTypeKind;
  const schema::LogicalType* type = constant_type(value);
  if (type == nullptr)
    return common::make_unexpected(internal("column output constant lost its logical type"));
  const LogicalTypeKind kind = type->kind();
  switch (kind) {
  case LogicalTypeKind::kInt8: {
    const auto narrowed = static_cast<std::int8_t>(std::get<std::int64_t>(value.storage()));
    store_unsigned_le(bytes, offset, std::bit_cast<std::uint8_t>(narrowed));
    return {};
  }
  case LogicalTypeKind::kInt16: {
    const auto narrowed = static_cast<std::int16_t>(std::get<std::int64_t>(value.storage()));
    store_unsigned_le(bytes, offset, std::bit_cast<std::uint16_t>(narrowed));
    return {};
  }
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kDate: {
    const auto narrowed = static_cast<std::int32_t>(std::get<std::int64_t>(value.storage()));
    store_unsigned_le(bytes, offset, std::bit_cast<std::uint32_t>(narrowed));
    return {};
  }
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kTimestampNs:
    store_unsigned_le(bytes, offset,
                      std::bit_cast<std::uint64_t>(std::get<std::int64_t>(value.storage())));
    return {};
  case LogicalTypeKind::kUInt8:
    store_unsigned_le(bytes, offset,
                      static_cast<std::uint8_t>(std::get<std::uint64_t>(value.storage())));
    return {};
  case LogicalTypeKind::kUInt16:
    store_unsigned_le(bytes, offset,
                      static_cast<std::uint16_t>(std::get<std::uint64_t>(value.storage())));
    return {};
  case LogicalTypeKind::kUInt32:
    store_unsigned_le(bytes, offset,
                      static_cast<std::uint32_t>(std::get<std::uint64_t>(value.storage())));
    return {};
  case LogicalTypeKind::kUInt64:
    store_unsigned_le(bytes, offset, std::get<std::uint64_t>(value.storage()));
    return {};
  case LogicalTypeKind::kFloat32:
    store_unsigned_le(bytes, offset,
                      std::bit_cast<std::uint32_t>(std::get<float>(value.storage())));
    return {};
  case LogicalTypeKind::kFloat64:
    store_unsigned_le(bytes, offset,
                      std::bit_cast<std::uint64_t>(std::get<double>(value.storage())));
    return {};
  case LogicalTypeKind::kDecimal: {
    const auto& coefficient = std::get<Decimal128Value>(value.storage()).coefficient;
    std::ranges::copy(coefficient, bytes.begin() + static_cast<std::ptrdiff_t>(offset));
    return {};
  }
  case LogicalTypeKind::kUuid: {
    const auto& uuid = std::get<common::Uuid>(value.storage()).bytes();
    std::ranges::copy(uuid, bytes.begin() + static_cast<std::ptrdiff_t>(offset));
    return {};
  }
  case LogicalTypeKind::kBool:
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
  case LogicalTypeKind::kBinary:
    return common::make_unexpected(internal("constant is not fixed-width"));
  }
  return common::make_unexpected(internal("constant logical type is invalid"));
}

[[nodiscard]] common::Result<columnar::OwnedPhysicalColumn>
materialize_constant(const ScalarValue& value, const OutputPlan& plan) {
  const common::Result<void> valid = validate_constant(value);
  if (!valid.has_value())
    return common::make_unexpected(valid.error());
  const schema::LogicalType* type_ptr = constant_type(value);
  if (type_ptr == nullptr)
    return common::make_unexpected(internal("column output constant lost its logical type"));
  const schema::LogicalType& type = *type_ptr;
  const bool nullable = value.is_null();
  columnar::ColumnVectorBuffers buffers;
  if (nullable)
    buffers.validity.resize(columnar::bitmap_size(plan.physical_row_count));

  if (type.kind() == schema::LogicalTypeKind::kBool) {
    buffers.values.resize(columnar::bitmap_size(plan.physical_row_count));
    if (!nullable && std::get<bool>(value.storage())) {
      for (std::uint32_t row = 0U; row < plan.physical_row_count; ++row)
        set_bit(buffers.values, row);
    }
  } else if (type.is_variable_width()) {
    const std::size_t offset_count = static_cast<std::size_t>(plan.physical_row_count) + 1U;
    buffers.offsets.resize(offset_count * sizeof(std::uint32_t));
    if (!nullable) {
      if (const auto* text = std::get_if<std::string>(&value.storage()); text != nullptr) {
        buffers.values.resize(text->size() * static_cast<std::size_t>(plan.physical_row_count));
        for (std::uint32_t row = 0U; row < plan.physical_row_count; ++row) {
          const std::size_t begin = static_cast<std::size_t>(row) * text->size();
          if (!text->empty())
            std::memcpy(buffers.values.data() + begin, text->data(), text->size());
          store_u32_le(buffers.offsets,
                       (static_cast<std::size_t>(row) + 1U) * sizeof(std::uint32_t),
                       static_cast<std::uint32_t>(begin + text->size()));
        }
      } else {
        const auto& binary = std::get<std::vector<std::byte>>(value.storage());
        buffers.values.resize(binary.size() * static_cast<std::size_t>(plan.physical_row_count));
        for (std::uint32_t row = 0U; row < plan.physical_row_count; ++row) {
          const std::size_t begin = static_cast<std::size_t>(row) * binary.size();
          std::ranges::copy(binary, buffers.values.begin() + static_cast<std::ptrdiff_t>(begin));
          store_u32_le(buffers.offsets,
                       (static_cast<std::size_t>(row) + 1U) * sizeof(std::uint32_t),
                       static_cast<std::uint32_t>(begin + binary.size()));
        }
      }
    }
  } else {
    const std::size_t width = fixed_width(type.kind());
    buffers.values.resize(width * static_cast<std::size_t>(plan.physical_row_count));
    if (!nullable && plan.physical_row_count != 0U) {
      common::Result<void> stored = store_constant_fixed_cell(value, buffers.values, 0U);
      if (!stored.has_value())
        return common::make_unexpected(stored.error());
      for (std::uint32_t row = 1U; row < plan.physical_row_count; ++row) {
        std::ranges::copy_n(buffers.values.begin(), static_cast<std::ptrdiff_t>(width),
                            buffers.values.begin() + static_cast<std::ptrdiff_t>(row * width));
      }
    }
  }

  return columnar::OwnedPhysicalColumn::create(
      {.type = type,
       .nullable = nullable,
       .row_count = plan.physical_row_count,
       .null_count = nullable ? plan.physical_row_count : 0U},
      std::move(buffers));
}

[[nodiscard]] common::Result<columnar::OwnedPhysicalColumn>
materialize_expression(const VectorExpression& expression, const VectorChunk& input,
                       const OutputPlan& plan, const std::uint32_t planned_variable_value_bytes) {
  const VectorExpressionShape& shape = expression.result_shape();
  columnar::ColumnVectorBuffers buffers;
  if (shape.nullable)
    buffers.validity.resize(columnar::bitmap_size(plan.physical_row_count));
  if (shape.type.is_variable_width()) {
    const std::size_t offset_count = static_cast<std::size_t>(plan.physical_row_count) + 1U;
    buffers.offsets.resize(offset_count * sizeof(std::uint32_t));
    buffers.values.resize(planned_variable_value_bytes);

    std::size_t cursor = 0U;
    std::uint32_t null_count = 0U;
    for (std::uint32_t output_row = 0U; output_row < plan.physical_row_count; ++output_row) {
      common::Result<detail::BorrowedVariableExpressionValue> value =
          detail::evaluate_variable_vector_expression_row(expression, input,
                                                          source_row(input, plan, output_row));
      if (!value.has_value())
        return common::make_unexpected(value.error());
      if (value->is_null) {
        ++null_count;
      } else {
        if (shape.nullable)
          set_bit(buffers.validity, output_row);
        if (value->bytes.size() > buffers.values.size() - cursor) {
          return common::make_unexpected(
              internal("computed variable output exceeded its admitted size"));
        }
        for (const std::byte byte : value->bytes)
          buffers.values[cursor++] = detail::transform_variable_byte(byte, value->transform);
      }
      store_u32_le(buffers.offsets,
                   (static_cast<std::size_t>(output_row) + 1U) * sizeof(std::uint32_t),
                   static_cast<std::uint32_t>(cursor));
    }
    if (cursor != buffers.values.size()) {
      return common::make_unexpected(
          internal("computed variable output size changed after admission"));
    }
    return columnar::OwnedPhysicalColumn::create({.type = shape.type,
                                                  .nullable = shape.nullable,
                                                  .row_count = plan.physical_row_count,
                                                  .null_count = null_count},
                                                 std::move(buffers));
  }
  const std::size_t width = fixed_width(shape.type.kind());
  if (shape.type.kind() == schema::LogicalTypeKind::kBool)
    buffers.values.resize(columnar::bitmap_size(plan.physical_row_count));
  else
    buffers.values.resize(width * static_cast<std::size_t>(plan.physical_row_count));

  std::uint32_t null_count = 0U;
  for (std::uint32_t output_row = 0U; output_row < plan.physical_row_count; ++output_row) {
    common::Result<ScalarValue> value = detail::evaluate_vector_expression_row(
        expression, input, source_row(input, plan, output_row));
    if (!value.has_value())
      return common::make_unexpected(value.error());
    if (value->is_null()) {
      ++null_count;
      continue;
    }
    if (shape.nullable)
      set_bit(buffers.validity, output_row);
    if (shape.type.kind() == schema::LogicalTypeKind::kBool) {
      const auto* boolean = std::get_if<bool>(&value->storage());
      if (boolean == nullptr)
        return common::make_unexpected(internal("computed BOOL storage is inconsistent"));
      if (*boolean)
        set_bit(buffers.values, output_row);
    } else {
      common::Result<void> stored = store_constant_fixed_cell(
          *value, buffers.values, static_cast<std::size_t>(output_row) * width);
      if (!stored.has_value())
        return common::make_unexpected(stored.error());
    }
  }
  return columnar::OwnedPhysicalColumn::create({.type = shape.type,
                                                .nullable = shape.nullable,
                                                .row_count = plan.physical_row_count,
                                                .null_count = null_count},
                                               std::move(buffers));
}

[[nodiscard]] common::Result<AccountedVectorChunk>
materialize_output(const QueryResourceContext& resources, const VectorChunk& input,
                   const std::vector<std::size_t>& input_column_ordinals,
                   const VectorChunkLimits limits) {
  common::Result<OutputPlan> plan = plan_output(input, input_column_ordinals, limits);
  if (!plan.has_value())
    return common::make_unexpected(plan.error());
  common::Result<QueryMemoryReservation> reservation = resources.reserve(plan->retained_charge);
  if (!reservation.has_value())
    return common::make_unexpected(reservation.error());

  try {
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.reserve(input_column_ordinals.size());
    for (const std::size_t ordinal : input_column_ordinals) {
      common::Result<columnar::OwnedPhysicalColumn> column =
          materialize_column(input, ordinal, *plan);
      if (!column.has_value())
        return common::make_unexpected(column.error());
      columns.push_back(std::move(*column));
    }

    common::Result<VectorSelection> selection =
        plan->compact_selected_rows ? VectorSelection::all(plan->physical_row_count)
                                    : VectorSelection::from_indices(plan->physical_row_count, {});
    if (!selection.has_value())
      return common::make_unexpected(selection.error());
    common::Result<VectorChunk> output =
        VectorChunk::create(std::move(columns), std::move(*selection), limits);
    if (!output.has_value())
      return common::make_unexpected(output.error());
    return AccountedVectorChunk::create(std::move(*output), std::move(*reservation), resources);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("source-column output allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("source-column output exceeds container limits"));
  }
}

[[nodiscard]] common::Result<AccountedVectorChunk>
materialize_output(const QueryResourceContext& resources, const VectorChunk& input,
                   const std::vector<ColumnOutputPosition>& positions,
                   const VectorChunkLimits limits) {
  common::Result<ColumnOutputPlan> plan = plan_output(input, positions, limits);
  if (!plan.has_value())
    return common::make_unexpected(plan.error());
  common::Result<QueryMemoryReservation> reservation =
      resources.reserve(plan->output.retained_charge);
  if (!reservation.has_value())
    return common::make_unexpected(reservation.error());

  try {
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.reserve(positions.size());
    for (std::size_t position_index = 0U; position_index < positions.size(); ++position_index) {
      const ColumnOutputPosition& position = positions[position_index];
      common::Result<columnar::OwnedPhysicalColumn> column =
          common::make_unexpected(internal("column output position is invalid"));
      if (const auto* source = std::get_if<SourceColumnOutputPosition>(&position);
          source != nullptr) {
        column = materialize_column(input, source->input_column_ordinal, plan->output);
      } else if (const auto* constant = std::get_if<ConstantColumnOutputPosition>(&position);
                 constant != nullptr) {
        column = materialize_constant(constant->value, plan->output);
      } else if (const auto* computed = std::get_if<ComputedColumnOutputPosition>(&position);
                 computed != nullptr) {
        column = materialize_expression(computed->expression, input, plan->output,
                                        plan->computed_variable_value_bytes[position_index]);
      }
      if (!column.has_value())
        return common::make_unexpected(column.error());
      columns.push_back(std::move(*column));
    }

    common::Result<VectorSelection> selection =
        plan->output.compact_selected_rows
            ? VectorSelection::all(plan->output.physical_row_count)
            : VectorSelection::from_indices(plan->output.physical_row_count, {});
    if (!selection.has_value())
      return common::make_unexpected(selection.error());
    common::Result<VectorChunk> output =
        VectorChunk::create(std::move(columns), std::move(*selection), limits);
    if (!output.has_value())
      return common::make_unexpected(output.error());
    return AccountedVectorChunk::create(std::move(*output), std::move(*reservation), resources);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("column output allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("column output exceeds container limits"));
  }
}

[[nodiscard]] common::Result<void>
validate_position_configuration(const std::vector<ColumnOutputPosition>& positions,
                                const VectorChunkLimits limits) {
  if (positions.size() > limits.maximum_columns || positions.size() > kMaximumColumnOutputWidth ||
      positions.capacity() > kMaximumColumnOutputWidth) {
    return common::make_unexpected(exhausted("column output configuration exceeds column limit"));
  }
  const std::optional<std::size_t> vector_bytes =
      common::checked_multiply(positions.capacity(), sizeof(ColumnOutputPosition));
  if (!vector_bytes.has_value())
    return common::make_unexpected(exhausted("column output configuration size overflowed"));
  std::size_t retained = *vector_bytes;
  for (const ColumnOutputPosition& position : positions) {
    const auto* constant = std::get_if<ConstantColumnOutputPosition>(&position);
    if (constant == nullptr) {
      const auto* computed = std::get_if<ComputedColumnOutputPosition>(&position);
      if (computed == nullptr)
        continue;
      const std::optional<std::size_t> next =
          common::checked_add(retained, computed->expression.retained_configuration_bytes());
      if (!next.has_value()) {
        return common::make_unexpected(exhausted("column output configuration size overflowed"));
      }
      retained = *next;
      continue;
    }
    const common::Result<void> valid = validate_constant(constant->value);
    if (!valid.has_value())
      return common::make_unexpected(valid.error());
    std::size_t capacity = 0U;
    if (const auto* text = std::get_if<std::string>(&constant->value.storage()); text != nullptr)
      capacity = text->capacity();
    else if (const auto* binary = std::get_if<std::vector<std::byte>>(&constant->value.storage());
             binary != nullptr)
      capacity = binary->capacity();
    const std::optional<std::size_t> next = common::checked_add(retained, capacity);
    if (!next.has_value())
      return common::make_unexpected(exhausted("column output configuration size overflowed"));
    retained = *next;
  }
  if (retained > limits.maximum_retained_buffer_bytes) {
    return common::make_unexpected(
        exhausted("column output configuration exceeds retained-byte limit"));
  }
  return {};
}

} // namespace

SourceColumnOutputOperator::SourceColumnOutputOperator(
    std::unique_ptr<PhysicalOperator> input, std::vector<std::size_t> input_column_ordinals,
    const VectorChunkLimits output_limits) noexcept
    : input_(std::move(input)), input_column_ordinals_(std::move(input_column_ordinals)),
      output_limits_(output_limits) {}

common::Result<std::unique_ptr<PhysicalOperator>>
SourceColumnOutputOperator::create(std::unique_ptr<PhysicalOperator> input,
                                   std::vector<std::size_t> input_column_ordinals,
                                   const VectorChunkLimits output_limits) {
  if (input == nullptr)
    return common::make_unexpected(invalid("source-column output input must be non-null"));
  const common::Result<void> valid_limits = validate_limits(output_limits);
  if (!valid_limits.has_value())
    return common::make_unexpected(valid_limits.error());
  if (input_column_ordinals.size() > output_limits.maximum_columns ||
      input_column_ordinals.size() > kMaximumSourceColumnOutputWidth ||
      input_column_ordinals.capacity() > kMaximumSourceColumnOutputWidth) {
    return common::make_unexpected(
        exhausted("source-column output configuration exceeds the column limit"));
  }
  try {
    return std::unique_ptr<PhysicalOperator>{new SourceColumnOutputOperator{
        std::move(input), std::move(input_column_ordinals), output_limits}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("source-column output operator allocation failed"));
  }
}

common::Result<PhysicalOperatorStep>
SourceColumnOutputOperator::next(const QueryResourceContext& resources) {
  if (ended_)
    return PhysicalOperatorStep::end();
  const common::Result<void> active = resources.check_cancelled();
  if (!active.has_value())
    return common::make_unexpected(active.error());

  common::Result<PhysicalOperatorStep> input = input_->next(resources);
  if (!input.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(input.error());
  }
  if (input->kind() == PhysicalOperatorStepKind::kEnd) {
    ended_ = true;
    input_.reset();
    return PhysicalOperatorStep::end();
  }
  common::Result<AccountedVectorChunk> chunk = std::move(*input).take_chunk();
  if (!chunk.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(chunk.error());
  }
  if (!chunk->belongs_to(resources)) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(
        invalid("source-column output received a chunk charged to another query"));
  }
  common::Result<AccountedVectorChunk> output =
      materialize_output(resources, chunk->chunk(), input_column_ordinals_, output_limits_);
  if (!output.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(output.error());
  }
  return PhysicalOperatorStep::chunk(std::move(*output));
}

ColumnOutputOperator::ColumnOutputOperator(std::unique_ptr<PhysicalOperator> input,
                                           std::vector<ColumnOutputPosition> positions,
                                           const VectorChunkLimits output_limits) noexcept
    : input_(std::move(input)), positions_(std::move(positions)), output_limits_(output_limits) {}

common::Result<std::unique_ptr<PhysicalOperator>>
ColumnOutputOperator::create(std::unique_ptr<PhysicalOperator> input,
                             std::vector<ColumnOutputPosition> positions,
                             const VectorChunkLimits output_limits) {
  if (input == nullptr)
    return common::make_unexpected(invalid("column output input must be non-null"));
  const common::Result<void> valid_limits = validate_limits(output_limits);
  if (!valid_limits.has_value())
    return common::make_unexpected(valid_limits.error());
  const common::Result<void> valid_positions =
      validate_position_configuration(positions, output_limits);
  if (!valid_positions.has_value())
    return common::make_unexpected(valid_positions.error());
  try {
    return std::unique_ptr<PhysicalOperator>{
        new ColumnOutputOperator{std::move(input), std::move(positions), output_limits}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("column output operator allocation failed"));
  }
}

common::Result<PhysicalOperatorStep>
ColumnOutputOperator::next(const QueryResourceContext& resources) {
  if (ended_)
    return PhysicalOperatorStep::end();
  const common::Result<void> active = resources.check_cancelled();
  if (!active.has_value())
    return common::make_unexpected(active.error());

  common::Result<PhysicalOperatorStep> input = input_->next(resources);
  if (!input.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(input.error());
  }
  if (input->kind() == PhysicalOperatorStepKind::kEnd) {
    ended_ = true;
    input_.reset();
    return PhysicalOperatorStep::end();
  }
  common::Result<AccountedVectorChunk> chunk = std::move(*input).take_chunk();
  if (!chunk.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(chunk.error());
  }
  if (!chunk->belongs_to(resources)) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(
        invalid("column output received a chunk charged to another query"));
  }
  common::Result<AccountedVectorChunk> output =
      materialize_output(resources, chunk->chunk(), positions_, output_limits_);
  if (!output.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(output.error());
  }
  return PhysicalOperatorStep::chunk(std::move(*output));
}

} // namespace chronos::query
