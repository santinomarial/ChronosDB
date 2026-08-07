#include "chronos/query/column_output.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
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
    return common::make_unexpected(invalid("source-column output limits must be nonzero"));
  }
  return {};
}

struct OutputPlan {
  std::uint32_t physical_row_count{};
  bool compact_selected_rows{};
  std::size_t retained_charge{};
};

[[nodiscard]] std::uint32_t source_row(const VectorChunk& input, const OutputPlan plan,
                                       const std::uint32_t output_row) noexcept {
  return plan.compact_selected_rows ? input.selection().indices()[output_row] : output_row;
}

[[nodiscard]] common::Result<std::size_t>
variable_value_bytes(const VectorChunk& input, const columnar::PhysicalColumnView& column,
                     const OutputPlan plan) {
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
      total = add(*total, columnar::bitmap_size(physical_rows),
                  "source-column output validity size overflowed");
      if (!total.has_value())
        return common::make_unexpected(total.error());
    }
    if (column->type().kind() == schema::LogicalTypeKind::kBool) {
      total = add(*total, columnar::bitmap_size(physical_rows),
                  "source-column Boolean output size overflowed");
    } else if (column->type().is_variable_width()) {
      common::Result<std::size_t> offset_count =
          add(output_rows, 1U, "source-column offset count overflowed");
      if (!offset_count.has_value())
        return common::make_unexpected(offset_count.error());
      common::Result<std::size_t> offsets = bytes_for(
          *offset_count, sizeof(std::uint32_t), "source-column offset buffer size overflowed");
      if (!offsets.has_value())
        return common::make_unexpected(offsets.error());
      total = add(*total, *offsets, "source-column output size overflowed");
      if (total.has_value()) {
        common::Result<std::size_t> values = variable_value_bytes(input, *column, plan);
        if (!values.has_value())
          return common::make_unexpected(values.error());
        total = add(*total, *values, "source-column output size overflowed");
      }
    } else {
      common::Result<std::size_t> values =
          bytes_for(output_rows, fixed_width(column->type().kind()),
                    "source-column fixed output size overflowed");
      if (!values.has_value())
        return common::make_unexpected(values.error());
      total = add(*total, *values, "source-column output size overflowed");
    }
    if (!total.has_value())
      return common::make_unexpected(total.error());
    if (*total > limits.maximum_buffer_bytes) {
      return common::make_unexpected(
          exhausted("source-column output exceeds the configured buffer-byte limit"));
    }
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

void set_bit(std::vector<std::byte>& bitmap, const std::uint32_t row) {
  bitmap[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
}

void store_u32_le(std::vector<std::byte>& bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

[[nodiscard]] common::Result<columnar::OwnedPhysicalColumn>
materialize_column(const VectorChunk& input, const std::size_t input_column_ordinal,
                   const OutputPlan plan) {
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

} // namespace chronos::query
