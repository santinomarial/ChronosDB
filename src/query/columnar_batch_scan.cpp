#include "chronos/query/columnar_batch_scan.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

inline constexpr std::size_t kConservativeAllocationOverheadBytes = 64U;

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status internal(const char* message) {
  return {common::StatusCode::kInternal, message};
}

[[nodiscard]] common::Result<std::size_t> add(const std::size_t left, const std::size_t right,
                                              const char* message) {
  const std::optional<std::size_t> result = common::checked_add(left, right);
  return result.has_value() ? common::Result<std::size_t>{*result}
                            : common::make_unexpected(exhausted(message));
}

[[nodiscard]] common::Result<std::size_t> multiply(const std::size_t left, const std::size_t right,
                                                   const char* message) {
  const std::optional<std::size_t> result = common::checked_multiply(left, right);
  return result.has_value() ? common::Result<std::size_t>{*result}
                            : common::make_unexpected(exhausted(message));
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

void set_bit(std::vector<std::byte>& bitmap, const std::uint32_t row) {
  bitmap[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
}

void store_u32_le(std::vector<std::byte>& bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

struct ChunkPlan {
  std::uint32_t row_count{};
  std::size_t charge{};
  std::vector<std::size_t> variable_value_bytes;
};

[[nodiscard]] common::Result<std::size_t> source_charge(const columnar::OwnedColumnarBatch& batch) {
  auto objects = multiply(batch.columns().size(), sizeof(columnar::OwnedColumnVector) + 64U,
                          "columnar batch source accounting overflowed");
  if (!objects.has_value())
    return common::make_unexpected(objects.error());
  auto total =
      add(batch.retained_buffer_bytes(), *objects, "columnar batch source accounting overflowed");
  return total.has_value() ? add(*total, sizeof(ColumnarBatchScanOperator) + 256U,
                                 "columnar batch source accounting overflowed")
                           : total;
}

[[nodiscard]] common::Result<ChunkPlan> plan_chunk(const columnar::OwnedColumnarBatch& batch,
                                                   const std::uint32_t first_row,
                                                   const std::uint32_t row_count,
                                                   const ColumnarBatchScanLimits& limits) {
  if (batch.columns().size() > limits.chunk.maximum_columns)
    return common::make_unexpected(exhausted("columnar batch scan exceeds the column limit"));
  ChunkPlan plan{.row_count = row_count};
  try {
    plan.variable_value_bytes.resize(batch.columns().size());
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("columnar batch planning allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("columnar batch column count exceeds limits"));
  }
  auto selection =
      multiply(row_count, sizeof(std::uint32_t), "columnar batch selection accounting overflowed");
  if (!selection.has_value())
    return common::make_unexpected(selection.error());
  std::size_t retained = *selection;
  for (std::size_t column = 0U; column < batch.columns().size(); ++column) {
    const columnar::OwnedColumnVector& source = batch.columns()[column];
    if (source.nullable()) {
      auto next = add(retained, columnar::bitmap_size(row_count),
                      "columnar batch validity accounting overflowed");
      if (!next.has_value())
        return common::make_unexpected(next.error());
      retained = *next;
    }
    if (source.type().kind() == schema::LogicalTypeKind::kBool) {
      auto next = add(retained, columnar::bitmap_size(row_count),
                      "columnar batch Boolean accounting overflowed");
      if (!next.has_value())
        return common::make_unexpected(next.error());
      retained = *next;
    } else if (source.type().is_variable_width()) {
      auto offsets = multiply(static_cast<std::size_t>(row_count) + 1U, sizeof(std::uint32_t),
                              "columnar batch offset accounting overflowed");
      if (!offsets.has_value())
        return common::make_unexpected(offsets.error());
      auto next = add(retained, *offsets, "columnar batch accounting overflowed");
      if (!next.has_value())
        return common::make_unexpected(next.error());
      retained = *next;
      std::size_t values{};
      for (std::uint32_t row = 0U; row < row_count; ++row) {
        auto cell = batch.cell({column, first_row + row});
        if (!cell.has_value())
          return common::make_unexpected(internal("columnar batch cell became inaccessible"));
        if (cell->is_null())
          continue;
        auto bytes = cell->bytes();
        if (!bytes.has_value())
          return common::make_unexpected(internal("columnar batch variable cell changed kind"));
        next = add(values, bytes->size(), "columnar batch variable values overflowed");
        if (!next.has_value())
          return common::make_unexpected(next.error());
        values = *next;
      }
      if (values > std::numeric_limits<std::uint32_t>::max())
        return common::make_unexpected(exhausted("columnar batch variable offsets overflow"));
      plan.variable_value_bytes[column] = values;
      next = add(retained, values, "columnar batch accounting overflowed");
      if (!next.has_value())
        return common::make_unexpected(next.error());
      retained = *next;
    } else {
      auto values = multiply(row_count, fixed_width(source.type().kind()),
                             "columnar batch fixed values overflowed");
      if (!values.has_value())
        return common::make_unexpected(values.error());
      auto next = add(retained, *values, "columnar batch accounting overflowed");
      if (!next.has_value())
        return common::make_unexpected(next.error());
      retained = *next;
    }
  }
  auto allocations =
      multiply(batch.columns().size(), 3U, "columnar batch allocation count overflowed");
  if (!allocations.has_value())
    return common::make_unexpected(allocations.error());
  auto overhead_count = add(*allocations, 3U, "columnar batch allocation count overflowed");
  if (!overhead_count.has_value())
    return common::make_unexpected(overhead_count.error());
  auto overhead = multiply(*overhead_count, kConservativeAllocationOverheadBytes,
                           "columnar batch allocation overhead overflowed");
  if (!overhead.has_value())
    return common::make_unexpected(overhead.error());
  auto charge = add(retained, *overhead, "columnar batch retained charge overflowed");
  if (!charge.has_value())
    return common::make_unexpected(charge.error());
  if (retained > limits.chunk.maximum_buffer_bytes ||
      retained > limits.chunk.maximum_retained_buffer_bytes) {
    return common::make_unexpected(exhausted("columnar batch scan exceeds chunk byte limits"));
  }
  plan.charge = *charge;
  return plan;
}

[[nodiscard]] common::Result<columnar::OwnedPhysicalColumn>
materialize_column(const columnar::OwnedColumnarBatch& batch, const std::uint32_t first_row,
                   const std::size_t ordinal, const ChunkPlan& plan) {
  const columnar::OwnedColumnVector& source = batch.columns()[ordinal];
  columnar::ColumnVectorBuffers buffers;
  if (source.nullable())
    buffers.validity.resize(columnar::bitmap_size(plan.row_count));
  if (source.type().kind() == schema::LogicalTypeKind::kBool) {
    buffers.values.resize(columnar::bitmap_size(plan.row_count));
  } else if (source.type().is_variable_width()) {
    buffers.offsets.resize((static_cast<std::size_t>(plan.row_count) + 1U) * sizeof(std::uint32_t));
    buffers.values.resize(plan.variable_value_bytes[ordinal]);
  } else {
    buffers.values.resize(static_cast<std::size_t>(plan.row_count) *
                          fixed_width(source.type().kind()));
  }

  std::size_t variable_cursor{};
  std::uint32_t null_count{};
  const std::size_t width = fixed_width(source.type().kind());
  for (std::uint32_t row = 0U; row < plan.row_count; ++row) {
    auto cell = batch.cell({ordinal, first_row + row});
    if (!cell.has_value())
      return common::make_unexpected(internal("columnar batch cell became inaccessible"));
    if (cell->is_null()) {
      ++null_count;
    } else {
      if (source.nullable())
        set_bit(buffers.validity, row);
      if (source.type().kind() == schema::LogicalTypeKind::kBool) {
        auto value = cell->boolean();
        if (!value.has_value())
          return common::make_unexpected(internal("columnar batch Boolean cell changed kind"));
        if (*value)
          set_bit(buffers.values, row);
      } else {
        auto bytes = cell->bytes();
        if (!bytes.has_value())
          return common::make_unexpected(internal("columnar batch byte cell changed kind"));
        if (source.type().is_variable_width()) {
          if (bytes->size() > buffers.values.size() - variable_cursor)
            return common::make_unexpected(internal("columnar batch variable plan changed"));
          std::ranges::copy(*bytes,
                            buffers.values.begin() + static_cast<std::ptrdiff_t>(variable_cursor));
          variable_cursor += bytes->size();
        } else {
          if (bytes->size() != width)
            return common::make_unexpected(internal("columnar batch fixed cell changed width"));
          std::ranges::copy(*bytes,
                            buffers.values.begin() + static_cast<std::ptrdiff_t>(row * width));
        }
      }
    }
    if (source.type().is_variable_width()) {
      store_u32_le(buffers.offsets, (static_cast<std::size_t>(row) + 1U) * sizeof(std::uint32_t),
                   static_cast<std::uint32_t>(variable_cursor));
    }
  }
  if (variable_cursor != plan.variable_value_bytes[ordinal])
    return common::make_unexpected(internal("columnar batch variable size changed"));
  return columnar::OwnedPhysicalColumn::create({.type = source.type(),
                                                .nullable = source.nullable(),
                                                .row_count = plan.row_count,
                                                .null_count = null_count},
                                               std::move(buffers));
}

} // namespace

ColumnarBatchScanOperator::ColumnarBatchScanOperator(
    std::shared_ptr<const columnar::OwnedColumnarBatch> batch,
    const ColumnarBatchScanLimits limits) noexcept
    : batch_(std::move(batch)), limits_(limits) {}

common::Result<std::unique_ptr<PhysicalOperator>>
ColumnarBatchScanOperator::create(std::shared_ptr<const columnar::OwnedColumnarBatch> batch,
                                  const ColumnarBatchScanLimits limits) {
  if (batch == nullptr || limits.maximum_rows_per_chunk == 0U || limits.chunk.maximum_rows == 0U ||
      limits.chunk.maximum_columns == 0U || limits.chunk.maximum_buffer_bytes == 0U ||
      limits.chunk.maximum_retained_buffer_bytes == 0U) {
    return common::make_unexpected(invalid("columnar batch scan input or limits are invalid"));
  }
  if (batch->columns().empty())
    return common::make_unexpected(invalid("columnar batch scan schema has no columns"));
  if (batch->columns().size() > limits.chunk.maximum_columns)
    return common::make_unexpected(exhausted("columnar batch scan exceeds the column limit"));
  try {
    return std::unique_ptr<PhysicalOperator>{
        new ColumnarBatchScanOperator{std::move(batch), limits}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("columnar batch source allocation failed"));
  }
}

common::Result<PhysicalOperatorStep>
ColumnarBatchScanOperator::next(const QueryResourceContext& resources) {
  if (ended_)
    return PhysicalOperatorStep::end();
  auto active = resources.check_cancelled();
  if (!active.has_value())
    return common::make_unexpected(active.error());
  if (source_reservation_.is_valid() && !resources.owns(source_reservation_)) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(invalid("columnar batch source belongs to another query"));
  }
  if (!source_reservation_.is_valid()) {
    auto charge = source_charge(*batch_);
    if (!charge.has_value())
      return common::make_unexpected(charge.error());
    auto reserved = resources.reserve_shared(*charge);
    if (!reserved.has_value())
      return common::make_unexpected(reserved.error());
    source_reservation_ = std::move(*reserved);
  }
  if (next_row_ == batch_->row_count()) {
    ended_ = true;
    batch_.reset();
    source_reservation_.reset();
    return PhysicalOperatorStep::end();
  }

  const std::uint32_t maximum =
      std::min(limits_.maximum_rows_per_chunk, limits_.chunk.maximum_rows);
  const std::uint32_t row_count = std::min(batch_->row_count() - next_row_, maximum);
  auto plan = plan_chunk(*batch_, next_row_, row_count, limits_);
  if (!plan.has_value())
    return common::make_unexpected(plan.error());
  auto reservation = resources.reserve(plan->charge);
  if (!reservation.has_value())
    return common::make_unexpected(reservation.error());
  try {
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.reserve(batch_->columns().size());
    for (std::size_t ordinal = 0U; ordinal < batch_->columns().size(); ++ordinal) {
      active = resources.check_cancelled();
      if (!active.has_value())
        return common::make_unexpected(active.error());
      auto materialized = materialize_column(*batch_, next_row_, ordinal, *plan);
      if (!materialized.has_value())
        return common::make_unexpected(materialized.error());
      columns.push_back(std::move(*materialized));
    }
    auto selection = VectorSelection::all(row_count);
    if (!selection.has_value())
      return common::make_unexpected(selection.error());
    auto chunk = VectorChunk::create(std::move(columns), std::move(*selection), limits_.chunk);
    if (!chunk.has_value())
      return common::make_unexpected(chunk.error());
    auto accounted =
        AccountedVectorChunk::create(std::move(*chunk), std::move(*reservation), resources);
    if (!accounted.has_value())
      return common::make_unexpected(accounted.error());
    next_row_ += row_count;
    return PhysicalOperatorStep::chunk(std::move(*accounted));
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("columnar batch scan allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("columnar batch scan exceeds container limits"));
  }
}

} // namespace chronos::query
