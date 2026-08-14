#include "chronos/query/scalar_snapshot_scan.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::query {
namespace {

inline constexpr std::size_t kConservativeAllocationOverheadBytes = 64U;

[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return common::Status{common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status internal(const char* message) {
  return common::Status{common::StatusCode::kInternal, message};
}

template <typename Value>
[[nodiscard]] const Value* optional_pointer(const std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
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

template <typename Unsigned>
void store_unsigned_le(std::vector<std::byte>& bytes, const std::size_t offset,
                       const Unsigned value) {
  static_assert(std::is_unsigned_v<Unsigned>);
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] =
        static_cast<std::byte>((value >> (index * 8U)) & static_cast<Unsigned>(0xffU));
  }
}

void store_u32_le(std::vector<std::byte>& bytes, const std::size_t offset,
                  const std::uint32_t value) {
  store_unsigned_le(bytes, offset, value);
}

[[nodiscard]] common::Result<void>
store_fixed(const ScalarValue& value, std::vector<std::byte>& bytes, const std::size_t offset) {
  using schema::LogicalTypeKind;
  const schema::LogicalType* value_type = optional_pointer(value.type());
  if (value_type == nullptr || value.is_null())
    return common::make_unexpected(internal("scalar snapshot fixed value is untyped or null"));
  switch (value_type->kind()) {
  case LogicalTypeKind::kInt8:
    store_unsigned_le(bytes, offset,
                      std::bit_cast<std::uint8_t>(
                          static_cast<std::int8_t>(std::get<std::int64_t>(value.storage()))));
    return {};
  case LogicalTypeKind::kInt16:
    store_unsigned_le(bytes, offset,
                      std::bit_cast<std::uint16_t>(
                          static_cast<std::int16_t>(std::get<std::int64_t>(value.storage()))));
    return {};
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kDate:
    store_unsigned_le(bytes, offset,
                      std::bit_cast<std::uint32_t>(
                          static_cast<std::int32_t>(std::get<std::int64_t>(value.storage()))));
    return {};
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
    return common::make_unexpected(internal("scalar snapshot value is not fixed-width"));
  }
  return common::make_unexpected(internal("scalar snapshot logical type is invalid"));
}

[[nodiscard]] common::Result<common::ByteView> variable_bytes(const ScalarValue& value) {
  if (const auto* text = std::get_if<std::string>(&value.storage()); text != nullptr) {
    return std::as_bytes(std::span<const char>{text->data(), text->size()});
  }
  if (const auto* binary = std::get_if<std::vector<std::byte>>(&value.storage());
      binary != nullptr) {
    return common::ByteView{*binary};
  }
  return common::make_unexpected(internal("scalar snapshot variable storage is inconsistent"));
}

struct ChunkPlan {
  std::uint32_t row_count{};
  std::size_t charge{};
  std::vector<std::size_t> variable_value_bytes;
};

struct ChunkRange {
  std::size_t first_row{};
  std::uint32_t row_count{};
};

[[nodiscard]] common::Result<ChunkPlan> plan_chunk(const ScalarTableSnapshot& snapshot,
                                                   const ChunkRange range,
                                                   const ScalarSnapshotScanLimits& limits) {
  const auto columns = snapshot.schema_ptr()->columns();
  if (columns.size() > limits.chunk.maximum_columns)
    return common::make_unexpected(exhausted("scalar snapshot scan exceeds the column limit"));
  ChunkPlan plan{.row_count = range.row_count, .variable_value_bytes = {}};
  plan.variable_value_bytes.resize(columns.size());
  common::Result<std::size_t> selection_bytes = multiply(
      range.row_count, sizeof(std::uint32_t), "scalar snapshot selection accounting overflowed");
  if (!selection_bytes.has_value())
    return common::make_unexpected(selection_bytes.error());
  std::size_t retained = *selection_bytes;
  for (std::size_t column = 0U; column < columns.size(); ++column) {
    const schema::ColumnDefinition& definition = columns[column];
    if (definition.nullable()) {
      common::Result<std::size_t> next = add(retained, columnar::bitmap_size(range.row_count),
                                             "scalar snapshot scan validity accounting overflowed");
      if (!next.has_value())
        return common::make_unexpected(next.error());
      retained = *next;
    }
    if (definition.type().kind() == schema::LogicalTypeKind::kBool) {
      common::Result<std::size_t> next = add(retained, columnar::bitmap_size(range.row_count),
                                             "scalar snapshot scan Boolean accounting overflowed");
      if (!next.has_value())
        return common::make_unexpected(next.error());
      retained = *next;
    } else if (definition.type().is_variable_width()) {
      common::Result<std::size_t> offsets =
          multiply(static_cast<std::size_t>(range.row_count) + 1U, sizeof(std::uint32_t),
                   "scalar snapshot scan offset accounting overflowed");
      if (!offsets.has_value())
        return common::make_unexpected(offsets.error());
      common::Result<std::size_t> next =
          add(retained, *offsets, "scalar snapshot scan accounting overflowed");
      if (!next.has_value())
        return common::make_unexpected(next.error());
      retained = *next;
      std::size_t values = 0U;
      for (std::uint32_t row = 0U; row < range.row_count; ++row) {
        const ScalarValue& value = snapshot.rows()[range.first_row + row].columns[column];
        if (value.is_null())
          continue;
        common::Result<common::ByteView> bytes = variable_bytes(value);
        if (!bytes.has_value())
          return common::make_unexpected(bytes.error());
        next = add(values, bytes->size(), "scalar snapshot variable values overflowed");
        if (!next.has_value())
          return common::make_unexpected(next.error());
        values = *next;
      }
      if (values > std::numeric_limits<std::uint32_t>::max())
        return common::make_unexpected(exhausted("scalar snapshot variable offsets overflow"));
      plan.variable_value_bytes[column] = values;
      next = add(retained, values, "scalar snapshot scan accounting overflowed");
      if (!next.has_value())
        return common::make_unexpected(next.error());
      retained = *next;
    } else {
      common::Result<std::size_t> values =
          multiply(range.row_count, fixed_width(definition.type().kind()),
                   "scalar snapshot fixed values overflowed");
      if (!values.has_value())
        return common::make_unexpected(values.error());
      common::Result<std::size_t> next =
          add(retained, *values, "scalar snapshot scan accounting overflowed");
      if (!next.has_value())
        return common::make_unexpected(next.error());
      retained = *next;
    }
  }
  const std::optional<std::size_t> allocations =
      common::checked_multiply(columns.size(), std::size_t{3U});
  const std::optional<std::size_t> with_containers =
      allocations.has_value() ? common::checked_add(*allocations, std::size_t{3U}) : std::nullopt;
  if (!with_containers.has_value())
    return common::make_unexpected(exhausted("scalar snapshot allocation count overflowed"));
  common::Result<std::size_t> overhead =
      multiply(*with_containers, kConservativeAllocationOverheadBytes,
               "scalar snapshot allocation overhead overflowed");
  if (!overhead.has_value())
    return common::make_unexpected(overhead.error());
  common::Result<std::size_t> charge =
      add(retained, *overhead, "scalar snapshot retained charge overflowed");
  if (!charge.has_value())
    return common::make_unexpected(charge.error());
  if (retained > limits.chunk.maximum_retained_buffer_bytes ||
      retained > limits.chunk.maximum_buffer_bytes)
    return common::make_unexpected(exhausted("scalar snapshot scan exceeds chunk byte limits"));
  plan.charge = *charge;
  return plan;
}

[[nodiscard]] common::Result<columnar::OwnedPhysicalColumn>
materialize_column(const ScalarTableSnapshot& snapshot, const std::size_t first_row,
                   const std::size_t column, const ChunkPlan& plan) {
  const schema::ColumnDefinition& definition = snapshot.schema_ptr()->columns()[column];
  columnar::ColumnVectorBuffers buffers;
  if (definition.nullable())
    buffers.validity.resize(columnar::bitmap_size(plan.row_count));
  if (definition.type().kind() == schema::LogicalTypeKind::kBool) {
    buffers.values.resize(columnar::bitmap_size(plan.row_count));
  } else if (definition.type().is_variable_width()) {
    buffers.offsets.resize((static_cast<std::size_t>(plan.row_count) + 1U) * sizeof(std::uint32_t));
    buffers.values.resize(plan.variable_value_bytes[column]);
  } else {
    buffers.values.resize(static_cast<std::size_t>(plan.row_count) *
                          fixed_width(definition.type().kind()));
  }

  std::size_t variable_cursor = 0U;
  std::uint32_t null_count = 0U;
  const std::size_t width = fixed_width(definition.type().kind());
  for (std::uint32_t row = 0U; row < plan.row_count; ++row) {
    const ScalarValue& value = snapshot.rows()[first_row + row].columns[column];
    if (value.is_null()) {
      ++null_count;
    } else {
      if (definition.nullable())
        set_bit(buffers.validity, row);
      if (definition.type().kind() == schema::LogicalTypeKind::kBool) {
        const auto* boolean = std::get_if<bool>(&value.storage());
        if (boolean == nullptr)
          return common::make_unexpected(internal("scalar snapshot Boolean storage changed"));
        if (*boolean)
          set_bit(buffers.values, row);
      } else if (definition.type().is_variable_width()) {
        common::Result<common::ByteView> bytes = variable_bytes(value);
        if (!bytes.has_value())
          return common::make_unexpected(bytes.error());
        if (bytes->size() > buffers.values.size() - variable_cursor)
          return common::make_unexpected(internal("scalar snapshot variable plan changed"));
        std::ranges::copy(*bytes,
                          buffers.values.begin() + static_cast<std::ptrdiff_t>(variable_cursor));
        variable_cursor += bytes->size();
      } else {
        common::Result<void> stored =
            store_fixed(value, buffers.values, static_cast<std::size_t>(row) * width);
        if (!stored.has_value())
          return common::make_unexpected(stored.error());
      }
    }
    if (definition.type().is_variable_width()) {
      store_u32_le(buffers.offsets, (static_cast<std::size_t>(row) + 1U) * sizeof(std::uint32_t),
                   static_cast<std::uint32_t>(variable_cursor));
    }
  }
  if (variable_cursor != plan.variable_value_bytes[column])
    return common::make_unexpected(internal("scalar snapshot variable size changed"));
  return columnar::OwnedPhysicalColumn::create({.type = definition.type(),
                                                .nullable = definition.nullable(),
                                                .row_count = plan.row_count,
                                                .null_count = null_count},
                                               std::move(buffers));
}

} // namespace

ScalarSnapshotScanOperator::ScalarSnapshotScanOperator(
    std::shared_ptr<const ScalarTableSnapshot> snapshot,
    const ScalarSnapshotScanLimits limits) noexcept
    : snapshot_(std::move(snapshot)), limits_(limits) {}

common::Result<std::unique_ptr<PhysicalOperator>>
ScalarSnapshotScanOperator::create(std::shared_ptr<const ScalarTableSnapshot> snapshot,
                                   const ScalarSnapshotScanLimits limits) {
  if (snapshot == nullptr || limits.maximum_rows_per_chunk == 0U ||
      limits.chunk.maximum_rows == 0U || limits.chunk.maximum_columns == 0U ||
      limits.chunk.maximum_buffer_bytes == 0U || limits.chunk.maximum_retained_buffer_bytes == 0U) {
    return common::make_unexpected(invalid("scalar snapshot scan input or limits are invalid"));
  }
  if (snapshot->schema_ptr()->columns().empty())
    return common::make_unexpected(invalid("scalar snapshot scan schema has no columns"));
  if (snapshot->schema_ptr()->columns().size() > limits.chunk.maximum_columns)
    return common::make_unexpected(exhausted("scalar snapshot scan exceeds the column limit"));
  return std::unique_ptr<PhysicalOperator>{
      new ScalarSnapshotScanOperator{std::move(snapshot), limits}};
}

common::Result<PhysicalOperatorStep>
ScalarSnapshotScanOperator::next(const QueryResourceContext& resources) {
  if (ended_)
    return PhysicalOperatorStep::end();
  common::Result<void> active = resources.check_cancelled();
  if (!active.has_value())
    return common::make_unexpected(active.error());
  if (next_row_ == snapshot_->rows().size()) {
    ended_ = true;
    snapshot_.reset();
    return PhysicalOperatorStep::end();
  }

  const std::size_t remaining = snapshot_->rows().size() - next_row_;
  const std::size_t maximum =
      std::min<std::size_t>(limits_.maximum_rows_per_chunk, limits_.chunk.maximum_rows);
  const std::uint32_t row_count = static_cast<std::uint32_t>(std::min(remaining, maximum));
  common::Result<ChunkPlan> plan =
      plan_chunk(*snapshot_, ChunkRange{.first_row = next_row_, .row_count = row_count}, limits_);
  if (!plan.has_value())
    return common::make_unexpected(plan.error());
  common::Result<QueryMemoryReservation> reservation = resources.reserve(plan->charge);
  if (!reservation.has_value())
    return common::make_unexpected(reservation.error());

  try {
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.reserve(snapshot_->schema_ptr()->columns().size());
    for (std::size_t column = 0U; column < snapshot_->schema_ptr()->columns().size(); ++column) {
      active = resources.check_cancelled();
      if (!active.has_value())
        return common::make_unexpected(active.error());
      common::Result<columnar::OwnedPhysicalColumn> materialized =
          materialize_column(*snapshot_, next_row_, column, *plan);
      if (!materialized.has_value())
        return common::make_unexpected(materialized.error());
      columns.push_back(std::move(*materialized));
    }
    common::Result<VectorSelection> selection = VectorSelection::all(row_count);
    if (!selection.has_value())
      return common::make_unexpected(selection.error());
    common::Result<VectorChunk> chunk =
        VectorChunk::create(std::move(columns), std::move(*selection), limits_.chunk);
    if (!chunk.has_value())
      return common::make_unexpected(chunk.error());
    common::Result<AccountedVectorChunk> accounted =
        AccountedVectorChunk::create(std::move(*chunk), std::move(*reservation), resources);
    if (!accounted.has_value())
      return common::make_unexpected(accounted.error());
    next_row_ += row_count;
    return PhysicalOperatorStep::chunk(std::move(*accounted));
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("scalar snapshot scan allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("scalar snapshot scan exceeded container limits"));
  }
}

} // namespace chronos::query
