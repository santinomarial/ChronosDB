#include "chronos/query/sort.hpp"

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

[[nodiscard]] common::Status out_of_range(const std::string_view message) {
  return common::Status{common::StatusCode::kOutOfRange, std::string{message}};
}

[[nodiscard]] common::Status internal(const std::string_view message) {
  return common::Status{common::StatusCode::kInternal, std::string{message}};
}

[[nodiscard]] common::Result<std::size_t> add_bytes(const std::size_t left, const std::size_t right,
                                                    const std::string_view message) {
  const std::optional<std::size_t> result = common::checked_add(left, right);
  if (!result.has_value())
    return common::make_unexpected(exhausted(message));
  return *result;
}

[[nodiscard]] common::Result<std::size_t>
multiply_bytes(const std::size_t count, const std::size_t width, const std::string_view message) {
  const std::optional<std::size_t> result = common::checked_multiply(count, width);
  if (!result.has_value())
    return common::make_unexpected(exhausted(message));
  return *result;
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

struct BufferedRow {
  std::size_t chunk_ordinal{};
  std::size_t selected_row{};
};

[[nodiscard]] common::Result<std::size_t> state_charge(const SortLimits& limits) {
  common::Result<std::size_t> rows = multiply_bytes(limits.maximum_rows, sizeof(BufferedRow) * 4U,
                                                    "sort row-reference state size overflowed");
  if (!rows.has_value())
    return rows;
  common::Result<std::size_t> chunks =
      multiply_bytes(limits.maximum_rows, sizeof(AccountedVectorChunk) * 2U,
                     "sort retained-chunk state size overflowed");
  if (!chunks.has_value())
    return chunks;
  common::Result<std::size_t> total =
      add_bytes(*rows, *chunks, "sort operator state size overflowed");
  if (!total.has_value())
    return total;
  common::Result<std::size_t> overhead = multiply_bytes(3U, kConservativeAllocationOverheadBytes,
                                                        "sort operator state overhead overflowed");
  if (!overhead.has_value())
    return overhead;
  return add_bytes(*total, *overhead, "sort operator state size overflowed");
}

[[nodiscard]] common::Result<void> validate_limits(const SortLimits& limits) {
  if (limits.maximum_rows == 0U || limits.maximum_keys == 0U || limits.maximum_state_bytes == 0U ||
      limits.output_limits.maximum_rows == 0U || limits.output_limits.maximum_columns == 0U ||
      limits.output_limits.maximum_buffer_bytes == 0U ||
      limits.output_limits.maximum_retained_buffer_bytes == 0U) {
    return common::make_unexpected(invalid("sort limits must be nonzero"));
  }
  const common::Result<std::size_t> charge = state_charge(limits);
  if (!charge.has_value())
    return common::make_unexpected(charge.error());
  if (*charge > limits.maximum_state_bytes)
    return common::make_unexpected(exhausted("sort state exceeds the configured byte limit"));
  return {};
}

[[nodiscard]] common::Result<void>
validate_chunk_shape(const VectorChunk& chunk, const std::vector<AccountedVectorChunk>& retained,
                     const std::vector<VectorSortKey>& keys) {
  for (const VectorSortKey& key : keys) {
    if (key.column_ordinal >= chunk.column_count())
      return common::make_unexpected(out_of_range("sort key ordinal is outside the input chunk"));
  }
  if (retained.empty())
    return {};
  const VectorChunk& expected = retained.front().chunk();
  if (chunk.column_count() != expected.column_count())
    return common::make_unexpected(invalid("sort input column count changed across chunks"));
  for (std::size_t ordinal = 0U; ordinal < chunk.column_count(); ++ordinal) {
    const columnar::PhysicalColumnView* actual = chunk.column(ordinal);
    const columnar::PhysicalColumnView* reference = expected.column(ordinal);
    if (actual == nullptr || reference == nullptr || actual->type() != reference->type() ||
        actual->nullable() != reference->nullable()) {
      return common::make_unexpected(invalid("sort input shape changed across chunks"));
    }
  }
  return {};
}

[[nodiscard]] common::Result<columnar::ColumnCellView>
row_cell(const std::vector<AccountedVectorChunk>& chunks, const BufferedRow row,
         const std::size_t column_ordinal) {
  if (row.chunk_ordinal >= chunks.size())
    return common::make_unexpected(internal("sort row references an absent chunk"));
  return chunks[row.chunk_ordinal].chunk().cell(
      {.column_ordinal = column_ordinal, .selected_row = row.selected_row});
}

// Left/right order is intrinsic to a comparison operation.
// NOLINTBEGIN(bugprone-easily-swappable-parameters)
[[nodiscard]] common::Result<int> compare_rows(const std::vector<AccountedVectorChunk>& chunks,
                                               const std::vector<VectorSortKey>& keys,
                                               const BufferedRow left, const BufferedRow right) {
  for (const VectorSortKey& key : keys) {
    const common::Result<columnar::ColumnCellView> left_cell =
        row_cell(chunks, left, key.column_ordinal);
    if (!left_cell.has_value())
      return common::make_unexpected(left_cell.error());
    const common::Result<columnar::ColumnCellView> right_cell =
        row_cell(chunks, right, key.column_ordinal);
    if (!right_cell.has_value())
      return common::make_unexpected(right_cell.error());
    const columnar::PhysicalColumnView* column =
        chunks[left.chunk_ordinal].chunk().column(key.column_ordinal);
    if (column == nullptr)
      return common::make_unexpected(internal("sort key column disappeared"));
    const bool includes_null = left_cell->is_null() || right_cell->is_null();
    common::Result<int> comparison =
        compare_physical_cells(column->type(), *left_cell, *right_cell, key.null_placement);
    if (!comparison.has_value())
      return common::make_unexpected(comparison.error());
    if (!includes_null && key.direction == PhysicalSortDirection::kDescending)
      *comparison = -*comparison;
    if (*comparison != 0)
      return *comparison;
  }
  return 0;
}
// NOLINTEND(bugprone-easily-swappable-parameters)

[[nodiscard]] common::Result<void>
stable_merge_sort(std::vector<BufferedRow>& rows, std::vector<BufferedRow>& scratch,
                  const std::vector<AccountedVectorChunk>& chunks,
                  const std::vector<VectorSortKey>& keys, const QueryResourceContext& resources) {
  try {
    scratch.resize(rows.size());
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("sort scratch allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("sort scratch exceeds container limits"));
  }
  bool rows_are_source = true;
  for (std::size_t width = 1U; width < rows.size();) {
    const common::Result<void> active = resources.check_cancelled();
    if (!active.has_value())
      return common::make_unexpected(active.error());
    std::vector<BufferedRow>& source = rows_are_source ? rows : scratch;
    std::vector<BufferedRow>& destination = rows_are_source ? scratch : rows;
    for (std::size_t begin = 0U; begin < rows.size();) {
      const std::size_t middle = std::min(rows.size(), begin + width);
      const std::size_t run_width = std::min(rows.size() - begin, width * 2U);
      const std::size_t end = begin + run_width;
      std::size_t left = begin;
      std::size_t right = middle;
      std::size_t output = begin;
      while (left < middle && right < end) {
        const common::Result<int> order = compare_rows(chunks, keys, source[left], source[right]);
        if (!order.has_value())
          return common::make_unexpected(order.error());
        destination[output++] = *order <= 0 ? source[left++] : source[right++];
      }
      while (left < middle)
        destination[output++] = source[left++];
      while (right < end)
        destination[output++] = source[right++];
      begin = end;
    }
    rows_are_source = !rows_are_source;
    width = width > rows.size() / 2U ? rows.size() : width * 2U;
  }
  if (!rows_are_source)
    rows.swap(scratch);
  return {};
}

struct OutputPlan {
  std::uint32_t row_count;
  std::size_t retained_charge;
};

[[nodiscard]] common::Result<OutputPlan>
plan_output(const std::vector<AccountedVectorChunk>& chunks, const std::vector<BufferedRow>& rows,
            const VectorChunkLimits& limits) {
  if (chunks.empty() || rows.empty())
    return common::make_unexpected(internal("sort output requires buffered rows"));
  const VectorChunk& first = chunks.front().chunk();
  if (rows.size() > limits.maximum_rows || first.column_count() > limits.maximum_columns)
    return common::make_unexpected(exhausted("sort output exceeds its row or column limit"));
  common::Result<std::size_t> total =
      multiply_bytes(rows.size(), sizeof(std::uint32_t), "sort selection size overflowed");
  if (!total.has_value())
    return common::make_unexpected(total.error());

  for (std::size_t ordinal = 0U; ordinal < first.column_count(); ++ordinal) {
    const columnar::PhysicalColumnView* column = first.column(ordinal);
    if (column == nullptr)
      return common::make_unexpected(internal("sort output column is absent"));
    if (column->nullable()) {
      total = add_bytes(*total, columnar::bitmap_size(static_cast<std::uint32_t>(rows.size())),
                        "sort validity size overflowed");
    }
    if (!total.has_value())
      return common::make_unexpected(total.error());
    if (column->type().kind() == schema::LogicalTypeKind::kBool) {
      total = add_bytes(*total, columnar::bitmap_size(static_cast<std::uint32_t>(rows.size())),
                        "sort Boolean size overflowed");
    } else if (column->type().is_variable_width()) {
      common::Result<std::size_t> offsets =
          multiply_bytes(rows.size() + 1U, sizeof(std::uint32_t), "sort offset size overflowed");
      if (!offsets.has_value())
        return common::make_unexpected(offsets.error());
      total = add_bytes(*total, *offsets, "sort output size overflowed");
      if (!total.has_value())
        return common::make_unexpected(total.error());
      std::size_t values = 0U;
      for (const BufferedRow row : rows) {
        const common::Result<columnar::ColumnCellView> cell = row_cell(chunks, row, ordinal);
        if (!cell.has_value())
          return common::make_unexpected(cell.error());
        if (cell->is_null())
          continue;
        const common::Result<common::ByteView> bytes = cell->bytes();
        if (!bytes.has_value())
          return common::make_unexpected(bytes.error());
        const common::Result<std::size_t> next =
            add_bytes(values, bytes->size(), "sort variable value size overflowed");
        if (!next.has_value() || *next > std::numeric_limits<std::uint32_t>::max()) {
          return common::make_unexpected(
              exhausted("sort variable value size exceeds the canonical offset domain"));
        }
        values = *next;
      }
      total = add_bytes(*total, values, "sort output size overflowed");
    } else {
      common::Result<std::size_t> values = multiply_bytes(
          rows.size(), fixed_width(column->type().kind()), "sort fixed value size overflowed");
      if (!values.has_value())
        return common::make_unexpected(values.error());
      total = add_bytes(*total, *values, "sort output size overflowed");
    }
    if (!total.has_value())
      return common::make_unexpected(total.error());
    if (*total > limits.maximum_buffer_bytes)
      return common::make_unexpected(exhausted("sort output exceeds its buffer-byte limit"));
  }

  common::Result<std::size_t> objects =
      multiply_bytes(first.column_count(), sizeof(columnar::OwnedPhysicalColumn),
                     "sort output column-container size overflowed");
  if (!objects.has_value())
    return common::make_unexpected(objects.error());
  common::Result<std::size_t> charge =
      add_bytes(*total, *objects, "sort retained output size overflowed");
  if (!charge.has_value())
    return common::make_unexpected(charge.error());
  const std::optional<std::size_t> buffer_allocations =
      common::checked_multiply(first.column_count(), std::size_t{3U});
  const std::optional<std::size_t> allocations =
      buffer_allocations.has_value() ? common::checked_add(*buffer_allocations, std::size_t{2U})
                                     : std::nullopt;
  if (!allocations.has_value())
    return common::make_unexpected(exhausted("sort allocation count overflowed"));
  common::Result<std::size_t> overhead = multiply_bytes(
      *allocations, kConservativeAllocationOverheadBytes, "sort allocation overhead overflowed");
  if (!overhead.has_value())
    return common::make_unexpected(overhead.error());
  charge = add_bytes(*charge, *overhead, "sort retained output size overflowed");
  if (!charge.has_value())
    return common::make_unexpected(charge.error());
  if (*charge > limits.maximum_retained_buffer_bytes)
    return common::make_unexpected(exhausted("sort output exceeds its retained-byte limit"));
  return OutputPlan{.row_count = static_cast<std::uint32_t>(rows.size()),
                    .retained_charge = *charge};
}

// The ordinal selects a column while row_count defines its output domain.
// NOLINTBEGIN(bugprone-easily-swappable-parameters)
[[nodiscard]] common::Result<columnar::OwnedPhysicalColumn>
materialize_column(const std::vector<AccountedVectorChunk>& chunks,
                   const std::vector<BufferedRow>& rows, const std::size_t ordinal,
                   const std::uint32_t row_count) {
  const columnar::PhysicalColumnView* source = chunks.front().chunk().column(ordinal);
  if (source == nullptr)
    return common::make_unexpected(internal("sort output column disappeared"));
  columnar::ColumnVectorBuffers buffers;
  if (source->nullable())
    buffers.validity.resize(columnar::bitmap_size(row_count));
  if (source->type().kind() == schema::LogicalTypeKind::kBool) {
    buffers.values.resize(columnar::bitmap_size(row_count));
  } else if (source->type().is_variable_width()) {
    buffers.offsets.resize((static_cast<std::size_t>(row_count) + 1U) * sizeof(std::uint32_t));
    std::size_t value_bytes = 0U;
    for (const BufferedRow row : rows) {
      const common::Result<columnar::ColumnCellView> cell = row_cell(chunks, row, ordinal);
      if (!cell.has_value())
        return common::make_unexpected(cell.error());
      if (cell->is_null())
        continue;
      const common::Result<common::ByteView> bytes = cell->bytes();
      if (!bytes.has_value())
        return common::make_unexpected(bytes.error());
      const common::Result<std::size_t> next =
          add_bytes(value_bytes, bytes->size(), "sort variable materialization size overflowed");
      if (!next.has_value())
        return common::make_unexpected(next.error());
      value_bytes = *next;
    }
    buffers.values.reserve(value_bytes);
  } else {
    buffers.values.resize(static_cast<std::size_t>(row_count) * fixed_width(source->type().kind()));
  }

  std::uint32_t null_count = 0U;
  const std::size_t width = fixed_width(source->type().kind());
  for (std::uint32_t output_row = 0U; output_row < row_count; ++output_row) {
    const common::Result<columnar::ColumnCellView> cell =
        row_cell(chunks, rows[output_row], ordinal);
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
        const common::Result<common::ByteView> bytes = cell->bytes();
        if (!bytes.has_value())
          return common::make_unexpected(bytes.error());
        if (source->type().is_variable_width()) {
          buffers.values.insert(buffers.values.end(), bytes->begin(), bytes->end());
        } else {
          if (bytes->size() != width)
            return common::make_unexpected(internal("sort fixed cell width changed"));
          std::ranges::copy(*bytes, buffers.values.begin() +
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
                                                .row_count = row_count,
                                                .null_count = null_count},
                                               std::move(buffers));
}
// NOLINTEND(bugprone-easily-swappable-parameters)

[[nodiscard]] common::Result<AccountedVectorChunk>
materialize_output(const QueryResourceContext& resources,
                   const std::vector<AccountedVectorChunk>& chunks,
                   const std::vector<BufferedRow>& rows, const VectorChunkLimits limits) {
  common::Result<OutputPlan> plan = plan_output(chunks, rows, limits);
  if (!plan.has_value())
    return common::make_unexpected(plan.error());
  common::Result<QueryMemoryReservation> reservation = resources.reserve(plan->retained_charge);
  if (!reservation.has_value())
    return common::make_unexpected(reservation.error());
  try {
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.reserve(chunks.front().chunk().column_count());
    for (std::size_t ordinal = 0U; ordinal < chunks.front().chunk().column_count(); ++ordinal) {
      const common::Result<void> active = resources.check_cancelled();
      if (!active.has_value())
        return common::make_unexpected(active.error());
      common::Result<columnar::OwnedPhysicalColumn> column =
          materialize_column(chunks, rows, ordinal, plan->row_count);
      if (!column.has_value())
        return common::make_unexpected(column.error());
      columns.push_back(std::move(*column));
    }
    common::Result<VectorSelection> selection = VectorSelection::all(plan->row_count);
    if (!selection.has_value())
      return common::make_unexpected(selection.error());
    common::Result<VectorChunk> output =
        VectorChunk::create(std::move(columns), std::move(*selection), limits);
    if (!output.has_value())
      return common::make_unexpected(output.error());
    return AccountedVectorChunk::create(std::move(*output), std::move(*reservation), resources);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("sort output allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("sort output exceeds container limits"));
  }
}

} // namespace

common::Result<std::size_t> sort_state_reservation_bytes(const SortLimits limits) {
  const common::Result<void> valid = validate_limits(limits);
  if (!valid.has_value())
    return common::make_unexpected(valid.error());
  return state_charge(limits);
}

class SortOperator::State {
public:
  std::vector<AccountedVectorChunk> chunks;
  std::vector<BufferedRow> rows;
  std::vector<BufferedRow> scratch;
  std::optional<QueryMemoryReservation> reservation;
};

SortOperator::~SortOperator() = default;

SortOperator::SortOperator(std::unique_ptr<PhysicalOperator> input, std::vector<VectorSortKey> keys,
                           const SortLimits limits, std::unique_ptr<State> state) noexcept
    : input_(std::move(input)), keys_(std::move(keys)), limits_(limits), state_(std::move(state)) {}

common::Result<std::unique_ptr<PhysicalOperator>>
SortOperator::create(std::unique_ptr<PhysicalOperator> input, std::vector<VectorSortKey> keys,
                     const SortLimits limits) {
  if (input == nullptr)
    return common::make_unexpected(invalid("sort input must be non-null"));
  const common::Result<std::size_t> state_bytes = sort_state_reservation_bytes(limits);
  if (!state_bytes.has_value())
    return common::make_unexpected(state_bytes.error());
  if (keys.empty())
    return common::make_unexpected(invalid("sort requires at least one key"));
  if (keys.size() > limits.maximum_keys || keys.capacity() > limits.maximum_keys)
    return common::make_unexpected(exhausted("sort key configuration exceeds its limit"));
  try {
    return std::unique_ptr<PhysicalOperator>{
        new SortOperator{std::move(input), std::move(keys), limits, std::make_unique<State>()}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("sort operator allocation failed"));
  }
}

common::Result<PhysicalOperatorStep> SortOperator::next(const QueryResourceContext& resources) {
  if (ended_)
    return PhysicalOperatorStep::end();
  const common::Result<void> active = resources.check_cancelled();
  if (!active.has_value())
    return common::make_unexpected(active.error());

  const auto fail = [&](const common::Status& status) -> common::Result<PhysicalOperatorStep> {
    static_cast<void>(resources.request_cancel());
    input_.reset();
    state_.reset();
    return common::make_unexpected(status);
  };

  try {
    const common::Result<std::size_t> charge = sort_state_reservation_bytes(limits_);
    if (!charge.has_value())
      return fail(charge.error());
    common::Result<QueryMemoryReservation> reservation = resources.reserve(*charge);
    if (!reservation.has_value())
      return fail(reservation.error());
    state_->reservation.emplace(std::move(*reservation));
    state_->chunks.reserve(limits_.maximum_rows);
    state_->rows.reserve(limits_.maximum_rows);
    state_->scratch.reserve(limits_.maximum_rows);
    common::Result<std::size_t> retained =
        multiply_bytes(state_->chunks.capacity(), sizeof(AccountedVectorChunk),
                       "sort retained-chunk capacity overflowed");
    if (retained.has_value()) {
      common::Result<std::size_t> row_capacity =
          add_bytes(state_->rows.capacity(), state_->scratch.capacity(),
                    "sort row-reference capacity overflowed");
      if (row_capacity.has_value()) {
        row_capacity = multiply_bytes(*row_capacity, sizeof(BufferedRow),
                                      "sort row-reference capacity overflowed");
      }
      if (row_capacity.has_value()) {
        retained = add_bytes(*retained, *row_capacity, "sort state capacity overflowed");
      } else {
        retained = common::make_unexpected(row_capacity.error());
      }
    }
    if (retained.has_value()) {
      retained = add_bytes(*retained, 3U * kConservativeAllocationOverheadBytes,
                           "sort state capacity overflowed");
    }
    if (!retained.has_value())
      return fail(retained.error());
    if (*retained > *charge)
      return fail(exhausted("sort state allocation exceeded its charge"));

    for (;;) {
      const common::Result<void> pulling = resources.check_cancelled();
      if (!pulling.has_value())
        return fail(pulling.error());
      common::Result<PhysicalOperatorStep> step = input_->next(resources);
      if (!step.has_value())
        return fail(step.error());
      if (step->kind() == PhysicalOperatorStepKind::kEnd) {
        input_.reset();
        break;
      }
      common::Result<AccountedVectorChunk> chunk = std::move(*step).take_chunk();
      if (!chunk.has_value())
        return fail(chunk.error());
      if (!chunk->belongs_to(resources))
        return fail(invalid("sort input belongs to another query"));
      const common::Result<void> shape =
          validate_chunk_shape(chunk->chunk(), state_->chunks, keys_);
      if (!shape.has_value())
        return fail(shape.error());
      const std::size_t selected = chunk->chunk().selected_row_count();
      if (selected == 0U)
        continue;
      if (selected > static_cast<std::size_t>(limits_.maximum_rows) - state_->rows.size())
        return fail(exhausted("sort input exceeds its row limit"));
      const std::size_t chunk_ordinal = state_->chunks.size();
      for (std::size_t row = 0U; row < selected; ++row)
        state_->rows.push_back({.chunk_ordinal = chunk_ordinal, .selected_row = row});
      state_->chunks.push_back(std::move(*chunk));
    }

    if (state_->rows.empty()) {
      state_.reset();
      ended_ = true;
      return PhysicalOperatorStep::end();
    }
    const common::Result<void> sorted =
        stable_merge_sort(state_->rows, state_->scratch, state_->chunks, keys_, resources);
    if (!sorted.has_value())
      return fail(sorted.error());
    common::Result<AccountedVectorChunk> output =
        materialize_output(resources, state_->chunks, state_->rows, limits_.output_limits);
    if (!output.has_value())
      return fail(output.error());
    state_.reset();
    ended_ = true;
    return PhysicalOperatorStep::chunk(std::move(*output));
  } catch (const std::bad_alloc&) {
    return fail(exhausted("sort state allocation failed"));
  } catch (const std::length_error&) {
    return fail(exhausted("sort state exceeds container limits"));
  }
}

} // namespace chronos::query
