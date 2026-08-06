#include "chronos/query/vector_chunk.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstddef>
#include <cstdint>
#include <new>
#include <numeric>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Result<std::size_t> index_bytes(const std::size_t count) {
  const auto bytes = common::checked_multiply(count, sizeof(std::uint32_t));
  if (!bytes.has_value()) {
    return common::make_unexpected(exhausted("vector selection byte accounting overflowed"));
  }
  return *bytes;
}

} // namespace

VectorSelection::VectorSelection(const std::uint32_t physical_row_count,
                                 std::vector<std::uint32_t> indices, const bool identity) noexcept
    : physical_row_count_(physical_row_count), indices_(std::move(indices)), identity_(identity) {}

common::Result<VectorSelection> VectorSelection::all(const std::uint32_t physical_row_count) {
  if (physical_row_count == 0U) {
    return common::make_unexpected(invalid("vector selection physical row count must be nonzero"));
  }
  try {
    std::vector<std::uint32_t> indices(physical_row_count);
    std::iota(indices.begin(), indices.end(), 0U);
    return VectorSelection{physical_row_count, std::move(indices), true};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("vector selection allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("vector selection exceeds container limits"));
  }
}

common::Result<VectorSelection>
VectorSelection::from_indices(const std::uint32_t physical_row_count,
                              std::vector<std::uint32_t> indices) {
  if (physical_row_count == 0U) {
    return common::make_unexpected(invalid("vector selection physical row count must be nonzero"));
  }
  bool identity = indices.size() == static_cast<std::size_t>(physical_row_count);
  for (std::size_t index = 0U; index < indices.size(); ++index) {
    if (indices[index] >= physical_row_count) {
      return common::make_unexpected(invalid("vector selection row is out of range"));
    }
    if (index > 0U && indices[index - 1U] >= indices[index]) {
      return common::make_unexpected(
          invalid("vector selection rows must be unique and strictly increasing"));
    }
    if (indices[index] != index) {
      identity = false;
    }
  }
  return VectorSelection{physical_row_count, std::move(indices), identity};
}

std::uint32_t VectorSelection::physical_row_count() const noexcept {
  return physical_row_count_;
}

std::span<const std::uint32_t> VectorSelection::indices() const noexcept {
  return indices_;
}

std::size_t VectorSelection::selected_row_count() const noexcept {
  return indices_.size();
}

bool VectorSelection::is_identity() const noexcept {
  return identity_;
}

std::size_t VectorSelection::buffer_bytes() const noexcept {
  return indices_.size() * sizeof(std::uint32_t);
}

std::size_t VectorSelection::retained_buffer_bytes() const noexcept {
  return indices_.capacity() * sizeof(std::uint32_t);
}

common::Result<std::uint32_t> VectorSelection::physical_row(const std::size_t selected_row) const {
  if (selected_row >= indices_.size()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kOutOfRange, "selected row is out of range"});
  }
  return indices_[selected_row];
}

common::Result<VectorSelection>
VectorSelection::where_true(VectorSelection selection,
                            const columnar::OwnedPhysicalColumn& predicate) {
  if (predicate.type().kind() != schema::LogicalTypeKind::kBool) {
    return common::make_unexpected(invalid("vector predicate column must have BOOL type"));
  }
  if (predicate.row_count() != selection.physical_row_count_) {
    return common::make_unexpected(
        invalid("vector predicate row count must match the selection physical row count"));
  }

  std::size_t output = 0U;
  for (const std::uint32_t physical_row : selection.indices_) {
    const common::Result<columnar::ColumnCellView> cell = predicate.cell(physical_row);
    if (!cell.has_value())
      return common::make_unexpected(cell.error());
    if (cell->kind() == columnar::ColumnCellView::Kind::kNull)
      continue;
    const common::Result<bool> value = cell->boolean();
    if (!value.has_value())
      return common::make_unexpected(value.error());
    if (*value) {
      selection.indices_[output] = physical_row;
      ++output;
    }
  }
  selection.indices_.resize(output);
  selection.identity_ = selection.identity_ && output == selection.physical_row_count_;
  return selection;
}

VectorChunk::VectorChunk(std::vector<columnar::OwnedPhysicalColumn> columns,
                         VectorSelection selection, const Accounting accounting) noexcept
    : columns_(std::move(columns)), selection_(std::move(selection)),
      buffer_bytes_(accounting.buffer_bytes),
      retained_buffer_bytes_(accounting.retained_buffer_bytes) {}

common::Result<VectorChunk> VectorChunk::create(std::vector<columnar::OwnedPhysicalColumn> columns,
                                                VectorSelection selection,
                                                const VectorChunkLimits limits) {
  if (limits.maximum_rows == 0U || limits.maximum_columns == 0U ||
      limits.maximum_buffer_bytes == 0U || limits.maximum_retained_buffer_bytes == 0U) {
    return common::make_unexpected(invalid("vector chunk limits must be nonzero"));
  }
  if (selection.physical_row_count() > limits.maximum_rows) {
    return common::make_unexpected(exhausted("vector chunk exceeds the physical row limit"));
  }
  if (columns.size() > limits.maximum_columns) {
    return common::make_unexpected(exhausted("vector chunk exceeds the column limit"));
  }

  const common::Result<std::size_t> selection_bytes = index_bytes(selection.selected_row_count());
  if (!selection_bytes.has_value()) {
    return common::make_unexpected(selection_bytes.error());
  }
  std::size_t buffer_bytes = *selection_bytes;
  std::size_t retained_bytes = selection.retained_buffer_bytes();
  if (buffer_bytes > limits.maximum_buffer_bytes) {
    return common::make_unexpected(exhausted("vector chunk exceeds the buffer-byte limit"));
  }
  if (retained_bytes > limits.maximum_retained_buffer_bytes) {
    return common::make_unexpected(
        exhausted("vector chunk exceeds the retained-buffer-byte limit"));
  }

  for (const columnar::OwnedPhysicalColumn& column : columns) {
    if (column.row_count() != selection.physical_row_count()) {
      return common::make_unexpected(
          invalid("all vector chunk columns must match the selection physical row count"));
    }
    const auto next_buffer = common::checked_add(buffer_bytes, column.buffer_bytes());
    if (!next_buffer.has_value()) {
      return common::make_unexpected(exhausted("vector chunk buffer accounting overflowed"));
    }
    buffer_bytes = *next_buffer;
    if (buffer_bytes > limits.maximum_buffer_bytes) {
      return common::make_unexpected(exhausted("vector chunk exceeds the buffer-byte limit"));
    }

    const auto next_retained = common::checked_add(retained_bytes, column.retained_buffer_bytes());
    if (!next_retained.has_value()) {
      return common::make_unexpected(
          exhausted("vector chunk retained-buffer accounting overflowed"));
    }
    retained_bytes = *next_retained;
    if (retained_bytes > limits.maximum_retained_buffer_bytes) {
      return common::make_unexpected(
          exhausted("vector chunk exceeds the retained-buffer-byte limit"));
    }
  }
  return VectorChunk{
      std::move(columns), std::move(selection),
      Accounting{.buffer_bytes = buffer_bytes, .retained_buffer_bytes = retained_bytes}};
}

std::uint32_t VectorChunk::physical_row_count() const noexcept {
  return selection_.physical_row_count();
}

std::size_t VectorChunk::selected_row_count() const noexcept {
  return selection_.selected_row_count();
}

std::span<const columnar::OwnedPhysicalColumn> VectorChunk::columns() const noexcept {
  return columns_;
}

const columnar::OwnedPhysicalColumn* VectorChunk::column(const std::size_t ordinal) const noexcept {
  return ordinal < columns_.size() ? &columns_[ordinal] : nullptr;
}

const VectorSelection& VectorChunk::selection() const noexcept {
  return selection_;
}

common::Result<columnar::ColumnCellView>
VectorChunk::cell(const SelectedVectorCell position) const {
  const columnar::OwnedPhysicalColumn* selected_column = column(position.column_ordinal);
  if (selected_column == nullptr) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kOutOfRange, "vector chunk column is out of range"});
  }
  const common::Result<std::uint32_t> physical_row = selection_.physical_row(position.selected_row);
  if (!physical_row.has_value()) {
    return common::make_unexpected(physical_row.error());
  }
  return selected_column->cell(*physical_row);
}

std::size_t VectorChunk::buffer_bytes() const noexcept {
  return buffer_bytes_;
}

std::size_t VectorChunk::retained_buffer_bytes() const noexcept {
  return retained_buffer_bytes_;
}

common::Result<VectorChunk> VectorChunk::where_true(VectorChunk chunk,
                                                    const std::size_t predicate_column) {
  const columnar::OwnedPhysicalColumn* predicate = chunk.column(predicate_column);
  if (predicate == nullptr) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kOutOfRange, "vector predicate column is out of range"});
  }
  const std::size_t old_selection_bytes = chunk.selection_.buffer_bytes();
  common::Result<VectorSelection> filtered =
      VectorSelection::where_true(std::move(chunk.selection_), *predicate);
  if (!filtered.has_value())
    return common::make_unexpected(filtered.error());
  chunk.selection_ = std::move(*filtered);
  chunk.buffer_bytes_ -= old_selection_bytes;
  chunk.buffer_bytes_ += chunk.selection_.buffer_bytes();
  return chunk;
}

} // namespace chronos::query
