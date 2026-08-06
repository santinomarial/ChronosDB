#include "chronos/query/vector_chunk.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <utility>
#include <vector>

namespace {

[[nodiscard]] chronos::columnar::OwnedPhysicalColumn
make_bool_column(const std::uint32_t rows, const std::span<const std::uint8_t> input) {
  chronos::columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(chronos::columnar::bitmap_size(rows));
  for (std::uint32_t row = 0U; row < rows; ++row) {
    if (!input.empty() && (input[row % input.size()] & 1U) != 0U)
      buffers.values[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
  }
  return chronos::columnar::OwnedPhysicalColumn::create(
             {.type = chronos::schema::LogicalType::create(chronos::schema::LogicalTypeKind::kBool)
                          .value(),
              .nullable = false,
              .row_count = rows,
              .null_count = 0U},
             std::move(buffers))
      .value();
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const std::span<const std::uint8_t> input{data, size};
  const std::uint32_t rows = size == 0U ? 1U : static_cast<std::uint32_t>(data[0]) + 1U;

  std::vector<std::uint32_t> hostile_indices;
  hostile_indices.reserve(size);
  for (std::size_t index = 1U; index < size; ++index)
    hostile_indices.push_back(data[index]);
  auto hostile = chronos::query::VectorSelection::from_indices(rows, std::move(hostile_indices));
  if (hostile.has_value()) {
    std::vector<chronos::columnar::OwnedPhysicalColumn> columns;
    columns.push_back(make_bool_column(rows, input));
    const auto chunk =
        chronos::query::VectorChunk::create(std::move(columns), std::move(*hostile),
                                            {.maximum_rows = 256U,
                                             .maximum_columns = 1U,
                                             .maximum_buffer_bytes = 4'096U,
                                             .maximum_retained_buffer_bytes = 4'096U});
    if (chunk.has_value() && chunk->selected_row_count() > 0U) {
      const auto cell = chunk->cell({.column_ordinal = 0U, .selected_row = 0U});
      if (cell.has_value())
        static_cast<void>(cell->kind());
    }
  }

  std::vector<chronos::columnar::OwnedPhysicalColumn> valid_columns;
  valid_columns.push_back(make_bool_column(rows, input));
  auto all = chronos::query::VectorSelection::all(rows).value();
  const auto valid = chronos::query::VectorChunk::create(std::move(valid_columns), std::move(all),
                                                         {.maximum_rows = 256U,
                                                          .maximum_columns = 1U,
                                                          .maximum_buffer_bytes = 4'096U,
                                                          .maximum_retained_buffer_bytes = 4'096U});
  if (valid.has_value()) {
    const auto cell = valid->cell({.column_ordinal = 0U, .selected_row = rows - 1U});
    if (cell.has_value())
      static_cast<void>(cell->kind());
  }
  return 0;
}
