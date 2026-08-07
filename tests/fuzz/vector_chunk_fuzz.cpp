#include "chronos/query/vector_chunk.hpp"
#include "chronos/schema/logical_type.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <utility>
#include <vector>

namespace {

class SingleColumnBacking final : public chronos::query::VectorChunkBacking {
public:
  explicit SingleColumnBacking(chronos::columnar::OwnedPhysicalColumn column)
      : column_(std::move(column)) {}

  [[nodiscard]] std::size_t column_count() const noexcept override {
    return 1U;
  }

  [[nodiscard]] const chronos::columnar::PhysicalColumnView*
  column(const std::size_t ordinal) const noexcept override {
    return ordinal == 0U ? &column_.view() : nullptr;
  }

  [[nodiscard]] std::size_t buffer_bytes() const noexcept override {
    return column_.buffer_bytes();
  }

  [[nodiscard]] std::size_t retained_buffer_bytes() const noexcept override {
    return sizeof(column_) + column_.retained_buffer_bytes();
  }

private:
  chronos::columnar::OwnedPhysicalColumn column_;
};

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
  auto valid = chronos::query::VectorChunk::create(std::move(valid_columns), std::move(all),
                                                   {.maximum_rows = 256U,
                                                    .maximum_columns = 1U,
                                                    .maximum_buffer_bytes = 4'096U,
                                                    .maximum_retained_buffer_bytes = 4'096U});
  if (valid.has_value()) {
    auto filtered = chronos::query::VectorChunk::where_true(std::move(*valid), 0U);
    if (filtered.has_value()) {
      auto limited = chronos::query::VectorChunk::take_first(
          std::move(*filtered), size == 0U ? 0U : static_cast<std::size_t>(data[size - 1U]));
      const std::array<std::size_t, 2> ordinals{
          size == 0U ? 0U : static_cast<std::size_t>(data[0] % 3U),
          size < 2U ? 0U : static_cast<std::size_t>(data[1] % 3U)};
      const std::size_t ordinal_count = size == 0U ? 0U : size % (ordinals.size() + 1U);
      auto projected = chronos::query::VectorChunk::project_columns(
          std::move(limited), std::span<const std::size_t>{ordinals}.first(ordinal_count));
      if (!projected.has_value() || projected->column_count() == 0U ||
          projected->selected_row_count() == 0U) {
        return 0;
      }
      const auto cell = projected->cell({.column_ordinal = 0U, .selected_row = 0U});
      if (cell.has_value())
        static_cast<void>(cell->kind());
    }
  }

  auto backing = std::make_shared<const SingleColumnBacking>(make_bool_column(rows, input));
  auto backed = chronos::query::VectorChunk::create_backed(
      std::move(backing), chronos::query::VectorSelection::all(rows).value(),
      {.maximum_rows = 256U,
       .maximum_columns = 1U,
       .maximum_buffer_bytes = 4'096U,
       .maximum_retained_buffer_bytes = 4'096U});
  if (backed.has_value()) {
    auto filtered = chronos::query::VectorChunk::where_true(std::move(*backed), 0U);
    if (filtered.has_value()) {
      auto projected = chronos::query::VectorChunk::project_columns(std::move(*filtered),
                                                                    std::array<std::size_t, 1>{0U});
      if (projected.has_value() && projected->selected_row_count() != 0U) {
        const auto cell = projected->cell({.column_ordinal = 0U, .selected_row = 0U});
        if (cell.has_value())
          static_cast<void>(cell->kind());
      }
    }
  }
  return 0;
}
