#include "chronos/query/physical_operator.hpp"
#include "chronos/query/vector_chunk.hpp"
#include "chronos/schema/logical_type.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
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

[[nodiscard]] chronos::columnar::OwnedPhysicalColumn
make_timestamp_column(const std::uint32_t rows, const std::span<const std::uint8_t> input) {
  chronos::columnar::ColumnVectorBuffers buffers;
  buffers.validity.resize(chronos::columnar::bitmap_size(rows));
  buffers.values.resize(static_cast<std::size_t>(rows) * sizeof(std::int64_t));
  std::uint32_t null_count = 0U;
  for (std::uint32_t row = 0U; row < rows; ++row) {
    const bool present = input.empty() || (input[row % input.size()] & 4U) == 0U;
    if (present) {
      buffers.validity[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
      const std::int64_t value =
          static_cast<std::int64_t>(row) - static_cast<std::int64_t>(rows / 2U);
      const std::uint64_t bits = std::bit_cast<std::uint64_t>(value);
      for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
        buffers.values[static_cast<std::size_t>(row) * sizeof(bits) + byte] =
            static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
      }
    } else {
      ++null_count;
    }
  }
  return chronos::columnar::OwnedPhysicalColumn::create(
             {.type = chronos::schema::LogicalType::create(
                          chronos::schema::LogicalTypeKind::kTimestampNs)
                          .value(),
              .nullable = true,
              .row_count = rows,
              .null_count = null_count},
             std::move(buffers))
      .value();
}

[[nodiscard]] std::int64_t fuzz_bound(const std::uint8_t value) noexcept {
  if (value == 0U)
    return std::numeric_limits<std::int64_t>::min();
  if (value == std::numeric_limits<std::uint8_t>::max())
    return std::numeric_limits<std::int64_t>::max();
  return static_cast<std::int64_t>(static_cast<std::int8_t>(value));
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

  std::vector<chronos::columnar::OwnedPhysicalColumn> timestamp_columns;
  timestamp_columns.push_back(make_timestamp_column(rows, input));
  auto timestamp_chunk = chronos::query::VectorChunk::create(
      std::move(timestamp_columns), chronos::query::VectorSelection::all(rows).value(),
      {.maximum_rows = 256U,
       .maximum_columns = 1U,
       .maximum_buffer_bytes = 4'096U,
       .maximum_retained_buffer_bytes = 4'096U});
  if (timestamp_chunk.has_value()) {
    chronos::query::TimestampRangePredicate predicate;
    if (size > 1U && (data[0] & 1U) != 0U) {
      predicate.lower = chronos::query::TimestampRangeBound{.value = fuzz_bound(data[1]),
                                                            .inclusive = (data[0] & 2U) != 0U};
    }
    if (size > 2U && (data[0] & 8U) != 0U) {
      predicate.upper = chronos::query::TimestampRangeBound{.value = fuzz_bound(data[2]),
                                                            .inclusive = (data[0] & 16U) != 0U};
    }
    const std::size_t ordinal = size > 3U ? static_cast<std::size_t>(data[3] % 2U) : 0U;
    auto filtered = chronos::query::VectorChunk::where_timestamp_in_range(
        std::move(*timestamp_chunk), ordinal, predicate);
    if (filtered.has_value() && filtered->selected_row_count() != 0U) {
      const auto cell = filtered->cell({.column_ordinal = 0U, .selected_row = 0U});
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

  auto shared_backing = std::make_shared<const SingleColumnBacking>(make_bool_column(rows, input));
  auto shared_chunk = chronos::query::VectorChunk::create_backed(
      std::move(shared_backing), chronos::query::VectorSelection::all(rows).value(),
      {.maximum_rows = 256U,
       .maximum_columns = 1U,
       .maximum_buffer_bytes = 4'096U,
       .maximum_retained_buffer_bytes = 4'096U});
  if (shared_chunk.has_value() && shared_chunk->retained_buffer_bytes() > 1U) {
    const std::size_t retained = shared_chunk->retained_buffer_bytes();
    const std::size_t shared_bytes =
        1U + (size == 0U ? 0U : static_cast<std::size_t>(data[0]) % (retained - 1U));
    const std::size_t local_bytes = retained - shared_bytes;
    auto resources = chronos::query::QueryResourceContext::create(8'192U).value();
    auto shared = resources.reserve_shared(shared_bytes).value();
    auto local = resources.reserve(local_bytes).value();
    auto accounted = chronos::query::AccountedVectorChunk::create(
        std::move(*shared_chunk), std::move(local), std::move(shared), resources);
    if (accounted.has_value()) {
      auto filtered = chronos::query::AccountedVectorChunk::where_true(std::move(*accounted), 0U);
      if (filtered.has_value())
        static_cast<void>(filtered->charged_memory_bytes());
    }
  }
  return 0;
}
