#include "chronos/query/vector_chunk.hpp"
#include "chronos/schema/logical_type.hpp"

#include <benchmark/benchmark.h>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

constexpr std::size_t kBenchmarkChunkMemoryLimit = std::size_t{64U} * 1024U * 1024U;

struct PredicateShape {
  std::uint32_t rows;
  std::uint32_t true_stride;
};

[[nodiscard]] columnar::OwnedPhysicalColumn make_column(const std::uint32_t rows) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(static_cast<std::size_t>(rows) * sizeof(std::int64_t));
  for (std::uint32_t row = 0U; row < rows; ++row) {
    const std::uint64_t bits = std::bit_cast<std::uint64_t>(static_cast<std::int64_t>(row));
    for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
      buffers.values[static_cast<std::size_t>(row) * sizeof(bits) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
              .nullable = false,
              .row_count = rows,
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn make_predicate(const PredicateShape shape) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(columnar::bitmap_size(shape.rows));
  for (std::uint32_t row = 0U; row < shape.rows; ++row) {
    if ((row % shape.true_stride) == 0U)
      buffers.values[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = schema::LogicalType::create(schema::LogicalTypeKind::kBool).value(),
              .nullable = false,
              .row_count = shape.rows,
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] std::vector<std::uint32_t> selection_indices(const std::uint32_t rows,
                                                           const std::uint32_t stride) {
  std::vector<std::uint32_t> indices;
  indices.reserve(static_cast<std::size_t>(rows / stride) + 1U);
  for (std::uint32_t row = 0U; row < rows; row += stride)
    indices.push_back(row);
  return indices;
}

void build_selection(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto stride = static_cast<std::uint32_t>(state.range(1));
  const std::vector<std::uint32_t> source = selection_indices(rows, stride);
  for (auto _ : state) {
    static_cast<void>(_);
    auto indices = source;
    auto selection = VectorSelection::from_indices(rows, std::move(indices));
    benchmark::DoNotOptimize(selection);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(source.size()));
  state.counters["physical_rows"] = static_cast<double>(rows);
  state.counters["selection_density"] =
      static_cast<double>(source.size()) / static_cast<double>(rows);
}

void scan_selected_cells(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto stride = static_cast<std::uint32_t>(state.range(1));
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(make_column(rows));
  VectorChunk chunk =
      VectorChunk::create(
          std::move(columns),
          VectorSelection::from_indices(rows, selection_indices(rows, stride)).value(),
          {.maximum_rows = rows,
           .maximum_columns = 1U,
           .maximum_buffer_bytes = kBenchmarkChunkMemoryLimit,
           .maximum_retained_buffer_bytes = kBenchmarkChunkMemoryLimit})
          .value();
  for (auto _ : state) {
    static_cast<void>(_);
    for (std::size_t selected = 0U; selected < chunk.selected_row_count(); ++selected) {
      auto cell = chunk.cell({.column_ordinal = 0U, .selected_row = selected});
      benchmark::DoNotOptimize(cell);
    }
  }
  const auto selected = static_cast<std::int64_t>(chunk.selected_row_count());
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * selected);
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) * selected *
                          static_cast<std::int64_t>(sizeof(std::int64_t)));
  state.counters["physical_rows"] = static_cast<double>(rows);
  state.counters["selection_density"] =
      static_cast<double>(chunk.selected_row_count()) / static_cast<double>(rows);
}

void compact_selection_where_true(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto true_stride = static_cast<std::uint32_t>(state.range(1));
  const columnar::OwnedPhysicalColumn predicate =
      make_predicate({.rows = rows, .true_stride = true_stride});
  const std::vector<std::uint32_t> source = selection_indices(rows, 1U);
  for (auto _ : state) {
    static_cast<void>(_);
    VectorSelection selection = VectorSelection::from_indices(rows, source).value();
    auto filtered = VectorSelection::where_true(std::move(selection), predicate);
    benchmark::DoNotOptimize(filtered);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(source.size()));
  state.counters["physical_rows"] = static_cast<double>(rows);
  state.counters["true_density"] = 1.0 / static_cast<double>(true_stride);
}

BENCHMARK(build_selection)
    ->Args({64, 1})
    ->Args({1'024, 1})
    ->Args({4'096, 1})
    ->Args({64, 4})
    ->Args({1'024, 4})
    ->Args({4'096, 4})
    ->Args({64, 16})
    ->Args({1'024, 16})
    ->Args({4'096, 16});
BENCHMARK(scan_selected_cells)
    ->Args({64, 1})
    ->Args({1'024, 1})
    ->Args({4'096, 1})
    ->Args({64, 4})
    ->Args({1'024, 4})
    ->Args({4'096, 4})
    ->Args({64, 16})
    ->Args({1'024, 16})
    ->Args({4'096, 16});
BENCHMARK(compact_selection_where_true)
    ->Args({64, 1})
    ->Args({1'024, 1})
    ->Args({4'096, 1})
    ->Args({64, 4})
    ->Args({1'024, 4})
    ->Args({4'096, 4})
    ->Args({64, 16})
    ->Args({1'024, 16})
    ->Args({4'096, 16});

} // namespace
} // namespace chronos::query
