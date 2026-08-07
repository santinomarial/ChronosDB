#include "chronos/query/vector_chunk.hpp"
#include "chronos/schema/logical_type.hpp"

#include <benchmark/benchmark.h>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

constexpr std::size_t kBenchmarkChunkMemoryLimit = std::size_t{64U} * 1024U * 1024U;

struct PredicateShape {
  std::uint32_t rows;
  std::uint32_t true_stride;
};

struct ProjectionShape {
  std::uint32_t rows;
  std::size_t columns;
};

struct LimitShape {
  std::uint32_t rows;
  std::uint32_t selection_stride;
  std::size_t maximum_selected_rows;
};

class BenchmarkBacking final : public VectorChunkBacking {
public:
  explicit BenchmarkBacking(columnar::OwnedPhysicalColumn column) : column_(std::move(column)) {}

  [[nodiscard]] std::size_t column_count() const noexcept override {
    return 1U;
  }

  [[nodiscard]] const columnar::PhysicalColumnView*
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
  columnar::OwnedPhysicalColumn column_;
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

[[nodiscard]] columnar::OwnedPhysicalColumn make_timestamp_column(const std::uint32_t rows) {
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
             {.type = schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
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

void attach_pinned_chunk_backing(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto stride = static_cast<std::uint32_t>(state.range(1));
  const std::vector<std::uint32_t> source = selection_indices(rows, stride);
  const std::shared_ptr<const VectorChunkBacking> backing =
      std::make_shared<const BenchmarkBacking>(make_column(rows));
  for (auto _ : state) {
    static_cast<void>(_);
    VectorSelection selection = VectorSelection::from_indices(rows, source).value();
    auto chunk =
        VectorChunk::create_backed(backing, std::move(selection),
                                   {.maximum_rows = rows,
                                    .maximum_columns = 1U,
                                    .maximum_buffer_bytes = kBenchmarkChunkMemoryLimit,
                                    .maximum_retained_buffer_bytes = kBenchmarkChunkMemoryLimit});
    benchmark::DoNotOptimize(chunk);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(source.size()));
  state.counters["physical_rows"] = static_cast<double>(rows);
  state.counters["selection_density"] =
      static_cast<double>(source.size()) / static_cast<double>(rows);
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
    auto filtered = VectorSelection::where_true(std::move(selection), predicate.view());
    benchmark::DoNotOptimize(filtered);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(source.size()));
  state.counters["physical_rows"] = static_cast<double>(rows);
  state.counters["true_density"] = 1.0 / static_cast<double>(true_stride);
}

void compact_selection_timestamp_range(benchmark::State& state) {
  constexpr std::size_t kSelectionsPerIteration = 256U;
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto selection_stride = static_cast<std::uint32_t>(state.range(1));
  const columnar::OwnedPhysicalColumn timestamps = make_timestamp_column(rows);
  const std::vector<std::uint32_t> source = selection_indices(rows, selection_stride);
  const TimestampRangePredicate predicate{
      .lower =
          TimestampRangeBound{.value = static_cast<std::int64_t>(rows / 4U), .inclusive = true},
      .upper = TimestampRangeBound{.value = static_cast<std::int64_t>((rows * 3U) / 4U),
                                   .inclusive = false}};
  for (auto _ : state) {
    static_cast<void>(_);
    state.PauseTiming();
    {
      std::vector<VectorSelection> selections;
      selections.reserve(kSelectionsPerIteration);
      for (std::size_t index = 0U; index < kSelectionsPerIteration; ++index)
        selections.push_back(VectorSelection::from_indices(rows, source).value());
      state.ResumeTiming();
      for (VectorSelection& selection : selections) {
        auto filtered = VectorSelection::where_timestamp_in_range(std::move(selection),
                                                                  timestamps.view(), predicate);
        selection = std::move(filtered).value();
      }
      benchmark::DoNotOptimize(selections);
      state.PauseTiming();
    }
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(kSelectionsPerIteration) *
                          static_cast<std::int64_t>(source.size()));
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(kSelectionsPerIteration) *
                          static_cast<std::int64_t>(source.size()) *
                          static_cast<std::int64_t>(sizeof(std::int64_t)));
  state.counters["physical_rows"] = static_cast<double>(rows);
  state.counters["selection_density"] = 1.0 / static_cast<double>(selection_stride);
  state.counters["range_density"] = 0.5;
}

void project_column_subset(benchmark::State& state) {
  const ProjectionShape shape{.rows = static_cast<std::uint32_t>(state.range(0)),
                              .columns = static_cast<std::size_t>(state.range(1))};
  std::vector<std::size_t> ordinals;
  ordinals.reserve((shape.columns + 1U) / 2U);
  for (std::size_t ordinal = 0U; ordinal < shape.columns; ordinal += 2U)
    ordinals.push_back(ordinal);

  for (auto _ : state) {
    static_cast<void>(_);
    state.PauseTiming();
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.reserve(shape.columns);
    for (std::size_t column = 0U; column < shape.columns; ++column)
      columns.push_back(make_predicate({.rows = shape.rows, .true_stride = 2U}));
    VectorChunk chunk =
        VectorChunk::create(std::move(columns), VectorSelection::all(shape.rows).value(),
                            {.maximum_rows = shape.rows,
                             .maximum_columns = shape.columns,
                             .maximum_buffer_bytes = kBenchmarkChunkMemoryLimit,
                             .maximum_retained_buffer_bytes = kBenchmarkChunkMemoryLimit})
            .value();
    state.ResumeTiming();
    auto projected = VectorChunk::project_columns(std::move(chunk), ordinals);
    benchmark::DoNotOptimize(projected);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(shape.columns));
  state.counters["input_columns"] = static_cast<double>(shape.columns);
  state.counters["output_columns"] = static_cast<double>(ordinals.size());
  state.counters["physical_rows"] = static_cast<double>(shape.rows);
}

void truncate_selection_limit(benchmark::State& state) {
  constexpr std::size_t kSelectionsPerIteration = 256U;
  const LimitShape shape{.rows = static_cast<std::uint32_t>(state.range(0)),
                         .selection_stride = static_cast<std::uint32_t>(state.range(1)),
                         .maximum_selected_rows = static_cast<std::size_t>(state.range(2))};
  const std::vector<std::uint32_t> source = selection_indices(shape.rows, shape.selection_stride);
  for (auto _ : state) {
    static_cast<void>(_);
    state.PauseTiming();
    {
      std::vector<VectorSelection> selections;
      selections.reserve(kSelectionsPerIteration);
      for (std::size_t index = 0U; index < kSelectionsPerIteration; ++index)
        selections.push_back(VectorSelection::from_indices(shape.rows, source).value());
      state.ResumeTiming();
      for (VectorSelection& selection : selections) {
        selection = VectorSelection::take_first(std::move(selection), shape.maximum_selected_rows);
      }
      benchmark::DoNotOptimize(selections);
      state.PauseTiming();
    }
    state.ResumeTiming();
  }
  const std::size_t kept =
      shape.maximum_selected_rows < source.size() ? shape.maximum_selected_rows : source.size();
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(kSelectionsPerIteration));
  state.counters["input_selected_rows"] = static_cast<double>(source.size());
  state.counters["output_selected_rows"] = static_cast<double>(kept);
  state.counters["physical_rows"] = static_cast<double>(shape.rows);
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
BENCHMARK(attach_pinned_chunk_backing)
    ->Args({64, 1})
    ->Args({1'024, 1})
    ->Args({4'096, 1})
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
BENCHMARK(compact_selection_timestamp_range)
    ->Args({64, 1})
    ->Args({1'024, 1})
    ->Args({4'096, 1})
    ->Args({64, 4})
    ->Args({1'024, 4})
    ->Args({4'096, 4})
    ->Args({64, 16})
    ->Args({1'024, 16})
    ->Args({4'096, 16});
BENCHMARK(project_column_subset)
    ->Args({1'024, 1})
    ->Args({1'024, 8})
    ->Args({1'024, 64})
    ->Args({4'096, 1})
    ->Args({4'096, 8})
    ->Args({4'096, 64});
BENCHMARK(truncate_selection_limit)
    ->Args({1'024, 1, 0})
    ->Args({1'024, 1, 32})
    ->Args({1'024, 1, 1'024})
    ->Args({4'096, 4, 0})
    ->Args({4'096, 4, 32})
    ->Args({4'096, 4, 1'024});

} // namespace
} // namespace chronos::query
