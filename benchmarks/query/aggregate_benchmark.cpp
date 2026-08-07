#include "chronos/query/aggregate.hpp"
#include "support/counting_allocator.hpp"

#include <algorithm>
#include <benchmark/benchmark.h>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

constexpr std::size_t kBenchmarkMemoryLimit = std::size_t{256U} * 1024U * 1024U;

[[nodiscard]] schema::LogicalType int64_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn make_column(const std::uint32_t rows,
                                                        const std::uint64_t seed) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(static_cast<std::size_t>(rows) * sizeof(std::int64_t));
  std::uint64_t state = seed;
  for (std::uint32_t row = 0U; row < rows; ++row) {
    state = state * 6'364'136'223'846'793'005ULL + 1'442'695'040'888'963'407ULL;
    const std::int64_t value = static_cast<std::int64_t>(state % 2'000'001U) - 1'000'000;
    const std::uint64_t bits = std::bit_cast<std::uint64_t>(value);
    for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
      buffers.values[static_cast<std::size_t>(row) * sizeof(bits) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = int64_type(), .nullable = false, .row_count = rows, .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn make_group_key_column(const std::uint32_t rows,
                                                                  const std::uint32_t groups,
                                                                  const std::uint32_t offset) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(static_cast<std::size_t>(rows) * sizeof(std::int64_t));
  for (std::uint32_t row = 0U; row < rows; ++row) {
    const std::uint64_t bits = static_cast<std::uint64_t>((row + offset) % groups);
    for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
      buffers.values[static_cast<std::size_t>(row) * sizeof(bits) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = int64_type(), .nullable = false, .row_count = rows, .null_count = 0U},
             std::move(buffers))
      .value();
}

class ManyChunkSource final : public PhysicalOperator {
public:
  explicit ManyChunkSource(std::vector<AccountedVectorChunk> chunks) : chunks_(std::move(chunks)) {}

  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    if (next_ == chunks_.size())
      return PhysicalOperatorStep::end();
    return PhysicalOperatorStep::chunk(std::move(chunks_[next_++]));
  }

private:
  std::vector<AccountedVectorChunk> chunks_;
  std::size_t next_{};
};

[[nodiscard]] std::vector<std::uint32_t> selection(const std::uint32_t rows,
                                                   const std::uint32_t stride) {
  std::vector<std::uint32_t> selected;
  selected.reserve((static_cast<std::size_t>(rows) + stride - 1U) / stride);
  for (std::uint32_t row = 0U; row < rows; row += stride)
    selected.push_back(row);
  return selected;
}

[[nodiscard]] VectorAggregateDefinition aggregate(const VectorAggregateOperation operation) {
  return {.operation = operation,
          .input =
              VectorAggregateInput{.column_ordinal = 0U, .type = int64_type(), .nullable = false}};
}

[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
source(const QueryResourceContext& resources, const std::uint32_t total_rows,
       const std::uint32_t chunk_rows, const std::uint32_t selection_stride) {
  std::vector<AccountedVectorChunk> chunks;
  const std::size_t chunk_count =
      (static_cast<std::size_t>(total_rows) + chunk_rows - 1U) / chunk_rows;
  chunks.reserve(chunk_count);
  for (std::uint32_t begin = 0U; begin < total_rows; begin += chunk_rows) {
    const std::uint32_t rows = std::min(chunk_rows, total_rows - begin);
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.push_back(make_column(rows, begin + 1U));
    common::Result<VectorChunk> chunk = VectorChunk::create(
        std::move(columns),
        VectorSelection::from_indices(rows, selection(rows, selection_stride)).value(),
        {.maximum_rows = chunk_rows,
         .maximum_columns = 1U,
         .maximum_buffer_bytes = kBenchmarkMemoryLimit,
         .maximum_retained_buffer_bytes = kBenchmarkMemoryLimit});
    if (!chunk.has_value())
      return common::make_unexpected(chunk.error());
    common::Result<QueryMemoryReservation> reservation =
        resources.reserve(chunk->retained_buffer_bytes() + 1'024U);
    if (!reservation.has_value())
      return common::make_unexpected(reservation.error());
    common::Result<AccountedVectorChunk> accounted =
        AccountedVectorChunk::create(std::move(*chunk), std::move(*reservation), resources);
    if (!accounted.has_value())
      return common::make_unexpected(accounted.error());
    chunks.push_back(std::move(*accounted));
  }
  std::vector<VectorAggregateDefinition> definitions{
      {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt},
      aggregate(VectorAggregateOperation::kSum),
      aggregate(VectorAggregateOperation::kAverage),
      aggregate(VectorAggregateOperation::kMinimum),
      aggregate(VectorAggregateOperation::kMaximum),
      aggregate(VectorAggregateOperation::kVariancePopulation)};
  return UngroupedAggregateOperator::create(std::make_unique<ManyChunkSource>(std::move(chunks)),
                                            definitions);
}

[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
grouped_source(const QueryResourceContext& resources, const std::uint32_t total_rows,
               const std::uint32_t chunk_rows, const std::uint32_t group_count) {
  std::vector<AccountedVectorChunk> chunks;
  const std::size_t chunk_count =
      (static_cast<std::size_t>(total_rows) + chunk_rows - 1U) / chunk_rows;
  chunks.reserve(chunk_count);
  for (std::uint32_t begin = 0U; begin < total_rows; begin += chunk_rows) {
    const std::uint32_t rows = std::min(chunk_rows, total_rows - begin);
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.push_back(make_group_key_column(rows, group_count, begin));
    columns.push_back(make_column(rows, begin + 1U));
    common::Result<VectorChunk> chunk =
        VectorChunk::create(std::move(columns), VectorSelection::all(rows).value(),
                            {.maximum_rows = chunk_rows,
                             .maximum_columns = 2U,
                             .maximum_buffer_bytes = kBenchmarkMemoryLimit,
                             .maximum_retained_buffer_bytes = kBenchmarkMemoryLimit});
    if (!chunk.has_value())
      return common::make_unexpected(chunk.error());
    common::Result<QueryMemoryReservation> reservation =
        resources.reserve(chunk->retained_buffer_bytes() + 1'024U);
    if (!reservation.has_value())
      return common::make_unexpected(reservation.error());
    common::Result<AccountedVectorChunk> accounted =
        AccountedVectorChunk::create(std::move(*chunk), std::move(*reservation), resources);
    if (!accounted.has_value())
      return common::make_unexpected(accounted.error());
    chunks.push_back(std::move(*accounted));
  }
  const std::vector<VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = int64_type(), .nullable = false}};
  const std::vector<VectorAggregateDefinition> definitions{
      {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt},
      {.operation = VectorAggregateOperation::kSum,
       .input =
           VectorAggregateInput{.column_ordinal = 1U, .type = int64_type(), .nullable = false}}};
  return GroupedAggregateOperator::create(std::make_unique<ManyChunkSource>(std::move(chunks)),
                                          keys, definitions);
}

[[nodiscard]] common::Result<std::size_t> drain(PhysicalOperator& pipeline,
                                                const QueryResourceContext& resources) {
  std::size_t rows = 0U;
  for (;;) {
    common::Result<PhysicalOperatorStep> step = pipeline.next(resources);
    if (!step.has_value())
      return common::make_unexpected(step.error());
    if (step->kind() == PhysicalOperatorStepKind::kEnd)
      return rows;
    rows += step->chunk()->chunk().selected_row_count();
  }
}

void streaming_ungrouped_aggregates(benchmark::State& state) {
  const auto total_rows = static_cast<std::uint32_t>(state.range(0));
  const auto chunk_rows = static_cast<std::uint32_t>(state.range(1));
  const auto selection_stride = static_cast<std::uint32_t>(state.range(2));
  QueryResourceContext resources = QueryResourceContext::create(kBenchmarkMemoryLimit).value();
  std::size_t measured_allocations = 0U;
  {
    auto pipeline = source(resources, total_rows, chunk_rows, selection_stride);
    if (!pipeline.has_value()) {
      state.SkipWithError(pipeline.error().to_string());
      return;
    }
    benchmark_support::ScopedAllocationCounting counting;
    auto step = (*pipeline)->next(resources);
    measured_allocations = counting.stop().allocations;
    if (!step.has_value() || step->chunk() == nullptr) {
      state.SkipWithError(step.has_value() ? "aggregate benchmark returned no chunk"
                                           : step.error().to_string());
      return;
    }
  }

  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto pipeline = source(resources, total_rows, chunk_rows, selection_stride);
    if (!pipeline.has_value()) {
      state.SkipWithError(pipeline.error().to_string());
      return;
    }
    state.ResumeTiming();
    auto step = (*pipeline)->next(resources);
    benchmark::DoNotOptimize(step);
    if (!step.has_value()) {
      state.SkipWithError(step.error().to_string());
      return;
    }
  }
  const std::size_t selected =
      (static_cast<std::size_t>(total_rows) + selection_stride - 1U) / selection_stride;
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(selected));
  state.counters["chunk_rows"] = static_cast<double>(chunk_rows);
  state.counters["physical_rows"] = static_cast<double>(total_rows);
  state.counters["pull_allocations"] = static_cast<double>(measured_allocations);
  state.counters["selection_density"] = 1.0 / static_cast<double>(selection_stride);
  state.SetLabel("COUNT/SUM/AVG/MIN/MAX/VAR_POP; source construction excluded");
}

void bounded_grouped_aggregates(benchmark::State& state) {
  const auto total_rows = static_cast<std::uint32_t>(state.range(0));
  const auto chunk_rows = static_cast<std::uint32_t>(state.range(1));
  const auto group_count = static_cast<std::uint32_t>(state.range(2));
  QueryResourceContext resources = QueryResourceContext::create(kBenchmarkMemoryLimit).value();
  std::size_t measured_allocations = 0U;
  {
    auto pipeline = grouped_source(resources, total_rows, chunk_rows, group_count);
    if (!pipeline.has_value()) {
      state.SkipWithError(pipeline.error().to_string());
      return;
    }
    benchmark_support::ScopedAllocationCounting counting;
    common::Result<std::size_t> rows = drain(**pipeline, resources);
    measured_allocations = counting.stop().allocations;
    if (!rows.has_value() || *rows != group_count) {
      state.SkipWithError(rows.has_value() ? "grouped aggregate benchmark returned wrong groups"
                                           : rows.error().to_string());
      return;
    }
  }

  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto pipeline = grouped_source(resources, total_rows, chunk_rows, group_count);
    if (!pipeline.has_value()) {
      state.SkipWithError(pipeline.error().to_string());
      return;
    }
    state.ResumeTiming();
    common::Result<std::size_t> rows = drain(**pipeline, resources);
    benchmark::DoNotOptimize(rows);
    if (!rows.has_value() || *rows != group_count) {
      state.SkipWithError(rows.has_value() ? "grouped aggregate benchmark returned wrong groups"
                                           : rows.error().to_string());
      return;
    }
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(total_rows));
  state.counters["chunk_rows"] = static_cast<double>(chunk_rows);
  state.counters["groups"] = static_cast<double>(group_count);
  state.counters["pull_allocations"] = static_cast<double>(measured_allocations);
  state.SetLabel("INT64 key; COUNT/SUM; source construction excluded");
}

BENCHMARK(streaming_ungrouped_aggregates)
    ->Args({2'048, 2'048, 1})
    ->Args({32'768, 2'048, 1})
    ->Args({32'768, 256, 1})
    ->Args({32'768, 2'048, 4});

BENCHMARK(bounded_grouped_aggregates)->Args({32'768, 2'048, 16})->Args({32'768, 2'048, 256});

} // namespace
} // namespace chronos::query
