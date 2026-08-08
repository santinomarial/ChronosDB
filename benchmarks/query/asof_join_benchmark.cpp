#include "chronos/query/asof_join.hpp"
#include "support/counting_allocator.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

inline constexpr std::size_t kMemoryLimit = std::size_t{256U} * 1024U * 1024U;

class OneChunkSource final : public PhysicalOperator {
public:
  explicit OneChunkSource(AccountedVectorChunk chunk) : chunk_(std::move(chunk)) {}

  common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    if (!chunk_.has_value())
      return PhysicalOperatorStep::end();
    AccountedVectorChunk output = std::move(*chunk_);
    chunk_.reset();
    return PhysicalOperatorStep::chunk(std::move(output));
  }

private:
  std::optional<AccountedVectorChunk> chunk_;
};

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn fixed_column(const schema::LogicalTypeKind kind,
                                                         const std::vector<std::uint64_t>& values,
                                                         const std::size_t width) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(values.size() * width);
  for (std::size_t row = 0U; row < values.size(); ++row) {
    for (std::size_t byte = 0U; byte < width; ++byte) {
      buffers.values[row * width + byte] =
          static_cast<std::byte>((values[row] >> (byte * 8U)) & 0xffU);
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(kind),
              .nullable = false,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn uuid_column(const std::uint32_t rows) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(static_cast<std::size_t>(rows) * 16U);
  for (std::uint32_t row = 0U; row < rows; ++row)
    buffers.values[static_cast<std::size_t>(row) * 16U] = std::byte{1U};
  return columnar::OwnedPhysicalColumn::create({.type = type(schema::LogicalTypeKind::kUuid),
                                                .nullable = false,
                                                .row_count = rows,
                                                .null_count = 0U},
                                               std::move(buffers))
      .value();
}

[[nodiscard]] AccountedVectorChunk accounted(std::vector<columnar::OwnedPhysicalColumn> columns,
                                             const std::uint32_t rows,
                                             const QueryResourceContext& resources) {
  VectorChunk chunk = VectorChunk::create(std::move(columns), VectorSelection::all(rows).value(),
                                          {.maximum_rows = rows,
                                           .maximum_columns = 8U,
                                           .maximum_buffer_bytes = kMemoryLimit,
                                           .maximum_retained_buffer_bytes = kMemoryLimit})
                          .value();
  const std::size_t charge = chunk.retained_buffer_bytes() + 1'024U;
  return AccountedVectorChunk::create(std::move(chunk), resources.reserve(charge).value(),
                                      resources)
      .value();
}

// Row count and equality-key cardinality are the independent benchmark axes.
// NOLINTBEGIN(bugprone-easily-swappable-parameters)
[[nodiscard]] AccountedVectorChunk left_input(const QueryResourceContext& resources,
                                              const std::uint32_t rows,
                                              const std::uint32_t groups) {
  std::vector<std::uint64_t> keys(rows);
  std::vector<std::uint64_t> timestamps(rows);
  std::vector<std::uint64_t> payloads(rows);
  for (std::uint32_t row = 0U; row < rows; ++row) {
    keys[row] = row % groups;
    timestamps[row] = static_cast<std::uint64_t>(row) * 4U + 3U;
    payloads[row] = row;
  }
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(fixed_column(schema::LogicalTypeKind::kInt64, keys, 8U));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kTimestampNs, timestamps, 8U));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kInt64, payloads, 8U));
  return accounted(std::move(columns), rows, resources);
}

[[nodiscard]] AccountedVectorChunk right_input(const QueryResourceContext& resources,
                                               const std::uint32_t rows,
                                               const std::uint32_t groups) {
  std::vector<std::uint64_t> keys(rows);
  std::vector<std::uint64_t> timestamps(rows);
  std::vector<std::uint64_t> payloads(rows);
  std::vector<std::uint64_t> physical(rows);
  std::vector<std::uint64_t> sequences(rows);
  std::vector<std::uint64_t> row_ordinals(rows);
  std::vector<std::uint64_t> operations(rows, 1U);
  for (std::uint32_t row = 0U; row < rows; ++row) {
    keys[row] = row % groups;
    timestamps[row] = static_cast<std::uint64_t>(row) * 2U;
    payloads[row] = row;
    physical[row] = row;
    sequences[row] = static_cast<std::uint64_t>(row) + 1U;
    row_ordinals[row] = row & 7U;
  }
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(fixed_column(schema::LogicalTypeKind::kInt64, keys, 8U));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kTimestampNs, timestamps, 8U));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kInt64, payloads, 8U));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kInt64, physical, 8U));
  columns.push_back(uuid_column(rows));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kUInt64, sequences, 8U));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kUInt32, row_ordinals, 4U));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kUInt8, operations, 1U));
  return accounted(std::move(columns), rows, resources);
}

[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
pipeline(const QueryResourceContext& resources, const std::uint32_t left_rows,
         const std::uint32_t right_rows, const std::uint32_t groups) {
  return AsofJoinOperator::create(
      std::make_unique<OneChunkSource>(left_input(resources, left_rows, groups)),
      std::make_unique<OneChunkSource>(right_input(resources, right_rows, groups)),
      {.left_input_columns = {{.type = type(schema::LogicalTypeKind::kInt64), .nullable = false},
                              {.type = type(schema::LogicalTypeKind::kTimestampNs),
                               .nullable = false},
                              {.type = type(schema::LogicalTypeKind::kInt64), .nullable = false}},
       .right_input_columns = {{.type = type(schema::LogicalTypeKind::kInt64), .nullable = false},
                               {.type = type(schema::LogicalTypeKind::kTimestampNs),
                                .nullable = false},
                               {.type = type(schema::LogicalTypeKind::kInt64), .nullable = false},
                               {.type = type(schema::LogicalTypeKind::kInt64), .nullable = false},
                               {.type = type(schema::LogicalTypeKind::kUuid), .nullable = false},
                               {.type = type(schema::LogicalTypeKind::kUInt64), .nullable = false},
                               {.type = type(schema::LogicalTypeKind::kUInt32), .nullable = false},
                               {.type = type(schema::LogicalTypeKind::kUInt8), .nullable = false}},
       .equality_keys = {{.left_column_ordinal = 0U, .right_column_ordinal = 0U}},
       .left_timestamp_column_ordinal = 1U,
       .right_timestamp_column_ordinal = 1U,
       .right_physical_ordering_key_ordinals = {3U},
       .right_row_version_first_column_ordinal = 4U,
       .left_output_column_ordinals = {0U, 1U, 2U},
       .right_output_column_ordinals = {0U, 1U, 2U, 3U, 4U, 5U, 6U, 7U},
       .left_outer = true},
      {.maximum_left_rows = left_rows,
       .maximum_right_rows = right_rows,
       .maximum_equality_keys = 1U,
       .maximum_physical_ordering_keys = 1U,
       .maximum_state_bytes = kMemoryLimit,
       .output_limits = {.maximum_rows = left_rows,
                         .maximum_columns = 12U,
                         .maximum_buffer_bytes = kMemoryLimit,
                         .maximum_retained_buffer_bytes = kMemoryLimit}});
}
// NOLINTEND(bugprone-easily-swappable-parameters)

void benchmark_asof_join(benchmark::State& state) {
  const auto left_rows = static_cast<std::uint32_t>(state.range(0));
  const auto right_rows = static_cast<std::uint32_t>(state.range(1));
  const auto groups = static_cast<std::uint32_t>(state.range(2));
  QueryResourceContext resources = QueryResourceContext::create(kMemoryLimit).value();
  std::size_t measured_allocations = 0U;
  std::size_t measured_allocated_bytes = 0U;
  {
    std::unique_ptr<PhysicalOperator> join =
        pipeline(resources, left_rows, right_rows, groups).value();
    chronos::benchmark_support::ScopedAllocationCounting allocations;
    auto step = join->next(resources);
    const chronos::benchmark_support::AllocationCounts counts = allocations.stop();
    measured_allocations = counts.allocations;
    measured_allocated_bytes = counts.allocated_bytes;
    if (!step.has_value() || step->kind() != PhysicalOperatorStepKind::kChunk) {
      state.SkipWithError("ASOF benchmark preflight failed");
      return;
    }
  }
  std::size_t observed_rows = 0U;
  for (auto iteration : state) {
    static_cast<void>(iteration);
    state.PauseTiming();
    std::unique_ptr<PhysicalOperator> join =
        pipeline(resources, left_rows, right_rows, groups).value();
    state.ResumeTiming();
    auto step = join->next(resources);
    benchmark::DoNotOptimize(step);
    state.PauseTiming();
    if (!step.has_value() || step->kind() != PhysicalOperatorStepKind::kChunk) {
      state.SkipWithError("ASOF benchmark execution failed");
      state.ResumeTiming();
      break;
    }
    observed_rows += step->chunk()->chunk().selected_row_count();
    state.ResumeTiming();
  }
  benchmark::DoNotOptimize(observed_rows);
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(left_rows) * right_rows);
  state.counters["left_rows"] = left_rows;
  state.counters["right_rows"] = right_rows;
  state.counters["groups"] = groups;
  state.counters["pull_allocations"] = static_cast<double>(measured_allocations);
  state.counters["pull_allocated_bytes"] = static_cast<double>(measured_allocated_bytes);
  state.SetLabel("one equality key; exact nested-loop physical/version winner");
}

BENCHMARK(benchmark_asof_join)->Args({32, 64, 8})->Args({128, 256, 32})->Args({256, 512, 16});

} // namespace
} // namespace chronos::query
