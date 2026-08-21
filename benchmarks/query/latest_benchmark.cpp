#include "chronos/query/latest.hpp"
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

// Byte value and encoded width are deliberately separate fixture controls.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
void store(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint64_t value,
           const std::size_t width) {
  for (std::size_t byte = 0U; byte < width; ++byte)
    bytes[offset + byte] = static_cast<std::byte>((value >> (byte * 8U)) & 0xffU);
}

[[nodiscard]] columnar::OwnedPhysicalColumn u64_column(const schema::LogicalTypeKind kind,
                                                       const std::vector<std::uint64_t>& values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(values.size() * sizeof(std::uint64_t));
  for (std::size_t row = 0U; row < values.size(); ++row)
    store(buffers.values, row * sizeof(std::uint64_t), values[row], sizeof(std::uint64_t));
  return columnar::OwnedPhysicalColumn::create(
             {.type = schema::LogicalType::create(kind).value(),
              .nullable = false,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = 0U},
             std::move(buffers))
      .value();
}

// Row count and repeated fixture value are independent benchmark controls.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] columnar::OwnedPhysicalColumn u32_column(const std::uint32_t rows,
                                                       const std::uint32_t value) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(static_cast<std::size_t>(rows) * sizeof(std::uint32_t));
  for (std::uint32_t row = 0U; row < rows; ++row)
    store(buffers.values, static_cast<std::size_t>(row) * sizeof(std::uint32_t), value,
          sizeof(std::uint32_t));
  return columnar::OwnedPhysicalColumn::create(
             {.type = schema::LogicalType::create(schema::LogicalTypeKind::kUInt32).value(),
              .nullable = false,
              .row_count = rows,
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn u8_column(const std::uint32_t rows,
                                                      const std::uint8_t value) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.assign(rows, static_cast<std::byte>(value));
  return columnar::OwnedPhysicalColumn::create(
             {.type = schema::LogicalType::create(schema::LogicalTypeKind::kUInt8).value(),
              .nullable = false,
              .row_count = rows,
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn uuid_column(const std::uint32_t rows) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(static_cast<std::size_t>(rows) * 16U);
  return columnar::OwnedPhysicalColumn::create(
             {.type = schema::LogicalType::create(schema::LogicalTypeKind::kUuid).value(),
              .nullable = false,
              .row_count = rows,
              .null_count = 0U},
             std::move(buffers))
      .value();
}

// Row count and group cardinality are the independent benchmark axes.
// NOLINTBEGIN(bugprone-easily-swappable-parameters)
[[nodiscard]] AccountedVectorChunk input(const QueryResourceContext& resources,
                                         const std::uint32_t rows, const std::uint32_t groups) {
  std::vector<std::uint64_t> group_values(rows);
  std::vector<std::uint64_t> timestamps(rows);
  std::vector<std::uint64_t> physical(rows);
  std::vector<std::uint64_t> sequences(rows);
  std::uint64_t random = 0x4c41544553545f42ULL;
  for (std::uint32_t row = 0U; row < rows; ++row) {
    random = random * 6'364'136'223'846'793'005ULL + 1'442'695'040'888'963'407ULL;
    group_values[row] = random % groups;
    random = random * 6'364'136'223'846'793'005ULL + 1'442'695'040'888'963'407ULL;
    timestamps[row] = random;
    physical[row] = row;
    sequences[row] = row;
  }
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(u64_column(schema::LogicalTypeKind::kInt64, group_values));
  columns.push_back(u64_column(schema::LogicalTypeKind::kTimestampNs, timestamps));
  columns.push_back(u64_column(schema::LogicalTypeKind::kInt64, physical));
  columns.push_back(uuid_column(rows));
  columns.push_back(u64_column(schema::LogicalTypeKind::kUInt64, sequences));
  columns.push_back(u32_column(rows, 0U));
  columns.push_back(u8_column(rows, 1U));
  VectorChunk chunk = VectorChunk::create(std::move(columns), VectorSelection::all(rows).value(),
                                          {.maximum_rows = rows,
                                           .maximum_columns = 7U,
                                           .maximum_buffer_bytes = kMemoryLimit,
                                           .maximum_retained_buffer_bytes = kMemoryLimit})
                          .value();
  const std::size_t charge = chunk.retained_buffer_bytes() + 1'024U;
  return AccountedVectorChunk::create(std::move(chunk), resources.reserve(charge).value(),
                                      resources)
      .value();
}

[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
pipeline(const QueryResourceContext& resources, const std::uint32_t rows,
         const std::uint32_t groups) {
  return LatestByOperator::create(
      std::make_unique<OneChunkSource>(input(resources, rows, groups)),
      {.key_column_ordinals = {0U},
       .timestamp_column_ordinal = 1U,
       .physical_ordering_key_ordinals = {2U},
       .row_version_first_column_ordinal = 3U},
      {.maximum_group_keys = 1U,
       .maximum_physical_ordering_keys = 1U,
       .sort_limits = {.maximum_rows = rows,
                       .maximum_keys = 6U,
                       .maximum_state_bytes = kMemoryLimit,
                       .output_limits = {.maximum_rows = rows,
                                         .maximum_columns = 7U,
                                         .maximum_buffer_bytes = kMemoryLimit,
                                         .maximum_retained_buffer_bytes = kMemoryLimit}}});
}
// NOLINTEND(bugprone-easily-swappable-parameters)

void benchmark_latest_by(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto groups = static_cast<std::uint32_t>(state.range(1));
  QueryResourceContext resources = QueryResourceContext::create(kMemoryLimit).value();
  std::size_t observed_rows = 0U;
  for (auto iteration : state) {
    static_cast<void>(iteration);
    state.PauseTiming();
    std::unique_ptr<PhysicalOperator> latest = pipeline(resources, rows, groups).value();
    state.ResumeTiming();
    auto step = latest->next(resources);
    benchmark::DoNotOptimize(step);
    state.PauseTiming();
    if (!step.has_value() || step->kind() != PhysicalOperatorStepKind::kChunk) {
      state.SkipWithError("LATEST BY benchmark execution failed");
      state.ResumeTiming();
      break;
    }
    observed_rows += step->chunk()->chunk().selected_row_count();
    state.ResumeTiming();
  }
  benchmark::DoNotOptimize(observed_rows);
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(rows));
  state.counters["rows"] = rows;
  state.counters["groups"] = groups;
  state.SetLabel("one group key; exact timestamp, physical-key, and row-version ties");
}

BENCHMARK(benchmark_latest_by)->Args({64, 16})->Args({1'024, 64})->Args({2'048, 1'024});

} // namespace
} // namespace chronos::query
