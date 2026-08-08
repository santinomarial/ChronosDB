#include "chronos/common/uuid.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/executor.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/query/snapshot.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"

#include <algorithm>
#include <benchmark/benchmark.h>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] std::shared_ptr<const QueryCatalogSnapshot> benchmark_catalog() {
  const schema::LogicalType timestamp =
      schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value();
  const schema::LogicalType int64 =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(
      schema::ColumnDefinition::create(id<schema::ColumnId>(3U), "ts", timestamp, false).value());
  columns.push_back(
      schema::ColumnDefinition::create(id<schema::ColumnId>(4U), "value", int64, false).value());
  auto table = std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(id<schema::TableId>(1U), id<schema::SchemaId>(2U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns),
                                  {.event_time_column = id<schema::ColumnId>(3U),
                                   .physical_ordering_key = {id<schema::ColumnId>(3U)},
                                   .partition_columns = {id<schema::ColumnId>(3U)},
                                   .shard_key = {id<schema::ColumnId>(3U)},
                                   .deduplication_key = {id<schema::ColumnId>(3U)}})
          .value());
  const std::vector<QueryCatalogTableInput> tables{
      {.name = "metrics", .quoted = false, .schema = std::move(table)}};
  return std::make_shared<const QueryCatalogSnapshot>(
      QueryCatalogSnapshot::create(1U, tables).value());
}

[[nodiscard]] BoundSqlSelect benchmark_select() {
  return bind_sql_v1_select(parse_sql_v1_select("SELECT value + 1 AS adjusted FROM metrics "
                                                "WHERE value BETWEEN 10 AND 100 LIMIT 32")
                                .value(),
                            benchmark_catalog())
      .value();
}

[[nodiscard]] BoundSqlSelect benchmark_aggregate_select() {
  return bind_sql_v1_select(
             parse_sql_v1_select(
                 "SELECT sum(value + 1) + count(*) AS total, avg(value) AS mean, "
                 "min(value) AS minimum, max(value) AS maximum, var_pop(value) AS variance "
                 "FROM metrics WHERE value BETWEEN 10 AND 100 LIMIT 1")
                 .value(),
             benchmark_catalog())
      .value();
}

[[nodiscard]] BoundSqlSelect benchmark_grouped_aggregate_select() {
  return bind_sql_v1_select(
             parse_sql_v1_select("SELECT value % 16 AS bucket, count(*) AS rows, "
                                 "sum(value + 1) + count(*) AS total FROM metrics "
                                 "WHERE value BETWEEN 10 AND 100 GROUP BY value % 16 LIMIT 32")
                 .value(),
             benchmark_catalog())
      .value();
}

[[nodiscard]] BoundSqlSelect benchmark_ordered_select() {
  return bind_sql_v1_select(parse_sql_v1_select("SELECT value + 1 AS adjusted FROM metrics "
                                                "ORDER BY adjusted DESC, ts ASC LIMIT 32")
                                .value(),
                            benchmark_catalog())
      .value();
}

[[nodiscard]] BoundSqlSelect benchmark_ordered_grouped_select() {
  return bind_sql_v1_select(
             parse_sql_v1_select("SELECT value % 16 AS bucket, count(*) AS rows FROM metrics "
                                 "GROUP BY value % 16 ORDER BY rows DESC, sum(value) DESC LIMIT 32")
                 .value(),
             benchmark_catalog())
      .value();
}

[[nodiscard]] BoundSqlSelect benchmark_latest_select() {
  return bind_sql_v1_select(
             parse_sql_v1_select(
                 "SELECT value + 1 AS adjusted FROM metrics LATEST BY (value) ON "
                 "time_bucket(INTERVAL '1 second', ts) ORDER BY adjusted DESC LIMIT 32")
                 .value(),
             benchmark_catalog())
      .value();
}

[[nodiscard]] BoundSqlSelect benchmark_asof_select() {
  return bind_sql_v1_select(
             parse_sql_v1_select(
                 "SELECT r.value AS matched FROM metrics AS l LATEST BY (value) ON l.ts "
                 "ASOF LEFT JOIN metrics AS r ON l.value + 0 = r.value + 0 AND r.ts <= l.ts "
                 "WHERE l.value > 0 ORDER BY r.ts DESC, matched ASC LIMIT 32")
                 .value(),
             benchmark_catalog())
      .value();
}

class EmptySource final : public PhysicalOperator {
public:
  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    return PhysicalOperatorStep::end();
  }
};

class BenchmarkChunkSource final : public PhysicalOperator {
public:
  explicit BenchmarkChunkSource(std::vector<AccountedVectorChunk> chunks)
      : chunks_(std::move(chunks)) {}

  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    if (next_ == chunks_.size()) {
      std::vector<AccountedVectorChunk>{}.swap(chunks_);
      return PhysicalOperatorStep::end();
    }
    return PhysicalOperatorStep::chunk(std::move(chunks_[next_++]));
  }

private:
  std::vector<AccountedVectorChunk> chunks_;
  std::size_t next_{};
};

class BenchmarkSnapshotProvider final : public ScalarSnapshotProvider {
public:
  explicit BenchmarkSnapshotProvider(std::shared_ptr<const ScalarTableSnapshot> snapshot)
      : snapshot_(std::move(snapshot)) {}

  [[nodiscard]] common::Result<std::shared_ptr<const ScalarTableSnapshot>>
  resolve(const std::shared_ptr<const schema::TableSchema>&,
          const std::optional<std::int64_t>) const override {
    return snapshot_;
  }

private:
  std::shared_ptr<const ScalarTableSnapshot> snapshot_;
};

[[nodiscard]] columnar::OwnedPhysicalColumn
benchmark_signed_column(const schema::LogicalTypeKind kind,
                        const std::span<const std::int64_t> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(values.size() * sizeof(std::int64_t));
  for (std::size_t row = 0U; row < values.size(); ++row) {
    const std::uint64_t bits = std::bit_cast<std::uint64_t>(values[row]);
    for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
      buffers.values[row * sizeof(bits) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = schema::LogicalType::create(kind).value(),
              .nullable = false,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] std::vector<std::int64_t> benchmark_values(const std::size_t row_count) {
  std::vector<std::int64_t> values;
  values.reserve(row_count);
  std::uint64_t state = 0x9e3779b97f4a7c15ULL;
  for (std::size_t row = 0U; row < row_count; ++row) {
    state = state * 6'364'136'223'846'793'005ULL + 1'442'695'040'888'963'407ULL;
    values.push_back(static_cast<std::int64_t>((state >> 17U) % 2'001U) - 1'000);
  }
  return values;
}

[[nodiscard]] std::vector<AccountedVectorChunk>
benchmark_input_chunks(const QueryResourceContext& resources,
                       const std::span<const std::int64_t> values, const std::size_t batch_rows) {
  std::vector<AccountedVectorChunk> chunks;
  chunks.reserve((values.size() + batch_rows - 1U) / batch_rows);
  std::vector<std::int64_t> timestamps;
  timestamps.reserve(batch_rows);
  for (std::size_t offset = 0U; offset < values.size(); offset += batch_rows) {
    const std::size_t rows = std::min(batch_rows, values.size() - offset);
    timestamps.clear();
    for (std::size_t row = 0U; row < rows; ++row)
      timestamps.push_back(static_cast<std::int64_t>(offset + row));
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.push_back(benchmark_signed_column(schema::LogicalTypeKind::kTimestampNs, timestamps));
    columns.push_back(
        benchmark_signed_column(schema::LogicalTypeKind::kInt64, values.subspan(offset, rows)));
    VectorChunk chunk =
        VectorChunk::create(std::move(columns),
                            VectorSelection::all(static_cast<std::uint32_t>(rows)).value())
            .value();
    const std::size_t reservation_bytes = chunk.retained_buffer_bytes();
    chunks.push_back(AccountedVectorChunk::create(
                         std::move(chunk), resources.reserve(reservation_bytes).value(), resources)
                         .value());
  }
  return chunks;
}

[[nodiscard]] BoundSqlSelect benchmark_end_to_end_select() {
  return bind_sql_v1_select(
             parse_sql_v1_select(
                 "SELECT value % 32 AS bucket, count(*) AS rows, sum(value) AS total "
                 "FROM metrics GROUP BY value % 32 "
                 "ORDER BY rows DESC, bucket ASC LIMIT 16")
                 .value(),
             benchmark_catalog())
      .value();
}

[[nodiscard]] std::shared_ptr<const ScalarTableSnapshot>
benchmark_scalar_snapshot(const std::span<const std::int64_t> values) {
  const std::shared_ptr<const schema::TableSchema> table =
      benchmark_catalog()->tables()[0].schema_ptr();
  std::vector<ScalarInputRow> rows;
  rows.reserve(values.size());
  common::Uuid::Bytes wal_bytes{};
  wal_bytes.front() = std::byte{1U};
  const common::Uuid wal{wal_bytes};
  for (std::size_t row = 0U; row < values.size(); ++row) {
    std::vector<ScalarValue> columns;
    columns.push_back(
        ScalarValue::signed_value(
            schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
            static_cast<std::int64_t>(row))
            .value());
    columns.push_back(
        ScalarValue::signed_value(
            schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(), values[row])
            .value());
    rows.push_back({.columns = std::move(columns),
                    .generated_logical_identity = {},
                    .wal_id = wal,
                    .record_sequence = row + 1U,
                    .system_commit_position = row + 1U,
                    .row_ordinal = 0U});
  }
  return std::make_shared<const ScalarTableSnapshot>(
      ScalarTableSnapshot::create(table, static_cast<std::uint64_t>(values.size()), std::move(rows))
          .value());
}

[[nodiscard]] std::vector<PhysicalColumnShape> bool_shape() {
  return {{.type = schema::LogicalType::create(schema::LogicalTypeKind::kBool).value(),
           .nullable = true}};
}

[[nodiscard]] std::vector<PhysicalPipelineStage> make_stages(const std::size_t count) {
  std::vector<PhysicalPipelineStage> stages;
  stages.reserve(count);
  for (std::size_t index = 0U; index < count; ++index) {
    if ((index & 1U) == 0U)
      stages.emplace_back(BooleanFilterStage{0U});
    else
      stages.emplace_back(LimitStage{std::numeric_limits<std::uint64_t>::max()});
  }
  return stages;
}

void validate_physical_pipeline_plan(benchmark::State& state) {
  const auto stage_count = static_cast<std::size_t>(state.range(0));
  const std::vector<PhysicalColumnShape> shape = bool_shape();
  const std::vector<PhysicalPipelineStage> stages = make_stages(stage_count);
  for (auto _ : state) {
    static_cast<void>(_);
    auto plan = PhysicalPipelinePlan::create(shape, stages);
    benchmark::DoNotOptimize(plan);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(stage_count));
  state.counters["stages"] = static_cast<double>(stage_count);
}

void instantiate_physical_pipeline_plan(benchmark::State& state) {
  const auto stage_count = static_cast<std::size_t>(state.range(0));
  const PhysicalPipelinePlan plan =
      PhysicalPipelinePlan::create(bool_shape(), make_stages(stage_count)).value();
  for (auto _ : state) {
    static_cast<void>(_);
    auto pipeline = plan.instantiate(std::make_unique<EmptySource>());
    benchmark::DoNotOptimize(pipeline);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(stage_count));
  state.counters["stages"] = static_cast<double>(stage_count);
}

void lower_bound_select_pipeline(benchmark::State& state) {
  const BoundSqlSelect select = benchmark_select();
  for (auto iteration : state) {
    static_cast<void>(iteration);
    auto plan = lower_bound_sql_select(select);
    benchmark::DoNotOptimize(plan);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * 3);
  state.counters["outputs"] = 1.0;
  state.counters["physical_stages"] = 4.0;
  state.SetLabel("bound SELECT retained; parse and bind excluded");
}

void lower_bound_global_aggregate_pipeline(benchmark::State& state) {
  const BoundSqlSelect select = benchmark_aggregate_select();
  for (auto iteration : state) {
    static_cast<void>(iteration);
    auto plan = lower_bound_sql_select(select);
    benchmark::DoNotOptimize(plan);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * 5);
  state.counters["outputs"] = 5.0;
  state.counters["physical_stages"] = 6.0;
  state.SetLabel("bound global aggregate retained; parse and bind excluded");
}

void lower_bound_grouped_aggregate_pipeline(benchmark::State& state) {
  const BoundSqlSelect select = benchmark_grouped_aggregate_select();
  for (auto iteration : state) {
    static_cast<void>(iteration);
    auto plan = lower_bound_sql_select(select);
    benchmark::DoNotOptimize(plan);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * 5);
  state.counters["group_keys"] = 1.0;
  state.counters["outputs"] = 3.0;
  state.counters["physical_stages"] = 6.0;
  state.SetLabel("bound grouped aggregate retained; parse and bind excluded");
}

void lower_bound_ordered_pipeline(benchmark::State& state) {
  const BoundSqlSelect select = benchmark_ordered_select();
  for (auto iteration : state) {
    static_cast<void>(iteration);
    auto plan = lower_bound_sql_select(select);
    benchmark::DoNotOptimize(plan);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * 2);
  state.counters["order_keys"] = 2.0;
  state.counters["outputs"] = 1.0;
  state.counters["physical_stages"] = 4.0;
  state.SetLabel("bound base ORDER BY retained; parse and bind excluded");
}

void lower_bound_ordered_grouped_pipeline(benchmark::State& state) {
  const BoundSqlSelect select = benchmark_ordered_grouped_select();
  for (auto iteration : state) {
    static_cast<void>(iteration);
    auto plan = lower_bound_sql_select(select);
    benchmark::DoNotOptimize(plan);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * 3);
  state.counters["order_keys"] = 2.0;
  state.counters["outputs"] = 2.0;
  state.counters["physical_stages"] = 6.0;
  state.SetLabel("bound grouped ORDER BY retained; parse and bind excluded");
}

void lower_bound_latest_pipeline(benchmark::State& state) {
  const BoundSqlSelect select = benchmark_latest_select();
  for (auto iteration : state) {
    static_cast<void>(iteration);
    auto plan = lower_bound_sql_select(select);
    benchmark::DoNotOptimize(plan);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * 3);
  state.counters["latest_keys"] = 1.0;
  state.counters["order_keys"] = 1.0;
  state.counters["outputs"] = 1.0;
  state.counters["physical_stages"] = 7.0;
  state.SetLabel("bound LATEST BY plus ORDER BY retained; parse and bind excluded");
}

void lower_bound_asof_pipeline(benchmark::State& state) {
  const BoundSqlSelect select = benchmark_asof_select();
  for (auto iteration : state) {
    static_cast<void>(iteration);
    auto plan = lower_bound_sql_asof_select(select);
    benchmark::DoNotOptimize(plan);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * 6);
  state.counters["asof_keys"] = 1.0;
  state.counters["order_keys"] = 2.0;
  state.counters["outputs"] = 1.0;
  state.SetLabel("bound LATEST/ASOF/WHERE/ORDER/LIMIT; parse and bind excluded");
}

void execute_bound_grouped_vector_plan(benchmark::State& state) {
  const std::size_t row_count = static_cast<std::size_t>(state.range(0));
  const std::size_t batch_rows = static_cast<std::size_t>(state.range(1));
  const std::vector<std::int64_t> values = benchmark_values(row_count);
  const PhysicalPipelinePlan plan = lower_bound_sql_select(benchmark_end_to_end_select()).value();
  std::vector<std::uint64_t> execution_nanoseconds;
  execution_nanoseconds.reserve(100'000U);
  std::size_t maximum_peak_bytes = 0U;
  for (auto iteration : state) {
    static_cast<void>(iteration);
    state.PauseTiming();
    QueryResourceContext resources = QueryResourceContext::create(std::size_t{64U} << 20U).value();
    auto source = std::make_unique<BenchmarkChunkSource>(
        benchmark_input_chunks(resources, values, batch_rows));
    state.ResumeTiming();
    const auto started = std::chrono::steady_clock::now();
    std::unique_ptr<PhysicalOperator> pipeline = plan.instantiate(std::move(source)).value();
    std::size_t output_rows = 0U;
    for (;;) {
      PhysicalOperatorStep step = std::move(pipeline->next(resources)).value();
      if (step.kind() == PhysicalOperatorStepKind::kEnd)
        break;
      output_rows += step.chunk()->chunk().selected_row_count();
    }
    pipeline.reset();
    const auto finished = std::chrono::steady_clock::now();
    execution_nanoseconds.push_back(static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(finished - started).count()));
    maximum_peak_bytes = std::max(maximum_peak_bytes, resources.peak_reserved_memory_bytes());
    benchmark::DoNotOptimize(output_rows);
  }
  std::ranges::sort(execution_nanoseconds);
  const auto percentile = [&execution_nanoseconds](const std::size_t numerator) {
    return execution_nanoseconds[((execution_nanoseconds.size() - 1U) * numerator) / 100U];
  };
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(row_count));
  state.counters["batch_rows"] = static_cast<double>(batch_rows);
  state.counters["group_cardinality"] = 32.0;
  state.counters["input_rows"] = static_cast<double>(row_count);
  state.counters["max_peak_bytes"] = static_cast<double>(maximum_peak_bytes);
  state.counters["p50_execution_ns"] = static_cast<double>(percentile(50U));
  state.counters["p95_execution_ns"] = static_cast<double>(percentile(95U));
  state.counters["p99_execution_ns"] = static_cast<double>(percentile(99U));
  state.SetLabel("instantiate plus grouped expression/aggregate/order/limit execution");
}

void execute_grouped_scalar_reference(benchmark::State& state) {
  const std::size_t row_count = static_cast<std::size_t>(state.range(0));
  const std::vector<std::int64_t> values = benchmark_values(row_count);
  const BoundSqlSelect select = benchmark_end_to_end_select();
  const BenchmarkSnapshotProvider provider{benchmark_scalar_snapshot(values)};
  for (auto iteration : state) {
    static_cast<void>(iteration);
    SqlResult<ScalarQueryResult> result = execute_sql_v1_select(select, provider);
    benchmark::DoNotOptimize(result);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(row_count));
  state.counters["input_rows"] = static_cast<double>(row_count);
  state.SetLabel("same bound SQL through scalar reference; stable snapshot retained");
}

BENCHMARK(validate_physical_pipeline_plan)->Arg(1)->Arg(8)->Arg(64)->Arg(256);
BENCHMARK(instantiate_physical_pipeline_plan)->Arg(1)->Arg(8)->Arg(64)->Arg(256);
BENCHMARK(lower_bound_select_pipeline);
BENCHMARK(lower_bound_global_aggregate_pipeline);
BENCHMARK(lower_bound_grouped_aggregate_pipeline);
BENCHMARK(lower_bound_ordered_pipeline);
BENCHMARK(lower_bound_ordered_grouped_pipeline);
BENCHMARK(lower_bound_latest_pipeline);
BENCHMARK(lower_bound_asof_pipeline);
BENCHMARK(execute_bound_grouped_vector_plan)
    ->Args({4'096, 1})
    ->Args({4'096, 16})
    ->Args({4'096, 256})
    ->Args({4'096, 2'048});
BENCHMARK(execute_grouped_scalar_reference)->Arg(4'096);

} // namespace
} // namespace chronos::query
