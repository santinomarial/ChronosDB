#include "chronos/common/uuid.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
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

BENCHMARK(validate_physical_pipeline_plan)->Arg(1)->Arg(8)->Arg(64)->Arg(256);
BENCHMARK(instantiate_physical_pipeline_plan)->Arg(1)->Arg(8)->Arg(64)->Arg(256);
BENCHMARK(lower_bound_select_pipeline);
BENCHMARK(lower_bound_global_aggregate_pipeline);
BENCHMARK(lower_bound_grouped_aggregate_pipeline);
BENCHMARK(lower_bound_ordered_pipeline);
BENCHMARK(lower_bound_ordered_grouped_pipeline);
BENCHMARK(lower_bound_latest_pipeline);
BENCHMARK(lower_bound_asof_pipeline);

} // namespace
} // namespace chronos::query
