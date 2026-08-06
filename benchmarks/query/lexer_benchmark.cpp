#include "chronos/common/uuid.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/evaluator.hpp"
#include "chronos/query/lexer.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/value.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"

#include <array>
#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

constexpr std::array<std::string_view, 3> kStatements{
    "SELECT * FROM metrics",
    "SELECT tenant, metric, time_bucket(INTERVAL '1 minute', ts) AS bucket, AVG(value) AS mean "
    "FROM metrics WHERE ts BETWEEN TIMESTAMP '2026-08-01 00:00:00Z' AND TIMESTAMP "
    "'2026-08-02 00:00:00Z' GROUP BY tenant, metric, time_bucket(INTERVAL '1 minute', ts) "
    "ORDER BY bucket ASC LIMIT 1000",
    "SELECT t.symbol, t.price, q.bid_price FROM trades AS t ASOF LEFT JOIN quotes AS q ON "
    "t.symbol = q.symbol AND t.venue = q.venue AND q.ts <= t.ts WHERE t.symbol IN "
    "(CAST('AAPL' AS SYMBOL), CAST('MSFT' AS SYMBOL), CAST('NVDA' AS SYMBOL)) "
    "ORDER BY t.ts DESC NULLS LAST LIMIT 10000",
};

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

struct BenchmarkColumn {
  std::string_view name;
  schema::LogicalTypeKind kind;
};

struct SchemaSeed {
  std::uint8_t value;
};

[[nodiscard]] std::shared_ptr<const schema::TableSchema>
make_schema(const SchemaSeed seed, const std::span<const BenchmarkColumn> definitions) {
  std::vector<schema::ColumnDefinition> columns;
  for (std::size_t ordinal = 0U; ordinal < definitions.size(); ++ordinal) {
    columns.push_back(
        schema::ColumnDefinition::create(
            id<schema::ColumnId>(static_cast<std::uint8_t>(seed.value + ordinal + 2U)),
            std::string{definitions[ordinal].name},
            schema::LogicalType::create(definitions[ordinal].kind).value(), ordinal != 0U)
            .value());
  }
  const schema::ColumnId event_time = columns.front().id();
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(
          id<schema::TableId>(seed.value), id<schema::SchemaId>(seed.value + 1U),
          schema::SchemaVersion::initial(), std::nullopt, std::move(columns),
          {.event_time_column = event_time,
           .physical_ordering_key = {event_time},
           .partition_columns = {event_time},
           .shard_key = {event_time},
           .deduplication_key = {}})
          .value());
}

[[nodiscard]] std::shared_ptr<const QueryCatalogSnapshot> benchmark_catalog() {
  static constexpr std::array<BenchmarkColumn, 4> kMetrics{{
      {"ts", schema::LogicalTypeKind::kTimestampNs},
      {"tenant", schema::LogicalTypeKind::kSymbol},
      {"metric", schema::LogicalTypeKind::kSymbol},
      {"value", schema::LogicalTypeKind::kFloat64},
  }};
  static constexpr std::array<BenchmarkColumn, 4> kTrades{{
      {"ts", schema::LogicalTypeKind::kTimestampNs},
      {"symbol", schema::LogicalTypeKind::kSymbol},
      {"venue", schema::LogicalTypeKind::kSymbol},
      {"price", schema::LogicalTypeKind::kFloat64},
  }};
  static constexpr std::array<BenchmarkColumn, 4> kQuotes{{
      {"ts", schema::LogicalTypeKind::kTimestampNs},
      {"symbol", schema::LogicalTypeKind::kSymbol},
      {"venue", schema::LogicalTypeKind::kSymbol},
      {"bid_price", schema::LogicalTypeKind::kFloat64},
  }};
  const std::vector<QueryCatalogTableInput> inputs{
      {.name = "metrics", .quoted = false, .schema = make_schema(SchemaSeed{1U}, kMetrics)},
      {.name = "trades", .quoted = false, .schema = make_schema(SchemaSeed{32U}, kTrades)},
      {.name = "quotes", .quoted = false, .schema = make_schema(SchemaSeed{64U}, kQuotes)},
  };
  QueryCatalogSnapshot snapshot = QueryCatalogSnapshot::create(1U, inputs).value();
  return std::make_shared<const QueryCatalogSnapshot>(std::move(snapshot));
}

void tokenize_statement(benchmark::State& state) {
  const std::string_view sql = kStatements[static_cast<std::size_t>(state.range(0))];
  for (auto _ : state) {
    static_cast<void>(_);
    SqlResult<SqlTokenStream> result = tokenize_sql_v1(sql);
    benchmark::DoNotOptimize(result);
  }
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(sql.size()));
}

void parse_statement(benchmark::State& state) {
  const std::string_view sql = kStatements[static_cast<std::size_t>(state.range(0))];
  for (auto _ : state) {
    static_cast<void>(_);
    SqlResult<ParsedSqlSelect> result = parse_sql_v1_select(sql);
    benchmark::DoNotOptimize(result);
  }
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(sql.size()));
}

void parse_and_bind_statement(benchmark::State& state) {
  const std::string_view sql = kStatements[static_cast<std::size_t>(state.range(0))];
  const std::shared_ptr<const QueryCatalogSnapshot> catalog = benchmark_catalog();
  for (auto _ : state) {
    static_cast<void>(_);
    SqlResult<ParsedSqlSelect> parsed = parse_sql_v1_select(sql);
    if (!parsed.has_value()) {
      state.SkipWithError("benchmark SQL did not parse");
      break;
    }
    SqlResult<BoundSqlSelect> bound = bind_sql_v1_select(std::move(*parsed), catalog);
    if (!bound.has_value()) {
      state.SkipWithError(bound.error().status().message());
      break;
    }
    benchmark::DoNotOptimize(bound);
  }
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(sql.size()));
}

void evaluate_scalar_expression(benchmark::State& state) {
  const std::shared_ptr<const QueryCatalogSnapshot> catalog = benchmark_catalog();
  ParsedSqlSelect syntax =
      parse_sql_v1_select("SELECT abs(value) + CAST(1 AS FLOAT64) AS adjusted FROM metrics")
          .value();
  BoundSqlSelect plan = bind_sql_v1_select(std::move(syntax), catalog).value();
  const std::vector<ScalarValue> row{
      ScalarValue::signed_value(
          schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
          1'700'000'000'000'000'000LL)
          .value(),
      ScalarValue::text(schema::LogicalType::create(schema::LogicalTypeKind::kSymbol).value(),
                        "tenant")
          .value(),
      ScalarValue::text(schema::LogicalType::create(schema::LogicalTypeKind::kSymbol).value(),
                        "cpu")
          .value(),
      ScalarValue::float64(-41.5).value(),
  };
  const ScalarSourceRow source{row};
  const ScalarEvaluationContext context{.sources = {&source, 1U}};
  for (auto _ : state) {
    static_cast<void>(_);
    SqlResult<ScalarValue> result =
        evaluate_sql_v1_expression(plan, *plan.syntax().items()[0].expression(), context);
    benchmark::DoNotOptimize(result);
  }
}

void evaluate_decimal_expression(benchmark::State& state) {
  const std::shared_ptr<const QueryCatalogSnapshot> catalog = benchmark_catalog();
  ParsedSqlSelect syntax =
      parse_sql_v1_select("SELECT (CAST(9000000000000000000 AS DECIMAL(38,0)) * "
                          "CAST(9000000000000000000 AS DECIMAL(38,0))) / "
                          "CAST(9000000000000000000 AS DECIMAL(38,0)) AS exact FROM metrics")
          .value();
  BoundSqlSelect plan = bind_sql_v1_select(std::move(syntax), catalog).value();
  const ScalarEvaluationContext context;
  for (auto _ : state) {
    static_cast<void>(_);
    SqlResult<ScalarValue> result =
        evaluate_sql_v1_expression(plan, *plan.syntax().items()[0].expression(), context);
    benchmark::DoNotOptimize(result);
  }
}

BENCHMARK(tokenize_statement)->DenseRange(0, 2);
BENCHMARK(parse_statement)->DenseRange(0, 2);
BENCHMARK(parse_and_bind_statement)->DenseRange(0, 2);
BENCHMARK(evaluate_scalar_expression);
BENCHMARK(evaluate_decimal_expression);

} // namespace
} // namespace chronos::query
