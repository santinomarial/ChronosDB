#include "chronos/query/lexer.hpp"

#include <array>
#include <benchmark/benchmark.h>
#include <cstdint>
#include <string_view>

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
    "('AAPL', 'MSFT', 'NVDA') ORDER BY t.ts DESC NULLS LAST LIMIT 10000",
};

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

BENCHMARK(tokenize_statement)->DenseRange(0, 2);

} // namespace
} // namespace chronos::query
