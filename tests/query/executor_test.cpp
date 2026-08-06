#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/executor.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/snapshot.hpp"
#include "chronos/query/value.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

struct TestColumn {
  std::string_view name;
  schema::LogicalTypeKind kind;
  bool nullable{};
};

[[nodiscard]] std::shared_ptr<const schema::TableSchema>
make_schema(const std::uint8_t seed, const std::span<const TestColumn> definitions) {
  std::vector<schema::ColumnDefinition> columns;
  for (std::size_t ordinal = 0U; ordinal < definitions.size(); ++ordinal) {
    columns.push_back(schema::ColumnDefinition::create(
                          id<schema::ColumnId>(static_cast<std::uint8_t>(seed + ordinal + 2U)),
                          std::string{definitions[ordinal].name}, type(definitions[ordinal].kind),
                          definitions[ordinal].nullable)
                          .value());
  }
  const schema::ColumnId timestamp = columns[0].id();
  const schema::ColumnId symbol = columns[1].id();
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(id<schema::TableId>(seed), id<schema::SchemaId>(seed + 1U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns),
                                  {.event_time_column = timestamp,
                                   .physical_ordering_key = {symbol, timestamp},
                                   .partition_columns = {timestamp},
                                   .shard_key = {symbol},
                                   .deduplication_key = {symbol, timestamp}})
          .value());
}

struct Fixtures {
  std::shared_ptr<const schema::TableSchema> trades;
  std::shared_ptr<const schema::TableSchema> quotes;
  std::shared_ptr<const QueryCatalogSnapshot> catalog;
};

[[nodiscard]] Fixtures fixtures() {
  static constexpr std::array<TestColumn, 3> kTrades{{
      {"ts", schema::LogicalTypeKind::kTimestampNs, false},
      {"symbol", schema::LogicalTypeKind::kSymbol, false},
      {"price", schema::LogicalTypeKind::kFloat64, true},
  }};
  static constexpr std::array<TestColumn, 3> kQuotes{{
      {"ts", schema::LogicalTypeKind::kTimestampNs, false},
      {"symbol", schema::LogicalTypeKind::kSymbol, false},
      {"bid", schema::LogicalTypeKind::kFloat64, true},
  }};
  Fixtures result;
  result.trades = make_schema(1U, kTrades);
  result.quotes = make_schema(32U, kQuotes);
  const std::vector<QueryCatalogTableInput> inputs{
      {.name = "trades", .quoted = false, .schema = result.trades},
      {.name = "quotes", .quoted = false, .schema = result.quotes},
  };
  result.catalog = std::make_shared<const QueryCatalogSnapshot>(
      QueryCatalogSnapshot::create(1U, inputs).value());
  return result;
}

[[nodiscard]] ScalarInputRow input_row(const std::int64_t timestamp, const std::string_view symbol,
                                       const double value, const std::uint64_t sequence) {
  common::Uuid::Bytes wal{};
  wal.front() = std::byte{9U};
  return ScalarInputRow{
      .columns =
          {
              ScalarValue::signed_value(type(schema::LogicalTypeKind::kTimestampNs), timestamp)
                  .value(),
              ScalarValue::text(type(schema::LogicalTypeKind::kSymbol), std::string{symbol})
                  .value(),
              ScalarValue::float64(value).value(),
          },
      .wal_id = common::Uuid{wal},
      .record_sequence = sequence,
      .system_commit_position = sequence,
      .row_ordinal = 0U,
  };
}

class TestProvider final : public ScalarSnapshotProvider {
public:
  TestProvider(std::shared_ptr<const ScalarTableSnapshot> trades,
               std::shared_ptr<const ScalarTableSnapshot> quotes)
      : trades_(std::move(trades)), quotes_(std::move(quotes)) {}

  [[nodiscard]] common::Result<std::shared_ptr<const ScalarTableSnapshot>>
  resolve(const std::shared_ptr<const schema::TableSchema>& bound_schema,
          const std::optional<std::int64_t> as_of_system_time_ns) const override {
    requests.push_back(as_of_system_time_ns);
    if (bound_schema->table_id() == trades_->schema_ptr()->table_id())
      return trades_;
    if (bound_schema->table_id() == quotes_->schema_ptr()->table_id())
      return quotes_;
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "test snapshot is absent"});
  }

  mutable std::vector<std::optional<std::int64_t>> requests;

private:
  std::shared_ptr<const ScalarTableSnapshot> trades_;
  std::shared_ptr<const ScalarTableSnapshot> quotes_;
};

[[nodiscard]] BoundSqlSelect bind(const std::string_view sql, const Fixtures& data) {
  return bind_sql_v1_select(parse_sql_v1_select(sql).value(), data.catalog).value();
}

[[nodiscard]] TestProvider provider(const Fixtures& data, std::vector<ScalarInputRow> trades,
                                    std::vector<ScalarInputRow> quotes) {
  auto trade_snapshot = std::make_shared<const ScalarTableSnapshot>(
      ScalarTableSnapshot::create(data.trades, 1'000'000U, std::move(trades)).value());
  auto quote_snapshot = std::make_shared<const ScalarTableSnapshot>(
      ScalarTableSnapshot::create(data.quotes, 1'000'000U, std::move(quotes)).value());
  return TestProvider{std::move(trade_snapshot), std::move(quote_snapshot)};
}

TEST(ScalarExecutorTest, ExecutesLatestAsofLeftWhereProjectionOrderAndLimit) {
  const Fixtures data = fixtures();
  BoundSqlSelect plan =
      bind("SELECT t.symbol AS sym, t.ts, q.bid FROM trades AS t "
           "FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-01 00:00:00.000000050Z' "
           "LATEST BY (symbol) ON t.ts "
           "ASOF LEFT JOIN quotes AS q ON t.symbol = q.symbol AND q.ts <= t.ts "
           "WHERE t.price > CAST(0 AS FLOAT64) ORDER BY q.bid DESC NULLS LAST LIMIT 2",
           data);
  TestProvider snapshots = provider(
      data,
      {input_row(10, "A", 1.0, 1U), input_row(20, "A", 2.0, 2U), input_row(15, "B", -1.0, 3U),
       input_row(12, "C", 3.0, 4U)},
      {input_row(5, "A", 10.0, 5U), input_row(18, "A", 11.0, 6U), input_row(21, "A", 12.0, 7U)});

  SqlResult<ScalarQueryResult> result = execute_sql_v1_select(plan, snapshots);
  ASSERT_TRUE(result.has_value()) << result.error().status().to_string();
  ASSERT_EQ(result->columns().size(), 3U);
  ASSERT_EQ(result->rows().size(), 2U);
  EXPECT_EQ(*std::get_if<std::string>(&result->rows()[0][0].storage()), "A");
  EXPECT_EQ(*std::get_if<std::int64_t>(&result->rows()[0][1].storage()), 20);
  EXPECT_DOUBLE_EQ(*std::get_if<double>(&result->rows()[0][2].storage()), 11.0);
  EXPECT_EQ(*std::get_if<std::string>(&result->rows()[1][0].storage()), "C");
  EXPECT_TRUE(result->rows()[1][2].is_null());
  ASSERT_EQ(snapshots.requests.size(), 2U);
  EXPECT_EQ(snapshots.requests[0], 50);
  EXPECT_FALSE(snapshots.requests[1].has_value());
}

TEST(ScalarExecutorTest, UsesStableVersionTieBreakersAndEnforcesLimits) {
  const Fixtures data = fixtures();
  BoundSqlSelect plan =
      bind("SELECT t.price FROM trades AS t LATEST BY (symbol) ON t.ts ORDER BY t.price", data);
  TestProvider snapshots =
      provider(data, {input_row(10, "A", 1.0, 1U), input_row(10, "A", 2.0, 2U)}, {});
  const auto result = execute_sql_v1_select(plan, snapshots);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->rows().size(), 1U);
  EXPECT_DOUBLE_EQ(*std::get_if<double>(&result->rows()[0][0].storage()), 2.0);

  const auto limited = execute_sql_v1_select(plan, snapshots,
                                             {.maximum_rows_per_source = 1U,
                                              .maximum_intermediate_rows = 1U,
                                              .maximum_output_rows = 1U,
                                              .maximum_groups = 1U});
  ASSERT_FALSE(limited.has_value());
  EXPECT_EQ(limited.error().status().code(), common::StatusCode::kResourceExhausted);
}

TEST(ScalarExecutorTest, ExecutesGroupedAggregatesNullRulesAndStableOrdering) {
  const Fixtures data = fixtures();
  BoundSqlSelect plan =
      bind("SELECT symbol, count(*) AS n, count(price) AS present, sum(price) AS total, "
           "avg(price) AS mean, min(price) AS low, max(price) AS high, "
           "var_pop(price) AS population, var_samp(price) AS sample "
           "FROM trades GROUP BY symbol ORDER BY symbol",
           data);
  ScalarInputRow null_price = input_row(3, "B", 0.0, 3U);
  null_price.columns[2] = ScalarValue::null(type(schema::LogicalTypeKind::kFloat64));
  TestProvider snapshots = provider(
      data, {input_row(1, "A", 1.0, 1U), input_row(2, "A", 3.0, 2U), std::move(null_price)}, {});
  const auto result = execute_sql_v1_select(plan, snapshots);
  ASSERT_TRUE(result.has_value()) << result.error().status().to_string();
  ASSERT_EQ(result->rows().size(), 2U);
  const auto& first = result->rows()[0];
  EXPECT_EQ(*std::get_if<std::string>(&first[0].storage()), "A");
  EXPECT_EQ(*std::get_if<std::int64_t>(&first[1].storage()), 2);
  EXPECT_EQ(*std::get_if<std::int64_t>(&first[2].storage()), 2);
  EXPECT_DOUBLE_EQ(*std::get_if<double>(&first[3].storage()), 4.0);
  EXPECT_DOUBLE_EQ(*std::get_if<double>(&first[4].storage()), 2.0);
  EXPECT_DOUBLE_EQ(*std::get_if<double>(&first[5].storage()), 1.0);
  EXPECT_DOUBLE_EQ(*std::get_if<double>(&first[6].storage()), 3.0);
  EXPECT_DOUBLE_EQ(*std::get_if<double>(&first[7].storage()), 1.0);
  EXPECT_DOUBLE_EQ(*std::get_if<double>(&first[8].storage()), 2.0);
  const auto& second = result->rows()[1];
  EXPECT_EQ(*std::get_if<std::string>(&second[0].storage()), "B");
  EXPECT_EQ(*std::get_if<std::int64_t>(&second[1].storage()), 1);
  EXPECT_EQ(*std::get_if<std::int64_t>(&second[2].storage()), 0);
  for (std::size_t index = 3U; index < second.size(); ++index)
    EXPECT_TRUE(second[index].is_null());
}

TEST(ScalarExecutorTest, ImplementsEmptyAggregatesAndExactWidenedIntegerSum) {
  const Fixtures data = fixtures();
  BoundSqlSelect empty_plan = bind("SELECT count(*) AS n, sum(price) AS total FROM trades", data);
  TestProvider empty_snapshots = provider(data, {}, {});
  const auto empty = execute_sql_v1_select(empty_plan, empty_snapshots);
  ASSERT_TRUE(empty.has_value());
  ASSERT_EQ(empty->rows().size(), 1U);
  EXPECT_EQ(*std::get_if<std::int64_t>(&empty->rows()[0][0].storage()), 0);
  EXPECT_TRUE(empty->rows()[0][1].is_null());

  BoundSqlSelect sum_plan = bind("SELECT sum(CAST(price AS INT64)) AS total FROM trades", data);
  TestProvider cancelling = provider(data,
                                     {input_row(1, "A", 9.0e18, 1U), input_row(2, "B", 9.0e18, 2U),
                                      input_row(3, "C", -9.0e18, 3U)},
                                     {});
  const auto exact = execute_sql_v1_select(sum_plan, cancelling);
  ASSERT_TRUE(exact.has_value()) << exact.error().status().to_string();
  EXPECT_EQ(*std::get_if<std::int64_t>(&exact->rows()[0][0].storage()),
            9'000'000'000'000'000'000LL);

  TestProvider overflowing =
      provider(data, {input_row(1, "A", 9.0e18, 1U), input_row(2, "B", 9.0e18, 2U)}, {});
  const auto overflow = execute_sql_v1_select(sum_plan, overflowing);
  ASSERT_FALSE(overflow.has_value());
  EXPECT_EQ(overflow.error().status().code(), common::StatusCode::kOutOfRange);

  BoundSqlSelect decimal_sum_plan =
      bind("SELECT CAST(sum(CAST(price AS DECIMAL(38,0))) AS FLOAT64) AS total FROM trades", data);
  TestProvider decimal_cancelling =
      provider(data,
               {input_row(1, "A", 9.0e37, 1U), input_row(2, "B", 9.0e37, 2U),
                input_row(3, "C", -9.0e37, 3U)},
               {});
  EXPECT_TRUE(execute_sql_v1_select(decimal_sum_plan, decimal_cancelling).has_value());
  TestProvider decimal_overflowing =
      provider(data, {input_row(1, "A", 9.0e37, 1U), input_row(2, "B", 9.0e37, 2U)}, {});
  const auto decimal_overflow = execute_sql_v1_select(decimal_sum_plan, decimal_overflowing);
  ASSERT_FALSE(decimal_overflow.has_value());
  EXPECT_EQ(decimal_overflow.error().status().code(), common::StatusCode::kOutOfRange);
}

TEST(ScalarExecutorPropertyTest, GroupedCountSumAndAverageMatchDeterministicReference) {
  const Fixtures data = fixtures();
  BoundSqlSelect plan =
      bind("SELECT symbol, count(price) AS n, sum(price) AS total, avg(price) AS mean "
           "FROM trades GROUP BY symbol ORDER BY symbol",
           data);
  std::vector<ScalarInputRow> rows;
  std::array<std::int64_t, 2> counts{};
  std::array<double, 2> sums{};
  for (std::size_t index = 0U; index < 200U; ++index) {
    const std::size_t group = index % 2U;
    const double value = static_cast<double>(static_cast<std::int64_t>(index % 17U) - 8);
    ScalarInputRow generated =
        input_row(static_cast<std::int64_t>(index), group == 0U ? "A" : "B", value, index + 1U);
    if (index % 11U == 0U) {
      generated.columns[2] = ScalarValue::null(type(schema::LogicalTypeKind::kFloat64));
    } else {
      ++counts[group];
      sums[group] += value;
    }
    rows.push_back(std::move(generated));
  }
  TestProvider snapshots = provider(data, std::move(rows), {});
  const auto result = execute_sql_v1_select(plan, snapshots);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->rows().size(), 2U);
  for (std::size_t group = 0U; group < 2U; ++group) {
    EXPECT_EQ(*std::get_if<std::int64_t>(&result->rows()[group][1].storage()), counts[group]);
    EXPECT_DOUBLE_EQ(*std::get_if<double>(&result->rows()[group][2].storage()), sums[group]);
    EXPECT_DOUBLE_EQ(*std::get_if<double>(&result->rows()[group][3].storage()),
                     sums[group] / static_cast<double>(counts[group]));
  }
}

TEST(ScalarExecutorPropertyTest, LatestAndAsofMatchDeterministicSmallDatabaseModel) {
  const Fixtures data = fixtures();
  BoundSqlSelect plan =
      bind("SELECT t.symbol, t.ts, q.ts AS quote_ts FROM trades AS t LATEST BY (symbol) ON t.ts "
           "ASOF LEFT JOIN quotes AS q ON t.symbol = q.symbol AND q.ts <= t.ts ORDER BY t.symbol",
           data);
  struct ModelRow {
    std::size_t symbol{};
    std::int64_t timestamp{};
    std::uint64_t sequence{};
  };
  std::uint64_t random = 0x6a09e667f3bcc909ULL;
  const auto next = [&random]() {
    random = random * 6'364'136'223'846'793'005ULL + 1'442'695'040'888'963'407ULL;
    return random;
  };
  constexpr std::array<std::string_view, 3> kSymbols{"A", "B", "C"};
  for (std::size_t trial = 0U; trial < 40U; ++trial) {
    std::vector<ModelRow> trade_model;
    std::vector<ModelRow> quote_model;
    std::vector<ScalarInputRow> trades;
    std::vector<ScalarInputRow> quotes;
    const std::size_t trade_count = 1U + static_cast<std::size_t>(next() % 24U);
    const std::size_t quote_count = static_cast<std::size_t>(next() % 32U);
    for (std::size_t index = 0U; index < trade_count; ++index) {
      const ModelRow row{.symbol = static_cast<std::size_t>(next() % kSymbols.size()),
                         .timestamp = static_cast<std::int64_t>(next() % 20U),
                         .sequence = index + 1U};
      trade_model.push_back(row);
      trades.push_back(
          input_row(row.timestamp, kSymbols[row.symbol], static_cast<double>(index), row.sequence));
    }
    for (std::size_t index = 0U; index < quote_count; ++index) {
      const ModelRow row{.symbol = static_cast<std::size_t>(next() % kSymbols.size()),
                         .timestamp = static_cast<std::int64_t>(next() % 20U),
                         .sequence = trade_count + index + 1U};
      quote_model.push_back(row);
      quotes.push_back(
          input_row(row.timestamp, kSymbols[row.symbol], static_cast<double>(index), row.sequence));
    }

    TestProvider snapshots = provider(data, std::move(trades), std::move(quotes));
    const auto result = execute_sql_v1_select(plan, snapshots);
    ASSERT_TRUE(result.has_value()) << "trial " << trial;
    std::size_t output = 0U;
    for (std::size_t symbol = 0U; symbol < kSymbols.size(); ++symbol) {
      const ModelRow* latest = nullptr;
      for (const ModelRow& candidate : trade_model) {
        if (candidate.symbol == symbol &&
            (latest == nullptr || candidate.timestamp > latest->timestamp ||
             (candidate.timestamp == latest->timestamp && candidate.sequence > latest->sequence))) {
          latest = std::addressof(candidate);
        }
      }
      if (latest == nullptr)
        continue;
      ASSERT_LT(output, result->rows().size());
      EXPECT_EQ(std::get<std::string>(result->rows()[output][0].storage()), kSymbols[symbol]);
      EXPECT_EQ(std::get<std::int64_t>(result->rows()[output][1].storage()), latest->timestamp);
      const ModelRow* quote = nullptr;
      for (const ModelRow& candidate : quote_model) {
        if (candidate.symbol == symbol && candidate.timestamp <= latest->timestamp &&
            (quote == nullptr || candidate.timestamp > quote->timestamp ||
             (candidate.timestamp == quote->timestamp && candidate.sequence > quote->sequence))) {
          quote = std::addressof(candidate);
        }
      }
      if (quote == nullptr)
        EXPECT_TRUE(result->rows()[output][2].is_null());
      else
        EXPECT_EQ(std::get<std::int64_t>(result->rows()[output][2].storage()), quote->timestamp);
      ++output;
    }
    EXPECT_EQ(result->rows().size(), output);
  }
}

} // namespace
} // namespace chronos::query
