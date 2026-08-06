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
      {"price", schema::LogicalTypeKind::kFloat64, false},
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
      ScalarTableSnapshot::create(data.trades, 100U, std::move(trades)).value());
  auto quote_snapshot = std::make_shared<const ScalarTableSnapshot>(
      ScalarTableSnapshot::create(data.quotes, 100U, std::move(quotes)).value());
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

} // namespace
} // namespace chronos::query
