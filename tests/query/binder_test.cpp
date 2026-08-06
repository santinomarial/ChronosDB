#include "chronos/common/uuid.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <span>
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

struct TestColumn {
  std::string_view name;
  schema::LogicalTypeKind kind;
  bool nullable{};
};

struct SchemaSeed {
  std::uint8_t value;
};

[[nodiscard]] std::shared_ptr<const schema::TableSchema>
make_schema(const SchemaSeed seed, const std::span<const TestColumn> definitions) {
  std::vector<schema::ColumnDefinition> columns;
  columns.reserve(definitions.size());
  for (std::size_t ordinal = 0U; ordinal < definitions.size(); ++ordinal) {
    columns.push_back(
        schema::ColumnDefinition::create(
            id<schema::ColumnId>(static_cast<std::uint8_t>(seed.value + ordinal + 2U)),
            std::string{definitions[ordinal].name},
            schema::LogicalType::create(definitions[ordinal].kind).value(),
            definitions[ordinal].nullable)
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

[[nodiscard]] std::shared_ptr<const QueryCatalogSnapshot> make_catalog() {
  static constexpr std::array<TestColumn, 5> kTrades{{
      {"ts", schema::LogicalTypeKind::kTimestampNs, false},
      {"symbol", schema::LogicalTypeKind::kSymbol, false},
      {"price", schema::LogicalTypeKind::kFloat64, true},
      {"quantity", schema::LogicalTypeKind::kInt64, false},
      {"sequence", schema::LogicalTypeKind::kUInt64, false},
  }};
  static constexpr std::array<TestColumn, 3> kQuotes{{
      {"ts", schema::LogicalTypeKind::kTimestampNs, false},
      {"symbol", schema::LogicalTypeKind::kSymbol, false},
      {"bid", schema::LogicalTypeKind::kFloat64, true},
  }};
  const std::vector<QueryCatalogTableInput> inputs{
      {.name = "trades", .quoted = false, .schema = make_schema(SchemaSeed{1U}, kTrades)},
      {.name = "quotes", .quoted = false, .schema = make_schema(SchemaSeed{32U}, kQuotes)},
  };
  QueryCatalogSnapshot catalog = QueryCatalogSnapshot::create(41U, inputs).value();
  return std::make_shared<const QueryCatalogSnapshot>(std::move(catalog));
}

[[nodiscard]] SqlResult<BoundSqlSelect> bind(const std::string_view sql) {
  SqlResult<ParsedSqlSelect> parsed = parse_sql_v1_select(sql);
  if (!parsed.has_value())
    return std::unexpected(parsed.error());
  return bind_sql_v1_select(std::move(*parsed), make_catalog());
}

TEST(SqlBinderTest, PinsCatalogAndResolvesExactColumnIdentitiesAndTypes) {
  SqlResult<BoundSqlSelect> bound =
      bind("SELECT t.symbol AS sym, t.price + CAST(1 AS FLOAT64) AS adjusted FROM trades AS t "
           "WHERE t.quantity >= 10");
  ASSERT_TRUE(bound.has_value()) << bound.error().status().to_string();
  EXPECT_EQ(bound->catalog()->generation(), 41U);
  ASSERT_EQ(bound->sources().size(), 1U);
  EXPECT_EQ(bound->sources()[0].exposed_name(), "t");
  EXPECT_EQ(bound->sources()[0].schema_ptr()->table_id(), id<schema::TableId>(1U));
  ASSERT_EQ(bound->outputs().size(), 2U);
  EXPECT_EQ(bound->outputs()[0].name, "sym");
  EXPECT_EQ(bound->outputs()[0].type.kind(), schema::LogicalTypeKind::kSymbol);
  EXPECT_EQ(bound->outputs()[1].type.kind(), schema::LogicalTypeKind::kFloat64);
  EXPECT_TRUE(bound->outputs()[1].nullable);
  ASSERT_GE(bound->column_references().size(), 3U);
  const BoundColumnReference* symbol =
      bound->find_column_reference(bound->syntax().items()[0].expression()->span());
  ASSERT_NE(symbol, nullptr);
  EXPECT_EQ(symbol->column_id, id<schema::ColumnId>(4U));
}

TEST(SqlBinderTest, RejectsUnknownAndAmbiguousNamesDeterministically) {
  EXPECT_EQ(bind("SELECT * FROM missing").error().code(), SqlDiagnosticCode::kUnknownTable);
  EXPECT_EQ(bind("SELECT missing FROM trades").error().code(), SqlDiagnosticCode::kUnknownColumn);
  EXPECT_EQ(
      bind("SELECT symbol FROM trades AS t ASOF JOIN quotes AS q ON t.ts >= q.ts").error().code(),
      SqlDiagnosticCode::kAmbiguousColumn);
  EXPECT_EQ(bind("SELECT trades.symbol FROM trades AS t").error().code(),
            SqlDiagnosticCode::kUnknownTable);
}

TEST(SqlBinderTest, ExpandsStarsWithStableUniqueNames) {
  SqlResult<BoundSqlSelect> one = bind("SELECT * FROM trades");
  ASSERT_TRUE(one.has_value()) << one.error().status().to_string();
  ASSERT_EQ(one->outputs().size(), 5U);
  EXPECT_EQ(one->outputs()[0].name, "ts");
  EXPECT_EQ(one->outputs()[4].name, "sequence");

  SqlResult<BoundSqlSelect> joined = bind(
      "SELECT * FROM trades AS t ASOF JOIN quotes AS q ON t.symbol = q.symbol AND q.ts <= t.ts");
  ASSERT_TRUE(joined.has_value()) << joined.error().status().to_string();
  ASSERT_EQ(joined->outputs().size(), 8U);
  EXPECT_EQ(joined->outputs()[0].name, "t.ts");
  EXPECT_EQ(joined->outputs()[2].name, "price");
  EXPECT_EQ(joined->outputs()[5].name, "q.ts");
  EXPECT_EQ(joined->outputs()[7].name, "bid");
}

TEST(SqlBinderTest, EnforcesV1ImplicitConversionAndPredicateRules) {
  EXPECT_EQ(bind("SELECT quantity + sequence FROM trades").error().code(),
            SqlDiagnosticCode::kTypeMismatch);
  EXPECT_EQ(bind("SELECT symbol + 1 FROM trades").error().code(), SqlDiagnosticCode::kTypeMismatch);
  EXPECT_EQ(bind("SELECT price FROM trades WHERE price").error().code(),
            SqlDiagnosticCode::kTypeMismatch);
  EXPECT_TRUE(bind("SELECT quantity + 1 AS total FROM trades WHERE price IS NULL").has_value());
}

TEST(SqlBinderTest, BindsAggregatesAndRejectsNestedOrUntypedOutputs) {
  SqlResult<BoundSqlSelect> aggregate =
      bind("SELECT symbol, count(*) AS n, avg(price) AS mean FROM trades GROUP BY symbol");
  ASSERT_TRUE(aggregate.has_value()) << aggregate.error().status().to_string();
  ASSERT_EQ(aggregate->outputs().size(), 3U);
  EXPECT_TRUE(aggregate->outputs()[1].contains_aggregate);
  EXPECT_FALSE(aggregate->outputs()[1].nullable);
  EXPECT_EQ(aggregate->outputs()[2].type.kind(), schema::LogicalTypeKind::kFloat64);
  EXPECT_TRUE(aggregate->outputs()[2].nullable);
  EXPECT_EQ(bind("SELECT sum(count(*)) AS bad FROM trades").error().code(),
            SqlDiagnosticCode::kTypeMismatch);
  EXPECT_EQ(bind("SELECT count(count(*)) AS bad FROM trades").error().code(),
            SqlDiagnosticCode::kTypeMismatch);
  EXPECT_EQ(bind("SELECT symbol, count(*) AS n FROM trades").error().code(),
            SqlDiagnosticCode::kTypeMismatch);
  EXPECT_TRUE(bind("SELECT symbol, count(*) AS n FROM trades AS t GROUP BY t.symbol").has_value());
  EXPECT_EQ(bind("SELECT NULL AS no_type FROM trades").error().code(),
            SqlDiagnosticCode::kTypeMismatch);
}

TEST(SqlBinderTest, ResolvesOrderByAliasesButNotWhereOrGroupByAliases) {
  SqlResult<BoundSqlSelect> bound =
      bind("SELECT time_bucket(INTERVAL '1 minute', ts) AS bucket, count(*) AS n "
           "FROM trades GROUP BY time_bucket(INTERVAL '1 minute', ts) ORDER BY bucket, n DESC");
  ASSERT_TRUE(bound.has_value()) << bound.error().status().to_string();
  const SourceSpan first_order = bound->syntax().order_by()[0].expression.span();
  const SourceSpan second_order = bound->syntax().order_by()[1].expression.span();
  ASSERT_NE(bound->find_expression(first_order), nullptr);
  ASSERT_NE(bound->find_expression(second_order), nullptr);
  EXPECT_EQ(bound->find_expression(first_order)->output_ordinal, 0U);
  EXPECT_EQ(bound->find_expression(second_order)->output_ordinal, 1U);
  EXPECT_EQ(bind("SELECT symbol AS sym FROM trades WHERE sym = 'A'").error().code(),
            SqlDiagnosticCode::kUnknownColumn);
  EXPECT_EQ(bind("SELECT symbol AS sym, count(*) AS n FROM trades GROUP BY sym").error().code(),
            SqlDiagnosticCode::kUnknownColumn);
}

TEST(SqlBinderTest, EnforcesResourceAndOutputIdentityLimits) {
  EXPECT_EQ(bind("SELECT symbol AS x, price AS x FROM trades").error().code(),
            SqlDiagnosticCode::kDuplicateOutputName);
  ParsedSqlSelect parsed = parse_sql_v1_select("SELECT * FROM trades").value();
  EXPECT_EQ(bind_sql_v1_select(std::move(parsed), make_catalog(), {.maximum_output_columns = 2U})
                .error()
                .code(),
            SqlDiagnosticCode::kResourceLimit);
}

} // namespace
} // namespace chronos::query
