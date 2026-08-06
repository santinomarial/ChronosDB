#include "chronos/query/parser.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>

namespace chronos::query {
namespace {

template <typename Value>
[[nodiscard]] const Value* optional_pointer(const std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(value.value()) : nullptr;
}

TEST(SqlParserTest, ParsesCompleteAnalyticalSelectIntoOwnedGoldenShape) {
  constexpr std::string_view sql =
      "EXPLAIN ANALYZE SELECT t.symbol AS symbol, count(*) AS n, "
      "time_bucket(INTERVAL '1 minute', t.ts) AS bucket FROM trades AS t "
      "FOR SYSTEM_TIME AS OF TIMESTAMP '2026-08-06 12:00:00Z' "
      "LATEST BY (symbol, venue) ON t.ts "
      "ASOF LEFT JOIN quotes AS q ON t.symbol = q.symbol AND q.ts <= t.ts "
      "WHERE t.price BETWEEN 10 AND 20 AND t.venue IN ('XNYS', 'XNAS') "
      "GROUP BY t.symbol, time_bucket(INTERVAL '1 minute', t.ts) "
      "ORDER BY bucket DESC NULLS LAST LIMIT 1000;";
  const SqlResult<ParsedSqlSelect> parsed = parse_sql_v1_select(sql);
  ASSERT_TRUE(parsed.has_value()) << parsed.error().status().to_string();
  EXPECT_EQ(parsed->mode(), SqlSelectMode::kExplainAnalyze);
  ASSERT_EQ(parsed->items().size(), 3U);
  EXPECT_EQ(parsed->items()[0].expression()->kind(), SqlExpressionKind::kColumn);
  ASSERT_EQ(parsed->items()[0].expression()->name().size(), 2U);
  EXPECT_EQ(parsed->items()[0].expression()->name()[0].text(), "t");
  const SqlIdentifier* output_alias = optional_pointer(parsed->items()[0].alias());
  ASSERT_NE(output_alias, nullptr);
  EXPECT_EQ(output_alias->text(), "symbol");
  EXPECT_EQ(parsed->items()[1].expression()->kind(), SqlExpressionKind::kFunction);
  EXPECT_EQ(parsed->items()[1].expression()->text(), "count");
  ASSERT_EQ(parsed->items()[1].expression()->children().size(), 1U);
  EXPECT_EQ(parsed->items()[1].expression()->children().front().kind(), SqlExpressionKind::kStar);
  EXPECT_EQ(parsed->source().table.text(), "trades");
  const SqlIdentifier* source_alias = optional_pointer(parsed->source().alias);
  const std::string* system_time = optional_pointer(parsed->system_time());
  const SqlLatestBy* latest = optional_pointer(parsed->latest_by());
  ASSERT_NE(source_alias, nullptr);
  ASSERT_NE(system_time, nullptr);
  ASSERT_NE(latest, nullptr);
  EXPECT_EQ(source_alias->text(), "t");
  EXPECT_EQ(*system_time, "2026-08-06 12:00:00Z");
  EXPECT_EQ(latest->keys.size(), 2U);
  ASSERT_EQ(parsed->asof_joins().size(), 1U);
  EXPECT_TRUE(parsed->asof_joins().front().left);
  EXPECT_EQ(parsed->asof_joins().front().source.table.text(), "quotes");
  ASSERT_NE(parsed->where(), nullptr);
  EXPECT_EQ(parsed->where()->operation(), SqlOperator::kAnd);
  EXPECT_EQ(parsed->group_by().size(), 2U);
  ASSERT_EQ(parsed->order_by().size(), 1U);
  EXPECT_EQ(parsed->order_by().front().direction, SqlOrderDirection::kDescending);
  EXPECT_EQ(parsed->order_by().front().null_order, SqlNullOrder::kLast);
  const std::uint64_t* limit = optional_pointer(parsed->limit());
  ASSERT_NE(limit, nullptr);
  EXPECT_EQ(*limit, 1000U);
  EXPECT_EQ(parsed->span().begin.byte_offset, 0U);
  EXPECT_EQ(parsed->span().byte_length, sql.size());
}

TEST(SqlParserTest, ImplementsTheSpecifiedExpressionPrecedence) {
  const SqlResult<ParsedSqlSelect> parsed =
      parse_sql_v1_select("SELECT NOT a = 1 OR b + 2 * -c BETWEEN 0 AND 9 AS predicate FROM t");
  ASSERT_TRUE(parsed.has_value()) << parsed.error().status().to_string();
  const SqlExpression& root = *parsed->items().front().expression();
  EXPECT_EQ(root.operation(), SqlOperator::kOr);
  ASSERT_EQ(root.children().size(), 2U);
  EXPECT_EQ(root.children()[0].operation(), SqlOperator::kNot);
  EXPECT_EQ(root.children()[0].children().front().operation(), SqlOperator::kEqual);
  EXPECT_EQ(root.children()[1].operation(), SqlOperator::kBetween);
  const SqlExpression& additive = root.children()[1].children()[0];
  EXPECT_EQ(additive.operation(), SqlOperator::kAdd);
  EXPECT_EQ(additive.children()[1].operation(), SqlOperator::kMultiply);
  EXPECT_EQ(additive.children()[1].children()[1].operation(), SqlOperator::kNegative);
}

TEST(SqlParserTest, ParsesNullInAndEveryCastTypeWithoutImplicitSemantics) {
  const SqlResult<ParsedSqlSelect> parsed =
      parse_sql_v1_select("SELECT CAST(v AS DECIMAL(38, 18)) AS d, CAST(v AS UINT64) AS u FROM t "
                          "WHERE v IS NOT NULL AND v NOT IN (1, 2, 3)");
  ASSERT_TRUE(parsed.has_value()) << parsed.error().status().to_string();
  ASSERT_TRUE(parsed->items()[0].expression()->cast_type().has_value());
  const schema::LogicalType* decimal_type =
      optional_pointer(parsed->items()[0].expression()->cast_type());
  const schema::LogicalType* unsigned_type =
      optional_pointer(parsed->items()[1].expression()->cast_type());
  ASSERT_NE(decimal_type, nullptr);
  ASSERT_NE(unsigned_type, nullptr);
  EXPECT_EQ(decimal_type->parameter_0(), 38U);
  EXPECT_EQ(decimal_type->parameter_1(), 18U);
  EXPECT_EQ(unsigned_type->kind(), schema::LogicalTypeKind::kUInt64);
  ASSERT_EQ(parsed->where()->children().size(), 2U);
  EXPECT_EQ(parsed->where()->children()[0].operation(), SqlOperator::kIsNotNull);
  EXPECT_EQ(parsed->where()->children()[1].operation(), SqlOperator::kNotIn);
}

TEST(SqlParserTest, ParsesCanonicalCreateTableAndInsertStatements) {
  const SqlResult<ParsedSqlCreateTable> create = parse_sql_v1_create_table(
      "CREATE TABLE trades (ts TIMESTAMP_NS NOT NULL, symbol SYMBOL NOT NULL, "
      "price DECIMAL(20,8)) EVENT TIME ts ORDER KEY (symbol, ts) "
      "PARTITION BY time_bucket(INTERVAL '1 day', ts) SHARD KEY (symbol) "
      "DEDUP KEY (symbol, ts) RETENTION INTERVAL '10 days' "
      "SYSTEM HISTORY RETENTION INTERVAL '2 days' ALLOWED LATENESS INTERVAL '5 seconds';");
  ASSERT_TRUE(create.has_value()) << create.error().status().to_string();
  EXPECT_EQ(create->table().text(), "trades");
  ASSERT_EQ(create->columns().size(), 3U);
  EXPECT_FALSE(create->columns()[0].nullable);
  EXPECT_TRUE(create->columns()[2].nullable);
  EXPECT_EQ(create->columns()[2].type.parameter_0(), 20U);
  EXPECT_EQ(create->event_time().text(), "ts");
  EXPECT_EQ(create->ordering_key().size(), 2U);
  EXPECT_EQ(create->partition_expression().text(), "time_bucket");
  EXPECT_EQ(create->shard_key().size(), 1U);
  EXPECT_EQ(create->deduplication_key().size(), 2U);
  EXPECT_EQ(create->retention_interval(), "10 days");
  EXPECT_EQ(create->system_history_retention_interval(), "2 days");
  EXPECT_EQ(create->allowed_lateness_interval(), "5 seconds");

  const SqlResult<ParsedSqlInsert> insert = parse_sql_v1_insert(
      "INSERT INTO trades (ts, symbol, price) VALUES "
      "(TIMESTAMP '2026-08-06 12:00:00Z', CAST('A' AS SYMBOL), CAST(1 AS DECIMAL(20,8))), "
      "(TIMESTAMP '2026-08-06 12:00:01Z', CAST('B' AS SYMBOL), NULL);");
  ASSERT_TRUE(insert.has_value()) << insert.error().status().to_string();
  EXPECT_EQ(insert->table().text(), "trades");
  EXPECT_EQ(insert->columns().size(), 3U);
  ASSERT_EQ(insert->rows().size(), 2U);
  EXPECT_EQ(insert->rows()[0].size(), 3U);
  EXPECT_EQ(insert->rows()[0][0].literal_kind(), SqlLiteralKind::kTimestamp);
  EXPECT_EQ(insert->rows()[1][2].literal_kind(), SqlLiteralKind::kNull);
}

TEST(SqlParserTest, RejectsMalformedCreateTableAndInsertStatements) {
  for (const std::string_view sql : {
           "CREATE TABLE t (ts TIMESTAMP_NS) EVENT TIME ts ORDER KEY (ts)",
           "CREATE TABLE t () EVENT TIME ts ORDER KEY (ts) PARTITION BY ts SHARD KEY (ts) "
           "RETENTION INTERVAL '1 day' SYSTEM HISTORY RETENTION INTERVAL '1 day' "
           "ALLOWED LATENESS INTERVAL '1 second'",
           "CREATE TABLE t (ts TIMESTAMP_NS) EVENT TIME ts ORDER KEY () PARTITION BY ts "
           "SHARD KEY (ts) RETENTION INTERVAL '1 day' SYSTEM HISTORY RETENTION INTERVAL "
           "'1 day' ALLOWED LATENESS INTERVAL '1 second'",
       }) {
    SCOPED_TRACE(sql);
    EXPECT_FALSE(parse_sql_v1_create_table(sql).has_value());
  }
  for (const std::string_view sql : {
           "INSERT trades VALUES (1)",
           "INSERT INTO t VALUES",
           "INSERT INTO t VALUES ()",
           "INSERT INTO t (a,) VALUES (1)",
           "INSERT INTO t VALUES (1),",
       }) {
    SCOPED_TRACE(sql);
    EXPECT_FALSE(parse_sql_v1_insert(sql).has_value());
  }
  EXPECT_EQ(parse_sql_v1_insert("INSERT INTO t VALUES (1), (2)", {.maximum_list_elements = 1U})
                .error()
                .code(),
            SqlDiagnosticCode::kResourceLimit);
}

TEST(SqlParserTest, RejectsClauseOrderMissingSyntaxAndUnsupportedStatementsDeterministically) {
  for (const std::string_view sql : {
           "SELECT FROM t",
           "SELECT a t",
           "SELECT a FROM t LIMIT -1",
           "SELECT a FROM t WHERE",
           "SELECT a FROM t ORDER a",
           "SELECT a FROM t WHERE a IN ()",
           "SELECT CAST(a AS DECIMAL(39, 0)) FROM t",
           "CREATE TABLE t (a INT64)",
           "SELECT a FROM t;;",
       }) {
    SCOPED_TRACE(sql);
    const SqlResult<ParsedSqlSelect> first = parse_sql_v1_select(sql);
    const SqlResult<ParsedSqlSelect> second = parse_sql_v1_select(sql);
    ASSERT_FALSE(first.has_value());
    ASSERT_FALSE(second.has_value());
    EXPECT_EQ(first.error().code(), second.error().code());
    EXPECT_EQ(first.error().span(), second.error().span());
    EXPECT_FALSE(first.error().status().message().empty());
  }
}

TEST(SqlParserTest, EnforcesAstDepthNodeAndListLimits) {
  EXPECT_EQ(parse_sql_v1_select("SELECT a FROM t", {.maximum_ast_nodes = 2U}).error().code(),
            SqlDiagnosticCode::kResourceLimit);
  EXPECT_EQ(parse_sql_v1_select("SELECT a, b FROM t", {.maximum_list_elements = 1U}).error().code(),
            SqlDiagnosticCode::kResourceLimit);
  std::string deep{"SELECT "};
  for (std::size_t index = 0U; index < 16U; ++index) {
    deep.append("NOT ");
  }
  deep.append("TRUE FROM t");
  EXPECT_EQ(parse_sql_v1_select(deep, {.maximum_expression_depth = 8U}).error().code(),
            SqlDiagnosticCode::kResourceLimit);
  std::string nested{"SELECT "};
  nested.append(32U, '(');
  nested.append("1");
  nested.append(32U, ')');
  nested.append(" FROM t");
  EXPECT_EQ(parse_sql_v1_select(nested, {.maximum_expression_depth = 8U}).error().code(),
            SqlDiagnosticCode::kResourceLimit);
}

TEST(SqlParserPropertyTest, ParenthesesPreserveGeneratedArithmeticTreeShape) {
  for (std::int64_t left = -8; left <= 8; ++left) {
    for (std::int64_t right = -8; right <= 8; ++right) {
      const std::string sql = "SELECT (" + std::to_string(left) + ") + (" + std::to_string(right) +
                              ") * 2 AS value FROM numbers";
      const SqlResult<ParsedSqlSelect> parsed = parse_sql_v1_select(sql);
      ASSERT_TRUE(parsed.has_value()) << parsed.error().status().to_string();
      const SqlExpression& root = *parsed->items().front().expression();
      EXPECT_EQ(root.operation(), SqlOperator::kAdd);
      ASSERT_EQ(root.children().size(), 2U);
      EXPECT_EQ(root.children()[1].operation(), SqlOperator::kMultiply);
    }
  }
}

} // namespace
} // namespace chronos::query
