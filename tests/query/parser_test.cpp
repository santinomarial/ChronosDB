#include "chronos/query/parser.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <string>

namespace chronos::query {
namespace {

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
  EXPECT_EQ(parsed->items()[0].alias()->text(), "symbol");
  EXPECT_EQ(parsed->items()[1].expression()->kind(), SqlExpressionKind::kFunction);
  EXPECT_EQ(parsed->items()[1].expression()->text(), "count");
  ASSERT_EQ(parsed->items()[1].expression()->children().size(), 1U);
  EXPECT_EQ(parsed->items()[1].expression()->children().front().kind(), SqlExpressionKind::kStar);
  EXPECT_EQ(parsed->source().table.text(), "trades");
  EXPECT_EQ(parsed->source().alias->text(), "t");
  EXPECT_EQ(*parsed->system_time(), "2026-08-06 12:00:00Z");
  ASSERT_TRUE(parsed->latest_by().has_value());
  EXPECT_EQ(parsed->latest_by()->keys.size(), 2U);
  ASSERT_EQ(parsed->asof_joins().size(), 1U);
  EXPECT_TRUE(parsed->asof_joins().front().left);
  EXPECT_EQ(parsed->asof_joins().front().source.table.text(), "quotes");
  ASSERT_NE(parsed->where(), nullptr);
  EXPECT_EQ(parsed->where()->operation(), SqlOperator::kAnd);
  EXPECT_EQ(parsed->group_by().size(), 2U);
  ASSERT_EQ(parsed->order_by().size(), 1U);
  EXPECT_EQ(parsed->order_by().front().direction, SqlOrderDirection::kDescending);
  EXPECT_EQ(parsed->order_by().front().null_order, SqlNullOrder::kLast);
  EXPECT_EQ(*parsed->limit(), 1000U);
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
  EXPECT_EQ(parsed->items()[0].expression()->cast_type()->parameter_0(), 38U);
  EXPECT_EQ(parsed->items()[0].expression()->cast_type()->parameter_1(), 18U);
  EXPECT_EQ(parsed->items()[1].expression()->cast_type()->kind(), schema::LogicalTypeKind::kUInt64);
  ASSERT_EQ(parsed->where()->children().size(), 2U);
  EXPECT_EQ(parsed->where()->children()[0].operation(), SqlOperator::kIsNotNull);
  EXPECT_EQ(parsed->where()->children()[1].operation(), SqlOperator::kNotIn);
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
