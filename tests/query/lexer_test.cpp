#include "chronos/query/lexer.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace chronos::query {
namespace {

TEST(SqlLexerTest, TokenizesCanonicalSelectAndPreservesExactLocations) {
  constexpr std::string_view sql =
      "-- heading\nSELECT T.\"Mixed\"\"Name\", 'can''t', X'00fF', 12, 3.5e-2 "
      "FROM trades AS t WHERE t.ts >= TIMESTAMP '2026-08-06 12:00:00.123Z';";
  const SqlResult<SqlTokenStream> result = tokenize_sql_v1(sql);
  ASSERT_TRUE(result.has_value()) << result.error().status().to_string();
  const std::span<const SqlToken> tokens = result->tokens();
  ASSERT_EQ(tokens.size(), 25U);
  EXPECT_EQ(tokens[0].keyword(), SqlKeyword::kSelect);
  EXPECT_EQ(tokens[0].text(), "select");
  EXPECT_EQ(tokens[0].span().begin, (SourceLocation{.byte_offset = 11U, .line = 2U, .column = 1U}));
  EXPECT_EQ(tokens[1].kind(), SqlTokenKind::kIdentifier);
  EXPECT_EQ(tokens[1].text(), "t");
  EXPECT_EQ(tokens[3].kind(), SqlTokenKind::kQuotedIdentifier);
  EXPECT_EQ(tokens[3].text(), "Mixed\"Name");
  EXPECT_EQ(tokens[5].kind(), SqlTokenKind::kString);
  EXPECT_EQ(tokens[5].text(), "can't");
  EXPECT_EQ(tokens[7].kind(), SqlTokenKind::kBinary);
  ASSERT_EQ(tokens[7].text().size(), 2U);
  EXPECT_EQ(static_cast<unsigned char>(tokens[7].text()[0]), 0U);
  EXPECT_EQ(static_cast<unsigned char>(tokens[7].text()[1]), 0xffU);
  EXPECT_EQ(tokens[9].kind(), SqlTokenKind::kInteger);
  EXPECT_EQ(tokens[11].kind(), SqlTokenKind::kFloat);
  EXPECT_EQ(tokens.back().kind(), SqlTokenKind::kEnd);
  EXPECT_EQ(tokens.back().span().begin.byte_offset, sql.size());
}

TEST(SqlLexerTest, RecognizesCommentsOperatorsKeywordsAndIdentifierRules) {
  constexpr std::string_view sql =
      "/* not /* nested */ _Name <> != <= >= + - * / % = < > AND OR NOT NULL TRUE FALSE";
  const SqlResult<SqlTokenStream> result = tokenize_sql_v1(sql);
  ASSERT_TRUE(result.has_value()) << result.error().status().to_string();
  const auto tokens = result->tokens();
  ASSERT_EQ(tokens.size(), 20U);
  EXPECT_EQ(tokens.front().text(), "_name");
  EXPECT_EQ(tokens.front().kind(), SqlTokenKind::kIdentifier);
  EXPECT_EQ(tokens[1].kind(), SqlTokenKind::kNotEqual);
  EXPECT_EQ(tokens[2].kind(), SqlTokenKind::kNotEqual);
  EXPECT_EQ(tokens[3].kind(), SqlTokenKind::kLessEqual);
  EXPECT_EQ(tokens[4].kind(), SqlTokenKind::kGreaterEqual);
  for (std::size_t index = 13U; index < 19U; ++index) {
    EXPECT_EQ(tokens[index].kind(), SqlTokenKind::kKeyword);
  }
  EXPECT_EQ(sql_keyword_name(SqlKeyword::kSystemTime), "system_time");
  EXPECT_TRUE(sql_keyword_name(SqlKeyword::kNone).empty());
}

TEST(SqlLexerTest, ClassifiesEveryHostileLexicalFailureAtItsSource) {
  struct Case {
    std::string_view sql;
    SqlDiagnosticCode code;
    std::size_t offset;
  };
  constexpr std::array cases{
      Case{"!", SqlDiagnosticCode::kInvalidByte, 0U},
      Case{"\"unterminated", SqlDiagnosticCode::kUnterminatedQuotedIdentifier, 0U},
      Case{"'unterminated", SqlDiagnosticCode::kUnterminatedString, 0U},
      Case{"/* unterminated", SqlDiagnosticCode::kUnterminatedComment, 0U},
      Case{"X'0'", SqlDiagnosticCode::kInvalidBinaryLiteral, 0U},
      Case{"X'0g'", SqlDiagnosticCode::kInvalidBinaryLiteral, 0U},
      Case{"1e+", SqlDiagnosticCode::kInvalidNumber, 0U},
      Case{"SELECT\n@", SqlDiagnosticCode::kInvalidByte, 7U},
  };
  for (const Case& value : cases) {
    SCOPED_TRACE(value.sql);
    const SqlResult<SqlTokenStream> result = tokenize_sql_v1(value.sql);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), value.code);
    EXPECT_EQ(result.error().span().begin.byte_offset, value.offset);
    EXPECT_EQ(result.error().status().code(), common::StatusCode::kInvalidArgument);
  }
}

TEST(SqlLexerTest, EnforcesInputTokenAndTokenByteLimitsBeforeUnboundedGrowth) {
  EXPECT_EQ(tokenize_sql_v1("select", {.maximum_input_bytes = 5U}).error().code(),
            SqlDiagnosticCode::kResourceLimit);
  EXPECT_EQ(tokenize_sql_v1("a b", {.maximum_tokens = 2U}).error().code(),
            SqlDiagnosticCode::kResourceLimit);
  EXPECT_EQ(tokenize_sql_v1("'abc'", {.maximum_token_bytes = 2U}).error().code(),
            SqlDiagnosticCode::kResourceLimit);
  EXPECT_EQ(tokenize_sql_v1("", {.maximum_tokens = 0U}).error().code(),
            SqlDiagnosticCode::kResourceLimit);
}

TEST(SqlLexerPropertyTest, DeterministicGeneratedByteStringsNeverLoseTerminalState) {
  std::mt19937_64 random{0x514c5f4c45584552ULL};
  for (std::size_t iteration = 0U; iteration < 5'000U; ++iteration) {
    std::string sql;
    sql.resize(static_cast<std::size_t>(random() % 96U));
    for (char& value : sql) {
      value = static_cast<char>(random() & 0xffU);
    }
    const SqlResult<SqlTokenStream> first = tokenize_sql_v1(sql);
    const SqlResult<SqlTokenStream> second = tokenize_sql_v1(sql);
    ASSERT_EQ(first.has_value(), second.has_value());
    if (!first.has_value()) {
      EXPECT_EQ(first.error().code(), second.error().code());
      EXPECT_EQ(first.error().span(), second.error().span());
      continue;
    }
    ASSERT_EQ(first->tokens().size(), second->tokens().size());
    ASSERT_FALSE(first->tokens().empty());
    EXPECT_EQ(first->tokens().back().kind(), SqlTokenKind::kEnd);
    for (std::size_t index = 0U; index < first->tokens().size(); ++index) {
      EXPECT_EQ(first->tokens()[index].kind(), second->tokens()[index].kind());
      EXPECT_EQ(first->tokens()[index].keyword(), second->tokens()[index].keyword());
      EXPECT_EQ(first->tokens()[index].text(), second->tokens()[index].text());
      EXPECT_EQ(first->tokens()[index].span(), second->tokens()[index].span());
    }
  }
}

} // namespace
} // namespace chronos::query
