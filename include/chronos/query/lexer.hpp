#ifndef CHRONOS_QUERY_LEXER_HPP_
#define CHRONOS_QUERY_LEXER_HPP_

#include "chronos/query/diagnostic.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace chronos::query {

class SqlTokenStream;
namespace detail {
class SqlLexer;
}

enum class SqlTokenKind : std::uint16_t {
  kEnd = 0U,
  kIdentifier,
  kQuotedIdentifier,
  kString,
  kBinary,
  kInteger,
  kFloat,
  kLeftParen,
  kRightParen,
  kComma,
  kDot,
  kSemicolon,
  kPlus,
  kMinus,
  kStar,
  kSlash,
  kPercent,
  kEqual,
  kNotEqual,
  kLess,
  kLessEqual,
  kGreater,
  kGreaterEqual,
  kKeyword,
};

enum class SqlKeyword : std::uint16_t {
  kNone = 0U,
  kAll,
  kAllowed,
  kAnalyze,
  kAnd,
  kAs,
  kAsc,
  kAsof,
  kBetween,
  kBinary,
  kBool,
  kBy,
  kCast,
  kCoalesce,
  kCreate,
  kDate,
  kDecimal,
  kDedup,
  kDesc,
  kEvent,
  kExplain,
  kFalse,
  kFloat32,
  kFloat64,
  kFor,
  kFrom,
  kGroup,
  kHistory,
  kIn,
  kInsert,
  kInt8,
  kInt16,
  kInt32,
  kInt64,
  kInterval,
  kInto,
  kIs,
  kJoin,
  kKey,
  kLatest,
  kLeft,
  kLimit,
  kNot,
  kNull,
  kNulls,
  kOf,
  kOn,
  kOr,
  kOrder,
  kPartition,
  kRetention,
  kSelect,
  kShard,
  kString,
  kSubscribe,
  kSymbol,
  kSystem,
  kSystemTime,
  kTable,
  kTime,
  kTimestamp,
  kTimestampNs,
  kTrue,
  kUInt8,
  kUInt16,
  kUInt32,
  kUInt64,
  kUuid,
  kValues,
  kWhere,
  kFirst,
  kLast,
};

[[nodiscard]] std::string_view sql_keyword_name(SqlKeyword keyword) noexcept;

class SqlToken {
public:
  [[nodiscard]] SqlTokenKind kind() const noexcept;
  [[nodiscard]] SqlKeyword keyword() const noexcept;
  [[nodiscard]] const std::string& text() const noexcept;
  [[nodiscard]] const SourceSpan& span() const noexcept;

private:
  SqlToken(SqlTokenKind kind, SqlKeyword keyword, std::string text, SourceSpan span) noexcept;

  SqlTokenKind kind_;
  SqlKeyword keyword_;
  std::string text_;
  SourceSpan span_;

  friend class detail::SqlLexer;
};

struct SqlLexerLimits {
  std::size_t maximum_input_bytes{1U << 20U};
  std::size_t maximum_tokens{262'144U};
  std::size_t maximum_token_bytes{1U << 20U};
};

// Owns normalized token text and exact source spans. The terminal kEnd token is always present.
class SqlTokenStream {
public:
  SqlTokenStream() = delete;
  SqlTokenStream(const SqlTokenStream&) = delete;
  SqlTokenStream& operator=(const SqlTokenStream&) = delete;
  SqlTokenStream(SqlTokenStream&&) noexcept = default;
  SqlTokenStream& operator=(SqlTokenStream&&) noexcept = default;

  [[nodiscard]] std::span<const SqlToken> tokens() const noexcept;

private:
  explicit SqlTokenStream(std::vector<SqlToken> tokens) noexcept;

  std::vector<SqlToken> tokens_;

  friend class detail::SqlLexer;
};

[[nodiscard]] SqlResult<SqlTokenStream> tokenize_sql_v1(std::string_view sql,
                                                        SqlLexerLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_LEXER_HPP_
