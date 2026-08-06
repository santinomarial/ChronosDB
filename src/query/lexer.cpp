#include "chronos/query/lexer.hpp"

#include "chronos/common/status.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

struct KeywordEntry {
  std::string_view text;
  SqlKeyword keyword;
};

constexpr std::array kKeywords{
    KeywordEntry{"all", SqlKeyword::kAll},
    KeywordEntry{"allowed", SqlKeyword::kAllowed},
    KeywordEntry{"analyze", SqlKeyword::kAnalyze},
    KeywordEntry{"and", SqlKeyword::kAnd},
    KeywordEntry{"as", SqlKeyword::kAs},
    KeywordEntry{"asc", SqlKeyword::kAsc},
    KeywordEntry{"asof", SqlKeyword::kAsof},
    KeywordEntry{"between", SqlKeyword::kBetween},
    KeywordEntry{"binary", SqlKeyword::kBinary},
    KeywordEntry{"bool", SqlKeyword::kBool},
    KeywordEntry{"by", SqlKeyword::kBy},
    KeywordEntry{"cast", SqlKeyword::kCast},
    KeywordEntry{"coalesce", SqlKeyword::kCoalesce},
    KeywordEntry{"create", SqlKeyword::kCreate},
    KeywordEntry{"date", SqlKeyword::kDate},
    KeywordEntry{"decimal", SqlKeyword::kDecimal},
    KeywordEntry{"dedup", SqlKeyword::kDedup},
    KeywordEntry{"desc", SqlKeyword::kDesc},
    KeywordEntry{"event", SqlKeyword::kEvent},
    KeywordEntry{"explain", SqlKeyword::kExplain},
    KeywordEntry{"false", SqlKeyword::kFalse},
    KeywordEntry{"first", SqlKeyword::kFirst},
    KeywordEntry{"float32", SqlKeyword::kFloat32},
    KeywordEntry{"float64", SqlKeyword::kFloat64},
    KeywordEntry{"for", SqlKeyword::kFor},
    KeywordEntry{"from", SqlKeyword::kFrom},
    KeywordEntry{"group", SqlKeyword::kGroup},
    KeywordEntry{"history", SqlKeyword::kHistory},
    KeywordEntry{"in", SqlKeyword::kIn},
    KeywordEntry{"insert", SqlKeyword::kInsert},
    KeywordEntry{"int16", SqlKeyword::kInt16},
    KeywordEntry{"int32", SqlKeyword::kInt32},
    KeywordEntry{"int64", SqlKeyword::kInt64},
    KeywordEntry{"int8", SqlKeyword::kInt8},
    KeywordEntry{"interval", SqlKeyword::kInterval},
    KeywordEntry{"into", SqlKeyword::kInto},
    KeywordEntry{"is", SqlKeyword::kIs},
    KeywordEntry{"join", SqlKeyword::kJoin},
    KeywordEntry{"key", SqlKeyword::kKey},
    KeywordEntry{"last", SqlKeyword::kLast},
    KeywordEntry{"lateness", SqlKeyword::kLateness},
    KeywordEntry{"latest", SqlKeyword::kLatest},
    KeywordEntry{"left", SqlKeyword::kLeft},
    KeywordEntry{"limit", SqlKeyword::kLimit},
    KeywordEntry{"not", SqlKeyword::kNot},
    KeywordEntry{"null", SqlKeyword::kNull},
    KeywordEntry{"nulls", SqlKeyword::kNulls},
    KeywordEntry{"of", SqlKeyword::kOf},
    KeywordEntry{"on", SqlKeyword::kOn},
    KeywordEntry{"or", SqlKeyword::kOr},
    KeywordEntry{"order", SqlKeyword::kOrder},
    KeywordEntry{"partition", SqlKeyword::kPartition},
    KeywordEntry{"retention", SqlKeyword::kRetention},
    KeywordEntry{"select", SqlKeyword::kSelect},
    KeywordEntry{"shard", SqlKeyword::kShard},
    KeywordEntry{"string", SqlKeyword::kString},
    KeywordEntry{"subscribe", SqlKeyword::kSubscribe},
    KeywordEntry{"symbol", SqlKeyword::kSymbol},
    KeywordEntry{"system", SqlKeyword::kSystem},
    KeywordEntry{"system_time", SqlKeyword::kSystemTime},
    KeywordEntry{"table", SqlKeyword::kTable},
    KeywordEntry{"time", SqlKeyword::kTime},
    KeywordEntry{"timestamp", SqlKeyword::kTimestamp},
    KeywordEntry{"timestamp_ns", SqlKeyword::kTimestampNs},
    KeywordEntry{"true", SqlKeyword::kTrue},
    KeywordEntry{"uint16", SqlKeyword::kUInt16},
    KeywordEntry{"uint32", SqlKeyword::kUInt32},
    KeywordEntry{"uint64", SqlKeyword::kUInt64},
    KeywordEntry{"uint8", SqlKeyword::kUInt8},
    KeywordEntry{"uuid", SqlKeyword::kUuid},
    KeywordEntry{"values", SqlKeyword::kValues},
    KeywordEntry{"where", SqlKeyword::kWhere},
};

static_assert(std::ranges::is_sorted(kKeywords, {}, &KeywordEntry::text));

[[nodiscard]] constexpr bool ascii_alpha(const char value) noexcept {
  return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z');
}

[[nodiscard]] constexpr bool ascii_digit(const char value) noexcept {
  return value >= '0' && value <= '9';
}

[[nodiscard]] constexpr bool identifier_start(const char value) noexcept {
  return ascii_alpha(value) || value == '_';
}

[[nodiscard]] constexpr bool identifier_continue(const char value) noexcept {
  return identifier_start(value) || ascii_digit(value);
}

[[nodiscard]] constexpr char ascii_lower(const char value) noexcept {
  return value >= 'A' && value <= 'Z' ? static_cast<char>(value + ('a' - 'A')) : value;
}

[[nodiscard]] constexpr int hex_value(const char value) noexcept {
  if (value >= '0' && value <= '9') {
    return value - '0';
  }
  if (value >= 'a' && value <= 'f') {
    return value - 'a' + 10;
  }
  if (value >= 'A' && value <= 'F') {
    return value - 'A' + 10;
  }
  return -1;
}

[[nodiscard]] SqlKeyword keyword_for(const std::string_view text) noexcept {
  const KeywordEntry* const found =
      std::ranges::lower_bound(kKeywords, text, {}, &KeywordEntry::text);
  return found != kKeywords.end() && found->text == text ? found->keyword : SqlKeyword::kNone;
}

template <typename Value>
[[nodiscard]] Value* optional_pointer(std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
}

} // namespace

namespace detail {

class SqlLexer {
public:
  SqlLexer(const std::string_view input, const SqlLexerLimits limits) noexcept
      : input_(input), limits_(limits) {}

  [[nodiscard]] SqlResult<SqlTokenStream> run() {
    if (input_.size() > limits_.maximum_input_bytes) {
      return failure(SqlDiagnosticCode::kResourceLimit, location_, 0U,
                     common::StatusCode::kResourceExhausted,
                     "SQL input exceeds the configured byte limit");
    }
    if (limits_.maximum_tokens == 0U || limits_.maximum_token_bytes == 0U) {
      return failure(SqlDiagnosticCode::kResourceLimit, location_, 0U,
                     common::StatusCode::kInvalidArgument, "SQL lexer limits must be nonzero");
    }
    try {
      while (offset_ < input_.size()) {
        if (skip_space_or_comment()) {
          if (diagnostic_.has_value()) {
            return diagnostic_failure();
          }
          continue;
        }
        const SourceLocation begin = location_;
        const char current = input_[offset_];
        if ((current == 'x' || current == 'X') && peek(1U) == '\'') {
          if (!binary_literal(begin)) {
            return diagnostic_failure();
          }
        } else if (identifier_start(current)) {
          identifier(begin);
        } else if (current == '"') {
          if (!quoted(begin, '"', SqlTokenKind::kQuotedIdentifier,
                      SqlDiagnosticCode::kUnterminatedQuotedIdentifier)) {
            return diagnostic_failure();
          }
        } else if (current == '\'') {
          if (!quoted(begin, '\'', SqlTokenKind::kString, SqlDiagnosticCode::kUnterminatedString)) {
            return diagnostic_failure();
          }
        } else if (ascii_digit(current)) {
          if (!number(begin)) {
            return diagnostic_failure();
          }
        } else if (!punctuation(begin)) {
          return failure(SqlDiagnosticCode::kInvalidByte, begin, 1U,
                         common::StatusCode::kInvalidArgument,
                         "SQL input contains an invalid byte");
        }
        if (tokens_.size() >= limits_.maximum_tokens) {
          return failure(SqlDiagnosticCode::kResourceLimit, begin, offset_ - begin.byte_offset,
                         common::StatusCode::kResourceExhausted,
                         "SQL token count exceeds the configured limit");
        }
      }
      tokens_.push_back(SqlToken{SqlTokenKind::kEnd,
                                 SqlKeyword::kNone,
                                 {},
                                 SourceSpan{.begin = location_, .byte_length = 0U}});
      return SqlTokenStream{std::move(tokens_)};
    } catch (const std::bad_alloc&) {
      return failure(SqlDiagnosticCode::kResourceLimit, location_, 0U,
                     common::StatusCode::kResourceExhausted, "SQL tokenization allocation failed");
    } catch (const std::length_error&) {
      return failure(SqlDiagnosticCode::kResourceLimit, location_, 0U,
                     common::StatusCode::kResourceExhausted,
                     "SQL tokenization exceeds container limits");
    }
  }

private:
  [[nodiscard]] SqlResult<SqlTokenStream> diagnostic_failure() {
    SqlDiagnostic* value = optional_pointer(diagnostic_);
    if (value == nullptr) {
      return failure(SqlDiagnosticCode::kInvalidByte, location_, 0U, common::StatusCode::kInternal,
                     "SQL lexer lost its diagnostic");
    }
    return std::unexpected(std::move(*value));
  }
  [[nodiscard]] char peek(const std::size_t distance) const noexcept {
    return offset_ + distance < input_.size() ? input_[offset_ + distance] : '\0';
  }

  void advance() noexcept {
    if (input_[offset_] == '\n') {
      ++location_.line;
      location_.column = 1U;
    } else {
      ++location_.column;
    }
    ++offset_;
    location_.byte_offset = offset_;
  }

  [[nodiscard]] bool skip_space_or_comment() {
    const char current = input_[offset_];
    if (current == ' ' || current == '\t' || current == '\r' || current == '\n') {
      advance();
      return true;
    }
    if (current == '-' && peek(1U) == '-') {
      advance();
      advance();
      while (offset_ < input_.size() && input_[offset_] != '\n') {
        advance();
      }
      return true;
    }
    if (current != '/' || peek(1U) != '*') {
      return false;
    }
    const SourceLocation begin = location_;
    advance();
    advance();
    while (offset_ < input_.size()) {
      if (input_[offset_] == '*' && peek(1U) == '/') {
        advance();
        advance();
        return true;
      }
      advance();
    }
    diagnostic_.emplace(
        SqlDiagnosticCode::kUnterminatedComment,
        SourceSpan{.begin = begin, .byte_length = offset_ - begin.byte_offset},
        common::Status{common::StatusCode::kInvalidArgument, "SQL block comment is unterminated"});
    return true;
  }

  void identifier(const SourceLocation begin) {
    std::string text;
    while (offset_ < input_.size() && identifier_continue(input_[offset_])) {
      text.push_back(ascii_lower(input_[offset_]));
      advance();
    }
    const SqlKeyword keyword = keyword_for(text);
    push(keyword == SqlKeyword::kNone ? SqlTokenKind::kIdentifier : SqlTokenKind::kKeyword, keyword,
         std::move(text), begin);
  }

  [[nodiscard]] bool quoted(const SourceLocation begin, const char delimiter,
                            const SqlTokenKind kind, const SqlDiagnosticCode error) {
    advance();
    std::string text;
    while (offset_ < input_.size()) {
      if (input_[offset_] == delimiter) {
        advance();
        if (offset_ < input_.size() && input_[offset_] == delimiter) {
          text.push_back(delimiter);
          advance();
          continue;
        }
        push(kind, SqlKeyword::kNone, std::move(text), begin);
        return true;
      }
      if (text.size() >= limits_.maximum_token_bytes) {
        diagnostic_.emplace(SqlDiagnosticCode::kResourceLimit,
                            SourceSpan{.begin = begin, .byte_length = offset_ - begin.byte_offset},
                            common::Status{common::StatusCode::kResourceExhausted,
                                           "SQL token exceeds the configured byte limit"});
        return false;
      }
      text.push_back(input_[offset_]);
      advance();
    }
    diagnostic_.emplace(
        error, SourceSpan{.begin = begin, .byte_length = offset_ - begin.byte_offset},
        common::Status{common::StatusCode::kInvalidArgument,
                       kind == SqlTokenKind::kString ? "SQL string literal is unterminated"
                                                     : "SQL quoted identifier is unterminated"});
    return false;
  }

  [[nodiscard]] bool binary_literal(const SourceLocation begin) {
    advance();
    advance();
    std::string bytes;
    int high = -1;
    while (offset_ < input_.size() && input_[offset_] != '\'') {
      const int value = hex_value(input_[offset_]);
      if (value < 0) {
        diagnostic_.emplace(
            SqlDiagnosticCode::kInvalidBinaryLiteral,
            SourceSpan{.begin = begin, .byte_length = offset_ - begin.byte_offset + 1U},
            common::Status{common::StatusCode::kInvalidArgument,
                           "SQL binary literal contains a non-hex digit"});
        return false;
      }
      if (high < 0) {
        high = value;
      } else {
        if (bytes.size() >= limits_.maximum_token_bytes) {
          diagnostic_.emplace(
              SqlDiagnosticCode::kResourceLimit,
              SourceSpan{.begin = begin, .byte_length = offset_ - begin.byte_offset},
              common::Status{common::StatusCode::kResourceExhausted,
                             "SQL binary literal exceeds the configured byte limit"});
          return false;
        }
        bytes.push_back(static_cast<char>((high << 4) | value));
        high = -1;
      }
      advance();
    }
    if (offset_ == input_.size() || high >= 0) {
      diagnostic_.emplace(SqlDiagnosticCode::kInvalidBinaryLiteral,
                          SourceSpan{.begin = begin, .byte_length = offset_ - begin.byte_offset},
                          common::Status{common::StatusCode::kInvalidArgument,
                                         offset_ == input_.size()
                                             ? "SQL binary literal is unterminated"
                                             : "SQL binary literal has an odd hex length"});
      return false;
    }
    advance();
    push(SqlTokenKind::kBinary, SqlKeyword::kNone, std::move(bytes), begin);
    return true;
  }

  [[nodiscard]] bool number(const SourceLocation begin) {
    while (ascii_digit(peek(0U))) {
      advance();
    }
    bool floating = false;
    if (peek(0U) == '.') {
      if (!ascii_digit(peek(1U))) {
        push(SqlTokenKind::kInteger, SqlKeyword::kNone,
             std::string{input_.substr(begin.byte_offset, offset_ - begin.byte_offset)}, begin);
        return true;
      }
      floating = true;
      advance();
      while (ascii_digit(peek(0U))) {
        advance();
      }
    }
    if (peek(0U) == 'e' || peek(0U) == 'E') {
      floating = true;
      advance();
      if (peek(0U) == '+' || peek(0U) == '-') {
        advance();
      }
      if (!ascii_digit(peek(0U))) {
        diagnostic_.emplace(SqlDiagnosticCode::kInvalidNumber,
                            SourceSpan{.begin = begin, .byte_length = offset_ - begin.byte_offset},
                            common::Status{common::StatusCode::kInvalidArgument,
                                           "SQL floating literal exponent has no digits"});
        return false;
      }
      while (ascii_digit(peek(0U))) {
        advance();
      }
    }
    const std::size_t length = offset_ - begin.byte_offset;
    if (length > limits_.maximum_token_bytes) {
      diagnostic_.emplace(SqlDiagnosticCode::kResourceLimit,
                          SourceSpan{.begin = begin, .byte_length = length},
                          common::Status{common::StatusCode::kResourceExhausted,
                                         "SQL numeric literal exceeds the configured byte limit"});
      return false;
    }
    push(floating ? SqlTokenKind::kFloat : SqlTokenKind::kInteger, SqlKeyword::kNone,
         std::string{input_.substr(begin.byte_offset, length)}, begin);
    return true;
  }

  [[nodiscard]] bool punctuation(const SourceLocation begin) {
    const char current = input_[offset_];
    SqlTokenKind kind{};
    std::size_t length = 1U;
    switch (current) {
    case '(':
      kind = SqlTokenKind::kLeftParen;
      break;
    case ')':
      kind = SqlTokenKind::kRightParen;
      break;
    case ',':
      kind = SqlTokenKind::kComma;
      break;
    case '.':
      kind = SqlTokenKind::kDot;
      break;
    case ';':
      kind = SqlTokenKind::kSemicolon;
      break;
    case '+':
      kind = SqlTokenKind::kPlus;
      break;
    case '-':
      kind = SqlTokenKind::kMinus;
      break;
    case '*':
      kind = SqlTokenKind::kStar;
      break;
    case '/':
      kind = SqlTokenKind::kSlash;
      break;
    case '%':
      kind = SqlTokenKind::kPercent;
      break;
    case '=':
      kind = SqlTokenKind::kEqual;
      break;
    case '<':
      if (peek(1U) == '=' || peek(1U) == '>') {
        kind = peek(1U) == '=' ? SqlTokenKind::kLessEqual : SqlTokenKind::kNotEqual;
        length = 2U;
      } else {
        kind = SqlTokenKind::kLess;
      }
      break;
    case '>':
      if (peek(1U) == '=') {
        kind = SqlTokenKind::kGreaterEqual;
        length = 2U;
      } else {
        kind = SqlTokenKind::kGreater;
      }
      break;
    case '!':
      if (peek(1U) != '=') {
        return false;
      }
      kind = SqlTokenKind::kNotEqual;
      length = 2U;
      break;
    default:
      return false;
    }
    for (std::size_t index = 0U; index < length; ++index) {
      advance();
    }
    push(kind, SqlKeyword::kNone, std::string{input_.substr(begin.byte_offset, length)}, begin);
    return true;
  }

  void push(const SqlTokenKind kind, const SqlKeyword keyword, std::string text,
            const SourceLocation begin) {
    tokens_.push_back(
        SqlToken{kind, keyword, std::move(text),
                 SourceSpan{.begin = begin, .byte_length = offset_ - begin.byte_offset}});
  }

  [[nodiscard]] SqlResult<SqlTokenStream> static failure(const SqlDiagnosticCode code,
                                                         const SourceLocation begin,
                                                         const std::size_t length,
                                                         const common::StatusCode status_code,
                                                         std::string message) {
    return std::unexpected(SqlDiagnostic{code, SourceSpan{.begin = begin, .byte_length = length},
                                         common::Status{status_code, std::move(message)}});
  }

  std::string_view input_;
  SqlLexerLimits limits_;
  std::size_t offset_{};
  SourceLocation location_{};
  std::vector<SqlToken> tokens_;
  std::optional<SqlDiagnostic> diagnostic_;
};

} // namespace detail

std::string_view sql_keyword_name(const SqlKeyword keyword) noexcept {
  for (const KeywordEntry& entry : kKeywords) {
    if (entry.keyword == keyword) {
      return entry.text;
    }
  }
  return {};
}

SqlToken::SqlToken(const SqlTokenKind kind, const SqlKeyword keyword, std::string text,
                   const SourceSpan span) noexcept
    : kind_(kind), keyword_(keyword), text_(std::move(text)), span_(span) {}

SqlTokenKind SqlToken::kind() const noexcept {
  return kind_;
}

SqlKeyword SqlToken::keyword() const noexcept {
  return keyword_;
}

const std::string& SqlToken::text() const noexcept {
  return text_;
}

const SourceSpan& SqlToken::span() const noexcept {
  return span_;
}

SqlTokenStream::SqlTokenStream(std::vector<SqlToken> tokens) noexcept
    : tokens_(std::move(tokens)) {}

std::span<const SqlToken> SqlTokenStream::tokens() const noexcept {
  return tokens_;
}

SqlResult<SqlTokenStream> tokenize_sql_v1(const std::string_view sql, const SqlLexerLimits limits) {
  return detail::SqlLexer{sql, limits}.run();
}

} // namespace chronos::query
