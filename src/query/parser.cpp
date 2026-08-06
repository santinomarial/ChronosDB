#include "chronos/query/parser.hpp"

#include "chronos/common/status.hpp"

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] SourceSpan span_between(const SourceLocation begin, const SourceSpan& end) noexcept {
  return {.begin = begin,
          .byte_length = end.begin.byte_offset + end.byte_length - begin.byte_offset};
}

struct ParseFailure {
  SqlDiagnostic diagnostic;
};

} // namespace

namespace detail {

class SqlParser {
public:
  SqlParser(SqlTokenStream tokens, const SqlParserLimits limits) noexcept
      : tokens_(std::move(tokens)), limits_(limits) {}

  [[nodiscard]] SqlResult<ParsedSqlSelect> run() {
    if (limits_.maximum_ast_nodes == 0U || limits_.maximum_expression_depth == 0U ||
        limits_.maximum_list_elements == 0U) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, current().span(),
                                        common::StatusCode::kInvalidArgument,
                                        "SQL parser limits must be nonzero"));
    }
    try {
      const SourceLocation begin = current().span().begin;
      SqlSelectMode mode = SqlSelectMode::kSelect;
      if (match(SqlKeyword::kExplain)) {
        mode =
            match(SqlKeyword::kAnalyze) ? SqlSelectMode::kExplainAnalyze : SqlSelectMode::kExplain;
      } else if (match(SqlKeyword::kSubscribe)) {
        mode = SqlSelectMode::kSubscribe;
      }
      require(SqlKeyword::kSelect, "Expected SELECT statement");

      std::vector<SqlSelectItem> items;
      do {
        check_list(items.size(), current().span(), "SELECT item count exceeds the limit");
        items.push_back(parse_select_item());
      } while (match(SqlTokenKind::kComma));
      require(SqlKeyword::kFrom, "SELECT requires FROM after its output list");
      SqlSource source = parse_source();

      std::optional<std::string> system_time;
      if (match(SqlKeyword::kFor)) {
        require(SqlKeyword::kSystemTime, "FOR must be followed by SYSTEM_TIME");
        require(SqlKeyword::kAs, "FOR SYSTEM_TIME must be followed by AS OF");
        require(SqlKeyword::kOf, "FOR SYSTEM_TIME AS must be followed by OF");
        require(SqlKeyword::kTimestamp, "FOR SYSTEM_TIME AS OF requires TIMESTAMP literal");
        const SqlToken& literal =
            require(SqlTokenKind::kString, "TIMESTAMP must be followed by a quoted UTC value");
        system_time = literal.text();
      }

      std::optional<SqlLatestBy> latest;
      if (match(SqlKeyword::kLatest)) {
        require(SqlKeyword::kBy, "LATEST must be followed by BY");
        require(SqlTokenKind::kLeftParen, "LATEST BY requires a key list");
        std::vector<SqlIdentifier> keys;
        do {
          check_list(keys.size(), current().span(), "LATEST BY key count exceeds the limit");
          keys.push_back(parse_identifier("LATEST BY requires identifier keys"));
        } while (match(SqlTokenKind::kComma));
        require(SqlTokenKind::kRightParen, "LATEST BY key list is missing ')'");
        require(SqlKeyword::kOn, "LATEST BY requires ON timestamp expression");
        latest.emplace(SqlLatestBy{.keys = std::move(keys), .timestamp = parse_expression()});
      }

      std::vector<SqlAsofJoin> joins;
      while (match(SqlKeyword::kAsof)) {
        check_list(joins.size(), previous().span(), "ASOF JOIN count exceeds the limit");
        const bool left = match(SqlKeyword::kLeft);
        require(SqlKeyword::kJoin, "ASOF must be followed by JOIN");
        SqlSource right = parse_source();
        require(SqlKeyword::kOn, "ASOF JOIN requires ON condition");
        joins.push_back(
            SqlAsofJoin{.left = left, .source = std::move(right), .condition = parse_expression()});
        account(previous().span());
      }

      std::vector<SqlExpression> where;
      if (match(SqlKeyword::kWhere)) {
        where.push_back(parse_expression());
      }
      std::vector<SqlExpression> group_by;
      if (match(SqlKeyword::kGroup)) {
        require(SqlKeyword::kBy, "GROUP must be followed by BY");
        do {
          check_list(group_by.size(), current().span(), "GROUP BY item count exceeds the limit");
          group_by.push_back(parse_expression());
        } while (match(SqlTokenKind::kComma));
      }
      std::vector<SqlOrderItem> order_by;
      if (match(SqlKeyword::kOrder)) {
        require(SqlKeyword::kBy, "ORDER must be followed by BY");
        do {
          check_list(order_by.size(), current().span(), "ORDER BY item count exceeds the limit");
          SqlExpression expression = parse_expression();
          const SqlOrderDirection direction =
              match(SqlKeyword::kDesc) ? SqlOrderDirection::kDescending
                                       : (match(SqlKeyword::kAsc) ? SqlOrderDirection::kAscending
                                                                  : SqlOrderDirection::kAscending);
          SqlNullOrder null_order = SqlNullOrder::kDefault;
          if (match(SqlKeyword::kNulls)) {
            if (match(SqlKeyword::kFirst)) {
              null_order = SqlNullOrder::kFirst;
            } else if (match(SqlKeyword::kLast)) {
              null_order = SqlNullOrder::kLast;
            } else {
              fail(SqlDiagnosticCode::kUnexpectedToken, current().span(),
                   "NULLS must be followed by FIRST or LAST");
            }
          }
          order_by.push_back(SqlOrderItem{.expression = std::move(expression),
                                          .direction = direction,
                                          .null_order = null_order});
        } while (match(SqlTokenKind::kComma));
      }

      std::optional<std::uint64_t> limit;
      if (match(SqlKeyword::kLimit)) {
        const SqlToken& value =
            require(SqlTokenKind::kInteger, "LIMIT requires an unsigned integer literal");
        std::uint64_t parsed = 0U;
        const auto conversion =
            std::from_chars(value.text().data(), value.text().data() + value.text().size(), parsed);
        if (conversion.ec != std::errc{} ||
            conversion.ptr != value.text().data() + value.text().size()) {
          fail(SqlDiagnosticCode::kInvalidNumber, value.span(), "LIMIT literal exceeds uint64");
        }
        limit = parsed;
      }

      if (match(SqlTokenKind::kSemicolon) && current().kind() == SqlTokenKind::kSemicolon) {
        fail(SqlDiagnosticCode::kUnexpectedToken, current().span(),
             "SQL statement has more than one terminator");
      }
      const SqlToken& end =
          require(SqlTokenKind::kEnd, "Unexpected token after complete SELECT statement");
      account(end.span());
      return ParsedSqlSelect{mode,
                             std::move(items),
                             std::move(source),
                             std::move(system_time),
                             std::move(latest),
                             std::move(joins),
                             std::move(where),
                             std::move(group_by),
                             std::move(order_by),
                             limit,
                             span_between(begin, end.span())};
    } catch (ParseFailure& failure) {
      return std::unexpected(std::move(failure.diagnostic));
    } catch (const std::bad_alloc&) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, current().span(),
                                        common::StatusCode::kResourceExhausted,
                                        "SQL parsing allocation failed"));
    } catch (const std::length_error&) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, current().span(),
                                        common::StatusCode::kResourceExhausted,
                                        "SQL AST exceeds container limits"));
    }
  }

private:
  class ExpressionRecursionGuard {
  public:
    explicit ExpressionRecursionGuard(SqlParser& parser) noexcept : parser_(parser) {
      ++parser_.expression_recursion_;
    }
    ~ExpressionRecursionGuard() {
      --parser_.expression_recursion_;
    }
    ExpressionRecursionGuard(const ExpressionRecursionGuard&) = delete;
    ExpressionRecursionGuard& operator=(const ExpressionRecursionGuard&) = delete;

  private:
    SqlParser& parser_;
  };

  [[nodiscard]] const SqlToken& current() const noexcept {
    return tokens_.tokens()[position_];
  }
  [[nodiscard]] const SqlToken& previous() const noexcept {
    return tokens_.tokens()[position_ == 0U ? 0U : position_ - 1U];
  }

  [[nodiscard]] bool check(const SqlTokenKind kind) const noexcept {
    return current().kind() == kind;
  }
  [[nodiscard]] bool check(const SqlKeyword keyword) const noexcept {
    return current().kind() == SqlTokenKind::kKeyword && current().keyword() == keyword;
  }
  [[nodiscard]] bool peek_keyword(const std::size_t distance,
                                  const SqlKeyword keyword) const noexcept {
    const std::size_t index = std::min(position_ + distance, tokens_.tokens().size() - 1U);
    return tokens_.tokens()[index].kind() == SqlTokenKind::kKeyword &&
           tokens_.tokens()[index].keyword() == keyword;
  }

  [[nodiscard]] bool contextual_identifier(const SqlToken& token) const noexcept {
    if (token.kind() != SqlTokenKind::kKeyword)
      return false;
    switch (token.keyword()) {
    case SqlKeyword::kBinary:
    case SqlKeyword::kBool:
    case SqlKeyword::kDate:
    case SqlKeyword::kDecimal:
    case SqlKeyword::kFloat32:
    case SqlKeyword::kFloat64:
    case SqlKeyword::kInt8:
    case SqlKeyword::kInt16:
    case SqlKeyword::kInt32:
    case SqlKeyword::kInt64:
    case SqlKeyword::kString:
    case SqlKeyword::kSymbol:
    case SqlKeyword::kTimestampNs:
    case SqlKeyword::kUInt8:
    case SqlKeyword::kUInt16:
    case SqlKeyword::kUInt32:
    case SqlKeyword::kUInt64:
    case SqlKeyword::kUuid:
      return true;
    default:
      return false;
    }
  }

  [[nodiscard]] bool match(const SqlTokenKind kind) noexcept {
    if (!check(kind))
      return false;
    ++position_;
    return true;
  }
  [[nodiscard]] bool match(const SqlKeyword keyword) noexcept {
    if (!check(keyword))
      return false;
    ++position_;
    return true;
  }

  const SqlToken& require(const SqlTokenKind kind, const std::string_view message) {
    if (!check(kind)) {
      fail(SqlDiagnosticCode::kUnexpectedToken, current().span(), message);
    }
    return tokens_.tokens()[position_++];
  }
  const SqlToken& require(const SqlKeyword keyword, const std::string_view message) {
    if (!check(keyword)) {
      fail(SqlDiagnosticCode::kUnexpectedToken, current().span(), message);
    }
    return tokens_.tokens()[position_++];
  }

  [[noreturn]] void fail(const SqlDiagnosticCode code, const SourceSpan span,
                         const std::string_view message) const {
    throw ParseFailure{diagnostic(code, span, common::StatusCode::kInvalidArgument, message)};
  }

  [[nodiscard]] SqlDiagnostic diagnostic(const SqlDiagnosticCode code, const SourceSpan span,
                                         const common::StatusCode status_code,
                                         const std::string_view message) const {
    return SqlDiagnostic{code, span, common::Status{status_code, std::string{message}}};
  }

  void account(const SourceSpan span) {
    if (ast_nodes_ >= limits_.maximum_ast_nodes) {
      fail(SqlDiagnosticCode::kResourceLimit, span, "SQL AST node count exceeds the limit");
    }
    ++ast_nodes_;
  }

  void check_list(const std::size_t size, const SourceSpan span,
                  const std::string_view message) const {
    if (size >= limits_.maximum_list_elements) {
      fail(SqlDiagnosticCode::kResourceLimit, span, message);
    }
  }

  [[nodiscard]] SqlIdentifier parse_identifier(const std::string_view message) {
    if (!check(SqlTokenKind::kIdentifier) && !check(SqlTokenKind::kQuotedIdentifier) &&
        !contextual_identifier(current())) {
      fail(SqlDiagnosticCode::kUnexpectedToken, current().span(), message);
    }
    const SqlToken& token = tokens_.tokens()[position_++];
    account(token.span());
    return SqlIdentifier{token.text(), token.kind() == SqlTokenKind::kQuotedIdentifier,
                         token.span()};
  }

  [[nodiscard]] SqlSource parse_source() {
    SqlIdentifier table = parse_identifier("FROM/JOIN requires a table identifier");
    std::optional<SqlIdentifier> alias;
    if (match(SqlKeyword::kAs)) {
      alias = parse_identifier("AS requires a table alias");
    }
    return {.table = std::move(table), .alias = std::move(alias)};
  }

  [[nodiscard]] SqlSelectItem parse_select_item() {
    const SourceLocation begin = current().span().begin;
    if (match(SqlTokenKind::kStar)) {
      account(previous().span());
      return SqlSelectItem{
          SqlSelectItemKind::kStar, {}, std::nullopt, std::nullopt, previous().span()};
    }
    if ((check(SqlTokenKind::kIdentifier) || check(SqlTokenKind::kQuotedIdentifier) ||
         contextual_identifier(current())) &&
        position_ + 2U < tokens_.tokens().size() &&
        tokens_.tokens()[position_ + 1U].kind() == SqlTokenKind::kDot &&
        tokens_.tokens()[position_ + 2U].kind() == SqlTokenKind::kStar) {
      SqlIdentifier qualifier = parse_identifier("Qualified star requires an identifier");
      require(SqlTokenKind::kDot, "Qualified star requires '.'");
      const SqlToken& star = require(SqlTokenKind::kStar, "Qualified star requires '*'");
      account(star.span());
      return SqlSelectItem{SqlSelectItemKind::kQualifiedStar,
                           {},
                           std::move(qualifier),
                           std::nullopt,
                           span_between(begin, star.span())};
    }
    std::vector<SqlExpression> expression;
    expression.push_back(parse_expression());
    std::optional<SqlIdentifier> alias;
    if (match(SqlKeyword::kAs)) {
      alias = parse_identifier("Select item AS requires an output alias");
    }
    const SourceSpan end = alias.has_value() ? alias->span() : expression.front().span();
    account(end);
    return SqlSelectItem{SqlSelectItemKind::kExpression, std::move(expression), std::nullopt,
                         std::move(alias), span_between(begin, end)};
  }

  [[nodiscard]] SqlExpression parse_expression() {
    if (expression_recursion_ >= limits_.maximum_expression_depth) {
      fail(SqlDiagnosticCode::kResourceLimit, current().span(),
           "SQL expression nesting exceeds the limit");
    }
    ExpressionRecursionGuard guard{*this};
    return parse_or();
  }

  [[nodiscard]] SqlExpression parse_or() {
    SqlExpression left = parse_and();
    while (match(SqlKeyword::kOr)) {
      left = make_operation(SqlExpressionKind::kBinary, SqlOperator::kOr, std::move(left),
                            parse_and());
    }
    return left;
  }

  [[nodiscard]] SqlExpression parse_and() {
    SqlExpression left = parse_not();
    while (match(SqlKeyword::kAnd)) {
      left = make_operation(SqlExpressionKind::kBinary, SqlOperator::kAnd, std::move(left),
                            parse_not());
    }
    return left;
  }

  [[nodiscard]] SqlExpression parse_not() {
    std::vector<SourceLocation> operators;
    while (match(SqlKeyword::kNot)) {
      if (operators.size() >= limits_.maximum_expression_depth - 1U) {
        fail(SqlDiagnosticCode::kResourceLimit, previous().span(),
             "SQL expression depth exceeds the limit");
      }
      operators.push_back(previous().span().begin);
    }
    SqlExpression expression = parse_comparison();
    for (auto iterator = operators.rbegin(); iterator != operators.rend(); ++iterator) {
      expression = make_unary(SqlOperator::kNot, *iterator, std::move(expression));
    }
    return expression;
  }

  [[nodiscard]] SqlExpression parse_comparison() {
    SqlExpression left = parse_additive();
    SqlOperator operation = SqlOperator::kNone;
    switch (current().kind()) {
    case SqlTokenKind::kEqual:
      operation = SqlOperator::kEqual;
      break;
    case SqlTokenKind::kNotEqual:
      operation = SqlOperator::kNotEqual;
      break;
    case SqlTokenKind::kLess:
      operation = SqlOperator::kLess;
      break;
    case SqlTokenKind::kLessEqual:
      operation = SqlOperator::kLessEqual;
      break;
    case SqlTokenKind::kGreater:
      operation = SqlOperator::kGreater;
      break;
    case SqlTokenKind::kGreaterEqual:
      operation = SqlOperator::kGreaterEqual;
      break;
    default:
      break;
    }
    if (operation != SqlOperator::kNone) {
      ++position_;
      return make_operation(SqlExpressionKind::kBinary, operation, std::move(left),
                            parse_additive());
    }
    if (match(SqlKeyword::kIs)) {
      const bool negated = match(SqlKeyword::kNot);
      const SqlToken& end = require(SqlKeyword::kNull, "IS supports only [NOT] NULL in SQL v1");
      const SourceLocation begin = left.span().begin;
      std::vector<SqlExpression> children;
      children.push_back(std::move(left));
      return make_expression(SqlExpressionKind::kIsNull, SqlLiteralKind::kNull,
                             negated ? SqlOperator::kIsNotNull : SqlOperator::kIsNull, {}, {},
                             std::move(children), std::nullopt, span_between(begin, end.span()));
    }
    bool negated = false;
    if (check(SqlKeyword::kNot) &&
        (peek_keyword(1U, SqlKeyword::kBetween) || peek_keyword(1U, SqlKeyword::kIn))) {
      ++position_;
      negated = true;
    }
    if (match(SqlKeyword::kBetween)) {
      const SourceLocation begin = left.span().begin;
      std::vector<SqlExpression> children;
      children.push_back(std::move(left));
      children.push_back(parse_additive());
      require(SqlKeyword::kAnd, "BETWEEN requires AND between its bounds");
      children.push_back(parse_additive());
      const SourceSpan span = span_between(begin, children.back().span());
      return make_expression(SqlExpressionKind::kBetween, SqlLiteralKind::kNull,
                             negated ? SqlOperator::kNotBetween : SqlOperator::kBetween, {}, {},
                             std::move(children), std::nullopt, span);
    }
    if (match(SqlKeyword::kIn)) {
      const SourceLocation begin = left.span().begin;
      require(SqlTokenKind::kLeftParen, "IN requires '('");
      std::vector<SqlExpression> children;
      children.push_back(std::move(left));
      if (check(SqlTokenKind::kRightParen)) {
        fail(SqlDiagnosticCode::kUnexpectedToken, current().span(), "IN list cannot be empty");
      }
      do {
        check_list(children.size() - 1U, current().span(), "IN list exceeds the limit");
        children.push_back(parse_expression());
      } while (match(SqlTokenKind::kComma));
      const SqlToken& end = require(SqlTokenKind::kRightParen, "IN list is missing ')'");
      return make_expression(SqlExpressionKind::kIn, SqlLiteralKind::kNull,
                             negated ? SqlOperator::kNotIn : SqlOperator::kIn, {}, {},
                             std::move(children), std::nullopt, span_between(begin, end.span()));
    }
    if (negated) {
      fail(SqlDiagnosticCode::kUnexpectedToken, previous().span(),
           "NOT after an expression requires BETWEEN or IN");
    }
    return left;
  }

  [[nodiscard]] SqlExpression parse_additive() {
    SqlExpression left = parse_multiplicative();
    while (check(SqlTokenKind::kPlus) || check(SqlTokenKind::kMinus)) {
      const SqlOperator operation =
          match(SqlTokenKind::kPlus) ? SqlOperator::kAdd : SqlOperator::kSubtract;
      left = make_operation(SqlExpressionKind::kBinary, operation, std::move(left),
                            parse_multiplicative());
    }
    return left;
  }

  [[nodiscard]] SqlExpression parse_multiplicative() {
    SqlExpression left = parse_unary();
    while (check(SqlTokenKind::kStar) || check(SqlTokenKind::kSlash) ||
           check(SqlTokenKind::kPercent)) {
      SqlOperator operation = SqlOperator::kMultiply;
      if (match(SqlTokenKind::kStar)) {
        operation = SqlOperator::kMultiply;
      } else if (match(SqlTokenKind::kSlash)) {
        operation = SqlOperator::kDivide;
      } else {
        require(SqlTokenKind::kPercent, "Expected multiplicative operator");
        operation = SqlOperator::kRemainder;
      }
      left = make_operation(SqlExpressionKind::kBinary, operation, std::move(left), parse_unary());
    }
    return left;
  }

  [[nodiscard]] SqlExpression parse_unary() {
    std::vector<std::pair<SqlOperator, SourceLocation>> operators;
    while (check(SqlTokenKind::kPlus) || check(SqlTokenKind::kMinus)) {
      SqlOperator operation = SqlOperator::kPositive;
      if (!match(SqlTokenKind::kPlus)) {
        require(SqlTokenKind::kMinus, "Expected unary operator");
        operation = SqlOperator::kNegative;
      }
      if (operators.size() >= limits_.maximum_expression_depth - 1U) {
        fail(SqlDiagnosticCode::kResourceLimit, previous().span(),
             "SQL expression depth exceeds the limit");
      }
      operators.emplace_back(operation, previous().span().begin);
    }
    SqlExpression expression = parse_primary();
    for (auto iterator = operators.rbegin(); iterator != operators.rend(); ++iterator) {
      expression = make_unary(iterator->first, iterator->second, std::move(expression));
    }
    return expression;
  }

  [[nodiscard]] SqlExpression parse_primary() {
    const SqlToken& token = current();
    if (match(SqlTokenKind::kInteger))
      return literal(SqlLiteralKind::kInteger, token);
    if (match(SqlTokenKind::kFloat))
      return literal(SqlLiteralKind::kFloat, token);
    if (match(SqlTokenKind::kString))
      return literal(SqlLiteralKind::kString, token);
    if (match(SqlTokenKind::kBinary))
      return literal(SqlLiteralKind::kBinary, token);
    if (match(SqlKeyword::kNull))
      return literal(SqlLiteralKind::kNull, token);
    if (match(SqlKeyword::kTrue) || match(SqlKeyword::kFalse)) {
      return literal(SqlLiteralKind::kBoolean, token);
    }
    const bool contextual_typed_literal =
        (check(SqlKeyword::kDate) || check(SqlKeyword::kUuid)) &&
        position_ + 1U < tokens_.tokens().size() &&
        tokens_.tokens()[position_ + 1U].kind() == SqlTokenKind::kString;
    if (check(SqlKeyword::kTimestamp) || check(SqlKeyword::kInterval) || contextual_typed_literal) {
      const SqlKeyword kind = current().keyword();
      const SourceLocation begin = current().span().begin;
      ++position_;
      const SqlToken& value =
          require(SqlTokenKind::kString, "Typed literal requires a quoted value");
      const SqlLiteralKind literal_kind =
          kind == SqlKeyword::kTimestamp
              ? SqlLiteralKind::kTimestamp
              : (kind == SqlKeyword::kDate
                     ? SqlLiteralKind::kDate
                     : (kind == SqlKeyword::kInterval ? SqlLiteralKind::kInterval
                                                      : SqlLiteralKind::kUuid));
      return make_expression(SqlExpressionKind::kLiteral, literal_kind, SqlOperator::kNone,
                             value.text(), {}, {}, std::nullopt, span_between(begin, value.span()));
    }
    if (match(SqlKeyword::kCast)) {
      const SourceLocation begin = token.span().begin;
      require(SqlTokenKind::kLeftParen, "CAST requires '('");
      std::vector<SqlExpression> children;
      children.push_back(parse_expression());
      require(SqlKeyword::kAs, "CAST requires AS type");
      schema::LogicalType type = parse_type();
      const SqlToken& end = require(SqlTokenKind::kRightParen, "CAST is missing ')'");
      return make_expression(SqlExpressionKind::kCast, SqlLiteralKind::kNull, SqlOperator::kNone,
                             {}, {}, std::move(children), std::move(type),
                             span_between(begin, end.span()));
    }
    if (match(SqlTokenKind::kLeftParen)) {
      SqlExpression expression = parse_expression();
      require(SqlTokenKind::kRightParen, "Parenthesized expression is missing ')'");
      return expression;
    }
    if (check(SqlTokenKind::kIdentifier) || check(SqlTokenKind::kQuotedIdentifier) ||
        contextual_identifier(current()) || check(SqlKeyword::kCoalesce)) {
      return parse_name_or_function();
    }
    fail(SqlDiagnosticCode::kUnexpectedToken, token.span(), "Expected SQL expression");
  }

  [[nodiscard]] SqlExpression parse_name_or_function() {
    const SourceLocation begin = current().span().begin;
    std::vector<SqlIdentifier> name;
    if (check(SqlKeyword::kCoalesce)) {
      const SqlToken& token = tokens_.tokens()[position_++];
      name.push_back(SqlIdentifier{token.text(), false, token.span()});
      account(token.span());
    } else {
      name.push_back(parse_identifier("Expected identifier"));
    }
    if (match(SqlTokenKind::kLeftParen)) {
      std::vector<SqlExpression> arguments;
      if (match(SqlTokenKind::kStar)) {
        arguments.push_back(make_expression(SqlExpressionKind::kStar, SqlLiteralKind::kNull,
                                            SqlOperator::kNone, "*", {}, {}, std::nullopt,
                                            previous().span()));
      } else if (!check(SqlTokenKind::kRightParen)) {
        do {
          check_list(arguments.size(), current().span(), "Function argument count exceeds limit");
          arguments.push_back(parse_expression());
        } while (match(SqlTokenKind::kComma));
      }
      const SqlToken& end = require(SqlTokenKind::kRightParen, "Function call is missing ')'");
      return make_expression(SqlExpressionKind::kFunction, SqlLiteralKind::kNull,
                             SqlOperator::kNone, name.front().text(), {}, std::move(arguments),
                             std::nullopt, span_between(begin, end.span()));
    }
    if (match(SqlTokenKind::kDot)) {
      name.push_back(parse_identifier("Qualified column requires identifier after '.'"));
    }
    return make_expression(SqlExpressionKind::kColumn, SqlLiteralKind::kNull, SqlOperator::kNone,
                           {}, std::move(name), {}, std::nullopt,
                           span_between(begin, previous().span()));
  }

  [[nodiscard]] schema::LogicalType parse_type() {
    const SqlToken& token = current();
    schema::LogicalTypeKind kind{};
    if (match(SqlKeyword::kBool))
      kind = schema::LogicalTypeKind::kBool;
    else if (match(SqlKeyword::kInt8))
      kind = schema::LogicalTypeKind::kInt8;
    else if (match(SqlKeyword::kInt16))
      kind = schema::LogicalTypeKind::kInt16;
    else if (match(SqlKeyword::kInt32))
      kind = schema::LogicalTypeKind::kInt32;
    else if (match(SqlKeyword::kInt64))
      kind = schema::LogicalTypeKind::kInt64;
    else if (match(SqlKeyword::kUInt8))
      kind = schema::LogicalTypeKind::kUInt8;
    else if (match(SqlKeyword::kUInt16))
      kind = schema::LogicalTypeKind::kUInt16;
    else if (match(SqlKeyword::kUInt32))
      kind = schema::LogicalTypeKind::kUInt32;
    else if (match(SqlKeyword::kUInt64))
      kind = schema::LogicalTypeKind::kUInt64;
    else if (match(SqlKeyword::kFloat32))
      kind = schema::LogicalTypeKind::kFloat32;
    else if (match(SqlKeyword::kFloat64))
      kind = schema::LogicalTypeKind::kFloat64;
    else if (match(SqlKeyword::kTimestampNs))
      kind = schema::LogicalTypeKind::kTimestampNs;
    else if (match(SqlKeyword::kDate))
      kind = schema::LogicalTypeKind::kDate;
    else if (match(SqlKeyword::kSymbol))
      kind = schema::LogicalTypeKind::kSymbol;
    else if (match(SqlKeyword::kString))
      kind = schema::LogicalTypeKind::kString;
    else if (match(SqlKeyword::kBinary))
      kind = schema::LogicalTypeKind::kBinary;
    else if (match(SqlKeyword::kUuid))
      kind = schema::LogicalTypeKind::kUuid;
    else if (match(SqlKeyword::kDecimal)) {
      require(SqlTokenKind::kLeftParen, "DECIMAL requires precision and scale");
      const SqlToken& precision_token =
          require(SqlTokenKind::kInteger, "DECIMAL precision must be an integer");
      require(SqlTokenKind::kComma, "DECIMAL requires precision, scale");
      const SqlToken& scale_token =
          require(SqlTokenKind::kInteger, "DECIMAL scale must be an integer");
      require(SqlTokenKind::kRightParen, "DECIMAL type is missing ')'");
      std::uint16_t precision = 0U;
      std::uint16_t scale = 0U;
      const auto precision_result =
          std::from_chars(precision_token.text().data(),
                          precision_token.text().data() + precision_token.text().size(), precision);
      const auto scale_result = std::from_chars(
          scale_token.text().data(), scale_token.text().data() + scale_token.text().size(), scale);
      const common::Result<schema::LogicalType> decimal =
          schema::LogicalType::decimal(precision, scale);
      if (precision_result.ec != std::errc{} || scale_result.ec != std::errc{} ||
          !decimal.has_value()) {
        fail(SqlDiagnosticCode::kTypeMismatch, token.span(),
             "DECIMAL precision or scale is outside its supported range");
      }
      return *decimal;
    } else {
      fail(SqlDiagnosticCode::kTypeMismatch, token.span(), "CAST names an unsupported type");
    }
    return schema::LogicalType::create(kind).value();
  }

  [[nodiscard]] SqlExpression literal(const SqlLiteralKind kind, const SqlToken& token) {
    return make_expression(SqlExpressionKind::kLiteral, kind, SqlOperator::kNone, token.text(), {},
                           {}, std::nullopt, token.span());
  }

  [[nodiscard]] SqlExpression make_unary(const SqlOperator operation, const SourceLocation begin,
                                         SqlExpression operand) {
    const SourceSpan span = span_between(begin, operand.span());
    std::vector<SqlExpression> children;
    children.push_back(std::move(operand));
    return make_expression(SqlExpressionKind::kUnary, SqlLiteralKind::kNull, operation, {}, {},
                           std::move(children), std::nullopt, span);
  }

  [[nodiscard]] SqlExpression make_operation(const SqlExpressionKind kind,
                                             const SqlOperator operation, SqlExpression left,
                                             SqlExpression right) {
    const SourceSpan span = span_between(left.span().begin, right.span());
    std::vector<SqlExpression> children;
    children.push_back(std::move(left));
    children.push_back(std::move(right));
    return make_expression(kind, SqlLiteralKind::kNull, operation, {}, {}, std::move(children),
                           std::nullopt, span);
  }

  [[nodiscard]] SqlExpression
  make_expression(const SqlExpressionKind kind, const SqlLiteralKind literal_kind,
                  const SqlOperator operation, std::string text, std::vector<SqlIdentifier> name,
                  std::vector<SqlExpression> children, std::optional<schema::LogicalType> cast_type,
                  const SourceSpan span) {
    std::size_t depth = 1U;
    for (const SqlExpression& child : children) {
      depth = std::max(depth, expression_depth(child) + 1U);
    }
    if (depth > limits_.maximum_expression_depth) {
      fail(SqlDiagnosticCode::kResourceLimit, span, "SQL expression depth exceeds the limit");
    }
    account(span);
    return SqlExpression{kind,
                         literal_kind,
                         operation,
                         std::move(text),
                         std::move(name),
                         std::move(children),
                         std::move(cast_type),
                         span};
  }

  [[nodiscard]] std::size_t expression_depth(const SqlExpression& expression) const noexcept {
    std::size_t depth = 1U;
    for (const SqlExpression& child : expression.children()) {
      depth = std::max(depth, expression_depth(child) + 1U);
    }
    return depth;
  }

  SqlTokenStream tokens_;
  SqlParserLimits limits_;
  std::size_t position_{};
  std::size_t ast_nodes_{};
  std::size_t expression_recursion_{};
};

} // namespace detail

SqlResult<ParsedSqlSelect> parse_sql_v1_select(const std::string_view sql,
                                               const SqlParserLimits limits) {
  SqlResult<SqlTokenStream> tokens = tokenize_sql_v1(sql, limits.lexer);
  if (!tokens.has_value()) {
    return std::unexpected(tokens.error());
  }
  return detail::SqlParser{std::move(*tokens), limits}.run();
}

} // namespace chronos::query
