#ifndef CHRONOS_QUERY_AST_HPP_
#define CHRONOS_QUERY_AST_HPP_

#include "chronos/query/diagnostic.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace chronos::query {

namespace detail {
class SqlParser;
class SqlStatementBinder;
} // namespace detail

class SqlIdentifier {
public:
  [[nodiscard]] const std::string& text() const noexcept;
  [[nodiscard]] bool quoted() const noexcept;
  [[nodiscard]] const SourceSpan& span() const noexcept;

private:
  SqlIdentifier(std::string text, bool quoted, SourceSpan span) noexcept;

  std::string text_;
  bool quoted_{};
  SourceSpan span_;

  friend class detail::SqlParser;
  friend class detail::SqlStatementBinder;
};

enum class SqlLiteralKind : std::uint8_t {
  kNull,
  kBoolean,
  kInteger,
  kFloat,
  kString,
  kBinary,
  kTimestamp,
  kDate,
  kInterval,
  kUuid,
};

enum class SqlExpressionKind : std::uint8_t {
  kStar,
  kLiteral,
  kColumn,
  kFunction,
  kCast,
  kUnary,
  kBinary,
  kIsNull,
  kBetween,
  kIn,
};

enum class SqlOperator : std::uint8_t {
  kNone,
  kOr,
  kAnd,
  kNot,
  kEqual,
  kNotEqual,
  kLess,
  kLessEqual,
  kGreater,
  kGreaterEqual,
  kAdd,
  kSubtract,
  kMultiply,
  kDivide,
  kRemainder,
  kPositive,
  kNegative,
  kIsNull,
  kIsNotNull,
  kBetween,
  kNotBetween,
  kIn,
  kNotIn,
};

// One owned expression node. children() order is semantic: unary operand; binary left/right;
// BETWEEN value/lower/upper; IN value/list...; function arguments; or CAST operand.
class SqlExpression {
public:
  [[nodiscard]] SqlExpressionKind kind() const noexcept;
  [[nodiscard]] SqlLiteralKind literal_kind() const noexcept;
  [[nodiscard]] SqlOperator operation() const noexcept;
  [[nodiscard]] const std::string& text() const noexcept;
  [[nodiscard]] std::span<const SqlIdentifier> name() const noexcept;
  [[nodiscard]] std::span<const SqlExpression> children() const noexcept;
  [[nodiscard]] const std::optional<schema::LogicalType>& cast_type() const noexcept;
  [[nodiscard]] const SourceSpan& span() const noexcept;

private:
  SqlExpression(SqlExpressionKind kind, SqlLiteralKind literal_kind, SqlOperator operation,
                std::string text, std::vector<SqlIdentifier> name,
                std::vector<SqlExpression> children, std::optional<schema::LogicalType> cast_type,
                SourceSpan span) noexcept;

  SqlExpressionKind kind_;
  SqlLiteralKind literal_kind_{SqlLiteralKind::kNull};
  SqlOperator operation_{SqlOperator::kNone};
  std::string text_;
  std::vector<SqlIdentifier> name_;
  std::vector<SqlExpression> children_;
  std::optional<schema::LogicalType> cast_type_;
  SourceSpan span_;

  friend class detail::SqlParser;
  friend class detail::SqlStatementBinder;
};

enum class SqlSelectItemKind : std::uint8_t {
  kExpression,
  kStar,
  kQualifiedStar,
};

class SqlSelectItem {
public:
  [[nodiscard]] SqlSelectItemKind kind() const noexcept;
  [[nodiscard]] const SqlExpression* expression() const noexcept;
  [[nodiscard]] const std::optional<SqlIdentifier>& qualifier() const noexcept;
  [[nodiscard]] const std::optional<SqlIdentifier>& alias() const noexcept;
  [[nodiscard]] const SourceSpan& span() const noexcept;

private:
  SqlSelectItem(SqlSelectItemKind kind, std::vector<SqlExpression> expression,
                std::optional<SqlIdentifier> qualifier, std::optional<SqlIdentifier> alias,
                SourceSpan span) noexcept;

  SqlSelectItemKind kind_;
  std::vector<SqlExpression> expression_;
  std::optional<SqlIdentifier> qualifier_;
  std::optional<SqlIdentifier> alias_;
  SourceSpan span_;

  friend class detail::SqlParser;
  friend class detail::SqlStatementBinder;
};

struct SqlSource {
  SqlIdentifier table;
  std::optional<SqlIdentifier> alias;
};

struct SqlAsofJoin {
  bool left{};
  SqlSource source;
  SqlExpression condition;
};

struct SqlLatestBy {
  std::vector<SqlIdentifier> keys;
  SqlExpression timestamp;
};

enum class SqlOrderDirection : std::uint8_t { kAscending, kDescending };
enum class SqlNullOrder : std::uint8_t { kDefault, kFirst, kLast };

struct SqlOrderItem {
  SqlExpression expression;
  SqlOrderDirection direction{SqlOrderDirection::kAscending};
  SqlNullOrder null_order{SqlNullOrder::kDefault};
};

enum class SqlSelectMode : std::uint8_t {
  kSelect,
  kExplain,
  kExplainAnalyze,
  kSubscribe,
};

// Complete owned SELECT-family AST. FOR SYSTEM_TIME retains the normalized timestamp literal.
class ParsedSqlSelect {
public:
  ParsedSqlSelect() = delete;
  ParsedSqlSelect(const ParsedSqlSelect&) = delete;
  ParsedSqlSelect& operator=(const ParsedSqlSelect&) = delete;
  ParsedSqlSelect(ParsedSqlSelect&&) noexcept = default;
  ParsedSqlSelect& operator=(ParsedSqlSelect&&) noexcept = default;

  [[nodiscard]] SqlSelectMode mode() const noexcept;
  [[nodiscard]] std::span<const SqlSelectItem> items() const noexcept;
  [[nodiscard]] const SqlSource& source() const noexcept;
  [[nodiscard]] const std::optional<std::string>& system_time() const noexcept;
  [[nodiscard]] const std::optional<SqlLatestBy>& latest_by() const noexcept;
  [[nodiscard]] std::span<const SqlAsofJoin> asof_joins() const noexcept;
  [[nodiscard]] const SqlExpression* where() const noexcept;
  [[nodiscard]] std::span<const SqlExpression> group_by() const noexcept;
  [[nodiscard]] std::span<const SqlOrderItem> order_by() const noexcept;
  [[nodiscard]] const std::optional<std::uint64_t>& limit() const noexcept;
  [[nodiscard]] const SourceSpan& span() const noexcept;

private:
  ParsedSqlSelect(SqlSelectMode mode, std::vector<SqlSelectItem> items, SqlSource source,
                  std::optional<std::string> system_time, std::optional<SqlLatestBy> latest_by,
                  std::vector<SqlAsofJoin> asof_joins, std::vector<SqlExpression> where,
                  std::vector<SqlExpression> group_by, std::vector<SqlOrderItem> order_by,
                  std::optional<std::uint64_t> limit, SourceSpan span) noexcept;

  SqlSelectMode mode_;
  std::vector<SqlSelectItem> items_;
  SqlSource source_;
  std::optional<std::string> system_time_;
  std::optional<SqlLatestBy> latest_by_;
  std::vector<SqlAsofJoin> asof_joins_;
  std::vector<SqlExpression> where_;
  std::vector<SqlExpression> group_by_;
  std::vector<SqlOrderItem> order_by_;
  std::optional<std::uint64_t> limit_;
  SourceSpan span_;

  friend class detail::SqlParser;
  friend class detail::SqlStatementBinder;
};

struct SqlColumnDeclaration {
  SqlIdentifier name;
  schema::LogicalType type;
  bool nullable{true};
};

class ParsedSqlCreateTable {
public:
  ParsedSqlCreateTable() = delete;
  ParsedSqlCreateTable(const ParsedSqlCreateTable&) = delete;
  ParsedSqlCreateTable& operator=(const ParsedSqlCreateTable&) = delete;
  ParsedSqlCreateTable(ParsedSqlCreateTable&&) noexcept = default;
  ParsedSqlCreateTable& operator=(ParsedSqlCreateTable&&) noexcept = default;

  [[nodiscard]] const SqlIdentifier& table() const noexcept;
  [[nodiscard]] std::span<const SqlColumnDeclaration> columns() const noexcept;
  [[nodiscard]] const SqlIdentifier& event_time() const noexcept;
  [[nodiscard]] std::span<const SqlIdentifier> ordering_key() const noexcept;
  [[nodiscard]] const SqlExpression& partition_expression() const noexcept;
  [[nodiscard]] std::span<const SqlIdentifier> shard_key() const noexcept;
  [[nodiscard]] std::span<const SqlIdentifier> deduplication_key() const noexcept;
  [[nodiscard]] const std::string& retention_interval() const noexcept;
  [[nodiscard]] const std::string& system_history_retention_interval() const noexcept;
  [[nodiscard]] const std::string& allowed_lateness_interval() const noexcept;
  [[nodiscard]] const SourceSpan& span() const noexcept;

private:
  ParsedSqlCreateTable(SqlIdentifier table, std::vector<SqlColumnDeclaration> columns,
                       SqlIdentifier event_time, std::vector<SqlIdentifier> ordering_key,
                       SqlExpression partition_expression, std::vector<SqlIdentifier> shard_key,
                       std::vector<SqlIdentifier> deduplication_key, std::string retention_interval,
                       std::string system_history_retention_interval,
                       std::string allowed_lateness_interval, SourceSpan span) noexcept;

  SqlIdentifier table_;
  std::vector<SqlColumnDeclaration> columns_;
  SqlIdentifier event_time_;
  std::vector<SqlIdentifier> ordering_key_;
  SqlExpression partition_expression_;
  std::vector<SqlIdentifier> shard_key_;
  std::vector<SqlIdentifier> deduplication_key_;
  std::string retention_interval_;
  std::string system_history_retention_interval_;
  std::string allowed_lateness_interval_;
  SourceSpan span_;

  friend class detail::SqlParser;
};

class ParsedSqlInsert {
public:
  ParsedSqlInsert() = delete;
  ParsedSqlInsert(const ParsedSqlInsert&) = delete;
  ParsedSqlInsert& operator=(const ParsedSqlInsert&) = delete;
  ParsedSqlInsert(ParsedSqlInsert&&) noexcept = default;
  ParsedSqlInsert& operator=(ParsedSqlInsert&&) noexcept = default;

  [[nodiscard]] const SqlIdentifier& table() const noexcept;
  [[nodiscard]] std::span<const SqlIdentifier> columns() const noexcept;
  [[nodiscard]] std::span<const std::vector<SqlExpression>> rows() const noexcept;
  [[nodiscard]] const SourceSpan& span() const noexcept;

private:
  ParsedSqlInsert(SqlIdentifier table, std::vector<SqlIdentifier> columns,
                  std::vector<std::vector<SqlExpression>> rows, SourceSpan span) noexcept;

  SqlIdentifier table_;
  std::vector<SqlIdentifier> columns_;
  std::vector<std::vector<SqlExpression>> rows_;
  SourceSpan span_;

  friend class detail::SqlParser;
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_AST_HPP_
