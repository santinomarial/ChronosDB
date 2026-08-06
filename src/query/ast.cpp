#include "chronos/query/ast.hpp"

#include <utility>

namespace chronos::query {

SqlIdentifier::SqlIdentifier(std::string text, const bool quoted, const SourceSpan span) noexcept
    : text_(std::move(text)), quoted_(quoted), span_(span) {}

const std::string& SqlIdentifier::text() const noexcept {
  return text_;
}
bool SqlIdentifier::quoted() const noexcept {
  return quoted_;
}
const SourceSpan& SqlIdentifier::span() const noexcept {
  return span_;
}

SqlExpression::SqlExpression(const SqlExpressionKind kind, const SqlLiteralKind literal_kind,
                             const SqlOperator operation, std::string text,
                             std::vector<SqlIdentifier> name, std::vector<SqlExpression> children,
                             std::optional<schema::LogicalType> cast_type,
                             const SourceSpan span) noexcept
    : kind_(kind), literal_kind_(literal_kind), operation_(operation), text_(std::move(text)),
      name_(std::move(name)), children_(std::move(children)), cast_type_(cast_type), span_(span) {}

SqlExpressionKind SqlExpression::kind() const noexcept {
  return kind_;
}
SqlLiteralKind SqlExpression::literal_kind() const noexcept {
  return literal_kind_;
}
SqlOperator SqlExpression::operation() const noexcept {
  return operation_;
}
const std::string& SqlExpression::text() const noexcept {
  return text_;
}
std::span<const SqlIdentifier> SqlExpression::name() const noexcept {
  return name_;
}
std::span<const SqlExpression> SqlExpression::children() const noexcept {
  return children_;
}
const std::optional<schema::LogicalType>& SqlExpression::cast_type() const noexcept {
  return cast_type_;
}
const SourceSpan& SqlExpression::span() const noexcept {
  return span_;
}

SqlSelectItem::SqlSelectItem(const SqlSelectItemKind kind, std::vector<SqlExpression> expression,
                             std::optional<SqlIdentifier> qualifier,
                             std::optional<SqlIdentifier> alias, const SourceSpan span) noexcept
    : kind_(kind), expression_(std::move(expression)), qualifier_(std::move(qualifier)),
      alias_(std::move(alias)), span_(span) {}

SqlSelectItemKind SqlSelectItem::kind() const noexcept {
  return kind_;
}
const SqlExpression* SqlSelectItem::expression() const noexcept {
  return expression_.empty() ? nullptr : &expression_.front();
}
const std::optional<SqlIdentifier>& SqlSelectItem::qualifier() const noexcept {
  return qualifier_;
}
const std::optional<SqlIdentifier>& SqlSelectItem::alias() const noexcept {
  return alias_;
}
const SourceSpan& SqlSelectItem::span() const noexcept {
  return span_;
}

ParsedSqlSelect::ParsedSqlSelect(const SqlSelectMode mode, std::vector<SqlSelectItem> items,
                                 SqlSource source, std::optional<std::string> system_time,
                                 std::optional<SqlLatestBy> latest_by,
                                 std::vector<SqlAsofJoin> asof_joins,
                                 std::vector<SqlExpression> where,
                                 std::vector<SqlExpression> group_by,
                                 std::vector<SqlOrderItem> order_by,
                                 std::optional<std::uint64_t> limit, const SourceSpan span) noexcept
    : mode_(mode), items_(std::move(items)), source_(std::move(source)),
      system_time_(std::move(system_time)), latest_by_(std::move(latest_by)),
      asof_joins_(std::move(asof_joins)), where_(std::move(where)), group_by_(std::move(group_by)),
      order_by_(std::move(order_by)), limit_(limit), span_(span) {}

SqlSelectMode ParsedSqlSelect::mode() const noexcept {
  return mode_;
}
std::span<const SqlSelectItem> ParsedSqlSelect::items() const noexcept {
  return items_;
}
const SqlSource& ParsedSqlSelect::source() const noexcept {
  return source_;
}
const std::optional<std::string>& ParsedSqlSelect::system_time() const noexcept {
  return system_time_;
}
const std::optional<SqlLatestBy>& ParsedSqlSelect::latest_by() const noexcept {
  return latest_by_;
}
std::span<const SqlAsofJoin> ParsedSqlSelect::asof_joins() const noexcept {
  return asof_joins_;
}
const SqlExpression* ParsedSqlSelect::where() const noexcept {
  return where_.empty() ? nullptr : &where_.front();
}
std::span<const SqlExpression> ParsedSqlSelect::group_by() const noexcept {
  return group_by_;
}
std::span<const SqlOrderItem> ParsedSqlSelect::order_by() const noexcept {
  return order_by_;
}
const std::optional<std::uint64_t>& ParsedSqlSelect::limit() const noexcept {
  return limit_;
}
const SourceSpan& ParsedSqlSelect::span() const noexcept {
  return span_;
}

ParsedSqlCreateTable::ParsedSqlCreateTable(
    SqlIdentifier table, std::vector<SqlColumnDeclaration> columns, SqlIdentifier event_time,
    std::vector<SqlIdentifier> ordering_key, SqlExpression partition_expression,
    std::vector<SqlIdentifier> shard_key, std::vector<SqlIdentifier> deduplication_key,
    std::string retention_interval, std::string system_history_retention_interval,
    std::string allowed_lateness_interval, const SourceSpan span) noexcept
    : table_(std::move(table)), columns_(std::move(columns)), event_time_(std::move(event_time)),
      ordering_key_(std::move(ordering_key)),
      partition_expression_(std::move(partition_expression)), shard_key_(std::move(shard_key)),
      deduplication_key_(std::move(deduplication_key)),
      retention_interval_(std::move(retention_interval)),
      system_history_retention_interval_(std::move(system_history_retention_interval)),
      allowed_lateness_interval_(std::move(allowed_lateness_interval)), span_(span) {}

const SqlIdentifier& ParsedSqlCreateTable::table() const noexcept {
  return table_;
}
std::span<const SqlColumnDeclaration> ParsedSqlCreateTable::columns() const noexcept {
  return columns_;
}
const SqlIdentifier& ParsedSqlCreateTable::event_time() const noexcept {
  return event_time_;
}
std::span<const SqlIdentifier> ParsedSqlCreateTable::ordering_key() const noexcept {
  return ordering_key_;
}
const SqlExpression& ParsedSqlCreateTable::partition_expression() const noexcept {
  return partition_expression_;
}
std::span<const SqlIdentifier> ParsedSqlCreateTable::shard_key() const noexcept {
  return shard_key_;
}
std::span<const SqlIdentifier> ParsedSqlCreateTable::deduplication_key() const noexcept {
  return deduplication_key_;
}
const std::string& ParsedSqlCreateTable::retention_interval() const noexcept {
  return retention_interval_;
}
const std::string& ParsedSqlCreateTable::system_history_retention_interval() const noexcept {
  return system_history_retention_interval_;
}
const std::string& ParsedSqlCreateTable::allowed_lateness_interval() const noexcept {
  return allowed_lateness_interval_;
}
const SourceSpan& ParsedSqlCreateTable::span() const noexcept {
  return span_;
}

ParsedSqlInsert::ParsedSqlInsert(SqlIdentifier table, std::vector<SqlIdentifier> columns,
                                 std::vector<std::vector<SqlExpression>> rows,
                                 const SourceSpan span) noexcept
    : table_(std::move(table)), columns_(std::move(columns)), rows_(std::move(rows)), span_(span) {}

const SqlIdentifier& ParsedSqlInsert::table() const noexcept {
  return table_;
}
std::span<const SqlIdentifier> ParsedSqlInsert::columns() const noexcept {
  return columns_;
}
std::span<const std::vector<SqlExpression>> ParsedSqlInsert::rows() const noexcept {
  return rows_;
}
const SourceSpan& ParsedSqlInsert::span() const noexcept {
  return span_;
}

} // namespace chronos::query
