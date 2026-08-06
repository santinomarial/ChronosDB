#include "chronos/query/statement_binder.hpp"

#include "chronos/common/status.hpp"
#include "chronos/query/literal.hpp"
#include "chronos/schema/column_definition.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] SqlDiagnostic diagnostic(const SqlDiagnosticCode code, const SourceSpan span,
                                       const common::StatusCode status,
                                       const std::string_view message) {
  return SqlDiagnostic{code, span, common::Status{status, std::string{message}}};
}

[[nodiscard]] SqlDiagnostic invalid(const SqlDiagnosticCode code, const SourceSpan span,
                                    const std::string_view message) {
  return diagnostic(code, span, common::StatusCode::kInvalidArgument, message);
}

[[nodiscard]] std::optional<std::size_t> find_column(const ParsedSqlCreateTable& syntax,
                                                     const SqlIdentifier& name) noexcept {
  const auto found =
      std::ranges::find_if(syntax.columns(), [&](const SqlColumnDeclaration& column) {
        return column.name.text() == name.text();
      });
  if (found == syntax.columns().end())
    return std::nullopt;
  return static_cast<std::size_t>(found - syntax.columns().begin());
}

[[nodiscard]] SqlResult<std::vector<std::size_t>>
bind_role(const ParsedSqlCreateTable& syntax, const std::span<const SqlIdentifier> names,
          const std::string_view label) {
  std::vector<std::size_t> ordinals;
  try {
    ordinals.reserve(names.size());
    for (const SqlIdentifier& name : names) {
      const std::optional<std::size_t> ordinal = find_column(syntax, name);
      if (!ordinal.has_value()) {
        return std::unexpected(invalid(SqlDiagnosticCode::kUnknownColumn, name.span(),
                                       std::string{label} + " references an unknown column"));
      }
      if (std::ranges::find(ordinals, *ordinal) != ordinals.end()) {
        return std::unexpected(invalid(SqlDiagnosticCode::kTypeMismatch, name.span(),
                                       std::string{label} + " repeats a column"));
      }
      ordinals.push_back(*ordinal);
    }
    return ordinals;
  } catch (const std::bad_alloc&) {
    return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, syntax.span(),
                                      common::StatusCode::kResourceExhausted,
                                      "CREATE TABLE role binding allocation failed"));
  } catch (const std::length_error&) {
    return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, syntax.span(),
                                      common::StatusCode::kResourceExhausted,
                                      "CREATE TABLE role binding exceeds container limits"));
  }
}

[[nodiscard]] SqlResult<std::int64_t> bind_interval(const std::string_view text,
                                                    const SourceSpan span,
                                                    const std::string_view label,
                                                    const bool require_positive) {
  const common::Result<std::int64_t> value = parse_sql_interval_ns_literal(text);
  if (!value.has_value()) {
    return std::unexpected(
        diagnostic(SqlDiagnosticCode::kInvalidLiteral, span, value.error().code(),
                   std::string{label} + " contains an invalid INTERVAL literal"));
  }
  if (require_positive && *value == 0) {
    return std::unexpected(invalid(SqlDiagnosticCode::kInvalidLiteral, span,
                                   std::string{label} + " must be greater than zero"));
  }
  return *value;
}

[[nodiscard]] SqlResult<std::int64_t> bind_partition(const ParsedSqlCreateTable& syntax,
                                                     const std::size_t event_time_ordinal) {
  const SqlExpression& expression = syntax.partition_expression();
  if (expression.kind() != SqlExpressionKind::kFunction || expression.text() != "time_bucket" ||
      expression.children().size() != 2U) {
    return std::unexpected(invalid(SqlDiagnosticCode::kTypeMismatch, expression.span(),
                                   "PARTITION BY requires time_bucket(INTERVAL, event_time)"));
  }
  const SqlExpression& interval = expression.children()[0];
  const SqlExpression& column = expression.children()[1];
  if (interval.kind() != SqlExpressionKind::kLiteral ||
      interval.literal_kind() != SqlLiteralKind::kInterval ||
      column.kind() != SqlExpressionKind::kColumn || column.name().size() != 1U) {
    return std::unexpected(invalid(SqlDiagnosticCode::kTypeMismatch, expression.span(),
                                   "PARTITION BY requires time_bucket(INTERVAL, event_time)"));
  }
  const std::optional<std::size_t> ordinal = find_column(syntax, column.name().front());
  if (!ordinal.has_value()) {
    return std::unexpected(invalid(SqlDiagnosticCode::kUnknownColumn, column.span(),
                                   "PARTITION BY references an unknown column"));
  }
  if (*ordinal != event_time_ordinal) {
    return std::unexpected(invalid(SqlDiagnosticCode::kTypeMismatch, column.span(),
                                   "PARTITION BY must reference the EVENT TIME column"));
  }
  return bind_interval(interval.text(), interval.span(), "PARTITION BY interval", true);
}

[[nodiscard]] std::vector<schema::ColumnId>
role_ids(const std::span<const schema::ColumnId> identities,
         const std::span<const std::size_t> ordinals) {
  std::vector<schema::ColumnId> result;
  result.reserve(ordinals.size());
  for (const std::size_t ordinal : ordinals)
    result.push_back(identities[ordinal]);
  return result;
}

} // namespace

BoundSqlCreateTable::BoundSqlCreateTable(ParsedSqlCreateTable syntax,
                                         std::shared_ptr<const QueryCatalogSnapshot> catalog,
                                         const std::size_t event_time_ordinal,
                                         std::vector<std::size_t> ordering_key_ordinals,
                                         std::vector<std::size_t> shard_key_ordinals,
                                         std::vector<std::size_t> deduplication_key_ordinals,
                                         const BoundSqlTablePolicy policy) noexcept
    : syntax_(std::move(syntax)), catalog_(std::move(catalog)),
      event_time_ordinal_(event_time_ordinal),
      ordering_key_ordinals_(std::move(ordering_key_ordinals)),
      shard_key_ordinals_(std::move(shard_key_ordinals)),
      deduplication_key_ordinals_(std::move(deduplication_key_ordinals)), policy_(policy) {}

const ParsedSqlCreateTable& BoundSqlCreateTable::syntax() const noexcept {
  return syntax_;
}
const std::shared_ptr<const QueryCatalogSnapshot>& BoundSqlCreateTable::catalog() const noexcept {
  return catalog_;
}
std::size_t BoundSqlCreateTable::event_time_ordinal() const noexcept {
  return event_time_ordinal_;
}
std::span<const std::size_t> BoundSqlCreateTable::ordering_key_ordinals() const noexcept {
  return ordering_key_ordinals_;
}
std::span<const std::size_t> BoundSqlCreateTable::shard_key_ordinals() const noexcept {
  return shard_key_ordinals_;
}
std::span<const std::size_t> BoundSqlCreateTable::deduplication_key_ordinals() const noexcept {
  return deduplication_key_ordinals_;
}
const BoundSqlTablePolicy& BoundSqlCreateTable::policy() const noexcept {
  return policy_;
}

SqlResult<BoundSqlCreateTable>
bind_sql_v1_create_table(ParsedSqlCreateTable syntax,
                         std::shared_ptr<const QueryCatalogSnapshot> catalog) {
  if (catalog == nullptr) {
    return std::unexpected(invalid(SqlDiagnosticCode::kResourceLimit, syntax.span(),
                                   "CREATE TABLE binding requires a catalog snapshot"));
  }
  if (catalog->find(syntax.table()) != nullptr) {
    return std::unexpected(diagnostic(SqlDiagnosticCode::kUnknownTable, syntax.table().span(),
                                      common::StatusCode::kAlreadyExists,
                                      "CREATE TABLE name already exists in the catalog snapshot"));
  }
  for (std::size_t index = 0U; index < syntax.columns().size(); ++index) {
    for (std::size_t previous = 0U; previous < index; ++previous) {
      if (syntax.columns()[previous].name.text() == syntax.columns()[index].name.text()) {
        return std::unexpected(invalid(SqlDiagnosticCode::kDuplicateOutputName,
                                       syntax.columns()[index].name.span(),
                                       "CREATE TABLE repeats a column name"));
      }
    }
  }
  const std::optional<std::size_t> event_time = find_column(syntax, syntax.event_time());
  if (!event_time.has_value()) {
    return std::unexpected(invalid(SqlDiagnosticCode::kUnknownColumn, syntax.event_time().span(),
                                   "EVENT TIME references an unknown column"));
  }
  const SqlColumnDeclaration& event_column = syntax.columns()[*event_time];
  if (event_column.type.kind() != schema::LogicalTypeKind::kTimestampNs || event_column.nullable) {
    return std::unexpected(invalid(SqlDiagnosticCode::kTypeMismatch, syntax.event_time().span(),
                                   "EVENT TIME column must be non-null TIMESTAMP_NS"));
  }

  SqlResult<std::vector<std::size_t>> ordering =
      bind_role(syntax, syntax.ordering_key(), "ORDER KEY");
  if (!ordering.has_value())
    return std::unexpected(ordering.error());
  if (std::ranges::find(*ordering, *event_time) == ordering->end()) {
    return std::unexpected(invalid(SqlDiagnosticCode::kTypeMismatch, syntax.event_time().span(),
                                   "ORDER KEY must contain the EVENT TIME column"));
  }
  SqlResult<std::vector<std::size_t>> shard = bind_role(syntax, syntax.shard_key(), "SHARD KEY");
  if (!shard.has_value())
    return std::unexpected(shard.error());
  for (const std::size_t ordinal : *shard) {
    if (syntax.columns()[ordinal].nullable) {
      return std::unexpected(invalid(SqlDiagnosticCode::kTypeMismatch,
                                     syntax.columns()[ordinal].name.span(),
                                     "SHARD KEY columns must be non-null"));
    }
  }
  SqlResult<std::vector<std::size_t>> dedup =
      bind_role(syntax, syntax.deduplication_key(), "DEDUP KEY");
  if (!dedup.has_value())
    return std::unexpected(dedup.error());
  for (const std::size_t ordinal : *dedup) {
    if (syntax.columns()[ordinal].nullable) {
      return std::unexpected(invalid(SqlDiagnosticCode::kTypeMismatch,
                                     syntax.columns()[ordinal].name.span(),
                                     "DEDUP KEY columns must be non-null"));
    }
  }
  if (!dedup->empty()) {
    for (const std::size_t ordinal : *shard) {
      if (std::ranges::find(*dedup, ordinal) == dedup->end()) {
        return std::unexpected(invalid(SqlDiagnosticCode::kTypeMismatch,
                                       syntax.shard_key().front().span(),
                                       "SHARD KEY must be a subset of DEDUP KEY"));
      }
    }
  }

  const SqlResult<std::int64_t> partition = bind_partition(syntax, *event_time);
  const SqlResult<std::int64_t> retention =
      bind_interval(syntax.retention_interval(), syntax.span(), "RETENTION", true);
  const SqlResult<std::int64_t> history = bind_interval(
      syntax.system_history_retention_interval(), syntax.span(), "SYSTEM HISTORY RETENTION", true);
  const SqlResult<std::int64_t> lateness =
      bind_interval(syntax.allowed_lateness_interval(), syntax.span(), "ALLOWED LATENESS", false);
  for (const SqlResult<std::int64_t>* value : {&partition, &retention, &history, &lateness}) {
    if (!value->has_value())
      return std::unexpected(value->error());
  }
  return BoundSqlCreateTable{std::move(syntax),
                             std::move(catalog),
                             *event_time,
                             std::move(*ordering),
                             std::move(*shard),
                             std::move(*dedup),
                             {.partition_interval_ns = *partition,
                              .retention_ns = *retention,
                              .system_history_retention_ns = *history,
                              .allowed_lateness_ns = *lateness}};
}

SqlResult<schema::TableSchema>
materialize_sql_v1_table_schema(const BoundSqlCreateTable& statement,
                                const schema::TableId table_id, const schema::SchemaId schema_id,
                                const std::span<const schema::ColumnId> column_ids) {
  const ParsedSqlCreateTable& syntax = statement.syntax();
  if (column_ids.size() != syntax.columns().size()) {
    return std::unexpected(invalid(SqlDiagnosticCode::kResourceLimit, syntax.span(),
                                   "CREATE TABLE identity count must equal column count"));
  }
  try {
    std::vector<schema::ColumnDefinition> columns;
    columns.reserve(column_ids.size());
    for (std::size_t ordinal = 0U; ordinal < column_ids.size(); ++ordinal) {
      const SqlColumnDeclaration& declaration = syntax.columns()[ordinal];
      common::Result<schema::ColumnDefinition> column = schema::ColumnDefinition::create(
          column_ids[ordinal], declaration.name.text(), declaration.type, declaration.nullable);
      if (!column.has_value()) {
        return std::unexpected(diagnostic(SqlDiagnosticCode::kTypeMismatch, declaration.name.span(),
                                          column.error().code(), column.error().message()));
      }
      columns.push_back(std::move(*column));
    }
    const schema::TableSchemaRoles roles{
        .event_time_column = column_ids[statement.event_time_ordinal()],
        .physical_ordering_key = role_ids(column_ids, statement.ordering_key_ordinals()),
        .partition_columns = {column_ids[statement.event_time_ordinal()]},
        .shard_key = role_ids(column_ids, statement.shard_key_ordinals()),
        .deduplication_key = role_ids(column_ids, statement.deduplication_key_ordinals()),
    };
    common::Result<schema::TableSchema> result =
        schema::TableSchema::create(table_id, schema_id, schema::SchemaVersion::initial(),
                                    std::nullopt, std::move(columns), roles);
    if (!result.has_value()) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kTypeMismatch, syntax.span(),
                                        result.error().code(), result.error().message()));
    }
    return std::move(*result);
  } catch (const std::bad_alloc&) {
    return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, syntax.span(),
                                      common::StatusCode::kResourceExhausted,
                                      "CREATE TABLE materialization allocation failed"));
  } catch (const std::length_error&) {
    return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, syntax.span(),
                                      common::StatusCode::kResourceExhausted,
                                      "CREATE TABLE materialization exceeds container limits"));
  }
}

} // namespace chronos::query
