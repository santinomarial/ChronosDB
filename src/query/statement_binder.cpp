#include "chronos/query/statement_binder.hpp"

#include "chronos/common/status.hpp"
#include "chronos/query/evaluator.hpp"
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

[[nodiscard]] bool signed_integer(const schema::LogicalTypeKind kind) noexcept {
  return kind >= schema::LogicalTypeKind::kInt8 && kind <= schema::LogicalTypeKind::kInt64;
}

[[nodiscard]] bool unsigned_integer(const schema::LogicalTypeKind kind) noexcept {
  return kind >= schema::LogicalTypeKind::kUInt8 && kind <= schema::LogicalTypeKind::kUInt64;
}

[[nodiscard]] std::uint16_t integer_width(const schema::LogicalTypeKind kind) noexcept {
  switch (kind) {
  case schema::LogicalTypeKind::kInt8:
  case schema::LogicalTypeKind::kUInt8:
    return 8U;
  case schema::LogicalTypeKind::kInt16:
  case schema::LogicalTypeKind::kUInt16:
    return 16U;
  case schema::LogicalTypeKind::kInt32:
  case schema::LogicalTypeKind::kUInt32:
    return 32U;
  case schema::LogicalTypeKind::kInt64:
  case schema::LogicalTypeKind::kUInt64:
    return 64U;
  default:
    return 0U;
  }
}

[[nodiscard]] bool lossless_assignment(const std::optional<schema::LogicalType>& source,
                                       const schema::LogicalType target) noexcept {
  if (!source.has_value() || *source == target)
    return true;
  if (signed_integer(source->kind()) && signed_integer(target.kind()))
    return integer_width(source->kind()) <= integer_width(target.kind());
  if (unsigned_integer(source->kind()) && unsigned_integer(target.kind()))
    return integer_width(source->kind()) <= integer_width(target.kind());
  return source->kind() == schema::LogicalTypeKind::kFloat32 &&
         target.kind() == schema::LogicalTypeKind::kFloat64;
}

[[nodiscard]] bool forbidden_insert_expression(const SqlExpression& expression) noexcept {
  if (expression.kind() == SqlExpressionKind::kColumn ||
      expression.kind() == SqlExpressionKind::kStar)
    return true;
  if (expression.kind() == SqlExpressionKind::kFunction) {
    const std::string& name = expression.text();
    if (name == "count" || name == "sum" || name == "avg" || name == "min" || name == "max" ||
        name == "var_pop" || name == "var_samp") {
      return true;
    }
  }
  return std::ranges::any_of(expression.children(), forbidden_insert_expression);
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

BoundSqlInsert::BoundSqlInsert(ParsedSqlInsert syntax,
                               std::shared_ptr<const schema::TableSchema> schema,
                               std::vector<std::size_t> target_column_ordinals,
                               BoundSqlSelect expression_plan) noexcept
    : syntax_(std::move(syntax)), schema_(std::move(schema)),
      target_column_ordinals_(std::move(target_column_ordinals)),
      expression_plan_(std::move(expression_plan)) {}

const ParsedSqlInsert& BoundSqlInsert::syntax() const noexcept {
  return syntax_;
}
const std::shared_ptr<const schema::TableSchema>& BoundSqlInsert::schema_ptr() const noexcept {
  return schema_;
}
std::span<const std::size_t> BoundSqlInsert::target_column_ordinals() const noexcept {
  return target_column_ordinals_;
}

MaterializedSqlInsert::MaterializedSqlInsert(std::shared_ptr<const schema::TableSchema> schema,
                                             std::vector<std::vector<ScalarValue>> rows) noexcept
    : schema_(std::move(schema)), rows_(std::move(rows)) {}

const std::shared_ptr<const schema::TableSchema>&
MaterializedSqlInsert::schema_ptr() const noexcept {
  return schema_;
}
std::span<const std::vector<ScalarValue>> MaterializedSqlInsert::rows() const noexcept {
  return rows_;
}

namespace detail {

class SqlStatementBinder {
public:
  [[nodiscard]] static SqlResult<BoundSqlInsert>
  bind_insert(ParsedSqlInsert syntax, const std::shared_ptr<const QueryCatalogSnapshot>& catalog,
              const SqlInsertBinderLimits limits) {
    if (catalog == nullptr || limits.maximum_rows == 0U || limits.maximum_values == 0U) {
      return std::unexpected(invalid(SqlDiagnosticCode::kResourceLimit, syntax.span(),
                                     "INSERT binding requires a catalog and nonzero limits"));
    }
    const QueryCatalogTable* table = catalog->find(syntax.table());
    if (table == nullptr) {
      return std::unexpected(invalid(SqlDiagnosticCode::kUnknownTable, syntax.table().span(),
                                     "INSERT target table does not exist in the catalog snapshot"));
    }
    if (syntax.rows().size() > limits.maximum_rows) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, syntax.span(),
                                        common::StatusCode::kResourceExhausted,
                                        "INSERT row count exceeds the limit"));
    }
    try {
      std::vector<std::size_t> targets;
      if (syntax.columns().empty()) {
        targets.resize(table->schema().columns().size());
        for (std::size_t ordinal = 0U; ordinal < targets.size(); ++ordinal)
          targets[ordinal] = ordinal;
      } else {
        targets.reserve(syntax.columns().size());
        for (const SqlIdentifier& name : syntax.columns()) {
          const schema::ColumnDefinition* column = table->schema().find_column(name.text());
          if (column == nullptr) {
            return std::unexpected(invalid(SqlDiagnosticCode::kUnknownColumn, name.span(),
                                           "INSERT target names an unknown column"));
          }
          const std::optional<std::size_t> ordinal = table->schema().column_ordinal(column->id());
          if (!ordinal.has_value()) {
            return std::unexpected(invalid(SqlDiagnosticCode::kUnknownColumn, name.span(),
                                           "INSERT target column has no schema ordinal"));
          }
          if (std::ranges::find(targets, *ordinal) != targets.end()) {
            return std::unexpected(invalid(SqlDiagnosticCode::kDuplicateOutputName, name.span(),
                                           "INSERT target column list repeats a column"));
          }
          targets.push_back(*ordinal);
        }
      }
      for (std::size_t ordinal = 0U; ordinal < table->schema().columns().size(); ++ordinal) {
        if (!table->schema().columns()[ordinal].nullable() &&
            std::ranges::find(targets, ordinal) == targets.end()) {
          return std::unexpected(invalid(SqlDiagnosticCode::kTypeMismatch, syntax.span(),
                                         "INSERT omits a non-null column without a default"));
        }
      }

      std::size_t value_count = 0U;
      for (const std::vector<SqlExpression>& row : syntax.rows()) {
        if (row.size() != targets.size()) {
          return std::unexpected(invalid(SqlDiagnosticCode::kTypeMismatch, syntax.span(),
                                         "INSERT row width does not match its target column list"));
        }
        if (row.size() > limits.maximum_values - value_count) {
          return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, syntax.span(),
                                            common::StatusCode::kResourceExhausted,
                                            "INSERT value count exceeds the limit"));
        }
        value_count += row.size();
      }

      std::vector<SqlSelectItem> items;
      items.reserve(value_count);
      std::size_t flat = 0U;
      for (const std::vector<SqlExpression>& row : syntax.rows()) {
        for (std::size_t column = 0U; column < row.size(); ++column) {
          const SqlExpression& value = row[column];
          if (forbidden_insert_expression(value)) {
            return std::unexpected(invalid(
                SqlDiagnosticCode::kTypeMismatch, value.span(),
                "INSERT VALUES expressions cannot reference columns, stars, or aggregates"));
          }
          const schema::LogicalType target_type = table->schema().columns()[targets[column]].type();
          const SourceSpan synthetic{.begin = {.byte_offset = syntax.span().byte_length + flat + 1U,
                                               .line = 1U,
                                               .column = 1U},
                                     .byte_length = 0U};
          std::vector<SqlExpression> child;
          child.push_back(value);
          SqlExpression cast{
              SqlExpressionKind::kCast, SqlLiteralKind::kNull, SqlOperator::kNone, {}, {},
              std::move(child),         target_type,           synthetic};
          SqlIdentifier alias{"insert_value_" + std::to_string(flat), false, synthetic};
          items.push_back(SqlSelectItem{SqlSelectItemKind::kExpression,
                                        std::vector<SqlExpression>{std::move(cast)}, std::nullopt,
                                        std::move(alias), synthetic});
          ++flat;
        }
      }
      ParsedSqlSelect expression_syntax{SqlSelectMode::kSelect,
                                        std::move(items),
                                        SqlSource{.table = syntax.table(), .alias = std::nullopt},
                                        std::nullopt,
                                        std::nullopt,
                                        {},
                                        {},
                                        {},
                                        {},
                                        std::nullopt,
                                        syntax.span()};
      SqlResult<BoundSqlSelect> plan =
          bind_sql_v1_select(std::move(expression_syntax), catalog,
                             {.maximum_sources = 1U,
                              .maximum_bound_expressions = 262'144U,
                              .maximum_output_columns = limits.maximum_values});
      if (!plan.has_value())
        return std::unexpected(plan.error());

      flat = 0U;
      for (std::size_t row = 0U; row < syntax.rows().size(); ++row) {
        for (std::size_t column = 0U; column < syntax.rows()[row].size(); ++column) {
          const SqlExpression* wrapper = plan->syntax().items()[flat].expression();
          const SqlExpression& operand = wrapper->children().front();
          const BoundExpressionInfo* information = plan->find_expression(operand.span());
          const schema::LogicalType target_type = table->schema().columns()[targets[column]].type();
          if (information == nullptr || !lossless_assignment(information->type, target_type)) {
            return std::unexpected(
                invalid(SqlDiagnosticCode::kTypeMismatch, syntax.rows()[row][column].span(),
                        "INSERT value requires an explicit SQL v1 conversion to its target type"));
          }
          ++flat;
        }
      }
      return BoundSqlInsert{std::move(syntax), table->schema_ptr(), std::move(targets),
                            std::move(*plan)};
    } catch (const std::bad_alloc&) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, syntax.span(),
                                        common::StatusCode::kResourceExhausted,
                                        "INSERT binding allocation failed"));
    } catch (const std::length_error&) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, syntax.span(),
                                        common::StatusCode::kResourceExhausted,
                                        "INSERT binding exceeds container limits"));
    }
  }
};

} // namespace detail

SqlResult<BoundSqlInsert>
bind_sql_v1_insert(ParsedSqlInsert syntax,
                   const std::shared_ptr<const QueryCatalogSnapshot>& catalog,
                   const SqlInsertBinderLimits limits) {
  return detail::SqlStatementBinder::bind_insert(std::move(syntax), catalog, limits);
}

SqlResult<MaterializedSqlInsert> materialize_sql_v1_insert_rows(const BoundSqlInsert& statement) {
  try {
    std::vector<std::vector<ScalarValue>> rows;
    rows.reserve(statement.syntax().rows().size());
    std::size_t flat = 0U;
    for (std::size_t row = 0U; row < statement.syntax().rows().size(); ++row) {
      std::vector<ScalarValue> values;
      values.reserve(statement.schema_ptr()->columns().size());
      for (const schema::ColumnDefinition& column : statement.schema_ptr()->columns())
        values.push_back(ScalarValue::null(column.type()));
      for (std::size_t column = 0U; column < statement.target_column_ordinals().size(); ++column) {
        const SqlExpression* expression =
            statement.expression_plan_.syntax().items()[flat].expression();
        SqlResult<ScalarValue> value =
            evaluate_sql_v1_expression(statement.expression_plan_, *expression);
        if (!value.has_value())
          return std::unexpected(value.error());
        const std::size_t ordinal = statement.target_column_ordinals()[column];
        if (value->is_null() && !statement.schema_ptr()->columns()[ordinal].nullable()) {
          return std::unexpected(invalid(SqlDiagnosticCode::kTypeMismatch,
                                         statement.syntax().rows()[row][column].span(),
                                         "INSERT evaluated NULL for a non-null column"));
        }
        values[ordinal] = std::move(*value);
        ++flat;
      }
      rows.push_back(std::move(values));
    }
    return MaterializedSqlInsert{statement.schema_ptr(), std::move(rows)};
  } catch (const std::bad_alloc&) {
    return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, statement.syntax().span(),
                                      common::StatusCode::kResourceExhausted,
                                      "INSERT materialization allocation failed"));
  } catch (const std::length_error&) {
    return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, statement.syntax().span(),
                                      common::StatusCode::kResourceExhausted,
                                      "INSERT materialization exceeds container limits"));
  }
}

} // namespace chronos::query
