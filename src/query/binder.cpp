#include "chronos/query/binder.hpp"

#include "chronos/common/status.hpp"
#include "chronos/query/literal.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] bool same_span(const SourceSpan& left, const SourceSpan& right) noexcept {
  return left == right;
}

struct BindingFailure {
  SqlDiagnostic diagnostic;
};

[[nodiscard]] bool signed_integer(const schema::LogicalTypeKind kind) noexcept {
  return kind >= schema::LogicalTypeKind::kInt8 && kind <= schema::LogicalTypeKind::kInt64;
}

[[nodiscard]] bool unsigned_integer(const schema::LogicalTypeKind kind) noexcept {
  return kind >= schema::LogicalTypeKind::kUInt8 && kind <= schema::LogicalTypeKind::kUInt64;
}

[[nodiscard]] bool floating(const schema::LogicalTypeKind kind) noexcept {
  return kind == schema::LogicalTypeKind::kFloat32 || kind == schema::LogicalTypeKind::kFloat64;
}

[[nodiscard]] bool numeric(const schema::LogicalTypeKind kind) noexcept {
  return signed_integer(kind) || unsigned_integer(kind) || floating(kind) ||
         kind == schema::LogicalTypeKind::kDecimal;
}

[[nodiscard]] std::uint16_t integer_rank(const schema::LogicalTypeKind kind) noexcept {
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

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

template <typename Value>
[[nodiscard]] const Value* optional_pointer(const std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(value.value()) : nullptr;
}

} // namespace

BoundSqlSource::BoundSqlSource(std::string exposed_name, const bool exposed_name_quoted,
                               std::shared_ptr<const schema::TableSchema> schema) noexcept
    : exposed_name_(std::move(exposed_name)), exposed_name_quoted_(exposed_name_quoted),
      schema_(std::move(schema)) {}

const std::string& BoundSqlSource::exposed_name() const noexcept {
  return exposed_name_;
}
bool BoundSqlSource::exposed_name_quoted() const noexcept {
  return exposed_name_quoted_;
}
const std::shared_ptr<const schema::TableSchema>& BoundSqlSource::schema_ptr() const noexcept {
  return schema_;
}

BoundSqlSelect::BoundSqlSelect(
    ParsedSqlSelect syntax, std::shared_ptr<const QueryCatalogSnapshot> catalog,
    std::vector<BoundSqlSource> sources, std::vector<BoundColumnReference> column_references,
    std::vector<BoundExpressionInfo> expressions, std::vector<BoundOutputColumn> outputs,
    std::optional<BoundLatestBy> latest_by, std::vector<BoundAsofJoin> asof_joins) noexcept
    : syntax_(std::move(syntax)), catalog_(std::move(catalog)), sources_(std::move(sources)),
      column_references_(std::move(column_references)), expressions_(std::move(expressions)),
      outputs_(std::move(outputs)), latest_by_(std::move(latest_by)),
      asof_joins_(std::move(asof_joins)) {}

const ParsedSqlSelect& BoundSqlSelect::syntax() const noexcept {
  return syntax_;
}
const std::shared_ptr<const QueryCatalogSnapshot>& BoundSqlSelect::catalog() const noexcept {
  return catalog_;
}
std::span<const BoundSqlSource> BoundSqlSelect::sources() const noexcept {
  return sources_;
}
std::span<const BoundColumnReference> BoundSqlSelect::column_references() const noexcept {
  return column_references_;
}
std::span<const BoundExpressionInfo> BoundSqlSelect::expressions() const noexcept {
  return expressions_;
}
std::span<const BoundOutputColumn> BoundSqlSelect::outputs() const noexcept {
  return outputs_;
}
const std::optional<BoundLatestBy>& BoundSqlSelect::latest_by() const noexcept {
  return latest_by_;
}
std::span<const BoundAsofJoin> BoundSqlSelect::asof_joins() const noexcept {
  return asof_joins_;
}

const BoundExpressionInfo* BoundSqlSelect::find_expression(const SourceSpan& span) const noexcept {
  const auto found = std::ranges::find_if(expressions_, [&](const BoundExpressionInfo& value) {
    return same_span(value.expression_span, span);
  });
  return found == expressions_.end() ? nullptr : &*found;
}

const BoundColumnReference*
BoundSqlSelect::find_column_reference(const SourceSpan& span) const noexcept {
  const auto found =
      std::ranges::find_if(column_references_, [&](const BoundColumnReference& value) {
        return same_span(value.expression_span, span);
      });
  return found == column_references_.end() ? nullptr : &*found;
}

namespace detail {

class SqlBinder {
public:
  SqlBinder(ParsedSqlSelect syntax, std::shared_ptr<const QueryCatalogSnapshot> catalog,
            const SqlBinderLimits limits) noexcept
      : syntax_(std::move(syntax)), catalog_(std::move(catalog)), limits_(limits) {}

  [[nodiscard]] SqlResult<BoundSqlSelect> run() {
    if (catalog_ == nullptr || limits_.maximum_sources == 0U ||
        limits_.maximum_bound_expressions == 0U || limits_.maximum_output_columns == 0U) {
      return std::unexpected(make_diagnostic(SqlDiagnosticCode::kResourceLimit, syntax_.span(),
                                             common::StatusCode::kInvalidArgument,
                                             "SQL binder requires a catalog and nonzero limits"));
    }
    try {
      if (syntax_.system_time().has_value() &&
          !parse_sql_timestamp_ns_literal(syntax_.system_time().value()).has_value()) {
        fail(SqlDiagnosticCode::kInvalidLiteral, syntax_.span(),
             "FOR SYSTEM_TIME contains an invalid TIMESTAMP literal");
      }
      add_source(syntax_.source());
      for (const SqlAsofJoin& join : syntax_.asof_joins())
        add_source(join.source);

      std::size_t join_ordinal = 0U;
      for (const SqlAsofJoin& join : syntax_.asof_joins()) {
        const Inferred condition = bind_expression(join.condition);
        require_boolean(condition, join.condition.span(), "ASOF JOIN ON must be BOOL");
        if (condition.contains_aggregate) {
          fail(SqlDiagnosticCode::kTypeMismatch, join.condition.span(),
               "ASOF JOIN ON cannot contain an aggregate");
        }
        bind_asof_shape(join, join_ordinal + 1U);
        ++join_ordinal;
      }
      if (syntax_.latest_by().has_value()) {
        BoundLatestBy bound_latest;
        for (const SqlIdentifier& key : syntax_.latest_by()->keys) {
          const BoundColumnReference& bound_key = bind_column_in_source(key, 0U);
          bound_latest.key_column_ordinals.push_back(bound_key.column_ordinal);
        }
        const Inferred timestamp = bind_expression(syntax_.latest_by()->timestamp);
        if (!timestamp.type.has_value() ||
            timestamp.type->kind() != schema::LogicalTypeKind::kTimestampNs ||
            timestamp.contains_aggregate ||
            source_mask(syntax_.latest_by()->timestamp) != std::uint64_t{1U}) {
          fail(SqlDiagnosticCode::kTypeMismatch, syntax_.latest_by()->timestamp.span(),
               "LATEST BY ON requires a primary-source TIMESTAMP_NS expression");
        }
        bound_latest.timestamp_expression_span = syntax_.latest_by()->timestamp.span();
        latest_by_ = std::move(bound_latest);
      }
      if (syntax_.where() != nullptr) {
        const Inferred predicate = bind_expression(*syntax_.where());
        require_boolean(predicate, syntax_.where()->span(), "WHERE expression must be BOOL");
        if (predicate.contains_aggregate) {
          fail(SqlDiagnosticCode::kTypeMismatch, syntax_.where()->span(),
               "WHERE cannot contain an aggregate");
        }
      }
      for (const SqlExpression& expression : syntax_.group_by()) {
        const Inferred group = bind_expression(expression);
        if (group.contains_aggregate || !group.type.has_value()) {
          fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
               "GROUP BY requires typed non-aggregate expressions");
        }
      }
      bind_outputs();
      validate_unique_output_names();
      validate_grouping();
      bind_order_by();
      return BoundSqlSelect{std::move(syntax_),      std::move(catalog_),
                            std::move(sources_),     std::move(column_references_),
                            std::move(expressions_), std::move(outputs_),
                            std::move(latest_by_),   std::move(asof_joins_)};
    } catch (BindingFailure& failure) {
      return std::unexpected(std::move(failure.diagnostic));
    } catch (const std::bad_alloc&) {
      return std::unexpected(make_diagnostic(SqlDiagnosticCode::kResourceLimit, syntax_.span(),
                                             common::StatusCode::kResourceExhausted,
                                             "SQL binding allocation failed"));
    } catch (const std::length_error&) {
      return std::unexpected(make_diagnostic(SqlDiagnosticCode::kResourceLimit, syntax_.span(),
                                             common::StatusCode::kResourceExhausted,
                                             "Bound SQL plan exceeds container limits"));
    }
  }

private:
  struct Inferred {
    std::optional<schema::LogicalType> type;
    bool nullable{};
    bool contains_aggregate{};
  };

  [[nodiscard]] static SqlDiagnostic make_diagnostic(const SqlDiagnosticCode code,
                                                     const SourceSpan span,
                                                     const common::StatusCode status,
                                                     const std::string_view message) {
    return SqlDiagnostic{code, span, common::Status{status, std::string{message}}};
  }

  [[noreturn]] static void fail(const SqlDiagnosticCode code, const SourceSpan span,
                                const std::string_view message) {
    throw BindingFailure{
        make_diagnostic(code, span, common::StatusCode::kInvalidArgument, message)};
  }

  void add_source(const SqlSource& source) {
    if (sources_.size() >= limits_.maximum_sources) {
      fail(SqlDiagnosticCode::kResourceLimit, source.table.span(),
           "SQL source count exceeds the limit");
    }
    const QueryCatalogTable* table = catalog_->find(source.table);
    if (table == nullptr) {
      fail(SqlDiagnosticCode::kUnknownTable, source.table.span(),
           "SQL source table does not exist in the catalog snapshot");
    }
    const SqlIdentifier& exposed = source.alias.has_value() ? *source.alias : source.table;
    for (const BoundSqlSource& existing : sources_) {
      if (existing.exposed_name() == exposed.text() &&
          existing.exposed_name_quoted() == exposed.quoted()) {
        fail(SqlDiagnosticCode::kAmbiguousColumn, exposed.span(),
             "SQL source exposes a duplicate table name or alias");
      }
    }
    sources_.push_back(BoundSqlSource{exposed.text(), exposed.quoted(), table->schema_ptr()});
  }

  [[nodiscard]] const BoundColumnReference&
  bind_column_name(const std::span<const SqlIdentifier> name, const SourceSpan span) {
    const schema::ColumnDefinition* found_column = nullptr;
    std::size_t found_source = 0U;
    std::size_t found_ordinal = 0U;
    if (name.size() == 2U) {
      const auto source = std::ranges::find_if(sources_, [&](const BoundSqlSource& candidate) {
        return candidate.exposed_name() == name[0].text() &&
               candidate.exposed_name_quoted() == name[0].quoted();
      });
      if (source == sources_.end()) {
        fail(SqlDiagnosticCode::kUnknownTable, name[0].span(),
             "Qualified column names an unknown table alias");
      }
      found_source = static_cast<std::size_t>(source - sources_.begin());
      found_column = source->schema_ptr()->find_column(name[1].text());
      if (found_column != nullptr) {
        const std::optional<std::size_t> ordinal =
            source->schema_ptr()->column_ordinal(found_column->id());
        const std::size_t* ordinal_pointer = optional_pointer(ordinal);
        if (ordinal_pointer == nullptr) {
          fail(SqlDiagnosticCode::kUnknownColumn, span,
               "Catalog column identity has no schema ordinal");
        }
        found_ordinal = *ordinal_pointer;
      }
    } else {
      for (std::size_t source = 0U; source < sources_.size(); ++source) {
        const schema::ColumnDefinition* candidate =
            sources_[source].schema_ptr()->find_column(name.front().text());
        if (candidate == nullptr)
          continue;
        if (found_column != nullptr) {
          fail(SqlDiagnosticCode::kAmbiguousColumn, span,
               "Unqualified column reference is ambiguous");
        }
        found_column = candidate;
        found_source = source;
        const std::optional<std::size_t> ordinal =
            sources_[source].schema_ptr()->column_ordinal(candidate->id());
        const std::size_t* ordinal_pointer = optional_pointer(ordinal);
        if (ordinal_pointer == nullptr) {
          fail(SqlDiagnosticCode::kUnknownColumn, span,
               "Catalog column identity has no schema ordinal");
        }
        found_ordinal = *ordinal_pointer;
      }
    }
    if (found_column == nullptr) {
      fail(SqlDiagnosticCode::kUnknownColumn, span,
           "Column does not exist in the bound source schemas");
    }
    column_references_.push_back(BoundColumnReference{
        .expression_span = span,
        .source_ordinal = found_source,
        .column_ordinal = found_ordinal,
        .table_id = sources_[found_source].schema_ptr()->table_id(),
        .column_id = found_column->id(),
        .type = found_column->type(),
        .nullable = found_column->nullable(),
    });
    return column_references_.back();
  }

  [[nodiscard]] const BoundColumnReference& bind_column_in_source(const SqlIdentifier& identifier,
                                                                  const std::size_t source) {
    if (source >= sources_.size())
      fail(SqlDiagnosticCode::kUnknownTable, identifier.span(), "SQL source does not exist");
    const schema::ColumnDefinition* column =
        sources_[source].schema_ptr()->find_column(identifier.text());
    if (column == nullptr) {
      fail(SqlDiagnosticCode::kUnknownColumn, identifier.span(),
           "Column does not exist in the required SQL source");
    }
    const std::optional<std::size_t> ordinal =
        sources_[source].schema_ptr()->column_ordinal(column->id());
    if (!ordinal.has_value()) {
      fail(SqlDiagnosticCode::kUnknownColumn, identifier.span(),
           "Catalog column identity has no schema ordinal");
    }
    column_references_.push_back(BoundColumnReference{
        .expression_span = identifier.span(),
        .source_ordinal = source,
        .column_ordinal = *ordinal,
        .table_id = sources_[source].schema_ptr()->table_id(),
        .column_id = column->id(),
        .type = column->type(),
        .nullable = column->nullable(),
    });
    return column_references_.back();
  }

  void record(const SqlExpression& expression, const Inferred& inferred) {
    if (expressions_.size() >= limits_.maximum_bound_expressions) {
      fail(SqlDiagnosticCode::kResourceLimit, expression.span(),
           "Bound expression count exceeds the limit");
    }
    expressions_.push_back(BoundExpressionInfo{.expression_span = expression.span(),
                                               .type = inferred.type,
                                               .output_ordinal = std::nullopt,
                                               .nullable = inferred.nullable,
                                               .contains_aggregate = inferred.contains_aggregate});
  }

  [[nodiscard]] Inferred bind_expression(const SqlExpression& expression) {
    Inferred inferred;
    switch (expression.kind()) {
    case SqlExpressionKind::kStar:
      inferred = {};
      break;
    case SqlExpressionKind::kColumn: {
      const BoundColumnReference& column = bind_column_name(expression.name(), expression.span());
      inferred = {.type = column.type, .nullable = column.nullable, .contains_aggregate = false};
      break;
    }
    case SqlExpressionKind::kLiteral:
      inferred = bind_literal(expression);
      break;
    case SqlExpressionKind::kCast: {
      const Inferred operand = bind_expression(expression.children().front());
      inferred = {.type = expression.cast_type(),
                  .nullable = operand.nullable,
                  .contains_aggregate = operand.contains_aggregate};
      break;
    }
    case SqlExpressionKind::kUnary: {
      const Inferred operand = bind_expression(expression.children().front());
      if (expression.operation() == SqlOperator::kNot) {
        require_boolean(operand, expression.span(), "NOT operand must be BOOL");
        inferred = {.type = type(schema::LogicalTypeKind::kBool),
                    .nullable = operand.nullable,
                    .contains_aggregate = operand.contains_aggregate};
      } else {
        if (!operand.type.has_value() || !numeric(operand.type->kind())) {
          fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
               "Unary numeric operator requires numeric operand");
        }
        if (expression.operation() == SqlOperator::kNegative &&
            unsigned_integer(operand.type->kind())) {
          fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
               "Unary negation of an unsigned value requires an explicit signed CAST");
        }
        inferred = operand;
      }
      break;
    }
    case SqlExpressionKind::kBinary:
      inferred = bind_binary(expression);
      break;
    case SqlExpressionKind::kIsNull: {
      const Inferred operand = bind_expression(expression.children().front());
      inferred = {.type = type(schema::LogicalTypeKind::kBool),
                  .nullable = false,
                  .contains_aggregate = operand.contains_aggregate};
      break;
    }
    case SqlExpressionKind::kBetween:
    case SqlExpressionKind::kIn:
      inferred = bind_multi_comparison(expression);
      break;
    case SqlExpressionKind::kFunction:
      inferred = bind_function(expression);
      break;
    }
    record(expression, inferred);
    return inferred;
  }

  [[nodiscard]] static Inferred bind_literal(const SqlExpression& expression) {
    switch (expression.literal_kind()) {
    case SqlLiteralKind::kNull:
      return {.type = std::nullopt, .nullable = true};
    case SqlLiteralKind::kBoolean:
      return {.type = type(schema::LogicalTypeKind::kBool), .nullable = false};
    case SqlLiteralKind::kInteger: {
      if (!parse_sql_integer_literal(expression.text()).has_value()) {
        fail(SqlDiagnosticCode::kInvalidNumber, expression.span(),
             "Integer literal exceeds the default INT64 range");
      }
      return {.type = type(schema::LogicalTypeKind::kInt64), .nullable = false};
    }
    case SqlLiteralKind::kFloat: {
      if (!parse_sql_float_literal(expression.text()).has_value()) {
        fail(SqlDiagnosticCode::kInvalidNumber, expression.span(),
             "Floating literal is invalid or exceeds FLOAT64");
      }
      return {.type = type(schema::LogicalTypeKind::kFloat64), .nullable = false};
    }
    case SqlLiteralKind::kString:
      return {.type = type(schema::LogicalTypeKind::kString), .nullable = false};
    case SqlLiteralKind::kBinary:
      return {.type = type(schema::LogicalTypeKind::kBinary), .nullable = false};
    case SqlLiteralKind::kTimestamp:
      if (!parse_sql_timestamp_ns_literal(expression.text()).has_value()) {
        fail(SqlDiagnosticCode::kInvalidLiteral, expression.span(),
             "TIMESTAMP literal is invalid or outside the nanosecond range");
      }
      return {.type = type(schema::LogicalTypeKind::kTimestampNs), .nullable = false};
    case SqlLiteralKind::kDate:
      if (!parse_sql_date_literal(expression.text()).has_value()) {
        fail(SqlDiagnosticCode::kInvalidLiteral, expression.span(), "DATE literal is invalid");
      }
      return {.type = type(schema::LogicalTypeKind::kDate), .nullable = false};
    case SqlLiteralKind::kInterval:
      if (!parse_sql_interval_ns_literal(expression.text()).has_value()) {
        fail(SqlDiagnosticCode::kInvalidLiteral, expression.span(), "INTERVAL literal is invalid");
      }
      return {.type = type(schema::LogicalTypeKind::kInt64), .nullable = false};
    case SqlLiteralKind::kUuid:
      if (!parse_sql_uuid_literal(expression.text()).has_value()) {
        fail(SqlDiagnosticCode::kInvalidLiteral, expression.span(), "UUID literal is invalid");
      }
      return {.type = type(schema::LogicalTypeKind::kUuid), .nullable = false};
    }
    fail(SqlDiagnosticCode::kTypeMismatch, expression.span(), "Unknown SQL literal kind");
  }

  [[nodiscard]] std::optional<schema::LogicalType> static common_type(
      const std::optional<schema::LogicalType>& left,
      const std::optional<schema::LogicalType>& right) {
    if (!left.has_value())
      return right;
    if (!right.has_value())
      return left;
    if (*left == *right)
      return left;
    if (signed_integer(left->kind()) && signed_integer(right->kind())) {
      return integer_rank(left->kind()) >= integer_rank(right->kind()) ? left : right;
    }
    if (unsigned_integer(left->kind()) && unsigned_integer(right->kind())) {
      return integer_rank(left->kind()) >= integer_rank(right->kind()) ? left : right;
    }
    if (floating(left->kind()) && floating(right->kind())) {
      return type(schema::LogicalTypeKind::kFloat64);
    }
    return std::nullopt;
  }

  [[nodiscard]] Inferred bind_binary(const SqlExpression& expression) {
    const Inferred left = bind_expression(expression.children()[0]);
    const Inferred right = bind_expression(expression.children()[1]);
    const bool aggregate = left.contains_aggregate || right.contains_aggregate;
    const bool nullable = left.nullable || right.nullable;
    if (expression.operation() == SqlOperator::kAnd || expression.operation() == SqlOperator::kOr) {
      require_boolean(left, expression.children()[0].span(), "Boolean operator requires BOOL");
      require_boolean(right, expression.children()[1].span(), "Boolean operator requires BOOL");
      return {.type = type(schema::LogicalTypeKind::kBool),
              .nullable = nullable,
              .contains_aggregate = aggregate};
    }
    const std::optional<schema::LogicalType> common = common_type(left.type, right.type);
    if (!common.has_value()) {
      fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
           "Binary operands do not have a permitted common type");
    }
    switch (expression.operation()) {
    case SqlOperator::kEqual:
    case SqlOperator::kNotEqual:
    case SqlOperator::kLess:
    case SqlOperator::kLessEqual:
    case SqlOperator::kGreater:
    case SqlOperator::kGreaterEqual:
      return {.type = type(schema::LogicalTypeKind::kBool),
              .nullable = nullable,
              .contains_aggregate = aggregate};
    case SqlOperator::kAdd:
    case SqlOperator::kSubtract:
    case SqlOperator::kMultiply:
    case SqlOperator::kDivide:
    case SqlOperator::kRemainder:
      if (!numeric(common->kind())) {
        fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
             "Arithmetic operators require numeric operands");
      }
      return {.type = common, .nullable = nullable, .contains_aggregate = aggregate};
    default:
      fail(SqlDiagnosticCode::kTypeMismatch, expression.span(), "Unsupported binary operator");
    }
  }

  [[nodiscard]] Inferred bind_multi_comparison(const SqlExpression& expression) {
    std::optional<schema::LogicalType> common;
    bool nullable = false;
    bool aggregate = false;
    for (const SqlExpression& child : expression.children()) {
      const Inferred value = bind_expression(child);
      common = common_type(common, value.type);
      if (!common.has_value() && value.type.has_value()) {
        fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
             "BETWEEN/IN values do not have a permitted common type");
      }
      nullable = nullable || value.nullable;
      aggregate = aggregate || value.contains_aggregate;
    }
    if (!common.has_value()) {
      fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
           "BETWEEN/IN cannot contain only untyped NULL values");
    }
    return {.type = type(schema::LogicalTypeKind::kBool),
            .nullable = nullable,
            .contains_aggregate = aggregate};
  }

  [[nodiscard]] Inferred bind_function(const SqlExpression& expression) {
    std::vector<Inferred> arguments;
    arguments.reserve(expression.children().size());
    for (const SqlExpression& child : expression.children())
      arguments.push_back(bind_expression(child));
    const std::string& name = expression.text();
    if (name == "count") {
      if (arguments.size() != 1U || arguments.front().contains_aggregate) {
        fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
             "COUNT requires one non-aggregate argument or *");
      }
      return {.type = type(schema::LogicalTypeKind::kInt64),
              .nullable = false,
              .contains_aggregate = true};
    }
    if (name == "sum" || name == "avg" || name == "min" || name == "max" || name == "var_pop" ||
        name == "var_samp") {
      if (arguments.size() != 1U) {
        fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
             "Aggregate requires exactly one input");
      }
      const schema::LogicalType* argument_type = optional_pointer(arguments.front().type);
      if (argument_type == nullptr ||
          (name != "min" && name != "max" && !numeric(argument_type->kind())) ||
          arguments.front().contains_aggregate) {
        fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
             "Aggregate has an invalid input type or nested aggregate");
      }
      schema::LogicalType result = *argument_type;
      if (name == "avg" || name == "var_pop" || name == "var_samp") {
        result = type(schema::LogicalTypeKind::kFloat64);
      }
      return {.type = result, .nullable = true, .contains_aggregate = true};
    }
    if (name == "abs") {
      if (arguments.size() != 1U) {
        fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
             "ABS requires one numeric argument");
      }
      const schema::LogicalType* argument_type = optional_pointer(arguments.front().type);
      if (argument_type == nullptr || !numeric(argument_type->kind())) {
        fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
             "ABS requires one numeric argument");
      }
      return arguments.front();
    }
    if (name == "lower" || name == "upper") {
      if (arguments.size() != 1U) {
        fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
             "LOWER/UPPER require one STRING or SYMBOL argument");
      }
      const schema::LogicalType* argument_type = optional_pointer(arguments.front().type);
      if (argument_type == nullptr || (argument_type->kind() != schema::LogicalTypeKind::kString &&
                                       argument_type->kind() != schema::LogicalTypeKind::kSymbol)) {
        fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
             "LOWER/UPPER require one STRING or SYMBOL argument");
      }
      return arguments.front();
    }
    if (name == "coalesce") {
      if (arguments.empty())
        fail(SqlDiagnosticCode::kTypeMismatch, expression.span(), "COALESCE requires arguments");
      std::optional<schema::LogicalType> common;
      bool aggregate = false;
      for (const Inferred& argument : arguments) {
        common = common_type(common, argument.type);
        aggregate = aggregate || argument.contains_aggregate;
      }
      if (!common.has_value())
        fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
             "COALESCE arguments have no common type");
      return {.type = common,
              .nullable = std::ranges::all_of(arguments, &Inferred::nullable),
              .contains_aggregate = aggregate};
    }
    if (name == "time_bucket") {
      if (arguments.size() != 2U) {
        fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
             "time_bucket requires INTERVAL and TIMESTAMP_NS");
      }
      const schema::LogicalType* timestamp_type = optional_pointer(arguments[1].type);
      if (expression.children()[0].literal_kind() != SqlLiteralKind::kInterval ||
          timestamp_type == nullptr ||
          timestamp_type->kind() != schema::LogicalTypeKind::kTimestampNs) {
        fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
             "time_bucket requires INTERVAL and TIMESTAMP_NS");
      }
      return {.type = type(schema::LogicalTypeKind::kTimestampNs),
              .nullable = arguments[1].nullable,
              .contains_aggregate = arguments[1].contains_aggregate};
    }
    fail(SqlDiagnosticCode::kUnsupportedSyntax, expression.span(),
         "SQL function is not supported in v1");
  }

  static void require_boolean(const Inferred& value, const SourceSpan span,
                              const std::string_view message) {
    if (!value.type.has_value() || value.type->kind() != schema::LogicalTypeKind::kBool) {
      fail(SqlDiagnosticCode::kTypeMismatch, span, message);
    }
  }

  void append_output(std::string name, const bool name_quoted,
                     const schema::LogicalType& output_type,
                     const std::optional<SourceSpan> expression_span,
                     const std::optional<std::size_t> source_ordinal,
                     const std::optional<std::size_t> column_ordinal, const bool nullable,
                     const bool aggregate, const SourceSpan diagnostic_span) {
    if (outputs_.size() >= limits_.maximum_output_columns) {
      fail(SqlDiagnosticCode::kResourceLimit, diagnostic_span,
           "SQL output column count exceeds the limit");
    }
    outputs_.push_back(BoundOutputColumn{.name = std::move(name),
                                         .name_quoted = name_quoted,
                                         .type = output_type,
                                         .expression_span = expression_span,
                                         .source_ordinal = source_ordinal,
                                         .column_ordinal = column_ordinal,
                                         .nullable = nullable,
                                         .contains_aggregate = aggregate});
  }

  void bind_outputs() {
    for (const SqlSelectItem& item : syntax_.items()) {
      if (item.kind() == SqlSelectItemKind::kExpression) {
        const SqlExpression* expression = item.expression();
        if (expression == nullptr) {
          fail(SqlDiagnosticCode::kTypeMismatch, item.span(),
               "SELECT expression item has no expression");
        }
        const Inferred inferred = bind_expression(*expression);
        const schema::LogicalType* inferred_type = optional_pointer(inferred.type);
        if (inferred_type == nullptr) {
          fail(SqlDiagnosticCode::kTypeMismatch, item.span(),
               "SELECT output expression has no resolved type");
        }
        std::string name = "expression";
        bool name_quoted = false;
        const SqlIdentifier* alias = optional_pointer(item.alias());
        if (alias != nullptr) {
          name = alias->text();
          name_quoted = alias->quoted();
        } else if (expression->kind() == SqlExpressionKind::kColumn) {
          name = expression->name().back().text();
          name_quoted = expression->name().back().quoted();
        } else if (expression->kind() == SqlExpressionKind::kFunction) {
          name = expression->text();
        }
        append_output(std::move(name), name_quoted, *inferred_type, expression->span(),
                      std::nullopt, std::nullopt, inferred.nullable, inferred.contains_aggregate,
                      item.span());
        continue;
      }
      if (item.kind() == SqlSelectItemKind::kQualifiedStar) {
        const auto source = std::ranges::find_if(sources_, [&](const BoundSqlSource& candidate) {
          return candidate.exposed_name() == item.qualifier()->text() &&
                 candidate.exposed_name_quoted() == item.qualifier()->quoted();
        });
        if (source == sources_.end()) {
          fail(SqlDiagnosticCode::kUnknownTable, item.span(),
               "Qualified star names an unknown source");
        }
        expand_source(static_cast<std::size_t>(source - sources_.begin()), false, item.span());
      } else {
        expand_all_sources(item.span());
      }
    }
  }

  void expand_all_sources(const SourceSpan span) {
    std::unordered_map<std::string_view, std::size_t> name_counts;
    for (const BoundSqlSource& source : sources_) {
      for (const schema::ColumnDefinition& column : source.schema_ptr()->columns()) {
        if (name_counts.size() > limits_.maximum_output_columns) {
          fail(SqlDiagnosticCode::kResourceLimit, span,
               "SQL star expansion exceeds the output column limit");
        }
        ++name_counts[column.name()];
      }
    }
    for (std::size_t source = 0U; source < sources_.size(); ++source) {
      std::size_t column_ordinal = 0U;
      for (const schema::ColumnDefinition& column : sources_[source].schema_ptr()->columns()) {
        std::string name;
        if (name_counts[column.name()] > 1U) {
          name = sources_[source].exposed_name();
          name.push_back('.');
        }
        name.append(column.name());
        append_output(std::move(name), false, column.type(), std::nullopt, source, column_ordinal,
                      column.nullable(), false, span);
        ++column_ordinal;
      }
    }
  }

  void expand_source(const std::size_t source, const bool qualify, const SourceSpan span) {
    std::size_t column_ordinal = 0U;
    for (const schema::ColumnDefinition& column : sources_[source].schema_ptr()->columns()) {
      std::string name;
      if (qualify) {
        name = sources_[source].exposed_name();
        name.push_back('.');
      }
      name.append(column.name());
      append_output(std::move(name), false, column.type(), std::nullopt, source, column_ordinal,
                    column.nullable(), false, span);
      ++column_ordinal;
    }
  }

  void validate_unique_output_names() const {
    for (std::size_t index = 0U; index < outputs_.size(); ++index) {
      for (std::size_t other = index + 1U; other < outputs_.size(); ++other) {
        if (outputs_[index].name == outputs_[other].name) {
          fail(SqlDiagnosticCode::kDuplicateOutputName, syntax_.span(),
               "SELECT output names are not unique");
        }
      }
    }
  }

  [[nodiscard]] static bool same_identifier(const SqlIdentifier& left,
                                            const SqlIdentifier& right) noexcept {
    return left.text() == right.text() && left.quoted() == right.quoted();
  }

  [[nodiscard]] const BoundColumnReference*
  reference_for(const SqlExpression& expression) const noexcept {
    if (expression.kind() != SqlExpressionKind::kColumn)
      return nullptr;
    const auto found = std::ranges::find_if(column_references_, [&](const auto& reference) {
      return same_span(reference.expression_span, expression.span());
    });
    return found == column_references_.end() ? nullptr : &*found;
  }

  [[nodiscard]] const BoundExpressionInfo*
  recorded_expression(const SourceSpan& span) const noexcept {
    const auto found = std::ranges::find_if(expressions_, [&](const BoundExpressionInfo& value) {
      return same_span(value.expression_span, span);
    });
    return found == expressions_.end() ? nullptr : &*found;
  }

  [[nodiscard]] std::uint64_t source_mask(const SqlExpression& expression) const noexcept {
    if (expression.kind() == SqlExpressionKind::kColumn) {
      const BoundColumnReference* reference = reference_for(expression);
      return reference == nullptr ? 0U : std::uint64_t{1U} << reference->source_ordinal;
    }
    std::uint64_t mask = 0U;
    for (const SqlExpression& child : expression.children())
      mask |= source_mask(child);
    return mask;
  }

  void inspect_asof_leaf(const SqlExpression& expression, const std::size_t right_source,
                         std::size_t& equality_count,
                         std::optional<SourceSpan>& right_timestamp) const {
    if (expression.kind() == SqlExpressionKind::kBinary &&
        expression.operation() == SqlOperator::kAnd) {
      inspect_asof_leaf(expression.children()[0], right_source, equality_count, right_timestamp);
      inspect_asof_leaf(expression.children()[1], right_source, equality_count, right_timestamp);
      return;
    }
    if (expression.kind() != SqlExpressionKind::kBinary) {
      fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
           "ASOF JOIN ON supports only equality keys and one timestamp inequality");
    }
    const SqlExpression& left = expression.children()[0];
    const SqlExpression& right = expression.children()[1];
    const std::uint64_t right_bit = std::uint64_t{1U} << right_source;
    const std::uint64_t left_mask = source_mask(left);
    const std::uint64_t right_mask = source_mask(right);
    const bool left_is_right = left_mask == right_bit;
    const bool right_is_right = right_mask == right_bit;
    const bool left_is_prior = left_mask != 0U && (left_mask & right_bit) == 0U;
    const bool right_is_prior = right_mask != 0U && (right_mask & right_bit) == 0U;
    if (expression.operation() == SqlOperator::kEqual &&
        ((left_is_right && right_is_prior) || (right_is_right && left_is_prior))) {
      ++equality_count;
      return;
    }
    const bool canonical_time =
        expression.operation() == SqlOperator::kLessEqual && left_is_right && right_is_prior;
    const bool reversed_time =
        expression.operation() == SqlOperator::kGreaterEqual && left_is_prior && right_is_right;
    if ((!canonical_time && !reversed_time) || right_timestamp.has_value()) {
      fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
           "ASOF JOIN requires exactly one right timestamp not greater than a left timestamp");
    }
    const SqlExpression& right_time = canonical_time ? left : right;
    const SqlExpression& left_time = canonical_time ? right : left;
    const BoundExpressionInfo* right_info = recorded_expression(right_time.span());
    const BoundExpressionInfo* left_info = recorded_expression(left_time.span());
    if (right_info == nullptr || left_info == nullptr || !right_info->type.has_value() ||
        !left_info->type.has_value() ||
        right_info->type->kind() != schema::LogicalTypeKind::kTimestampNs ||
        left_info->type->kind() != schema::LogicalTypeKind::kTimestampNs) {
      fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
           "ASOF JOIN timestamp inequality operands must be TIMESTAMP_NS");
    }
    right_timestamp = right_time.span();
  }

  void bind_asof_shape(const SqlAsofJoin& join, const std::size_t right_source) {
    std::size_t equality_count = 0U;
    std::optional<SourceSpan> right_timestamp;
    inspect_asof_leaf(join.condition, right_source, equality_count, right_timestamp);
    if (equality_count == 0U || !right_timestamp.has_value()) {
      fail(SqlDiagnosticCode::kTypeMismatch, join.condition.span(),
           "ASOF JOIN requires an equality key and timestamp inequality");
    }
    asof_joins_.push_back(BoundAsofJoin{.right_source_ordinal = right_source,
                                        .left = join.left,
                                        .right_timestamp_expression_span = *right_timestamp,
                                        .equality_key_count = equality_count});
  }

  [[nodiscard]] bool same_expression(const SqlExpression& left,
                                     const SqlExpression& right) const noexcept {
    if (left.kind() == SqlExpressionKind::kColumn && right.kind() == SqlExpressionKind::kColumn) {
      const BoundColumnReference* left_reference = reference_for(left);
      const BoundColumnReference* right_reference = reference_for(right);
      return left_reference != nullptr && right_reference != nullptr &&
             left_reference->source_ordinal == right_reference->source_ordinal &&
             left_reference->column_ordinal == right_reference->column_ordinal;
    }
    if (left.kind() != right.kind() || left.literal_kind() != right.literal_kind() ||
        left.operation() != right.operation() || left.text() != right.text() ||
        left.cast_type() != right.cast_type() || left.name().size() != right.name().size() ||
        left.children().size() != right.children().size()) {
      return false;
    }
    for (std::size_t index = 0U; index < left.name().size(); ++index) {
      if (!same_identifier(left.name()[index], right.name()[index]))
        return false;
    }
    for (std::size_t index = 0U; index < left.children().size(); ++index) {
      if (!same_expression(left.children()[index], right.children()[index]))
        return false;
    }
    return true;
  }

  [[nodiscard]] static bool aggregate_function(const SqlExpression& expression) noexcept {
    if (expression.kind() != SqlExpressionKind::kFunction)
      return false;
    const std::string& name = expression.text();
    return name == "count" || name == "sum" || name == "avg" || name == "min" || name == "max" ||
           name == "var_pop" || name == "var_samp";
  }

  [[nodiscard]] bool is_group_expression(const SqlExpression& expression) const noexcept {
    return std::ranges::any_of(syntax_.group_by(), [&](const SqlExpression& group) {
      return same_expression(expression, group);
    });
  }

  void validate_grouped_expression(const SqlExpression& expression) const {
    if (is_group_expression(expression) || aggregate_function(expression))
      return;
    if (expression.kind() == SqlExpressionKind::kColumn ||
        expression.kind() == SqlExpressionKind::kStar) {
      fail(SqlDiagnosticCode::kTypeMismatch, expression.span(),
           "Aggregate query references a column that is not grouped");
    }
    for (const SqlExpression& child : expression.children())
      validate_grouped_expression(child);
  }

  void validate_grouping() const {
    const bool aggregate_query =
        !syntax_.group_by().empty() ||
        std::ranges::any_of(outputs_, &BoundOutputColumn::contains_aggregate);
    if (!aggregate_query)
      return;
    for (const SqlSelectItem& item : syntax_.items()) {
      if (item.kind() != SqlSelectItemKind::kExpression) {
        fail(SqlDiagnosticCode::kTypeMismatch, item.span(),
             "Aggregate queries cannot project an ungrouped star");
      }
      validate_grouped_expression(*item.expression());
    }
  }

  void bind_order_by() {
    for (const SqlOrderItem& item : syntax_.order_by()) {
      if (item.expression.kind() == SqlExpressionKind::kColumn &&
          item.expression.name().size() == 1U) {
        const SqlIdentifier& identifier = item.expression.name().front();
        const auto output = std::ranges::find_if(outputs_, [&](const BoundOutputColumn& candidate) {
          return candidate.name == identifier.text() &&
                 candidate.name_quoted == identifier.quoted();
        });
        if (output != outputs_.end()) {
          if (expressions_.size() >= limits_.maximum_bound_expressions) {
            fail(SqlDiagnosticCode::kResourceLimit, item.expression.span(),
                 "Bound expression count exceeds the limit");
          }
          expressions_.push_back(BoundExpressionInfo{
              .expression_span = item.expression.span(),
              .type = output->type,
              .output_ordinal = static_cast<std::size_t>(output - outputs_.begin()),
              .nullable = output->nullable,
              .contains_aggregate = output->contains_aggregate,
          });
          continue;
        }
      }
      const Inferred order = bind_expression(item.expression);
      if (!order.type.has_value()) {
        fail(SqlDiagnosticCode::kTypeMismatch, item.expression.span(),
             "ORDER BY expression has no type");
      }
      const bool aggregate_query =
          !syntax_.group_by().empty() ||
          std::ranges::any_of(outputs_, &BoundOutputColumn::contains_aggregate);
      if (aggregate_query)
        validate_grouped_expression(item.expression);
    }
  }

  ParsedSqlSelect syntax_;
  std::shared_ptr<const QueryCatalogSnapshot> catalog_;
  SqlBinderLimits limits_;
  std::vector<BoundSqlSource> sources_;
  std::vector<BoundColumnReference> column_references_;
  std::vector<BoundExpressionInfo> expressions_;
  std::vector<BoundOutputColumn> outputs_;
  std::optional<BoundLatestBy> latest_by_;
  std::vector<BoundAsofJoin> asof_joins_;
};

} // namespace detail

SqlResult<BoundSqlSelect> bind_sql_v1_select(ParsedSqlSelect syntax,
                                             std::shared_ptr<const QueryCatalogSnapshot> catalog,
                                             const SqlBinderLimits limits) {
  return detail::SqlBinder{std::move(syntax), std::move(catalog), limits}.run();
}

} // namespace chronos::query
