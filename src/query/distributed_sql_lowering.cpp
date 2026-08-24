#include "chronos/query/distributed_sql_lowering.hpp"

#include "chronos/common/status.hpp"
#include "chronos/query/literal.hpp"

#include <algorithm>
#include <cstdint>
#include <new>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] SqlDiagnostic diagnostic(const SqlDiagnosticCode code, const SourceSpan span,
                                       const common::StatusCode status_code, const char* message) {
  return {code, span, common::Status{status_code, message}};
}

class LoweringFailure {
public:
  explicit LoweringFailure(SqlDiagnostic value) noexcept : value_(std::move(value)) {}

  [[nodiscard]] SqlDiagnostic take() noexcept {
    return std::move(value_);
  }

private:
  SqlDiagnostic value_;
};

[[noreturn]] void unsupported(const SourceSpan span, const char* message) {
  throw LoweringFailure{diagnostic(SqlDiagnosticCode::kUnsupportedSyntax, span,
                                   common::StatusCode::kNotSupported, message)};
}

void apply_lower(cseg::EventTimePredicate& predicate, const cseg::EventTimeBound candidate) {
  if (!predicate.lower.has_value() || candidate.value > predicate.lower->value) {
    predicate.lower = candidate;
    return;
  }
  if (candidate.value == predicate.lower->value)
    predicate.lower->inclusive = predicate.lower->inclusive && candidate.inclusive;
}

void apply_upper(cseg::EventTimePredicate& predicate, const cseg::EventTimeBound candidate) {
  if (!predicate.upper.has_value() || candidate.value < predicate.upper->value) {
    predicate.upper = candidate;
    return;
  }
  if (candidate.value == predicate.upper->value)
    predicate.upper->inclusive = predicate.upper->inclusive && candidate.inclusive;
}

[[nodiscard]] bool event_time_reference(const BoundColumnReference* const reference,
                                        const schema::TableSchema& schema) noexcept {
  return reference != nullptr && reference->source_ordinal == 0U &&
         reference->column_id == schema.event_time_column();
}

[[nodiscard]] std::int64_t timestamp_literal(const SqlExpression& literal) {
  if (literal.kind() != SqlExpressionKind::kLiteral ||
      literal.literal_kind() != SqlLiteralKind::kTimestamp) {
    unsupported(literal.span(), "Distributed event-time bounds must be TIMESTAMP literals");
  }
  const auto parsed = parse_sql_timestamp_ns_literal(literal.text());
  if (!parsed.has_value()) {
    throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, literal.span(),
                                     common::StatusCode::kInternal,
                                     "Bound TIMESTAMP literal could not be lowered")};
  }
  return *parsed;
}

void apply_comparison(cseg::EventTimePredicate& predicate, const SqlOperator operation,
                      const std::int64_t value, const bool column_on_left, const SourceSpan span) {
  SqlOperator normalized = operation;
  if (!column_on_left) {
    switch (operation) {
    case SqlOperator::kLess:
      normalized = SqlOperator::kGreater;
      break;
    case SqlOperator::kLessEqual:
      normalized = SqlOperator::kGreaterEqual;
      break;
    case SqlOperator::kGreater:
      normalized = SqlOperator::kLess;
      break;
    case SqlOperator::kGreaterEqual:
      normalized = SqlOperator::kLessEqual;
      break;
    default:
      break;
    }
  }
  switch (normalized) {
  case SqlOperator::kEqual:
    apply_lower(predicate, {.value = value, .inclusive = true});
    apply_upper(predicate, {.value = value, .inclusive = true});
    return;
  case SqlOperator::kGreater:
    apply_lower(predicate, {.value = value, .inclusive = false});
    return;
  case SqlOperator::kGreaterEqual:
    apply_lower(predicate, {.value = value, .inclusive = true});
    return;
  case SqlOperator::kLess:
    apply_upper(predicate, {.value = value, .inclusive = false});
    return;
  case SqlOperator::kLessEqual:
    apply_upper(predicate, {.value = value, .inclusive = true});
    return;
  default:
    unsupported(span, "Distributed WHERE supports only event-time range comparisons");
  }
}

void lower_event_time_leaf(const BoundSqlSelect& select, const schema::TableSchema& schema,
                           const SqlExpression& expression, cseg::EventTimePredicate& predicate) {
  if (expression.kind() == SqlExpressionKind::kBinary &&
      expression.operation() == SqlOperator::kAnd && expression.children().size() == 2U) {
    lower_event_time_leaf(select, schema, expression.children()[0], predicate);
    lower_event_time_leaf(select, schema, expression.children()[1], predicate);
    return;
  }
  if (expression.kind() == SqlExpressionKind::kBetween) {
    if (expression.operation() != SqlOperator::kBetween || expression.children().size() != 3U) {
      unsupported(expression.span(),
                  "Distributed WHERE supports inclusive event-time BETWEEN only");
    }
    const SqlExpression& value = expression.children()[0];
    if (!event_time_reference(select.find_column_reference(value.span()), schema)) {
      unsupported(value.span(), "Distributed WHERE can filter only the source event-time column");
    }
    apply_lower(predicate,
                {.value = timestamp_literal(expression.children()[1]), .inclusive = true});
    apply_upper(predicate,
                {.value = timestamp_literal(expression.children()[2]), .inclusive = true});
    return;
  }
  if (expression.kind() != SqlExpressionKind::kBinary || expression.children().size() != 2U) {
    unsupported(expression.span(),
                "Distributed WHERE requires event-time comparisons or BETWEEN joined by AND");
  }
  const SqlExpression& left = expression.children()[0];
  const SqlExpression& right = expression.children()[1];
  const BoundColumnReference* left_reference = select.find_column_reference(left.span());
  const BoundColumnReference* right_reference = select.find_column_reference(right.span());
  const bool left_literal = left.kind() == SqlExpressionKind::kLiteral &&
                            left.literal_kind() == SqlLiteralKind::kTimestamp;
  const bool right_literal = right.kind() == SqlExpressionKind::kLiteral &&
                             right.literal_kind() == SqlLiteralKind::kTimestamp;
  const bool column_on_left = left_reference != nullptr && right_literal;
  const bool column_on_right = right_reference != nullptr && left_literal;
  if (column_on_left == column_on_right) {
    unsupported(expression.span(),
                "Distributed WHERE requires one event-time column and one TIMESTAMP literal");
  }
  const BoundColumnReference& reference = column_on_left ? *left_reference : *right_reference;
  if (!event_time_reference(&reference, schema)) {
    unsupported(expression.span(),
                "Distributed WHERE can filter only the source event-time column");
  }
  const SqlExpression& literal = column_on_left ? right : left;
  apply_comparison(predicate, expression.operation(), timestamp_literal(literal), column_on_left,
                   expression.span());
}

[[nodiscard]] ScalarNullPlacement null_placement(const SqlOrderItem& item) noexcept {
  if (item.null_order == SqlNullOrder::kFirst)
    return ScalarNullPlacement::kFirst;
  if (item.null_order == SqlNullOrder::kLast)
    return ScalarNullPlacement::kLast;
  return item.direction == SqlOrderDirection::kAscending ? ScalarNullPlacement::kLast
                                                         : ScalarNullPlacement::kFirst;
}

struct DirectOutputBinding {
  std::size_t source_ordinal;
  std::size_t column_ordinal;
};

[[nodiscard]] std::optional<DirectOutputBinding>
direct_output_binding(const BoundSqlSelect& select, const BoundOutputColumn& output) noexcept {
  if (output.source_ordinal.has_value() && output.column_ordinal.has_value())
    return DirectOutputBinding{*output.source_ordinal, *output.column_ordinal};
  if (!output.expression_span.has_value())
    return std::nullopt;
  const BoundColumnReference* reference = select.find_column_reference(*output.expression_span);
  if (reference == nullptr)
    return std::nullopt;
  return DirectOutputBinding{reference->source_ordinal, reference->column_ordinal};
}

[[nodiscard]] std::optional<std::size_t> order_output_ordinal(const BoundSqlSelect& select,
                                                              const SqlOrderItem& item) noexcept {
  const BoundExpressionInfo* information = select.find_expression(item.expression.span());
  if (information != nullptr && information->output_ordinal.has_value())
    return information->output_ordinal;
  const BoundColumnReference* reference = select.find_column_reference(item.expression.span());
  if (reference == nullptr)
    return std::nullopt;
  const auto outputs = select.outputs();
  const auto output = std::ranges::find_if(outputs, [&](const BoundOutputColumn& candidate) {
    const std::optional<DirectOutputBinding> binding = direct_output_binding(select, candidate);
    return binding.has_value() && binding->source_ordinal == reference->source_ordinal &&
           binding->column_ordinal == reference->column_ordinal;
  });
  if (output == outputs.end())
    return std::nullopt;
  return static_cast<std::size_t>(output - outputs.begin());
}

} // namespace

SqlResult<DistributedVectorRowsSqlPlan> lower_bound_sql_select_to_distributed_vector_rows(
    const BoundSqlSelect& select, const DistributedVectorRowsSqlLoweringLimits limits) {
  try {
    if (limits.maximum_projection_columns == 0U ||
        limits.maximum_projection_columns > distributed_vector_plan_format::kMaximumInputColumns ||
        limits.maximum_output_columns == 0U ||
        limits.maximum_output_columns > distributed_vector_plan_format::kMaximumOutputColumns ||
        limits.maximum_order_keys == 0U ||
        limits.maximum_order_keys > distributed_vector_plan_format::kMaximumOrderKeys ||
        limits.maximum_result_name_bytes == 0U ||
        limits.maximum_result_name_bytes >
            distributed_vector_result_schema_format::kMaximumNameLength) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                                        common::StatusCode::kInvalidArgument,
                                        "Distributed SQL lowering limits are invalid"));
    }
    if (select.syntax().mode() != SqlSelectMode::kSelect || select.sources().size() != 1U ||
        !select.asof_joins().empty() || select.syntax().system_time().has_value() ||
        select.latest_by().has_value() || select.aggregate_query() ||
        !select.syntax().group_by().empty()) {
      return std::unexpected(
          diagnostic(SqlDiagnosticCode::kUnsupportedSyntax, select.syntax().span(),
                     common::StatusCode::kNotSupported,
                     "Distributed row lowering requires one current non-aggregate SELECT source"));
    }
    if (select.outputs().empty() || select.outputs().size() > limits.maximum_output_columns)
      return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                                        common::StatusCode::kResourceExhausted,
                                        "Distributed SELECT output width exceeds the limit"));

    const schema::TableSchema& source = *select.sources().front().schema_ptr();
    DistributedVectorRowsSqlPlan result{
        .table_id = source.table_id(),
        .destination_schema_id = source.schema_id(),
        .intent = {.mode = DistributedVectorPlanMode::kRows, .limit = select.syntax().limit()},
    };
    std::vector<std::int64_t> projected_index(source.columns().size(), -1);
    result.intent.row_output_indices.reserve(select.outputs().size());
    result.result_schema.columns.reserve(select.outputs().size());
    for (const BoundOutputColumn& output : select.outputs()) {
      const SourceSpan span = output.expression_span.value_or(select.syntax().span());
      const std::optional<DirectOutputBinding> binding = direct_output_binding(select, output);
      if (!binding.has_value() || binding->source_ordinal != 0U ||
          binding->column_ordinal >= source.columns().size()) {
        unsupported(span, "Distributed SELECT outputs must be direct source columns");
      }
      if (output.name.empty() || output.name.size() > limits.maximum_result_name_bytes) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kResourceLimit, span,
                                         common::StatusCode::kResourceExhausted,
                                         "Distributed result column name exceeds the limit")};
      }
      const std::size_t ordinal = binding->column_ordinal;
      if (projected_index[ordinal] < 0) {
        if (result.destination_column_ordinals.size() >= limits.maximum_projection_columns) {
          throw LoweringFailure{diagnostic(SqlDiagnosticCode::kResourceLimit, span,
                                           common::StatusCode::kResourceExhausted,
                                           "Distributed projection width exceeds the limit")};
        }
        projected_index[ordinal] =
            static_cast<std::int64_t>(result.destination_column_ordinals.size());
        result.destination_column_ordinals.push_back(static_cast<std::uint32_t>(ordinal));
      }
      result.intent.row_output_indices.push_back(
          static_cast<std::uint32_t>(projected_index[ordinal]));
      result.result_schema.columns.push_back(
          {.name = output.name, .type = output.type, .nullable = output.nullable});
    }

    if (const SqlExpression* where = select.syntax().where(); where != nullptr) {
      cseg::EventTimePredicate predicate;
      lower_event_time_leaf(select, source, *where, predicate);
      if (!predicate.lower.has_value() && !predicate.upper.has_value()) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, where->span(),
                                         common::StatusCode::kInternal,
                                         "Distributed WHERE produced no event-time bound")};
      }
      result.event_time_predicate = predicate;
    }

    std::vector<bool> ordered_outputs(select.outputs().size(), false);
    for (const SqlOrderItem& item : select.syntax().order_by()) {
      const std::optional<std::size_t> output = order_output_ordinal(select, item);
      if (!output.has_value() || *output >= select.outputs().size()) {
        unsupported(item.expression.span(),
                    "Distributed ORDER BY must reference a projected direct-column output");
      }
      if (ordered_outputs[*output])
        continue;
      if (result.intent.order_keys.size() >= limits.maximum_order_keys) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kResourceLimit, item.expression.span(),
                                         common::StatusCode::kResourceExhausted,
                                         "Distributed ORDER BY key count exceeds the limit")};
      }
      ordered_outputs[*output] = true;
      result.intent.order_keys.push_back(
          {.output_index = static_cast<std::uint32_t>(*output),
           .direction = item.direction == SqlOrderDirection::kAscending
                            ? PhysicalSortDirection::kAscending
                            : PhysicalSortDirection::kDescending,
           .null_placement = null_placement(item)});
    }

    const common::Status plan_status = validate_distributed_vector_plan_intent(
        result.intent, static_cast<std::uint32_t>(result.destination_column_ordinals.size()),
        limits.maximum_output_columns);
    if (!plan_status.is_ok()) {
      const SqlDiagnosticCode code = plan_status.code() == common::StatusCode::kResourceExhausted
                                         ? SqlDiagnosticCode::kResourceLimit
                                         : SqlDiagnosticCode::kExecutionFailure;
      return std::unexpected(SqlDiagnostic{code, select.syntax().span(), plan_status});
    }
    std::vector<PhysicalColumnShape> projected_shapes;
    projected_shapes.reserve(result.destination_column_ordinals.size());
    for (const std::uint32_t ordinal : result.destination_column_ordinals) {
      const schema::ColumnDefinition& column = source.columns()[ordinal];
      projected_shapes.push_back({.type = column.type(), .nullable = column.nullable()});
    }
    const common::Status schema_status = validate_distributed_vector_result_schema(
        result.intent, projected_shapes, result.result_schema);
    if (!schema_status.is_ok()) {
      const SqlDiagnosticCode code = schema_status.code() == common::StatusCode::kResourceExhausted
                                         ? SqlDiagnosticCode::kResourceLimit
                                         : SqlDiagnosticCode::kExecutionFailure;
      return std::unexpected(SqlDiagnostic{code, select.syntax().span(), schema_status});
    }
    return result;
  } catch (LoweringFailure& failure) {
    return std::unexpected(failure.take());
  } catch (const std::bad_alloc&) {
    return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                                      common::StatusCode::kResourceExhausted,
                                      "Distributed SQL lowering allocation failed"));
  } catch (const std::length_error&) {
    return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                                      common::StatusCode::kResourceExhausted,
                                      "Distributed SQL lowering exceeds container limits"));
  }
}

} // namespace chronos::query
