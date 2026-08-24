#include "chronos/query/distributed_sql_lowering.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"
#include "chronos/query/evaluator.hpp"
#include "chronos/query/literal.hpp"

#include <algorithm>
#include <cstdint>
#include <new>
#include <stdexcept>
#include <string_view>
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

[[nodiscard]] const SqlExpression* output_expression(const BoundSqlSelect& select,
                                                     const SourceSpan span) noexcept {
  for (const SqlSelectItem& item : select.syntax().items()) {
    const SqlExpression* expression = item.expression();
    if (expression != nullptr && expression->span() == span)
      return expression;
  }
  return nullptr;
}

[[nodiscard]] bool source_independent(const BoundSqlSelect& select,
                                      const SqlExpression& expression) noexcept {
  if (select.find_column_reference(expression.span()) != nullptr ||
      expression.kind() == SqlExpressionKind::kStar)
    return false;
  return std::ranges::all_of(expression.children(), [&](const SqlExpression& child) {
    return source_independent(select, child);
  });
}

[[nodiscard]] std::optional<VectorAggregateOperation>
distributed_aggregate_operation(const std::string_view name) noexcept {
  if (name == "count")
    return VectorAggregateOperation::kCount;
  if (name == "sum")
    return VectorAggregateOperation::kSum;
  if (name == "avg")
    return VectorAggregateOperation::kAverage;
  if (name == "min")
    return VectorAggregateOperation::kMinimum;
  if (name == "max")
    return VectorAggregateOperation::kMaximum;
  if (name == "var_pop")
    return VectorAggregateOperation::kVariancePopulation;
  if (name == "var_samp")
    return VectorAggregateOperation::kVarianceSample;
  return std::nullopt;
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
            distributed_vector_result_schema_format::kMaximumNameLength ||
        limits.maximum_constant_bytes == 0U) {
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
        .destination_column_ordinals = {},
        .event_time_predicate = std::nullopt,
        .intent = {.mode = DistributedVectorPlanMode::kRows,
                   .row_output_indices = {},
                   .visible_row_output_indices = {},
                   .group_key_input_indices = {},
                   .aggregates = {},
                   .order_keys = {},
                   .limit = select.syntax().limit()},
        .result_schema = {},
        .coordinator_projection = std::nullopt,
    };
    std::vector<std::int64_t> projected_index(source.columns().size(), -1);
    std::vector<std::int64_t> first_worker_output(source.columns().size(), -1);
    std::vector<std::optional<std::size_t>> select_worker_output(select.outputs().size());
    DistributedVectorRowCoordinatorProjection coordinator_projection;
    coordinator_projection.outputs.reserve(select.outputs().size());
    coordinator_projection.result_schema.columns.reserve(select.outputs().size());
    bool needs_coordinator_projection = false;
    std::size_t constant_bytes{};
    result.intent.row_output_indices.reserve(select.outputs().size());
    result.result_schema.columns.reserve(select.outputs().size());
    for (std::size_t output_index = 0U; output_index < select.outputs().size(); ++output_index) {
      const BoundOutputColumn& output = select.outputs()[output_index];
      const SourceSpan span = output.expression_span.value_or(select.syntax().span());
      if (output.name.empty() || output.name.size() > limits.maximum_result_name_bytes) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kResourceLimit, span,
                                         common::StatusCode::kResourceExhausted,
                                         "Distributed result column name exceeds the limit")};
      }
      const std::optional<DirectOutputBinding> binding = direct_output_binding(select, output);
      if (!binding.has_value() || binding->source_ordinal != 0U ||
          binding->column_ordinal >= source.columns().size()) {
        const SqlExpression* expression = output.expression_span.has_value()
                                              ? output_expression(select, *output.expression_span)
                                              : nullptr;
        if (expression == nullptr || !source_independent(select, *expression)) {
          unsupported(span, "Distributed SELECT computed outputs must be source-independent");
        }
        auto evaluated = evaluate_sql_v1_expression(select, *expression);
        if (!evaluated.has_value())
          throw LoweringFailure{std::move(evaluated.error())};
        if (!evaluated->type().has_value() || *evaluated->type() != output.type) {
          throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, span,
                                           common::StatusCode::kInternal,
                                           "Distributed constant output type is inconsistent")};
        }
        auto canonical = encode_canonical_scalar_value(*evaluated);
        if (!canonical.has_value()) {
          const SqlDiagnosticCode code =
              canonical.error().code() == common::StatusCode::kResourceExhausted
                  ? SqlDiagnosticCode::kResourceLimit
                  : SqlDiagnosticCode::kExecutionFailure;
          throw LoweringFailure{SqlDiagnostic{code, span, canonical.error()}};
        }
        const auto next_constant = common::checked_add(constant_bytes, canonical->size());
        if (!next_constant.has_value() || *next_constant > limits.maximum_constant_bytes) {
          throw LoweringFailure{diagnostic(SqlDiagnosticCode::kResourceLimit, span,
                                           common::StatusCode::kResourceExhausted,
                                           "Distributed constant output bytes exceed the limit")};
        }
        constant_bytes = *next_constant;
        needs_coordinator_projection = true;
        coordinator_projection.outputs.emplace_back(DistributedVectorRowConstantOutput{
            .is_null = evaluated->is_null(), .canonical_value = std::move(*canonical)});
        coordinator_projection.result_schema.columns.push_back(
            {.name = output.name, .type = output.type, .nullable = output.nullable});
        continue;
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
      const std::size_t worker_output = result.intent.row_output_indices.size() - 1U;
      select_worker_output[output_index] = worker_output;
      coordinator_projection.outputs.emplace_back(DistributedVectorRowSourceOutput{
          .worker_output_index = static_cast<std::uint32_t>(worker_output)});
      coordinator_projection.result_schema.columns.push_back(
          {.name = output.name, .type = output.type, .nullable = output.nullable});
      if (first_worker_output[ordinal] < 0) {
        first_worker_output[ordinal] = static_cast<std::int64_t>(worker_output);
      }
      result.result_schema.columns.push_back(
          {.name = output.name, .type = output.type, .nullable = output.nullable});
    }

    if (result.intent.row_output_indices.empty()) {
      const std::optional<std::size_t> event_ordinal =
          source.column_ordinal(source.event_time_column());
      if (!event_ordinal.has_value()) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                         select.syntax().span(), common::StatusCode::kInternal,
                                         "Distributed constant output has no row-count anchor")};
      }
      projected_index[*event_ordinal] = 0;
      first_worker_output[*event_ordinal] = 0;
      result.destination_column_ordinals.push_back(static_cast<std::uint32_t>(*event_ordinal));
      result.intent.row_output_indices.push_back(0U);
      const schema::ColumnDefinition& anchor = source.columns()[*event_ordinal];
      result.result_schema.columns.push_back(
          {.name = anchor.name(), .type = anchor.type(), .nullable = anchor.nullable()});
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

    std::vector<bool> ordered_outputs(result.intent.row_output_indices.size(), false);
    for (const SqlOrderItem& item : select.syntax().order_by()) {
      std::optional<std::size_t> output;
      const std::optional<std::size_t> select_output = order_output_ordinal(select, item);
      if (select_output.has_value()) {
        if (*select_output >= select_worker_output.size()) {
          return std::unexpected(diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                            item.expression.span(), common::StatusCode::kInternal,
                                            "Distributed ORDER BY output is unavailable"));
        }
        output = select_worker_output[*select_output];
        if (!output.has_value())
          continue;
      }
      if (!output.has_value()) {
        const BoundColumnReference* reference =
            select.find_column_reference(item.expression.span());
        if (reference == nullptr || reference->source_ordinal != 0U ||
            reference->column_ordinal >= source.columns().size()) {
          unsupported(item.expression.span(),
                      "Distributed ORDER BY must reference a direct source column");
        }
        const std::size_t ordinal = reference->column_ordinal;
        if (first_worker_output[ordinal] >= 0) {
          const std::size_t existing_output =
              static_cast<std::size_t>(first_worker_output[ordinal]);
          if (existing_output >= ordered_outputs.size()) {
            return std::unexpected(diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                              item.expression.span(), common::StatusCode::kInternal,
                                              "Distributed hidden ORDER BY output is unavailable"));
          }
          output = existing_output;
        }
        if (!output.has_value()) {
          if (result.intent.row_output_indices.size() >= limits.maximum_output_columns) {
            throw LoweringFailure{diagnostic(SqlDiagnosticCode::kResourceLimit,
                                             item.expression.span(),
                                             common::StatusCode::kResourceExhausted,
                                             "Distributed worker output width exceeds the limit")};
          }
          if (projected_index[ordinal] < 0) {
            if (result.destination_column_ordinals.size() >= limits.maximum_projection_columns) {
              throw LoweringFailure{diagnostic(SqlDiagnosticCode::kResourceLimit,
                                               item.expression.span(),
                                               common::StatusCode::kResourceExhausted,
                                               "Distributed projection width exceeds the limit")};
            }
            projected_index[ordinal] =
                static_cast<std::int64_t>(result.destination_column_ordinals.size());
            result.destination_column_ordinals.push_back(static_cast<std::uint32_t>(ordinal));
          }
          const schema::ColumnDefinition& column = source.columns()[ordinal];
          if (column.name().size() > limits.maximum_result_name_bytes) {
            throw LoweringFailure{diagnostic(SqlDiagnosticCode::kResourceLimit,
                                             item.expression.span(),
                                             common::StatusCode::kResourceExhausted,
                                             "Distributed hidden result name exceeds the limit")};
          }
          if (!needs_coordinator_projection && result.intent.visible_row_output_indices.empty()) {
            result.intent.visible_row_output_indices.reserve(select.outputs().size());
            for (std::size_t index = 0U; index < select.outputs().size(); ++index) {
              result.intent.visible_row_output_indices.push_back(static_cast<std::uint32_t>(index));
            }
          }
          output = result.intent.row_output_indices.size();
          result.intent.row_output_indices.push_back(
              static_cast<std::uint32_t>(projected_index[ordinal]));
          first_worker_output[ordinal] =
              static_cast<std::int64_t>(result.intent.row_output_indices.size() - 1U);
          result.result_schema.columns.push_back(
              {.name = column.name(), .type = column.type(), .nullable = column.nullable()});
          ordered_outputs.push_back(false);
        }
      }
      const std::size_t order_output = output.value_or(ordered_outputs.size());
      if (order_output >= ordered_outputs.size()) {
        return std::unexpected(diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                          item.expression.span(), common::StatusCode::kInternal,
                                          "Distributed ORDER BY output is unavailable"));
      }
      if (ordered_outputs[order_output])
        continue;
      if (result.intent.order_keys.size() >= limits.maximum_order_keys) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kResourceLimit, item.expression.span(),
                                         common::StatusCode::kResourceExhausted,
                                         "Distributed ORDER BY key count exceeds the limit")};
      }
      ordered_outputs[order_output] = true;
      result.intent.order_keys.push_back(
          {.output_index = static_cast<std::uint32_t>(order_output),
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
    if (needs_coordinator_projection) {
      const common::Status client_schema =
          validate_distributed_vector_result_schema_value(coordinator_projection.result_schema);
      if (!client_schema.is_ok()) {
        return std::unexpected(SqlDiagnostic{SqlDiagnosticCode::kExecutionFailure,
                                             select.syntax().span(), client_schema});
      }
      result.coordinator_projection.emplace(std::move(coordinator_projection));
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

SqlResult<DistributedVectorAggregateSqlPlan> lower_bound_sql_select_to_distributed_vector_aggregate(
    const BoundSqlSelect& select, const DistributedVectorAggregateSqlLoweringLimits limits) {
  try {
    if (limits.maximum_projection_columns == 0U ||
        limits.maximum_projection_columns > distributed_vector_plan_format::kMaximumInputColumns ||
        limits.maximum_aggregates == 0U ||
        limits.maximum_aggregates > distributed_vector_plan_format::kMaximumAggregates ||
        limits.maximum_result_name_bytes == 0U ||
        limits.maximum_result_name_bytes >
            distributed_vector_result_schema_format::kMaximumNameLength) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                                        common::StatusCode::kInvalidArgument,
                                        "Distributed aggregate SQL lowering limits are invalid"));
    }
    if (select.syntax().mode() != SqlSelectMode::kSelect || select.sources().size() != 1U ||
        !select.asof_joins().empty() || select.syntax().system_time().has_value() ||
        select.latest_by().has_value() || !select.aggregate_query() ||
        !select.syntax().group_by().empty()) {
      return std::unexpected(diagnostic(
          SqlDiagnosticCode::kUnsupportedSyntax, select.syntax().span(),
          common::StatusCode::kNotSupported,
          "Distributed aggregate lowering requires one current ungrouped aggregate source"));
    }
    if (!select.syntax().order_by().empty()) {
      unsupported(select.syntax().order_by().front().expression.span(),
                  "Distributed global aggregate ORDER BY is not supported");
    }
    if (select.outputs().empty() || select.outputs().size() > limits.maximum_aggregates) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                                        common::StatusCode::kResourceExhausted,
                                        "Distributed aggregate output width exceeds the limit"));
    }
    if (select.syntax().items().size() != select.outputs().size()) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                        select.syntax().span(), common::StatusCode::kInternal,
                                        "Bound aggregate output identity is inconsistent"));
    }

    const schema::TableSchema& source = *select.sources().front().schema_ptr();
    DistributedVectorAggregateSqlPlan result{
        .input_rows = {.table_id = source.table_id(),
                       .destination_schema_id = source.schema_id(),
                       .destination_column_ordinals = {},
                       .event_time_predicate = std::nullopt,
                       .intent = {.mode = DistributedVectorPlanMode::kRows,
                                  .row_output_indices = {},
                                  .visible_row_output_indices = {},
                                  .group_key_input_indices = {},
                                  .aggregates = {},
                                  .order_keys = {},
                                  .limit = std::nullopt},
                       .result_schema = {},
                       .coordinator_projection = std::nullopt},
        .intent = {.mode = DistributedVectorPlanMode::kUngroupedAggregate,
                   .row_output_indices = {},
                   .visible_row_output_indices = {},
                   .group_key_input_indices = {},
                   .aggregates = {},
                   .order_keys = {},
                   .limit = select.syntax().limit()},
        .result_schema = {},
    };
    std::vector<std::int64_t> projected_index(source.columns().size(), -1);
    result.input_rows.destination_column_ordinals.reserve(select.outputs().size());
    result.input_rows.intent.row_output_indices.reserve(select.outputs().size());
    result.input_rows.result_schema.columns.reserve(select.outputs().size());
    result.intent.aggregates.reserve(select.outputs().size());
    result.result_schema.columns.reserve(select.outputs().size());

    for (std::size_t output_index = 0U; output_index < select.outputs().size(); ++output_index) {
      const BoundOutputColumn& output = select.outputs()[output_index];
      const SqlExpression* expression = select.syntax().items()[output_index].expression();
      const SourceSpan span = output.expression_span.value_or(select.syntax().span());
      if (expression == nullptr || !output.expression_span.has_value() ||
          expression->span() != *output.expression_span ||
          expression->kind() != SqlExpressionKind::kFunction ||
          expression->children().size() != 1U) {
        unsupported(span, "Distributed aggregate outputs must be direct aggregate calls");
      }
      const std::optional<VectorAggregateOperation> operation =
          distributed_aggregate_operation(expression->text());
      if (!operation.has_value())
        unsupported(expression->span(), "Distributed aggregate function is unsupported");
      if (output.name.empty() || output.name.size() > limits.maximum_result_name_bytes) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kResourceLimit, span,
                                         common::StatusCode::kResourceExhausted,
                                         "Distributed aggregate result name exceeds the limit")};
      }

      const SqlExpression& child = expression->children().front();
      DistributedVectorAggregateIntent aggregate;
      VectorAggregateDefinition definition;
      if (child.kind() == SqlExpressionKind::kStar) {
        if (*operation != VectorAggregateOperation::kCount) {
          unsupported(child.span(), "Only distributed COUNT may use a star input");
        }
        aggregate.operation = VectorAggregateOperation::kCountStar;
        definition.operation = VectorAggregateOperation::kCountStar;
      } else {
        const BoundColumnReference* reference = select.find_column_reference(child.span());
        if (child.kind() != SqlExpressionKind::kColumn || reference == nullptr ||
            reference->source_ordinal != 0U ||
            reference->column_ordinal >= source.columns().size()) {
          unsupported(child.span(), "Distributed aggregate inputs must be direct source columns");
        }
        const std::size_t ordinal = reference->column_ordinal;
        if (projected_index[ordinal] < 0) {
          if (result.input_rows.destination_column_ordinals.size() >=
              limits.maximum_projection_columns) {
            throw LoweringFailure{diagnostic(SqlDiagnosticCode::kResourceLimit, child.span(),
                                             common::StatusCode::kResourceExhausted,
                                             "Distributed aggregate projection exceeds the limit")};
          }
          projected_index[ordinal] =
              static_cast<std::int64_t>(result.input_rows.destination_column_ordinals.size());
          result.input_rows.destination_column_ordinals.push_back(
              static_cast<std::uint32_t>(ordinal));
          result.input_rows.intent.row_output_indices.push_back(
              static_cast<std::uint32_t>(projected_index[ordinal]));
          const schema::ColumnDefinition& projected = source.columns()[ordinal];
          result.input_rows.result_schema.columns.push_back({.name = projected.name(),
                                                             .type = projected.type(),
                                                             .nullable = projected.nullable()});
        }
        const std::uint32_t input_index = static_cast<std::uint32_t>(projected_index[ordinal]);
        const schema::ColumnDefinition& column = source.columns()[ordinal];
        aggregate = {.operation = *operation, .input_index = input_index};
        definition = {.operation = *operation,
                      .input = VectorAggregateInput{.column_ordinal = input_index,
                                                    .type = column.type(),
                                                    .nullable = column.nullable()}};
      }
      auto shape = vector_aggregate_output_shape(definition);
      if (!shape.has_value()) {
        const SqlDiagnosticCode code =
            shape.error().code() == common::StatusCode::kResourceExhausted
                ? SqlDiagnosticCode::kResourceLimit
                : SqlDiagnosticCode::kUnsupportedSyntax;
        throw LoweringFailure{SqlDiagnostic{code, expression->span(), shape.error()}};
      }
      if (shape->type != output.type || shape->nullable != output.nullable) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, expression->span(),
                                         common::StatusCode::kInternal,
                                         "Bound aggregate result shape disagrees with its kernel")};
      }
      result.intent.aggregates.push_back(aggregate);
      result.result_schema.columns.push_back(
          {.name = output.name, .type = output.type, .nullable = output.nullable});
    }

    if (const SqlExpression* where = select.syntax().where(); where != nullptr) {
      cseg::EventTimePredicate predicate;
      lower_event_time_leaf(select, source, *where, predicate);
      if (!predicate.lower.has_value() && !predicate.upper.has_value()) {
        throw LoweringFailure{diagnostic(
            SqlDiagnosticCode::kExecutionFailure, where->span(), common::StatusCode::kInternal,
            "Distributed aggregate WHERE produced no event-time bound")};
      }
      result.input_rows.event_time_predicate = predicate;
    }

    if (result.input_rows.destination_column_ordinals.empty()) {
      const auto anchor = std::ranges::find(source.columns(), source.event_time_column(),
                                            &schema::ColumnDefinition::id);
      if (anchor == source.columns().end()) {
        return std::unexpected(diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                          select.syntax().span(), common::StatusCode::kInternal,
                                          "Distributed COUNT(*) projection anchor is absent"));
      }
      const std::uint32_t anchor_ordinal =
          static_cast<std::uint32_t>(anchor - source.columns().begin());
      result.input_rows.destination_column_ordinals.push_back(anchor_ordinal);
      result.input_rows.intent.row_output_indices.push_back(0U);
      result.input_rows.result_schema.columns.push_back(
          {.name = anchor->name(), .type = anchor->type(), .nullable = anchor->nullable()});
    }

    const common::Status plan_status = validate_distributed_vector_plan_intent(
        result.intent,
        static_cast<std::uint32_t>(result.input_rows.destination_column_ordinals.size()),
        limits.maximum_aggregates);
    if (!plan_status.is_ok()) {
      const SqlDiagnosticCode code = plan_status.code() == common::StatusCode::kResourceExhausted
                                         ? SqlDiagnosticCode::kResourceLimit
                                         : SqlDiagnosticCode::kExecutionFailure;
      return std::unexpected(SqlDiagnostic{code, select.syntax().span(), plan_status});
    }
    std::vector<PhysicalColumnShape> projected_shapes;
    projected_shapes.reserve(result.input_rows.destination_column_ordinals.size());
    for (const std::uint32_t ordinal : result.input_rows.destination_column_ordinals) {
      const schema::ColumnDefinition& column = source.columns()[ordinal];
      projected_shapes.push_back({.type = column.type(), .nullable = column.nullable()});
    }
    const common::Status input_plan_status = validate_distributed_vector_plan_intent(
        result.input_rows.intent, static_cast<std::uint32_t>(projected_shapes.size()),
        limits.maximum_projection_columns);
    if (!input_plan_status.is_ok()) {
      const SqlDiagnosticCode code =
          input_plan_status.code() == common::StatusCode::kResourceExhausted
              ? SqlDiagnosticCode::kResourceLimit
              : SqlDiagnosticCode::kExecutionFailure;
      return std::unexpected(SqlDiagnostic{code, select.syntax().span(), input_plan_status});
    }
    const common::Status input_schema_status = validate_distributed_vector_result_schema(
        result.input_rows.intent, projected_shapes, result.input_rows.result_schema);
    if (!input_schema_status.is_ok()) {
      const SqlDiagnosticCode code =
          input_schema_status.code() == common::StatusCode::kResourceExhausted
              ? SqlDiagnosticCode::kResourceLimit
              : SqlDiagnosticCode::kExecutionFailure;
      return std::unexpected(SqlDiagnostic{code, select.syntax().span(), input_schema_status});
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
                                      "Distributed aggregate SQL lowering allocation failed"));
  } catch (const std::length_error&) {
    return std::unexpected(
        diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                   common::StatusCode::kResourceExhausted,
                   "Distributed aggregate SQL lowering exceeds container limits"));
  }
}

} // namespace chronos::query
