#include "chronos/query/distributed_sql_lowering.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"
#include "chronos/query/evaluator.hpp"
#include "chronos/query/literal.hpp"
#include "chronos/query/physical_lowering.hpp"

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

[[nodiscard]] bool timestamp_literal_shape(const SqlExpression& expression) noexcept {
  return expression.kind() == SqlExpressionKind::kLiteral &&
         expression.literal_kind() == SqlLiteralKind::kTimestamp;
}

[[nodiscard]] bool event_time_predicate_shape(const BoundSqlSelect& select,
                                              const schema::TableSchema& schema,
                                              const SqlExpression& expression) noexcept {
  if (expression.kind() == SqlExpressionKind::kBinary &&
      expression.operation() == SqlOperator::kAnd && expression.children().size() == 2U) {
    return event_time_predicate_shape(select, schema, expression.children()[0]) &&
           event_time_predicate_shape(select, schema, expression.children()[1]);
  }
  if (expression.kind() == SqlExpressionKind::kBetween) {
    return expression.operation() == SqlOperator::kBetween && expression.children().size() == 3U &&
           event_time_reference(select.find_column_reference(expression.children()[0].span()),
                                schema) &&
           timestamp_literal_shape(expression.children()[1]) &&
           timestamp_literal_shape(expression.children()[2]);
  }
  if (expression.kind() != SqlExpressionKind::kBinary || expression.children().size() != 2U)
    return false;
  switch (expression.operation()) {
  case SqlOperator::kEqual:
  case SqlOperator::kLess:
  case SqlOperator::kLessEqual:
  case SqlOperator::kGreater:
  case SqlOperator::kGreaterEqual:
    break;
  default:
    return false;
  }
  const SqlExpression& left = expression.children()[0];
  const SqlExpression& right = expression.children()[1];
  return (event_time_reference(select.find_column_reference(left.span()), schema) &&
          timestamp_literal_shape(right)) ||
         (event_time_reference(select.find_column_reference(right.span()), schema) &&
          timestamp_literal_shape(left));
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

void collect_distributed_aggregates(const SqlExpression& expression,
                                    std::vector<const SqlExpression*>& aggregates) {
  if (expression.kind() == SqlExpressionKind::kFunction &&
      distributed_aggregate_operation(expression.text()).has_value()) {
    aggregates.push_back(&expression);
    return;
  }
  for (const SqlExpression& child : expression.children())
    collect_distributed_aggregates(child, aggregates);
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
        limits.maximum_constant_bytes == 0U ||
        limits.expression_limits.maximum_instructions == 0U ||
        limits.expression_limits.maximum_instructions > kMaximumVectorExpressionInstructions ||
        limits.expression_limits.maximum_retained_configuration_bytes == 0U ||
        limits.maximum_expression_configuration_bytes == 0U) {
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
    const SqlExpression* const where = select.syntax().where();
    const bool coordinator_where =
        where != nullptr && !event_time_predicate_shape(select, source, *where);
    std::vector<std::int64_t> projected_index(source.columns().size(), -1);
    std::vector<std::int64_t> first_worker_output(source.columns().size(), -1);
    std::vector<std::optional<std::size_t>> select_worker_output(select.outputs().size());
    std::vector<bool> select_row_dependent(select.outputs().size(), false);
    DistributedVectorRowCoordinatorProjection coordinator_projection;
    coordinator_projection.outputs.reserve(select.outputs().size());
    coordinator_projection.result_schema.columns.reserve(select.outputs().size());
    bool needs_coordinator_projection = false;
    std::size_t constant_bytes{};
    std::size_t expression_bytes{};
    bool has_row_dependent_output = false;
    for (std::size_t output_index = 0U; output_index < select.outputs().size(); ++output_index) {
      const BoundOutputColumn& output = select.outputs()[output_index];
      const std::optional<DirectOutputBinding> binding = direct_output_binding(select, output);
      if (binding.has_value() && binding->source_ordinal == 0U &&
          binding->column_ordinal < source.columns().size()) {
        continue;
      }
      const SqlExpression* expression = output.expression_span.has_value()
                                            ? output_expression(select, *output.expression_span)
                                            : nullptr;
      if (expression != nullptr && !source_independent(select, *expression)) {
        has_row_dependent_output = true;
        select_row_dependent[output_index] = true;
      }
    }
    bool has_computed_order = false;
    for (const SqlOrderItem& item : select.syntax().order_by()) {
      const std::optional<std::size_t> selected = order_output_ordinal(select, item);
      if (selected.has_value()) {
        if (*selected >= select.outputs().size()) {
          return std::unexpected(diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                            item.expression.span(), common::StatusCode::kInternal,
                                            "Distributed ORDER BY output is unavailable"));
        }
        const BoundOutputColumn& output = select.outputs()[*selected];
        const std::optional<DirectOutputBinding> binding = direct_output_binding(select, output);
        if (binding.has_value() && binding->source_ordinal == 0U &&
            binding->column_ordinal < source.columns().size()) {
          continue;
        }
        const SqlExpression* expression = output.expression_span.has_value()
                                              ? output_expression(select, *output.expression_span)
                                              : nullptr;
        if (expression == nullptr) {
          unsupported(item.expression.span(),
                      "Distributed ORDER BY output expression is unavailable");
        }
        if (!source_independent(select, *expression))
          has_computed_order = true;
        continue;
      }
      const BoundColumnReference* reference = select.find_column_reference(item.expression.span());
      if (reference != nullptr && reference->source_ordinal == 0U &&
          reference->column_ordinal < source.columns().size()) {
        continue;
      }
      if (!source_independent(select, item.expression))
        has_computed_order = true;
    }
    const bool needs_full_source =
        has_row_dependent_output || has_computed_order ||
        (coordinator_where && where != nullptr && !source_independent(select, *where));
    if (needs_full_source) {
      if (source.columns().size() > limits.maximum_projection_columns ||
          source.columns().size() > limits.maximum_output_columns) {
        throw LoweringFailure{
            diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                       common::StatusCode::kResourceExhausted,
                       "Distributed coordinator expression source width exceeds the limit")};
      }
      result.destination_column_ordinals.reserve(source.columns().size());
      result.intent.row_output_indices.reserve(source.columns().size());
      result.result_schema.columns.reserve(source.columns().size());
      for (std::size_t ordinal = 0U; ordinal < source.columns().size(); ++ordinal) {
        const schema::ColumnDefinition& column = source.columns()[ordinal];
        projected_index[ordinal] = static_cast<std::int64_t>(ordinal);
        first_worker_output[ordinal] = static_cast<std::int64_t>(ordinal);
        result.destination_column_ordinals.push_back(static_cast<std::uint32_t>(ordinal));
        result.intent.row_output_indices.push_back(static_cast<std::uint32_t>(ordinal));
        result.result_schema.columns.push_back(
            {.name = column.name(), .type = column.type(), .nullable = column.nullable()});
      }
      needs_coordinator_projection = true;
    }
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
        if (expression == nullptr) {
          unsupported(span, "Distributed SELECT output expression is unavailable");
        }
        if (!source_independent(select, *expression)) {
          auto lowered =
              lower_bound_sql_scalar_expression(select, *expression, limits.expression_limits);
          if (!lowered.has_value())
            throw LoweringFailure{std::move(lowered.error())};
          if (lowered->result_shape().type != output.type ||
              lowered->result_shape().nullable != output.nullable) {
            throw LoweringFailure{diagnostic(
                SqlDiagnosticCode::kExecutionFailure, span, common::StatusCode::kInternal,
                "Distributed expression output shape is inconsistent")};
          }
          const auto next_expression =
              common::checked_add(expression_bytes, lowered->retained_configuration_bytes());
          if (!next_expression.has_value() ||
              *next_expression > limits.maximum_expression_configuration_bytes) {
            throw LoweringFailure{diagnostic(
                SqlDiagnosticCode::kResourceLimit, span, common::StatusCode::kResourceExhausted,
                "Distributed expression configuration bytes exceed the limit")};
          }
          expression_bytes = *next_expression;
          coordinator_projection.outputs.emplace_back(
              DistributedVectorRowExpressionOutput{.expression = std::move(*lowered)});
          coordinator_projection.result_schema.columns.push_back(
              {.name = output.name, .type = output.type, .nullable = output.nullable});
          continue;
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
      std::size_t worker_output{};
      if (needs_full_source) {
        worker_output = ordinal;
      } else {
        result.intent.row_output_indices.push_back(
            static_cast<std::uint32_t>(projected_index[ordinal]));
        worker_output = result.intent.row_output_indices.size() - 1U;
      }
      select_worker_output[output_index] = worker_output;
      coordinator_projection.outputs.emplace_back(DistributedVectorRowSourceOutput{
          .worker_output_index = static_cast<std::uint32_t>(worker_output)});
      coordinator_projection.result_schema.columns.push_back(
          {.name = output.name, .type = output.type, .nullable = output.nullable});
      if (first_worker_output[ordinal] < 0) {
        first_worker_output[ordinal] = static_cast<std::int64_t>(worker_output);
      }
      if (!needs_full_source) {
        result.result_schema.columns.push_back(
            {.name = output.name, .type = output.type, .nullable = output.nullable});
      }
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

    if (where != nullptr && !coordinator_where) {
      cseg::EventTimePredicate predicate;
      lower_event_time_leaf(select, source, *where, predicate);
      if (!predicate.lower.has_value() && !predicate.upper.has_value()) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, where->span(),
                                         common::StatusCode::kInternal,
                                         "Distributed WHERE produced no event-time bound")};
      }
      result.event_time_predicate = predicate;
    } else if (where != nullptr) {
      auto lowered = lower_bound_sql_scalar_expression(select, *where, limits.expression_limits);
      if (!lowered.has_value())
        throw LoweringFailure{std::move(lowered.error())};
      if (lowered->result_shape().type.kind() != schema::LogicalTypeKind::kBool) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, where->span(),
                                         common::StatusCode::kInternal,
                                         "Distributed WHERE result is not Boolean")};
      }
      const auto next_expression =
          common::checked_add(expression_bytes, lowered->retained_configuration_bytes());
      if (!next_expression.has_value() ||
          *next_expression > limits.maximum_expression_configuration_bytes) {
        throw LoweringFailure{
            diagnostic(SqlDiagnosticCode::kResourceLimit, where->span(),
                       common::StatusCode::kResourceExhausted,
                       "Distributed expression configuration bytes exceed the limit")};
      }
      coordinator_projection.predicate.emplace(std::move(*lowered));
      needs_coordinator_projection = true;
    }

    if (has_computed_order) {
      coordinator_projection.order_keys.reserve(select.syntax().order_by().size());
      for (const SqlOrderItem& item : select.syntax().order_by()) {
        const SqlExpression* expression = &item.expression;
        const std::optional<std::size_t> selected = order_output_ordinal(select, item);
        if (selected.has_value()) {
          if (*selected >= select.outputs().size()) {
            return std::unexpected(diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                              item.expression.span(), common::StatusCode::kInternal,
                                              "Distributed ORDER BY output is unavailable"));
          }
          const BoundOutputColumn& output = select.outputs()[*selected];
          expression = output.expression_span.has_value()
                           ? output_expression(select, *output.expression_span)
                           : nullptr;
          if (expression == nullptr) {
            unsupported(item.expression.span(),
                        "Distributed ORDER BY output expression is unavailable");
          }
        }
        if (source_independent(select, *expression))
          continue;
        if (coordinator_projection.order_keys.size() >= limits.maximum_order_keys) {
          throw LoweringFailure{diagnostic(SqlDiagnosticCode::kResourceLimit,
                                           item.expression.span(),
                                           common::StatusCode::kResourceExhausted,
                                           "Distributed ORDER BY key count exceeds the limit")};
        }
        auto lowered =
            lower_bound_sql_scalar_expression(select, *expression, limits.expression_limits);
        if (!lowered.has_value())
          throw LoweringFailure{std::move(lowered.error())};
        const auto next_expression =
            common::checked_add(expression_bytes, lowered->retained_configuration_bytes());
        if (!next_expression.has_value() ||
            *next_expression > limits.maximum_expression_configuration_bytes) {
          throw LoweringFailure{
              diagnostic(SqlDiagnosticCode::kResourceLimit, item.expression.span(),
                         common::StatusCode::kResourceExhausted,
                         "Distributed expression configuration bytes exceed the limit")};
        }
        expression_bytes = *next_expression;
        coordinator_projection.order_keys.push_back(
            {.expression = std::move(*lowered),
             .direction = item.direction == SqlOrderDirection::kAscending
                              ? PhysicalSortDirection::kAscending
                              : PhysicalSortDirection::kDescending,
             .null_placement = null_placement(item)});
      }
      needs_coordinator_projection = true;
    }

    std::vector<bool> ordered_outputs(result.intent.row_output_indices.size(), false);
    std::span<const SqlOrderItem> direct_order_items = select.syntax().order_by();
    if (has_computed_order)
      direct_order_items = {};
    for (const SqlOrderItem& item : direct_order_items) {
      std::optional<std::size_t> output;
      const std::optional<std::size_t> select_output = order_output_ordinal(select, item);
      if (select_output.has_value()) {
        if (*select_output >= select_worker_output.size()) {
          return std::unexpected(diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                            item.expression.span(), common::StatusCode::kInternal,
                                            "Distributed ORDER BY output is unavailable"));
        }
        output = select_worker_output[*select_output];
        if (!output.has_value() && select_row_dependent[*select_output]) {
          unsupported(item.expression.span(),
                      "Distributed ORDER BY cannot use a computed row output");
        }
        if (!output.has_value())
          continue;
      }
      if (!output.has_value()) {
        const BoundColumnReference* reference =
            select.find_column_reference(item.expression.span());
        if (reference == nullptr || reference->source_ordinal != 0U ||
            reference->column_ordinal >= source.columns().size()) {
          if (source_independent(select, item.expression))
            continue;
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
            distributed_vector_result_schema_format::kMaximumNameLength ||
        limits.expression_limits.maximum_instructions == 0U ||
        limits.expression_limits.maximum_instructions > kMaximumVectorExpressionInstructions ||
        limits.expression_limits.maximum_retained_configuration_bytes == 0U ||
        limits.maximum_expression_configuration_bytes == 0U) {
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
    for (const SqlOrderItem& item : select.syntax().order_by()) {
      const std::optional<std::size_t> output = order_output_ordinal(select, item);
      if (!output.has_value()) {
        unsupported(item.expression.span(),
                    "Distributed global aggregate ORDER BY must name a SELECT output");
      }
      if (*output >= select.outputs().size()) {
        return std::unexpected(diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                          item.expression.span(), common::StatusCode::kInternal,
                                          "Distributed aggregate ORDER BY output is unavailable"));
      }
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
    std::vector<const SqlExpression*> aggregate_expressions;
    for (const SqlSelectItem& item : select.syntax().items()) {
      const SqlExpression* expression = item.expression();
      if (expression != nullptr)
        collect_distributed_aggregates(*expression, aggregate_expressions);
    }
    if (aggregate_expressions.empty() || aggregate_expressions.size() > limits.maximum_aggregates) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                                        common::StatusCode::kResourceExhausted,
                                        "Distributed aggregate state width exceeds the limit"));
    }
    bool identity_outputs = aggregate_expressions.size() == select.outputs().size();
    for (std::size_t index = 0U; identity_outputs && index < select.outputs().size(); ++index) {
      const SqlExpression* output = select.syntax().items()[index].expression();
      identity_outputs =
          output != nullptr && output->span() == aggregate_expressions[index]->span();
    }

    const schema::TableSchema& source = *select.sources().front().schema_ptr();
    const SqlExpression* const where = select.syntax().where();
    const bool coordinator_where =
        where != nullptr && !event_time_predicate_shape(select, source, *where);
    const bool needs_full_source =
        coordinator_where && where != nullptr && !source_independent(select, *where);
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
        .coordinator_predicate = std::nullopt,
        .coordinator_projection = std::nullopt,
    };
    std::vector<std::int64_t> projected_index(source.columns().size(), -1);
    if (needs_full_source) {
      if (source.columns().size() > limits.maximum_projection_columns) {
        throw LoweringFailure{
            diagnostic(SqlDiagnosticCode::kResourceLimit, where->span(),
                       common::StatusCode::kResourceExhausted,
                       "Distributed aggregate predicate source width exceeds the limit")};
      }
      result.input_rows.destination_column_ordinals.reserve(source.columns().size());
      result.input_rows.intent.row_output_indices.reserve(source.columns().size());
      result.input_rows.result_schema.columns.reserve(source.columns().size());
      for (std::size_t ordinal = 0U; ordinal < source.columns().size(); ++ordinal) {
        const schema::ColumnDefinition& column = source.columns()[ordinal];
        projected_index[ordinal] = static_cast<std::int64_t>(ordinal);
        result.input_rows.destination_column_ordinals.push_back(
            static_cast<std::uint32_t>(ordinal));
        result.input_rows.intent.row_output_indices.push_back(static_cast<std::uint32_t>(ordinal));
        result.input_rows.result_schema.columns.push_back(
            {.name = column.name(), .type = column.type(), .nullable = column.nullable()});
      }
    } else {
      result.input_rows.destination_column_ordinals.reserve(select.outputs().size());
      result.input_rows.intent.row_output_indices.reserve(select.outputs().size());
      result.input_rows.result_schema.columns.reserve(select.outputs().size());
    }
    for (std::size_t output_index = 0U; output_index < select.outputs().size(); ++output_index) {
      const BoundOutputColumn& output = select.outputs()[output_index];
      const SourceSpan span = output.expression_span.value_or(select.syntax().span());
      if (output.name.empty() || output.name.size() > limits.maximum_result_name_bytes) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kResourceLimit, span,
                                         common::StatusCode::kResourceExhausted,
                                         "Distributed aggregate result name exceeds the limit")};
      }
    }

    result.intent.aggregates.reserve(aggregate_expressions.size());
    result.result_schema.columns.reserve(aggregate_expressions.size());
    std::vector<VectorAggregateExpressionBinding> aggregate_bindings;
    aggregate_bindings.reserve(aggregate_expressions.size());
    for (std::size_t aggregate_index = 0U; aggregate_index < aggregate_expressions.size();
         ++aggregate_index) {
      const SqlExpression* expression = aggregate_expressions[aggregate_index];
      if (expression->children().size() != 1U) {
        unsupported(expression->span(), "Distributed aggregate function arity is unsupported");
      }
      const std::optional<VectorAggregateOperation> operation =
          distributed_aggregate_operation(expression->text());
      if (!operation.has_value())
        unsupported(expression->span(), "Distributed aggregate function is unsupported");

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
      const BoundExpressionInfo* bound_aggregate = select.find_expression(expression->span());
      if (bound_aggregate == nullptr || !bound_aggregate->type.has_value() ||
          shape->type != *bound_aggregate->type || shape->nullable != bound_aggregate->nullable) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, expression->span(),
                                         common::StatusCode::kInternal,
                                         "Bound aggregate result shape disagrees with its kernel")};
      }
      result.intent.aggregates.push_back(aggregate);
      result.result_schema.columns.push_back(
          {.name = identity_outputs ? select.outputs()[aggregate_index].name : "_",
           .type = shape->type,
           .nullable = shape->nullable});
      aggregate_bindings.push_back(
          {.expression_span = expression->span(),
           .input_column_ordinal = static_cast<std::uint32_t>(aggregate_index),
           .type = shape->type,
           .nullable = shape->nullable});
    }

    std::size_t expression_bytes{};
    if (!identity_outputs) {
      DistributedVectorAggregateCoordinatorProjection projection;
      projection.outputs.reserve(select.outputs().size());
      projection.result_schema.columns.reserve(select.outputs().size());
      for (std::size_t output_index = 0U; output_index < select.outputs().size(); ++output_index) {
        const BoundOutputColumn& output = select.outputs()[output_index];
        const SqlExpression* expression = select.syntax().items()[output_index].expression();
        if (expression == nullptr) {
          unsupported(output.expression_span.value_or(select.syntax().span()),
                      "Distributed aggregate output expression is unavailable");
        }
        auto lowered = lower_bound_sql_aggregate_scalar_expression(
            select, *expression, aggregate_bindings, limits.expression_limits);
        if (!lowered.has_value())
          throw LoweringFailure{std::move(lowered.error())};
        if (lowered->result_shape().type != output.type ||
            lowered->result_shape().nullable != output.nullable) {
          throw LoweringFailure{
              diagnostic(SqlDiagnosticCode::kExecutionFailure, expression->span(),
                         common::StatusCode::kInternal,
                         "Distributed aggregate output expression shape is inconsistent")};
        }
        const auto next =
            common::checked_add(expression_bytes, lowered->retained_configuration_bytes());
        if (!next.has_value() || *next > limits.maximum_expression_configuration_bytes) {
          throw LoweringFailure{
              diagnostic(SqlDiagnosticCode::kResourceLimit, expression->span(),
                         common::StatusCode::kResourceExhausted,
                         "Distributed aggregate expression configuration bytes exceed the limit")};
        }
        expression_bytes = *next;
        projection.outputs.push_back(std::move(*lowered));
        projection.result_schema.columns.push_back(
            {.name = output.name, .type = output.type, .nullable = output.nullable});
      }
      result.coordinator_projection.emplace(std::move(projection));
    }

    if (where != nullptr && !coordinator_where) {
      cseg::EventTimePredicate predicate;
      lower_event_time_leaf(select, source, *where, predicate);
      if (!predicate.lower.has_value() && !predicate.upper.has_value()) {
        throw LoweringFailure{diagnostic(
            SqlDiagnosticCode::kExecutionFailure, where->span(), common::StatusCode::kInternal,
            "Distributed aggregate WHERE produced no event-time bound")};
      }
      result.input_rows.event_time_predicate = predicate;
    } else if (where != nullptr) {
      auto lowered = lower_bound_sql_scalar_expression(select, *where, limits.expression_limits);
      if (!lowered.has_value())
        throw LoweringFailure{std::move(lowered.error())};
      if (lowered->result_shape().type.kind() != schema::LogicalTypeKind::kBool) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, where->span(),
                                         common::StatusCode::kInternal,
                                         "Distributed aggregate WHERE result is not Boolean")};
      }
      const auto next =
          common::checked_add(expression_bytes, lowered->retained_configuration_bytes());
      if (!next.has_value() || *next > limits.maximum_expression_configuration_bytes) {
        throw LoweringFailure{
            diagnostic(SqlDiagnosticCode::kResourceLimit, where->span(),
                       common::StatusCode::kResourceExhausted,
                       "Distributed aggregate expression configuration bytes exceed the limit")};
      }
      result.coordinator_predicate.emplace(std::move(*lowered));
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
    if (result.coordinator_projection.has_value()) {
      const common::Status projection_schema = validate_distributed_vector_result_schema_value(
          result.coordinator_projection->result_schema);
      if (!projection_schema.is_ok()) {
        return std::unexpected(SqlDiagnostic{SqlDiagnosticCode::kExecutionFailure,
                                             select.syntax().span(), projection_schema});
      }
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

SqlResult<DistributedVectorGroupedAggregateSqlPlan>
lower_bound_sql_select_to_distributed_vector_grouped_aggregate(
    const BoundSqlSelect& select, const DistributedVectorGroupedAggregateSqlLoweringLimits limits) {
  try {
    if (limits.maximum_projection_columns == 0U ||
        limits.maximum_projection_columns > distributed_vector_plan_format::kMaximumInputColumns ||
        limits.maximum_group_keys == 0U ||
        limits.maximum_group_keys > distributed_vector_plan_format::kMaximumGroupKeys ||
        limits.maximum_aggregates == 0U ||
        limits.maximum_aggregates > distributed_vector_plan_format::kMaximumAggregates ||
        limits.maximum_order_keys == 0U ||
        limits.maximum_order_keys > distributed_vector_plan_format::kMaximumOrderKeys ||
        limits.maximum_result_name_bytes == 0U ||
        limits.maximum_result_name_bytes >
            distributed_vector_result_schema_format::kMaximumNameLength) {
      return std::unexpected(
          diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                     common::StatusCode::kInvalidArgument,
                     "Distributed grouped aggregate SQL lowering limits are invalid"));
    }
    if (select.syntax().mode() != SqlSelectMode::kSelect || select.sources().size() != 1U ||
        !select.asof_joins().empty() || select.syntax().system_time().has_value() ||
        select.latest_by().has_value() || !select.aggregate_query() ||
        select.syntax().group_by().empty()) {
      return std::unexpected(diagnostic(
          SqlDiagnosticCode::kUnsupportedSyntax, select.syntax().span(),
          common::StatusCode::kNotSupported,
          "Distributed grouped aggregate lowering requires one current grouped SELECT source"));
    }
    const auto group_by = select.syntax().group_by();
    const auto outputs = select.outputs();
    if (select.syntax().items().size() != outputs.size()) {
      return std::unexpected(
          diagnostic(SqlDiagnosticCode::kExecutionFailure, select.syntax().span(),
                     common::StatusCode::kInternal,
                     "Distributed grouped aggregate bound output count is inconsistent"));
    }
    if (group_by.size() > limits.maximum_group_keys || outputs.size() < group_by.size() ||
        outputs.size() - group_by.size() > limits.maximum_aggregates) {
      return std::unexpected(
          diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                     common::StatusCode::kResourceExhausted,
                     "Distributed grouped aggregate SQL width exceeds the limit"));
    }
    const schema::TableSchema& source = *select.sources().front().schema_ptr();
    const SqlExpression* const where = select.syntax().where();
    if (where != nullptr && !event_time_predicate_shape(select, source, *where)) {
      unsupported(where->span(), "Distributed grouped aggregate WHERE must be an event-time range");
    }

    DistributedVectorGroupedAggregateSqlPlan result{
        .table_id = source.table_id(),
        .destination_schema_id = source.schema_id(),
        .destination_column_ordinals = {},
        .event_time_predicate = std::nullopt,
        .intent = {.mode = DistributedVectorPlanMode::kGroupedAggregate,
                   .row_output_indices = {},
                   .visible_row_output_indices = {},
                   .group_key_input_indices = {},
                   .aggregates = {},
                   .order_keys = {},
                   .limit = select.syntax().limit()},
        .result_schema = {},
    };
    result.destination_column_ordinals.reserve(
        std::min<std::size_t>(source.columns().size(), limits.maximum_projection_columns));
    result.intent.group_key_input_indices.reserve(group_by.size());
    result.intent.aggregates.reserve(outputs.size() - group_by.size());
    result.result_schema.columns.reserve(outputs.size());
    std::vector<std::int64_t> projected_index(source.columns().size(), -1);

    const auto project = [&](const std::size_t source_ordinal,
                             const SourceSpan span) -> std::uint32_t {
      if (source_ordinal >= source.columns().size()) {
        throw LoweringFailure{
            diagnostic(SqlDiagnosticCode::kExecutionFailure, span, common::StatusCode::kInternal,
                       "Distributed grouped aggregate source column is unavailable")};
      }
      if (projected_index[source_ordinal] < 0) {
        if (result.destination_column_ordinals.size() >= limits.maximum_projection_columns) {
          throw LoweringFailure{diagnostic(
              SqlDiagnosticCode::kResourceLimit, span, common::StatusCode::kResourceExhausted,
              "Distributed grouped aggregate projection exceeds the limit")};
        }
        projected_index[source_ordinal] =
            static_cast<std::int64_t>(result.destination_column_ordinals.size());
        result.destination_column_ordinals.push_back(static_cast<std::uint32_t>(source_ordinal));
      }
      return static_cast<std::uint32_t>(projected_index[source_ordinal]);
    };

    for (std::size_t index = 0U; index < group_by.size(); ++index) {
      const SqlExpression& group = group_by[index];
      const BoundColumnReference* const group_reference =
          select.find_column_reference(group.span());
      const SqlExpression* const output_expression = select.syntax().items()[index].expression();
      const BoundColumnReference* const output_reference =
          output_expression == nullptr ? nullptr
                                       : select.find_column_reference(output_expression->span());
      if (group.kind() != SqlExpressionKind::kColumn || group_reference == nullptr ||
          group_reference->source_ordinal != 0U || output_expression == nullptr ||
          output_expression->kind() != SqlExpressionKind::kColumn || output_reference == nullptr ||
          output_reference->source_ordinal != 0U ||
          output_reference->column_ordinal != group_reference->column_ordinal) {
        unsupported(group.span(),
                    "Distributed grouped aggregate keys must be direct source columns emitted "
                    "first in GROUP BY order");
      }
      const std::uint32_t input_index = project(group_reference->column_ordinal, group.span());
      if (std::ranges::find(result.intent.group_key_input_indices, input_index) !=
          result.intent.group_key_input_indices.end()) {
        unsupported(group.span(), "Distributed grouped aggregate keys must be unique");
      }
      const schema::ColumnDefinition& column = source.columns()[group_reference->column_ordinal];
      const BoundOutputColumn& output = outputs[index];
      if (output.name.empty() || output.name.size() > limits.maximum_result_name_bytes) {
        throw LoweringFailure{
            diagnostic(SqlDiagnosticCode::kResourceLimit,
                       output.expression_span.value_or(select.syntax().span()),
                       common::StatusCode::kResourceExhausted,
                       "Distributed grouped aggregate result name exceeds the limit")};
      }
      if (output.type != column.type() || output.nullable != column.nullable()) {
        throw LoweringFailure{diagnostic(
            SqlDiagnosticCode::kExecutionFailure, group.span(), common::StatusCode::kInternal,
            "Bound grouped key output shape disagrees with its source column")};
      }
      result.intent.group_key_input_indices.push_back(input_index);
      result.result_schema.columns.push_back(
          {.name = output.name, .type = output.type, .nullable = output.nullable});
    }

    for (std::size_t output_index = group_by.size(); output_index < outputs.size();
         ++output_index) {
      const SqlExpression* const expression = select.syntax().items()[output_index].expression();
      if (expression == nullptr || expression->kind() != SqlExpressionKind::kFunction ||
          expression->children().size() != 1U) {
        unsupported(outputs[output_index].expression_span.value_or(select.syntax().span()),
                    "Distributed grouped aggregate outputs must be direct aggregate calls");
      }
      const std::optional<VectorAggregateOperation> operation =
          distributed_aggregate_operation(expression->text());
      if (!operation.has_value())
        unsupported(expression->span(), "Distributed grouped aggregate function is unsupported");

      const SqlExpression& child = expression->children().front();
      DistributedVectorAggregateIntent aggregate;
      VectorAggregateDefinition definition;
      if (child.kind() == SqlExpressionKind::kStar) {
        if (*operation != VectorAggregateOperation::kCount) {
          unsupported(child.span(), "Only distributed grouped COUNT may use a star input");
        }
        aggregate.operation = VectorAggregateOperation::kCountStar;
        definition.operation = VectorAggregateOperation::kCountStar;
      } else {
        const BoundColumnReference* const reference = select.find_column_reference(child.span());
        if (child.kind() != SqlExpressionKind::kColumn || reference == nullptr ||
            reference->source_ordinal != 0U ||
            reference->column_ordinal >= source.columns().size()) {
          unsupported(child.span(),
                      "Distributed grouped aggregate inputs must be direct source columns");
        }
        const std::uint32_t input_index = project(reference->column_ordinal, child.span());
        const schema::ColumnDefinition& column = source.columns()[reference->column_ordinal];
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
      const BoundOutputColumn& output = outputs[output_index];
      if (output.name.empty() || output.name.size() > limits.maximum_result_name_bytes) {
        throw LoweringFailure{
            diagnostic(SqlDiagnosticCode::kResourceLimit,
                       output.expression_span.value_or(select.syntax().span()),
                       common::StatusCode::kResourceExhausted,
                       "Distributed grouped aggregate result name exceeds the limit")};
      }
      if (output.type != shape->type || output.nullable != shape->nullable) {
        throw LoweringFailure{diagnostic(
            SqlDiagnosticCode::kExecutionFailure, expression->span(), common::StatusCode::kInternal,
            "Bound grouped aggregate output shape disagrees with its kernel")};
      }
      result.intent.aggregates.push_back(aggregate);
      result.result_schema.columns.push_back(
          {.name = output.name, .type = output.type, .nullable = output.nullable});
    }

    if (where != nullptr) {
      cseg::EventTimePredicate predicate;
      lower_event_time_leaf(select, source, *where, predicate);
      if (!predicate.lower.has_value() && !predicate.upper.has_value()) {
        throw LoweringFailure{diagnostic(
            SqlDiagnosticCode::kExecutionFailure, where->span(), common::StatusCode::kInternal,
            "Distributed grouped aggregate WHERE produced no event-time bound")};
      }
      result.event_time_predicate = predicate;
    }

    std::vector<bool> ordered_outputs(outputs.size(), false);
    for (const SqlOrderItem& item : select.syntax().order_by()) {
      const std::optional<std::size_t> output = order_output_ordinal(select, item);
      if (!output.has_value()) {
        unsupported(item.expression.span(),
                    "Distributed grouped aggregate ORDER BY must name a SELECT output");
      }
      if (*output >= outputs.size()) {
        return std::unexpected(
            diagnostic(SqlDiagnosticCode::kExecutionFailure, item.expression.span(),
                       common::StatusCode::kInternal,
                       "Distributed grouped aggregate ORDER BY output is unavailable"));
      }
      if (ordered_outputs[*output])
        continue;
      if (result.intent.order_keys.size() >= limits.maximum_order_keys) {
        throw LoweringFailure{
            diagnostic(SqlDiagnosticCode::kResourceLimit, item.expression.span(),
                       common::StatusCode::kResourceExhausted,
                       "Distributed grouped aggregate ORDER BY key count exceeds the limit")};
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
        static_cast<std::uint32_t>(outputs.size()));
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
    return std::unexpected(
        diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                   common::StatusCode::kResourceExhausted,
                   "Distributed grouped aggregate SQL lowering allocation failed"));
  } catch (const std::length_error&) {
    return std::unexpected(
        diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                   common::StatusCode::kResourceExhausted,
                   "Distributed grouped aggregate SQL lowering exceeds container limits"));
  }
}

SqlResult<DistributedVectorGroupedSqlPlan> lower_bound_sql_select_to_distributed_vector_grouped(
    const BoundSqlSelect& select, const DistributedVectorGroupedSqlLoweringLimits limits) {
  try {
    if (limits.rows.maximum_projection_columns == 0U ||
        limits.rows.maximum_projection_columns >
            distributed_vector_plan_format::kMaximumInputColumns ||
        limits.rows.maximum_output_columns == 0U ||
        limits.rows.maximum_output_columns >
            distributed_vector_plan_format::kMaximumOutputColumns ||
        limits.rows.maximum_result_name_bytes == 0U ||
        limits.rows.maximum_result_name_bytes >
            distributed_vector_result_schema_format::kMaximumNameLength) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                                        common::StatusCode::kInvalidArgument,
                                        "Distributed grouped SQL lowering limits are invalid"));
    }
    if (select.syntax().mode() != SqlSelectMode::kSelect || select.sources().size() != 1U ||
        !select.asof_joins().empty() || select.syntax().system_time().has_value() ||
        select.latest_by().has_value() || !select.aggregate_query() ||
        select.syntax().group_by().empty()) {
      return std::unexpected(
          diagnostic(SqlDiagnosticCode::kUnsupportedSyntax, select.syntax().span(),
                     common::StatusCode::kNotSupported,
                     "Distributed grouped lowering requires one current grouped SELECT source"));
    }
    const schema::TableSchema& source = *select.sources().front().schema_ptr();
    if (source.columns().empty() ||
        source.columns().size() > limits.rows.maximum_projection_columns ||
        select.outputs().empty() || select.outputs().size() > limits.rows.maximum_output_columns) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                                        common::StatusCode::kResourceExhausted,
                                        "Distributed grouped SQL width exceeds the limit"));
    }

    auto physical = lower_bound_sql_select(select, limits.physical);
    if (!physical.has_value())
      return std::unexpected(std::move(physical.error()));
    if (physical->input_columns().size() != source.columns().size() ||
        physical->output_columns().size() != select.outputs().size()) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                        select.syntax().span(), common::StatusCode::kInternal,
                                        "Distributed grouped physical shape is inconsistent"));
    }

    DistributedVectorRowsSqlPlan input{
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
                   .limit = std::nullopt},
        .result_schema = {},
        .coordinator_projection = std::nullopt,
    };
    input.destination_column_ordinals.reserve(source.columns().size());
    input.intent.row_output_indices.reserve(source.columns().size());
    input.result_schema.columns.reserve(source.columns().size());
    for (std::size_t ordinal = 0U; ordinal < source.columns().size(); ++ordinal) {
      const schema::ColumnDefinition& column = source.columns()[ordinal];
      if (column.name().size() > limits.rows.maximum_result_name_bytes ||
          physical->input_columns()[ordinal] !=
              PhysicalColumnShape{.type = column.type(), .nullable = column.nullable()}) {
        return std::unexpected(
            diagnostic(SqlDiagnosticCode::kExecutionFailure, select.syntax().span(),
                       common::StatusCode::kInternal,
                       "Distributed grouped input shape disagrees with the bound schema"));
      }
      input.destination_column_ordinals.push_back(static_cast<std::uint32_t>(ordinal));
      input.intent.row_output_indices.push_back(static_cast<std::uint32_t>(ordinal));
      input.result_schema.columns.push_back(
          {.name = column.name(), .type = column.type(), .nullable = column.nullable()});
    }

    DistributedVectorResultSchema output_schema;
    output_schema.columns.reserve(select.outputs().size());
    for (std::size_t ordinal = 0U; ordinal < select.outputs().size(); ++ordinal) {
      const BoundOutputColumn& output = select.outputs()[ordinal];
      if (output.name.empty() || output.name.size() > limits.rows.maximum_result_name_bytes) {
        return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit,
                                          output.expression_span.value_or(select.syntax().span()),
                                          common::StatusCode::kResourceExhausted,
                                          "Distributed grouped result name exceeds the limit"));
      }
      if (physical->output_columns()[ordinal] !=
          PhysicalColumnShape{.type = output.type, .nullable = output.nullable}) {
        return std::unexpected(diagnostic(
            SqlDiagnosticCode::kExecutionFailure,
            output.expression_span.value_or(select.syntax().span()), common::StatusCode::kInternal,
            "Distributed grouped output shape disagrees with the bound result"));
      }
      output_schema.columns.push_back(
          {.name = output.name, .type = output.type, .nullable = output.nullable});
    }

    const common::Status plan_status = validate_distributed_vector_plan_intent(
        input.intent, static_cast<std::uint32_t>(source.columns().size()),
        static_cast<std::uint32_t>(source.columns().size()));
    const common::Status input_schema_status = validate_distributed_vector_result_schema(
        input.intent, physical->input_columns(), input.result_schema);
    const common::Status output_schema_status =
        validate_distributed_vector_result_schema_value(output_schema);
    if (!plan_status.is_ok() || !input_schema_status.is_ok() || !output_schema_status.is_ok()) {
      const common::Status& failure = !plan_status.is_ok()           ? plan_status
                                      : !input_schema_status.is_ok() ? input_schema_status
                                                                     : output_schema_status;
      const SqlDiagnosticCode code = failure.code() == common::StatusCode::kResourceExhausted
                                         ? SqlDiagnosticCode::kResourceLimit
                                         : SqlDiagnosticCode::kExecutionFailure;
      return std::unexpected(SqlDiagnostic{code, select.syntax().span(), failure});
    }
    return DistributedVectorGroupedSqlPlan{.input_rows = std::move(input),
                                           .coordinator_pipeline = std::move(*physical),
                                           .result_schema = std::move(output_schema)};
  } catch (const std::bad_alloc&) {
    return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                                      common::StatusCode::kResourceExhausted,
                                      "Distributed grouped SQL lowering allocation failed"));
  } catch (const std::length_error&) {
    return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                                      common::StatusCode::kResourceExhausted,
                                      "Distributed grouped SQL lowering exceeds container limits"));
  }
}

} // namespace chronos::query
