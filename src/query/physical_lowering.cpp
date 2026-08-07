#include "chronos/query/physical_lowering.hpp"

#include "chronos/common/status.hpp"
#include "chronos/query/literal.hpp"
#include "chronos/query/value.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] bool same_span(const SourceSpan& left, const SourceSpan& right) noexcept {
  return left.begin.byte_offset == right.begin.byte_offset && left.byte_length == right.byte_length;
}

[[nodiscard]] SqlDiagnostic diagnostic(const SqlDiagnosticCode code, const SourceSpan span,
                                       const common::StatusCode status,
                                       const std::string_view message) {
  return SqlDiagnostic{code, span, common::Status{status, std::string{message}}};
}

[[nodiscard]] SqlDiagnostic from_status(const SourceSpan span, const common::Status& status) {
  const SqlDiagnosticCode code = status.code() == common::StatusCode::kResourceExhausted
                                     ? SqlDiagnosticCode::kResourceLimit
                                     : SqlDiagnosticCode::kExecutionFailure;
  return SqlDiagnostic{code, span, status};
}

class LoweringFailure final {
public:
  explicit LoweringFailure(SqlDiagnostic diagnostic) : diagnostic_(std::move(diagnostic)) {}
  [[nodiscard]] SqlDiagnostic take() {
    return std::move(diagnostic_);
  }

private:
  SqlDiagnostic diagnostic_;
};

struct AggregatePhysicalBinding {
  SourceSpan expression_span;
  std::size_t column_ordinal;
  schema::LogicalType type;
  bool nullable;
};

class ExpressionLowerer {
public:
  ExpressionLowerer(const BoundSqlSelect& select, const VectorExpressionLimits limits,
                    const std::span<const AggregatePhysicalBinding> aggregates = {},
                    const bool source_columns_available = true) noexcept
      : select_(select), limits_(limits), aggregates_(aggregates),
        source_columns_available_(source_columns_available) {}

  [[nodiscard]] ColumnOutputPosition output(const SqlExpression& expression) {
    if (const AggregatePhysicalBinding* aggregate = find_aggregate(expression.span());
        aggregate != nullptr) {
      return SourceColumnOutputPosition{aggregate->column_ordinal};
    }
    if (expression.kind() == SqlExpressionKind::kColumn) {
      const BoundColumnReference& reference = column(expression);
      return SourceColumnOutputPosition{reference.column_ordinal};
    }
    if (expression.kind() == SqlExpressionKind::kLiteral)
      return ConstantColumnOutputPosition{literal(expression, type_of(expression))};
    instructions_.clear();
    instructions_.reserve(limits_.maximum_instructions);
    static_cast<void>(append(expression, type_of(expression)));
    common::Result<VectorExpression> program =
        VectorExpression::create(std::move(instructions_), limits_);
    if (!program.has_value()) {
      const SqlDiagnosticCode code =
          program.error().code() == common::StatusCode::kResourceExhausted
              ? SqlDiagnosticCode::kResourceLimit
              : (program.error().code() == common::StatusCode::kInvalidArgument
                     ? SqlDiagnosticCode::kUnsupportedSyntax
                     : SqlDiagnosticCode::kExecutionFailure);
      throw LoweringFailure{SqlDiagnostic{code, expression.span(), std::move(program.error())}};
    }
    return ComputedColumnOutputPosition{std::move(*program)};
  }

private:
  [[nodiscard]] const AggregatePhysicalBinding*
  find_aggregate(const SourceSpan span) const noexcept {
    for (const AggregatePhysicalBinding& aggregate : aggregates_) {
      if (same_span(aggregate.expression_span, span))
        return std::addressof(aggregate);
    }
    return nullptr;
  }

  [[nodiscard]] const schema::LogicalType& type_of(const SqlExpression& expression) const {
    const BoundExpressionInfo* information = select_.find_expression(expression.span());
    if (information == nullptr || !information->type.has_value()) {
      throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, expression.span(),
                                       common::StatusCode::kInternal,
                                       "Bound expression has no physical result type")};
    }
    return *information->type;
  }

  [[nodiscard]] const schema::LogicalType&
  type_or(const SqlExpression& expression, const schema::LogicalType& fallback) const noexcept {
    const BoundExpressionInfo* information = select_.find_expression(expression.span());
    return information != nullptr && information->type.has_value() ? *information->type : fallback;
  }

  [[nodiscard]] const schema::LogicalType& first_child_type(const SqlExpression& expression) const {
    for (const SqlExpression& child : expression.children()) {
      const BoundExpressionInfo* information = select_.find_expression(child.span());
      if (information != nullptr && information->type.has_value())
        return *information->type;
    }
    throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, expression.span(),
                                     common::StatusCode::kInternal,
                                     "Bound comparison has no typed operand")};
  }

  [[nodiscard]] const BoundColumnReference& column(const SqlExpression& expression) const {
    if (!source_columns_available_) {
      throw LoweringFailure{diagnostic(
          SqlDiagnosticCode::kExecutionFailure, expression.span(), common::StatusCode::kInternal,
          "Bound global aggregate output retained an ungrouped source column")};
    }
    const BoundColumnReference* reference = select_.find_column_reference(expression.span());
    if (reference == nullptr || reference->source_ordinal != 0U) {
      throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, expression.span(),
                                       common::StatusCode::kInvalidArgument,
                                       "Physical expression references an unavailable source")};
    }
    return *reference;
  }

  template <typename Value>
  [[nodiscard]] Value parsed(common::Result<Value> result, const SourceSpan span) const {
    if (!result.has_value())
      throw LoweringFailure{from_status(span, result.error())};
    return std::move(*result);
  }

  [[nodiscard]] ScalarValue literal(const SqlExpression& expression,
                                    const schema::LogicalType& expected) const {
    switch (expression.literal_kind()) {
    case SqlLiteralKind::kNull:
      return ScalarValue::null(expected);
    case SqlLiteralKind::kBoolean:
      return ScalarValue::boolean(expression.text() == "true").value();
    case SqlLiteralKind::kInteger:
      return parsed(
          ScalarValue::signed_value(
              expected, parsed(parse_sql_integer_literal(expression.text()), expression.span())),
          expression.span());
    case SqlLiteralKind::kFloat:
      return ScalarValue::float64(
                 parsed(parse_sql_float_literal(expression.text()), expression.span()))
          .value();
    case SqlLiteralKind::kString:
      return parsed(ScalarValue::text(expected, expression.text()), expression.span());
    case SqlLiteralKind::kBinary: {
      std::vector<std::byte> bytes(expression.text().size());
      if (!bytes.empty())
        std::memcpy(bytes.data(), expression.text().data(), bytes.size());
      return ScalarValue::binary(std::move(bytes));
    }
    case SqlLiteralKind::kTimestamp:
      return parsed(ScalarValue::signed_value(
                        expected, parsed(parse_sql_timestamp_ns_literal(expression.text()),
                                         expression.span())),
                    expression.span());
    case SqlLiteralKind::kDate:
      return parsed(
          ScalarValue::signed_value(
              expected, parsed(parse_sql_date_literal(expression.text()), expression.span())),
          expression.span());
    case SqlLiteralKind::kInterval:
      return parsed(ScalarValue::signed_value(
                        expected, parsed(parse_sql_interval_ns_literal(expression.text()),
                                         expression.span())),
                    expression.span());
    case SqlLiteralKind::kUuid:
      return ScalarValue::uuid(
          parsed(parse_sql_uuid_literal(expression.text()), expression.span()));
    }
    throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, expression.span(),
                                     common::StatusCode::kInternal,
                                     "SQL literal kind is invalid during physical lowering")};
  }

  [[nodiscard]] static VectorUnaryOperation unary(const SqlOperator operation,
                                                  const SourceSpan span) {
    switch (operation) {
    case SqlOperator::kPositive:
      return VectorUnaryOperation::kPositive;
    case SqlOperator::kNegative:
      return VectorUnaryOperation::kNegative;
    case SqlOperator::kNot:
      return VectorUnaryOperation::kNot;
    case SqlOperator::kIsNull:
      return VectorUnaryOperation::kIsNull;
    case SqlOperator::kIsNotNull:
      return VectorUnaryOperation::kIsNotNull;
    default:
      throw LoweringFailure{diagnostic(SqlDiagnosticCode::kUnsupportedSyntax, span,
                                       common::StatusCode::kInvalidArgument,
                                       "SQL unary operation has no physical kernel")};
    }
  }

  [[nodiscard]] static VectorBinaryOperation binary(const SqlOperator operation,
                                                    const SourceSpan span) {
    switch (operation) {
    case SqlOperator::kAnd:
      return VectorBinaryOperation::kAnd;
    case SqlOperator::kOr:
      return VectorBinaryOperation::kOr;
    case SqlOperator::kEqual:
      return VectorBinaryOperation::kEqual;
    case SqlOperator::kNotEqual:
      return VectorBinaryOperation::kNotEqual;
    case SqlOperator::kLess:
      return VectorBinaryOperation::kLess;
    case SqlOperator::kLessEqual:
      return VectorBinaryOperation::kLessEqual;
    case SqlOperator::kGreater:
      return VectorBinaryOperation::kGreater;
    case SqlOperator::kGreaterEqual:
      return VectorBinaryOperation::kGreaterEqual;
    case SqlOperator::kAdd:
      return VectorBinaryOperation::kAdd;
    case SqlOperator::kSubtract:
      return VectorBinaryOperation::kSubtract;
    case SqlOperator::kMultiply:
      return VectorBinaryOperation::kMultiply;
    case SqlOperator::kDivide:
      return VectorBinaryOperation::kDivide;
    case SqlOperator::kRemainder:
      return VectorBinaryOperation::kRemainder;
    default:
      throw LoweringFailure{diagnostic(SqlDiagnosticCode::kUnsupportedSyntax, span,
                                       common::StatusCode::kInvalidArgument,
                                       "SQL binary operation has no physical kernel")};
    }
  }

  template <typename Instruction>
  [[nodiscard]] std::size_t emit(Instruction instruction, const SourceSpan span) {
    if (instructions_.size() >= limits_.maximum_instructions) {
      throw LoweringFailure{diagnostic(SqlDiagnosticCode::kResourceLimit, span,
                                       common::StatusCode::kResourceExhausted,
                                       "Physical expression instruction limit exceeded")};
    }
    instructions_.emplace_back(std::move(instruction));
    return instructions_.size() - 1U;
  }

  [[nodiscard]] std::size_t append(const SqlExpression& expression,
                                   const schema::LogicalType& expected) {
    if (instructions_.size() >= limits_.maximum_instructions) {
      throw LoweringFailure{diagnostic(SqlDiagnosticCode::kResourceLimit, expression.span(),
                                       common::StatusCode::kResourceExhausted,
                                       "Physical expression instruction limit exceeded")};
    }
    if (const AggregatePhysicalBinding* aggregate = find_aggregate(expression.span());
        aggregate != nullptr) {
      return emit(VectorInputExpression{.input_column_ordinal = aggregate->column_ordinal,
                                        .type = aggregate->type,
                                        .nullable = aggregate->nullable},
                  expression.span());
    }
    switch (expression.kind()) {
    case SqlExpressionKind::kColumn: {
      const BoundColumnReference& reference = column(expression);
      static_cast<void>(emit(VectorInputExpression{.input_column_ordinal = reference.column_ordinal,
                                                   .type = reference.type,
                                                   .nullable = reference.nullable},
                             expression.span()));
      break;
    }
    case SqlExpressionKind::kLiteral:
      static_cast<void>(
          emit(VectorConstantExpression{literal(expression, expected)}, expression.span()));
      break;
    case SqlExpressionKind::kUnary: {
      const std::size_t operand =
          append(expression.children().front(), type_of(expression.children().front()));
      static_cast<void>(
          emit(VectorUnaryExpression{.operation = unary(expression.operation(), expression.span()),
                                     .operand_instruction = operand},
               expression.span()));
      break;
    }
    case SqlExpressionKind::kIsNull: {
      const SqlExpression& child = expression.children().front();
      const BoundExpressionInfo* information = select_.find_expression(child.span());
      const schema::LogicalType& child_type =
          information != nullptr && information->type.has_value() ? *information->type : expected;
      const std::size_t operand = append(child, child_type);
      static_cast<void>(
          emit(VectorUnaryExpression{.operation = unary(expression.operation(), expression.span()),
                                     .operand_instruction = operand},
               expression.span()));
      break;
    }
    case SqlExpressionKind::kBinary: {
      const SqlExpression& left = expression.children()[0];
      const SqlExpression& right = expression.children()[1];
      const BoundExpressionInfo* left_info = select_.find_expression(left.span());
      const BoundExpressionInfo* right_info = select_.find_expression(right.span());
      const schema::LogicalType& left_type =
          left_info != nullptr && left_info->type.has_value()
              ? *left_info->type
              : (right_info != nullptr && right_info->type.has_value() ? *right_info->type
                                                                       : expected);
      const schema::LogicalType& right_type =
          right_info != nullptr && right_info->type.has_value() ? *right_info->type : left_type;
      const std::size_t lhs = append(left, left_type);
      const std::size_t rhs = append(right, right_type);
      static_cast<void>(emit(
          VectorBinaryExpression{.operation = binary(expression.operation(), expression.span()),
                                 .left_instruction = lhs,
                                 .right_instruction = rhs},
          expression.span()));
      break;
    }
    case SqlExpressionKind::kBetween: {
      const SqlExpression& value = expression.children()[0];
      const SqlExpression& lower_expression = expression.children()[1];
      const schema::LogicalType& operand_type = type_or(value, first_child_type(expression));
      const std::size_t value_index = append(value, operand_type);
      const std::size_t lower = append(lower_expression, type_or(lower_expression, operand_type));
      const std::size_t low =
          emit(VectorBinaryExpression{.operation = VectorBinaryOperation::kGreaterEqual,
                                      .left_instruction = value_index,
                                      .right_instruction = lower},
               expression.span());
      const SqlExpression& upper_expression = expression.children()[2];
      const std::size_t upper = append(upper_expression, type_or(upper_expression, operand_type));
      const std::size_t high =
          emit(VectorBinaryExpression{.operation = VectorBinaryOperation::kLessEqual,
                                      .left_instruction = value_index,
                                      .right_instruction = upper},
               expression.span());
      static_cast<void>(emit(VectorBinaryExpression{.operation = VectorBinaryOperation::kAnd,
                                                    .left_instruction = low,
                                                    .right_instruction = high},
                             expression.span()));
      if (expression.operation() == SqlOperator::kNotBetween) {
        static_cast<void>(
            emit(VectorUnaryExpression{.operation = VectorUnaryOperation::kNot,
                                       .operand_instruction = instructions_.size() - 1U},
                 expression.span()));
      }
      break;
    }
    case SqlExpressionKind::kIn: {
      const SqlExpression& searched = expression.children().front();
      const schema::LogicalType& operand_type = type_or(searched, first_child_type(expression));
      const std::size_t searched_index = append(searched, operand_type);
      std::optional<std::size_t> accumulated;
      for (std::size_t index = 1U; index < expression.children().size(); ++index) {
        const SqlExpression& candidate_expression = expression.children()[index];
        const std::size_t candidate =
            append(candidate_expression, type_or(candidate_expression, operand_type));
        const std::size_t equal =
            emit(VectorBinaryExpression{.operation = VectorBinaryOperation::kEqual,
                                        .left_instruction = searched_index,
                                        .right_instruction = candidate},
                 expression.span());
        if (accumulated.has_value()) {
          static_cast<void>(emit(VectorBinaryExpression{.operation = VectorBinaryOperation::kOr,
                                                        .left_instruction = *accumulated,
                                                        .right_instruction = equal},
                                 expression.span()));
        }
        accumulated = instructions_.size() - 1U;
      }
      if (!accumulated.has_value()) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, expression.span(),
                                         common::StatusCode::kInternal,
                                         "Bound IN expression has no candidates")};
      }
      if (expression.operation() == SqlOperator::kNotIn) {
        static_cast<void>(emit(VectorUnaryExpression{.operation = VectorUnaryOperation::kNot,
                                                     .operand_instruction = *accumulated},
                               expression.span()));
      }
      break;
    }
    case SqlExpressionKind::kFunction:
      if (expression.text() == "abs" && expression.children().size() == 1U) {
        static_cast<void>(emit(VectorUnaryExpression{.operation = VectorUnaryOperation::kAbsolute,
                                                     .operand_instruction = append(
                                                         expression.children().front(),
                                                         type_of(expression.children().front()))},
                               expression.span()));
        break;
      }
      if ((expression.text() == "lower" || expression.text() == "upper") &&
          expression.children().size() == 1U) {
        static_cast<void>(emit(
            VectorUnaryExpression{
                .operation = expression.text() == "lower" ? VectorUnaryOperation::kLowerAscii
                                                          : VectorUnaryOperation::kUpperAscii,
                .operand_instruction =
                    append(expression.children().front(), type_of(expression.children().front()))},
            expression.span()));
        break;
      }
      if (expression.text() == "coalesce" && !expression.children().empty()) {
        const schema::LogicalType& result_type = type_of(expression);
        std::optional<std::size_t> accumulated;
        for (const SqlExpression& child : expression.children()) {
          const schema::LogicalType& child_type = type_or(child, result_type);
          std::size_t child_index = append(child, child_type);
          if (child_type != result_type) {
            child_index = emit(VectorCastExpression{.operand_instruction = child_index,
                                                    .target_type = result_type},
                               expression.span());
          }
          if (accumulated.has_value()) {
            child_index = emit(VectorBinaryExpression{.operation = VectorBinaryOperation::kCoalesce,
                                                      .left_instruction = *accumulated,
                                                      .right_instruction = child_index},
                               expression.span());
          }
          accumulated = child_index;
        }
        break;
      }
      if (expression.text() == "time_bucket" && expression.children().size() == 2U) {
        const std::size_t width =
            append(expression.children()[0], type_of(expression.children()[0]));
        const std::size_t point =
            append(expression.children()[1], type_of(expression.children()[1]));
        static_cast<void>(
            emit(VectorBinaryExpression{.operation = VectorBinaryOperation::kTimeBucket,
                                        .left_instruction = width,
                                        .right_instruction = point},
                 expression.span()));
        break;
      }
      {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kUnsupportedSyntax, expression.span(),
                                         common::StatusCode::kInvalidArgument,
                                         "SQL function has no physical kernel")};
      }
    case SqlExpressionKind::kCast: {
      const SqlExpression& child = expression.children().front();
      const schema::LogicalType& child_type = type_or(child, expected);
      const std::size_t operand = append(child, child_type);
      if (child_type == expected)
        return operand;
      static_cast<void>(
          emit(VectorCastExpression{.operand_instruction = operand, .target_type = expected},
               expression.span()));
      break;
    }
    case SqlExpressionKind::kStar:
      throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, expression.span(),
                                       common::StatusCode::kInternal,
                                       "Star cannot be lowered as a scalar expression")};
    }
    return instructions_.size() - 1U;
  }

  const BoundSqlSelect& select_;
  VectorExpressionLimits limits_;
  std::span<const AggregatePhysicalBinding> aggregates_;
  bool source_columns_available_;
  std::vector<VectorExpressionInstruction> instructions_;
};

[[nodiscard]] bool aggregate_function(const std::string_view name) noexcept {
  return name == "count" || name == "sum" || name == "avg" || name == "min" || name == "max" ||
         name == "var_pop" || name == "var_samp";
}

void collect_aggregates(const SqlExpression& expression,
                        std::vector<const SqlExpression*>& aggregates) {
  if (expression.kind() == SqlExpressionKind::kFunction && aggregate_function(expression.text())) {
    aggregates.push_back(std::addressof(expression));
    return;
  }
  for (const SqlExpression& child : expression.children())
    collect_aggregates(child, aggregates);
}

[[nodiscard]] VectorAggregateOperation aggregate_operation(const SqlExpression& expression) {
  if (expression.text() == "count")
    return VectorAggregateOperation::kCount;
  if (expression.text() == "sum")
    return VectorAggregateOperation::kSum;
  if (expression.text() == "avg")
    return VectorAggregateOperation::kAverage;
  if (expression.text() == "min")
    return VectorAggregateOperation::kMinimum;
  if (expression.text() == "max")
    return VectorAggregateOperation::kMaximum;
  if (expression.text() == "var_pop")
    return VectorAggregateOperation::kVariancePopulation;
  if (expression.text() == "var_samp")
    return VectorAggregateOperation::kVarianceSample;
  throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, expression.span(),
                                   common::StatusCode::kInternal,
                                   "Bound aggregate function is not recognized")};
}

[[nodiscard]] const BoundExpressionInfo& bound_expression(const BoundSqlSelect& select,
                                                          const SqlExpression& expression) {
  const BoundExpressionInfo* information = select.find_expression(expression.span());
  if (information == nullptr || !information->type.has_value()) {
    throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, expression.span(),
                                     common::StatusCode::kInternal,
                                     "Bound aggregate expression has no physical type")};
  }
  return *information;
}

[[nodiscard]] VectorAggregateDefinition
aggregate_definition(const BoundSqlSelect& select, const SqlExpression& expression,
                     const std::optional<std::size_t> input_ordinal) {
  if (expression.children().size() != 1U) {
    throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, expression.span(),
                                     common::StatusCode::kInternal,
                                     "Bound aggregate has invalid arity")};
  }
  const SqlExpression& child = expression.children().front();
  if (child.kind() == SqlExpressionKind::kStar) {
    if (expression.text() != "count" || input_ordinal.has_value()) {
      throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, expression.span(),
                                       common::StatusCode::kInternal,
                                       "Bound aggregate has an invalid star input")};
    }
    return {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt};
  }
  if (!input_ordinal.has_value()) {
    throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, expression.span(),
                                     common::StatusCode::kInternal,
                                     "Bound aggregate input has no physical ordinal")};
  }
  const BoundExpressionInfo& input = bound_expression(select, child);
  return {.operation = aggregate_operation(expression),
          .input = VectorAggregateInput{.column_ordinal = *input_ordinal,
                                        .type = input.type.value(),
                                        .nullable = input.nullable}};
}

[[nodiscard]] const SqlExpression* output_expression(const BoundSqlSelect& select,
                                                     const SourceSpan span) noexcept {
  for (const SqlSelectItem& item : select.syntax().items()) {
    const SqlExpression* expression = item.expression();
    if (expression != nullptr && same_span(expression->span(), span))
      return expression;
  }
  return nullptr;
}

} // namespace

SqlResult<PhysicalPipelinePlan> lower_bound_sql_select(const BoundSqlSelect& select,
                                                       const PhysicalSelectLoweringLimits limits) {
  try {
    if (limits.expression_limits.maximum_instructions == 0U ||
        limits.expression_limits.maximum_instructions > kMaximumVectorExpressionInstructions ||
        limits.expression_limits.maximum_retained_configuration_bytes == 0U) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                                        common::StatusCode::kInvalidArgument,
                                        "Physical expression limits are invalid"));
    }
    if (select.syntax().mode() != SqlSelectMode::kSelect) {
      return std::unexpected(
          diagnostic(SqlDiagnosticCode::kUnsupportedSyntax, select.syntax().span(),
                     common::StatusCode::kInvalidArgument,
                     "Only ordinary SELECT has an executable physical pipeline"));
    }
    if (select.sources().size() != 1U || !select.asof_joins().empty()) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kUnsupportedSyntax,
                                        select.syntax().span(),
                                        common::StatusCode::kInvalidArgument,
                                        "Physical lowering requires exactly one SQL source"));
    }
    if (!select.syntax().group_by().empty()) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kUnsupportedSyntax,
                                        select.syntax().span(),
                                        common::StatusCode::kInvalidArgument,
                                        "Physical grouped aggregate lowering is not implemented"));
    }
    if (select.latest_by().has_value()) {
      return std::unexpected(diagnostic(
          SqlDiagnosticCode::kUnsupportedSyntax, select.syntax().span(),
          common::StatusCode::kInvalidArgument, "Physical LATEST lowering is not implemented"));
    }
    if (!select.syntax().order_by().empty()) {
      return std::unexpected(diagnostic(
          SqlDiagnosticCode::kUnsupportedSyntax, select.syntax().span(),
          common::StatusCode::kInvalidArgument, "Physical ORDER BY lowering is not implemented"));
    }

    std::vector<PhysicalColumnShape> input_columns;
    input_columns.reserve(select.sources()[0].schema_ptr()->columns().size());
    for (const schema::ColumnDefinition& column : select.sources()[0].schema_ptr()->columns())
      input_columns.push_back({.type = column.type(), .nullable = column.nullable()});

    std::vector<PhysicalPipelineStage> stages;
    ExpressionLowerer source_lowerer{select, limits.expression_limits};
    if (const SqlExpression* where = select.syntax().where(); where != nullptr) {
      std::vector<ColumnOutputPosition> predicate_positions;
      predicate_positions.reserve(input_columns.size() + 1U);
      for (std::size_t ordinal = 0U; ordinal < input_columns.size(); ++ordinal)
        predicate_positions.emplace_back(SourceColumnOutputPosition{ordinal});
      predicate_positions.push_back(source_lowerer.output(*where));
      stages.emplace_back(ColumnOutputStage{.positions = std::move(predicate_positions),
                                            .output_limits = limits.output_limits});
      stages.emplace_back(BooleanFilterStage{.predicate_column = input_columns.size()});
    }

    const auto lower_outputs = [&select](ExpressionLowerer& lowerer,
                                         const bool source_outputs_available) {
      std::vector<ColumnOutputPosition> outputs;
      outputs.reserve(select.outputs().size());
      for (const BoundOutputColumn& output : select.outputs()) {
        if (output.source_ordinal.has_value() && output.column_ordinal.has_value()) {
          if (!source_outputs_available || *output.source_ordinal != 0U) {
            throw LoweringFailure{
                diagnostic(SqlDiagnosticCode::kExecutionFailure, select.syntax().span(),
                           common::StatusCode::kInvalidArgument,
                           "Bound output references an unavailable physical source")};
          }
          outputs.emplace_back(SourceColumnOutputPosition{*output.column_ordinal});
          continue;
        }
        if (!output.expression_span.has_value()) {
          throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                           select.syntax().span(), common::StatusCode::kInternal,
                                           "Bound output has no physical expression")};
        }
        const SqlExpression* expression = output_expression(select, *output.expression_span);
        if (expression == nullptr) {
          throw LoweringFailure{
              diagnostic(SqlDiagnosticCode::kExecutionFailure, *output.expression_span,
                         common::StatusCode::kInternal,
                         "Bound output expression is absent from the syntax tree")};
        }
        outputs.push_back(lowerer.output(*expression));
      }
      return outputs;
    };

    std::vector<ColumnOutputPosition> outputs;
    if (select.aggregate_query()) {
      std::vector<const SqlExpression*> aggregates;
      for (const SqlSelectItem& item : select.syntax().items()) {
        if (const SqlExpression* expression = item.expression(); expression != nullptr)
          collect_aggregates(*expression, aggregates);
      }
      if (aggregates.empty()) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                         select.syntax().span(), common::StatusCode::kInternal,
                                         "Bound aggregate query has no aggregate expressions")};
      }

      bool materialize_inputs = false;
      for (const SqlExpression* aggregate : aggregates) {
        if (aggregate->children().size() != 1U) {
          throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, aggregate->span(),
                                           common::StatusCode::kInternal,
                                           "Bound aggregate has invalid arity")};
        }
        const SqlExpression& child = aggregate->children().front();
        materialize_inputs = materialize_inputs || (child.kind() != SqlExpressionKind::kStar &&
                                                    child.kind() != SqlExpressionKind::kColumn);
      }

      std::vector<ColumnOutputPosition> aggregate_inputs;
      std::vector<VectorAggregateDefinition> definitions;
      std::vector<AggregatePhysicalBinding> bindings;
      definitions.reserve(aggregates.size());
      bindings.reserve(aggregates.size());
      if (materialize_inputs)
        aggregate_inputs.reserve(aggregates.size());

      for (const SqlExpression* aggregate : aggregates) {
        const SqlExpression& child = aggregate->children().front();
        std::optional<std::size_t> input_ordinal;
        if (child.kind() != SqlExpressionKind::kStar) {
          if (materialize_inputs) {
            input_ordinal = aggregate_inputs.size();
            aggregate_inputs.push_back(source_lowerer.output(child));
          } else {
            const BoundColumnReference* reference = select.find_column_reference(child.span());
            if (reference == nullptr || reference->source_ordinal != 0U) {
              throw LoweringFailure{diagnostic(
                  SqlDiagnosticCode::kExecutionFailure, child.span(), common::StatusCode::kInternal,
                  "Bound direct aggregate input has no physical source column")};
            }
            input_ordinal = reference->column_ordinal;
          }
        }

        VectorAggregateDefinition definition =
            aggregate_definition(select, *aggregate, input_ordinal);
        common::Result<VectorAggregateOutputShape> shape =
            vector_aggregate_output_shape(definition);
        if (!shape.has_value()) {
          const SqlDiagnosticCode code =
              shape.error().code() == common::StatusCode::kResourceExhausted
                  ? SqlDiagnosticCode::kResourceLimit
                  : SqlDiagnosticCode::kUnsupportedSyntax;
          throw LoweringFailure{SqlDiagnostic{code, aggregate->span(), shape.error()}};
        }
        const BoundExpressionInfo& result = bound_expression(select, *aggregate);
        if (result.type.value() != shape->type || result.nullable != shape->nullable) {
          throw LoweringFailure{
              diagnostic(SqlDiagnosticCode::kExecutionFailure, aggregate->span(),
                         common::StatusCode::kInternal,
                         "Bound aggregate result shape disagrees with the physical kernel")};
        }
        definitions.push_back(definition);
        bindings.push_back({.expression_span = aggregate->span(),
                            .column_ordinal = bindings.size(),
                            .type = shape->type,
                            .nullable = shape->nullable});
      }

      if (materialize_inputs) {
        stages.emplace_back(ColumnOutputStage{.positions = std::move(aggregate_inputs),
                                              .output_limits = limits.output_limits});
      }
      stages.emplace_back(UngroupedAggregateStage{.definitions = std::move(definitions),
                                                  .limits = limits.aggregate_limits});
      ExpressionLowerer result_lowerer{select, limits.expression_limits, bindings, false};
      outputs = lower_outputs(result_lowerer, false);
    } else {
      outputs = lower_outputs(source_lowerer, true);
    }
    stages.emplace_back(
        ColumnOutputStage{.positions = std::move(outputs), .output_limits = limits.output_limits});
    const std::optional<std::uint64_t>& maximum_rows = select.syntax().limit();
    if (maximum_rows.has_value())
      stages.emplace_back(LimitStage{.maximum_rows = maximum_rows.value()});

    common::Result<PhysicalPipelinePlan> plan = PhysicalPipelinePlan::create(
        std::move(input_columns), std::move(stages), limits.plan_limits);
    if (!plan.has_value())
      return std::unexpected(from_status(select.syntax().span(), plan.error()));
    return std::move(*plan);
  } catch (LoweringFailure& failure) {
    return std::unexpected(failure.take());
  } catch (const std::bad_alloc&) {
    return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                                      common::StatusCode::kResourceExhausted,
                                      "Physical SELECT lowering allocation failed"));
  } catch (const std::length_error&) {
    return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                                      common::StatusCode::kResourceExhausted,
                                      "Physical SELECT lowering exceeds container limits"));
  }
}

} // namespace chronos::query
