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

class ExpressionLowerer {
public:
  ExpressionLowerer(const BoundSqlSelect& select, const VectorExpressionLimits limits) noexcept
      : select_(select), limits_(limits) {}

  [[nodiscard]] ColumnOutputPosition output(const SqlExpression& expression) {
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

  [[nodiscard]] std::size_t append(const SqlExpression& expression,
                                   const schema::LogicalType& expected) {
    if (instructions_.size() >= limits_.maximum_instructions) {
      throw LoweringFailure{diagnostic(SqlDiagnosticCode::kResourceLimit, expression.span(),
                                       common::StatusCode::kResourceExhausted,
                                       "Physical expression instruction limit exceeded")};
    }
    switch (expression.kind()) {
    case SqlExpressionKind::kColumn: {
      const BoundColumnReference& reference = column(expression);
      instructions_.emplace_back(
          VectorInputExpression{.input_column_ordinal = reference.column_ordinal,
                                .type = reference.type,
                                .nullable = reference.nullable});
      break;
    }
    case SqlExpressionKind::kLiteral:
      instructions_.emplace_back(VectorConstantExpression{literal(expression, expected)});
      break;
    case SqlExpressionKind::kUnary: {
      const std::size_t operand =
          append(expression.children().front(), type_of(expression.children().front()));
      instructions_.emplace_back(
          VectorUnaryExpression{.operation = unary(expression.operation(), expression.span()),
                                .operand_instruction = operand});
      break;
    }
    case SqlExpressionKind::kIsNull: {
      const SqlExpression& child = expression.children().front();
      const BoundExpressionInfo* information = select_.find_expression(child.span());
      const schema::LogicalType& child_type =
          information != nullptr && information->type.has_value() ? *information->type : expected;
      const std::size_t operand = append(child, child_type);
      instructions_.emplace_back(
          VectorUnaryExpression{.operation = unary(expression.operation(), expression.span()),
                                .operand_instruction = operand});
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
      instructions_.emplace_back(
          VectorBinaryExpression{.operation = binary(expression.operation(), expression.span()),
                                 .left_instruction = lhs,
                                 .right_instruction = rhs});
      break;
    }
    case SqlExpressionKind::kBetween: {
      const SqlExpression& value = expression.children()[0];
      const SqlExpression& lower_expression = expression.children()[1];
      const schema::LogicalType& operand_type = type_or(value, first_child_type(expression));
      const std::size_t value_index = append(value, operand_type);
      const std::size_t lower = append(lower_expression, type_or(lower_expression, operand_type));
      instructions_.emplace_back(
          VectorBinaryExpression{.operation = VectorBinaryOperation::kGreaterEqual,
                                 .left_instruction = value_index,
                                 .right_instruction = lower});
      const std::size_t low = instructions_.size() - 1U;
      const SqlExpression& upper_expression = expression.children()[2];
      const std::size_t upper = append(upper_expression, type_or(upper_expression, operand_type));
      instructions_.emplace_back(
          VectorBinaryExpression{.operation = VectorBinaryOperation::kLessEqual,
                                 .left_instruction = value_index,
                                 .right_instruction = upper});
      const std::size_t high = instructions_.size() - 1U;
      instructions_.emplace_back(VectorBinaryExpression{.operation = VectorBinaryOperation::kAnd,
                                                        .left_instruction = low,
                                                        .right_instruction = high});
      if (expression.operation() == SqlOperator::kNotBetween) {
        instructions_.emplace_back(
            VectorUnaryExpression{.operation = VectorUnaryOperation::kNot,
                                  .operand_instruction = instructions_.size() - 1U});
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
        instructions_.emplace_back(
            VectorBinaryExpression{.operation = VectorBinaryOperation::kEqual,
                                   .left_instruction = searched_index,
                                   .right_instruction = candidate});
        const std::size_t equal = instructions_.size() - 1U;
        if (accumulated.has_value()) {
          instructions_.emplace_back(VectorBinaryExpression{.operation = VectorBinaryOperation::kOr,
                                                            .left_instruction = *accumulated,
                                                            .right_instruction = equal});
        }
        accumulated = instructions_.size() - 1U;
      }
      if (!accumulated.has_value()) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, expression.span(),
                                         common::StatusCode::kInternal,
                                         "Bound IN expression has no candidates")};
      }
      if (expression.operation() == SqlOperator::kNotIn) {
        instructions_.emplace_back(VectorUnaryExpression{.operation = VectorUnaryOperation::kNot,
                                                         .operand_instruction = *accumulated});
      }
      break;
    }
    case SqlExpressionKind::kFunction:
      if (expression.text() != "abs" || expression.children().size() != 1U) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kUnsupportedSyntax, expression.span(),
                                         common::StatusCode::kInvalidArgument,
                                         "SQL function has no physical kernel")};
      }
      instructions_.emplace_back(VectorUnaryExpression{
          .operation = VectorUnaryOperation::kAbsolute,
          .operand_instruction =
              append(expression.children().front(), type_of(expression.children().front()))});
      break;
    case SqlExpressionKind::kCast: {
      const SqlExpression& child = expression.children().front();
      if (type_of(child) != expected) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kUnsupportedSyntax, expression.span(),
                                         common::StatusCode::kInvalidArgument,
                                         "CAST has no physical kernel")};
      }
      return append(child, expected);
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
  std::vector<VectorExpressionInstruction> instructions_;
};

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
    if (select.aggregate_query() || !select.syntax().group_by().empty()) {
      return std::unexpected(diagnostic(
          SqlDiagnosticCode::kUnsupportedSyntax, select.syntax().span(),
          common::StatusCode::kInvalidArgument, "Physical aggregate lowering is not implemented"));
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
    ExpressionLowerer lowerer{select, limits.expression_limits};
    if (const SqlExpression* where = select.syntax().where(); where != nullptr) {
      std::vector<ColumnOutputPosition> predicate_positions;
      predicate_positions.reserve(input_columns.size() + 1U);
      for (std::size_t ordinal = 0U; ordinal < input_columns.size(); ++ordinal)
        predicate_positions.emplace_back(SourceColumnOutputPosition{ordinal});
      predicate_positions.push_back(lowerer.output(*where));
      stages.emplace_back(ColumnOutputStage{.positions = std::move(predicate_positions),
                                            .output_limits = limits.output_limits});
      stages.emplace_back(BooleanFilterStage{.predicate_column = input_columns.size()});
    }

    std::vector<ColumnOutputPosition> outputs;
    outputs.reserve(select.outputs().size());
    for (const BoundOutputColumn& output : select.outputs()) {
      if (output.source_ordinal.has_value() && output.column_ordinal.has_value()) {
        if (*output.source_ordinal != 0U)
          throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                           select.syntax().span(),
                                           common::StatusCode::kInvalidArgument,
                                           "Bound output references an unavailable source")};
        outputs.emplace_back(SourceColumnOutputPosition{*output.column_ordinal});
        continue;
      }
      if (!output.expression_span.has_value())
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                         select.syntax().span(), common::StatusCode::kInternal,
                                         "Bound output has no physical expression")};
      const SqlExpression* expression = output_expression(select, *output.expression_span);
      if (expression == nullptr)
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                         *output.expression_span, common::StatusCode::kInternal,
                                         "Bound output expression is absent from the syntax tree")};
      outputs.push_back(lowerer.output(*expression));
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
