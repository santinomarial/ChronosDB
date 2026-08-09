#include "chronos/query/physical_lowering.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"
#include "chronos/query/literal.hpp"
#include "chronos/query/row_version.hpp"
#include "chronos/query/value.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
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

struct GroupPhysicalBinding {
  const SqlExpression* expression;
  std::size_t column_ordinal;
  schema::LogicalType type;
  bool nullable;
};

[[nodiscard]] bool same_identifier(const SqlIdentifier& left, const SqlIdentifier& right) noexcept {
  return left.text() == right.text() && left.quoted() == right.quoted();
}

[[nodiscard]] bool same_bound_expression(const BoundSqlSelect& select, const SqlExpression& left,
                                         const SqlExpression& right) noexcept {
  if (left.kind() == SqlExpressionKind::kColumn && right.kind() == SqlExpressionKind::kColumn) {
    const BoundColumnReference* left_reference = select.find_column_reference(left.span());
    const BoundColumnReference* right_reference = select.find_column_reference(right.span());
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
    if (!same_bound_expression(select, left.children()[index], right.children()[index]))
      return false;
  }
  return true;
}

class ExpressionLowerer {
public:
  ExpressionLowerer(const BoundSqlSelect& select, const VectorExpressionLimits limits,
                    const std::span<const AggregatePhysicalBinding> aggregates = {},
                    const std::span<const GroupPhysicalBinding> groups = {},
                    const bool source_columns_available = true,
                    const std::span<const std::size_t> source_offsets = {},
                    const std::span<const PhysicalColumnShape> input_shapes = {}) noexcept
      : select_(select), limits_(limits), aggregates_(aggregates), groups_(groups),
        source_columns_available_(source_columns_available), source_offsets_(source_offsets),
        input_shapes_(input_shapes) {}

  [[nodiscard]] ColumnOutputPosition output(const SqlExpression& expression) {
    if (const AggregatePhysicalBinding* aggregate = find_aggregate(expression.span());
        aggregate != nullptr) {
      return SourceColumnOutputPosition{aggregate->column_ordinal};
    }
    if (const GroupPhysicalBinding* group = find_group(expression); group != nullptr)
      return SourceColumnOutputPosition{group->column_ordinal};
    if (expression.kind() == SqlExpressionKind::kColumn) {
      const BoundColumnReference& reference = column(expression);
      return SourceColumnOutputPosition{physical_ordinal(reference, expression.span())};
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

  [[nodiscard]] ColumnOutputPosition output_as(const SqlExpression& expression,
                                               const schema::LogicalType target) {
    const schema::LogicalType source = type_of(expression);
    if (source == target)
      return output(expression);
    instructions_.clear();
    instructions_.reserve(limits_.maximum_instructions);
    const std::size_t operand = append(expression, source);
    static_cast<void>(
        emit(VectorCastExpression{.operand_instruction = operand, .target_type = target},
             expression.span()));
    common::Result<VectorExpression> program =
        VectorExpression::create(std::move(instructions_), limits_);
    if (!program.has_value())
      throw LoweringFailure{from_status(expression.span(), program.error())};
    return ComputedColumnOutputPosition{std::move(*program)};
  }

  [[nodiscard]] std::size_t source_column_ordinal(const std::size_t source_ordinal,
                                                  const std::size_t column_ordinal,
                                                  const SourceSpan span) const {
    if (!source_columns_available_ || source_ordinal >= select_.sources().size()) {
      throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, span,
                                       common::StatusCode::kInvalidArgument,
                                       "Physical expression references an unavailable source")};
    }
    if (source_offsets_.empty()) {
      if (source_ordinal != 0U)
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, span,
                                         common::StatusCode::kInvalidArgument,
                                         "Physical expression references an unavailable source")};
      return column_ordinal;
    }
    if (source_ordinal >= source_offsets_.size()) {
      throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, span,
                                       common::StatusCode::kInvalidArgument,
                                       "Physical source has no joined-column offset")};
    }
    const std::optional<std::size_t> ordinal =
        common::checked_add(source_offsets_[source_ordinal], column_ordinal);
    if (!ordinal.has_value() || (!input_shapes_.empty() && *ordinal >= input_shapes_.size())) {
      throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, span,
                                       common::StatusCode::kInternal,
                                       "Physical source column ordinal is outside its input")};
    }
    return *ordinal;
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

  [[nodiscard]] const GroupPhysicalBinding*
  find_group(const SqlExpression& expression) const noexcept {
    for (const GroupPhysicalBinding& group : groups_) {
      if (group.expression != nullptr &&
          same_bound_expression(select_, expression, *group.expression)) {
        return std::addressof(group);
      }
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
          "Bound aggregate output retained an ungrouped source column")};
    }
    const BoundColumnReference* reference = select_.find_column_reference(expression.span());
    if (reference == nullptr) {
      throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, expression.span(),
                                       common::StatusCode::kInvalidArgument,
                                       "Physical expression references an unavailable source")};
    }
    return *reference;
  }

  [[nodiscard]] std::size_t physical_ordinal(const BoundColumnReference& reference,
                                             const SourceSpan span) const {
    return source_column_ordinal(reference.source_ordinal, reference.column_ordinal, span);
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
    if (const GroupPhysicalBinding* group = find_group(expression); group != nullptr) {
      return emit(VectorInputExpression{.input_column_ordinal = group->column_ordinal,
                                        .type = group->type,
                                        .nullable = group->nullable},
                  expression.span());
    }
    switch (expression.kind()) {
    case SqlExpressionKind::kColumn: {
      const BoundColumnReference& reference = column(expression);
      const std::size_t ordinal = physical_ordinal(reference, expression.span());
      const PhysicalColumnShape shape =
          input_shapes_.empty()
              ? PhysicalColumnShape{.type = reference.type, .nullable = reference.nullable}
              : input_shapes_[ordinal];
      static_cast<void>(emit(VectorInputExpression{.input_column_ordinal = ordinal,
                                                   .type = shape.type,
                                                   .nullable = shape.nullable},
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
  std::span<const GroupPhysicalBinding> groups_;
  bool source_columns_available_;
  std::span<const std::size_t> source_offsets_;
  std::span<const PhysicalColumnShape> input_shapes_;
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

[[nodiscard]] PhysicalSortDirection order_direction(const SqlOrderDirection direction) noexcept {
  return direction == SqlOrderDirection::kAscending ? PhysicalSortDirection::kAscending
                                                    : PhysicalSortDirection::kDescending;
}

[[nodiscard]] ScalarNullPlacement order_null_placement(const SqlOrderItem& item) noexcept {
  if (item.null_order == SqlNullOrder::kFirst)
    return ScalarNullPlacement::kFirst;
  if (item.null_order == SqlNullOrder::kLast)
    return ScalarNullPlacement::kLast;
  return item.direction == SqlOrderDirection::kAscending ? ScalarNullPlacement::kLast
                                                         : ScalarNullPlacement::kFirst;
}

[[nodiscard]] std::uint64_t expression_source_mask(const BoundSqlSelect& select,
                                                   const SqlExpression& expression) noexcept {
  if (expression.kind() == SqlExpressionKind::kColumn) {
    const BoundColumnReference* reference = select.find_column_reference(expression.span());
    return reference == nullptr || reference->source_ordinal >= 64U
               ? 0U
               : std::uint64_t{1U} << reference->source_ordinal;
  }
  std::uint64_t mask = 0U;
  for (const SqlExpression& child : expression.children())
    mask |= expression_source_mask(select, child);
  return mask;
}

struct AsofSyntaxShape {
  std::vector<std::pair<const SqlExpression*, const SqlExpression*>> equality;
  const SqlExpression* left_time{};
  const SqlExpression* right_time{};
};

void collect_asof_syntax(const BoundSqlSelect& select, const SqlExpression& expression,
                         const std::size_t right_source, AsofSyntaxShape& result) {
  if (expression.kind() == SqlExpressionKind::kBinary &&
      expression.operation() == SqlOperator::kAnd) {
    collect_asof_syntax(select, expression.children()[0], right_source, result);
    collect_asof_syntax(select, expression.children()[1], right_source, result);
    return;
  }
  if (expression.kind() != SqlExpressionKind::kBinary || expression.children().size() != 2U) {
    throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, expression.span(),
                                     common::StatusCode::kInternal,
                                     "Bound ASOF condition has an invalid physical shape")};
  }
  const SqlExpression& left = expression.children()[0];
  const SqlExpression& right = expression.children()[1];
  const std::uint64_t right_bit = std::uint64_t{1U} << right_source;
  const std::uint64_t prior_mask = right_bit - 1U;
  const std::uint64_t left_mask = expression_source_mask(select, left);
  const std::uint64_t right_mask = expression_source_mask(select, right);
  const bool left_is_right = left_mask == right_bit;
  const bool right_is_right = right_mask == right_bit;
  const bool left_is_prior = left_mask != 0U && (left_mask & ~prior_mask) == 0U;
  const bool right_is_prior = right_mask != 0U && (right_mask & ~prior_mask) == 0U;
  if (expression.operation() == SqlOperator::kEqual && left_is_prior && right_is_right) {
    result.equality.emplace_back(std::addressof(left), std::addressof(right));
    return;
  }
  if (expression.operation() == SqlOperator::kEqual && left_is_right && right_is_prior) {
    result.equality.emplace_back(std::addressof(right), std::addressof(left));
    return;
  }
  if (expression.operation() == SqlOperator::kLessEqual && left_is_right && right_is_prior) {
    result.left_time = std::addressof(right);
    result.right_time = std::addressof(left);
    return;
  }
  if (expression.operation() == SqlOperator::kGreaterEqual && left_is_prior && right_is_right) {
    result.left_time = std::addressof(left);
    result.right_time = std::addressof(right);
    return;
  }
  throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, expression.span(),
                                   common::StatusCode::kInternal,
                                   "Bound ASOF condition and physical shape disagree")};
}

[[nodiscard]] bool signed_integer(const schema::LogicalTypeKind kind) noexcept {
  return kind >= schema::LogicalTypeKind::kInt8 && kind <= schema::LogicalTypeKind::kInt64;
}

[[nodiscard]] bool unsigned_integer(const schema::LogicalTypeKind kind) noexcept {
  return kind >= schema::LogicalTypeKind::kUInt8 && kind <= schema::LogicalTypeKind::kUInt64;
}

[[nodiscard]] int integer_rank(const schema::LogicalTypeKind kind) noexcept {
  switch (kind) {
  case schema::LogicalTypeKind::kInt8:
  case schema::LogicalTypeKind::kUInt8:
    return 1;
  case schema::LogicalTypeKind::kInt16:
  case schema::LogicalTypeKind::kUInt16:
    return 2;
  case schema::LogicalTypeKind::kInt32:
  case schema::LogicalTypeKind::kUInt32:
    return 3;
  case schema::LogicalTypeKind::kInt64:
  case schema::LogicalTypeKind::kUInt64:
    return 4;
  default:
    return 0;
  }
}

[[nodiscard]] schema::LogicalType equality_common_type(const BoundSqlSelect& select,
                                                       const SqlExpression& left,
                                                       const SqlExpression& right) {
  const BoundExpressionInfo* left_info = select.find_expression(left.span());
  const BoundExpressionInfo* right_info = select.find_expression(right.span());
  if (left_info == nullptr || right_info == nullptr || !left_info->type.has_value() ||
      !right_info->type.has_value()) {
    throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, left.span(),
                                     common::StatusCode::kInternal,
                                     "Bound ASOF equality operand has no type")};
  }
  const schema::LogicalType left_type = *left_info->type;
  const schema::LogicalType right_type = *right_info->type;
  if (left_type == right_type)
    return left_type;
  if ((signed_integer(left_type.kind()) && signed_integer(right_type.kind())) ||
      (unsigned_integer(left_type.kind()) && unsigned_integer(right_type.kind()))) {
    return integer_rank(left_type.kind()) >= integer_rank(right_type.kind()) ? left_type
                                                                             : right_type;
  }
  if ((left_type.kind() == schema::LogicalTypeKind::kFloat32 ||
       left_type.kind() == schema::LogicalTypeKind::kFloat64) &&
      (right_type.kind() == schema::LogicalTypeKind::kFloat32 ||
       right_type.kind() == schema::LogicalTypeKind::kFloat64)) {
    return schema::LogicalType::create(schema::LogicalTypeKind::kFloat64).value();
  }
  throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure, left.span(),
                                   common::StatusCode::kInternal,
                                   "Bound ASOF equality has no physical common type")};
}

[[nodiscard]] std::vector<PhysicalColumnShape>
source_shape_with_version(const BoundSqlSource& source, const SourceSpan span) {
  std::vector<PhysicalColumnShape> result;
  const std::span<const schema::ColumnDefinition> columns = source.schema_ptr()->columns();
  common::Result<std::size_t> width =
      scan_output_column_count(columns.size(), RowVersionScanMode::kAppend);
  if (!width.has_value())
    throw LoweringFailure{from_status(span, width.error())};
  result.reserve(*width);
  for (const schema::ColumnDefinition& column : columns)
    result.push_back({.type = column.type(), .nullable = column.nullable()});
  for (const VectorRowVersionColumnKind kind : {
           VectorRowVersionColumnKind::kWalId,
           VectorRowVersionColumnKind::kRecordSequence,
           VectorRowVersionColumnKind::kRowOrdinal,
           VectorRowVersionColumnKind::kOperation,
       }) {
    common::Result<schema::LogicalType> suffix_type = vector_row_version_column_type(kind);
    if (!suffix_type.has_value())
      throw LoweringFailure{from_status(span, suffix_type.error())};
    result.push_back({.type = *suffix_type, .nullable = false});
  }
  return result;
}

[[nodiscard]] PhysicalColumnShape
output_position_shape(const ColumnOutputPosition& position,
                      const std::span<const PhysicalColumnShape> input) {
  if (const auto* source = std::get_if<SourceColumnOutputPosition>(&position); source != nullptr)
    return input[source->input_column_ordinal];
  if (const auto* constant = std::get_if<ConstantColumnOutputPosition>(&position);
      constant != nullptr) {
    return {.type = constant->value.type().value(), // NOLINT(bugprone-unchecked-optional-access)
            .nullable = constant->value.is_null() || constant->force_nullable};
  }
  const auto& computed = std::get<ComputedColumnOutputPosition>(position);
  return {.type = computed.expression.result_shape().type,
          .nullable = computed.expression.result_shape().nullable};
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
    if (select.syntax().mode() != SqlSelectMode::kSelect &&
        select.syntax().mode() != SqlSelectMode::kSubscribe) {
      return std::unexpected(
          diagnostic(SqlDiagnosticCode::kUnsupportedSyntax, select.syntax().span(),
                     common::StatusCode::kInvalidArgument,
                     "Only SELECT and SUBSCRIBE SELECT have executable physical pipelines"));
    }
    if (select.sources().size() != 1U || !select.asof_joins().empty()) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kUnsupportedSyntax,
                                        select.syntax().span(),
                                        common::StatusCode::kInvalidArgument,
                                        "Physical lowering requires exactly one SQL source"));
    }
    const bool ordered = !select.syntax().order_by().empty();
    const bool latest = select.latest_by().has_value();
    const schema::TableSchema& source_schema = *select.sources()[0].schema_ptr();
    if (ordered && !select.aggregate_query() && source_schema.deduplication_key().empty()) {
      return std::unexpected(diagnostic(
          SqlDiagnosticCode::kUnsupportedSyntax,
          select.syntax().order_by().front().expression.span(),
          common::StatusCode::kInvalidArgument,
          "Base-row physical ORDER BY requires a schema DEDUP KEY because generated logical "
          "identity is not exposed by vector sources"));
    }
    std::vector<PhysicalColumnShape> input_columns;
    const std::size_t source_column_count = source_schema.columns().size();
    std::size_t input_column_count = source_column_count;
    const bool needs_row_version = latest || (ordered && !select.aggregate_query());
    if (needs_row_version) {
      common::Result<std::size_t> ordered_input_count =
          scan_output_column_count(source_column_count, RowVersionScanMode::kAppend);
      if (!ordered_input_count.has_value())
        throw LoweringFailure{from_status(select.syntax().span(), ordered_input_count.error())};
      input_column_count = *ordered_input_count;
    }
    input_columns.reserve(input_column_count);
    for (const schema::ColumnDefinition& column : source_schema.columns())
      input_columns.push_back({.type = column.type(), .nullable = column.nullable()});
    if (needs_row_version) {
      for (const VectorRowVersionColumnKind kind : {
               VectorRowVersionColumnKind::kWalId,
               VectorRowVersionColumnKind::kRecordSequence,
               VectorRowVersionColumnKind::kRowOrdinal,
               VectorRowVersionColumnKind::kOperation,
           }) {
        common::Result<schema::LogicalType> suffix_type = vector_row_version_column_type(kind);
        if (!suffix_type.has_value())
          throw LoweringFailure{from_status(select.syntax().span(), suffix_type.error())};
        input_columns.push_back({.type = *suffix_type, .nullable = false});
      }
    }

    std::vector<PhysicalPipelineStage> stages;
    ExpressionLowerer source_lowerer{select, limits.expression_limits};
    if (latest) {
      const BoundLatestBy& bound_latest = *select.latest_by();
      const SqlLatestBy* syntax_latest = select.syntax().latest_by().has_value()
                                             ? std::addressof(*select.syntax().latest_by())
                                             : nullptr;
      if (syntax_latest == nullptr ||
          !same_span(syntax_latest->timestamp.span(), bound_latest.timestamp_expression_span)) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                         select.syntax().span(), common::StatusCode::kInternal,
                                         "Bound and parsed LATEST BY definitions disagree")};
      }
      std::vector<ColumnOutputPosition> latest_inputs;
      latest_inputs.reserve(input_columns.size() + 1U);
      for (std::size_t ordinal = 0U; ordinal < input_columns.size(); ++ordinal)
        latest_inputs.emplace_back(SourceColumnOutputPosition{ordinal});
      const std::size_t timestamp_ordinal = latest_inputs.size();
      latest_inputs.push_back(source_lowerer.output(syntax_latest->timestamp));
      stages.emplace_back(ColumnOutputStage{.positions = std::move(latest_inputs),
                                            .output_limits = limits.output_limits});

      std::vector<std::size_t> physical_keys;
      physical_keys.reserve(source_schema.physical_ordering_key().size());
      for (const schema::ColumnId column : source_schema.physical_ordering_key()) {
        const std::optional<std::size_t> ordinal = source_schema.column_ordinal(column);
        if (!ordinal.has_value()) {
          throw LoweringFailure{
              diagnostic(SqlDiagnosticCode::kExecutionFailure, syntax_latest->timestamp.span(),
                         common::StatusCode::kInternal,
                         "LATEST BY physical ordering key has no source schema ordinal")};
        }
        physical_keys.push_back(*ordinal);
      }
      stages.emplace_back(LatestByStage{
          .definition =
              VectorLatestByDefinition{
                  .key_column_ordinals = bound_latest.key_column_ordinals,
                  .timestamp_column_ordinal = timestamp_ordinal,
                  .physical_ordering_key_ordinals = std::move(physical_keys),
                  .row_version_first_column_ordinal = source_column_count,
              },
          .limits = limits.latest_by_limits,
      });
      std::vector<std::size_t> source_columns;
      source_columns.reserve(input_columns.size());
      for (std::size_t ordinal = 0U; ordinal < input_columns.size(); ++ordinal)
        source_columns.push_back(ordinal);
      stages.emplace_back(ColumnSubsetStage{.column_ordinals = std::move(source_columns)});
    }
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

    const auto lower_output = [&select](ExpressionLowerer& lowerer, const BoundOutputColumn& output,
                                        const bool source_outputs_available) {
      if (output.source_ordinal.has_value() && output.column_ordinal.has_value()) {
        if (source_outputs_available) {
          return ColumnOutputPosition{SourceColumnOutputPosition{lowerer.source_column_ordinal(
              *output.source_ordinal, *output.column_ordinal, select.syntax().span())}};
        }
      }
      if (!output.expression_span.has_value()) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                         select.syntax().span(), common::StatusCode::kInternal,
                                         "Bound output has no physical expression")};
      }
      const SqlExpression* expression = output_expression(select, *output.expression_span);
      if (expression == nullptr) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                         *output.expression_span, common::StatusCode::kInternal,
                                         "Bound output expression is absent from the syntax tree")};
      }
      return lowerer.output(*expression);
    };
    const auto lower_outputs = [&select, &lower_output](ExpressionLowerer& lowerer,
                                                        const bool source_outputs_available) {
      std::vector<ColumnOutputPosition> outputs;
      outputs.reserve(select.outputs().size());
      for (const BoundOutputColumn& output : select.outputs())
        outputs.push_back(lower_output(lowerer, output, source_outputs_available));
      return outputs;
    };

    std::vector<VectorSortKey> sort_keys;
    const auto append_order_outputs = [&select, &sort_keys,
                                       &lower_output](ExpressionLowerer& lowerer,
                                                      std::vector<ColumnOutputPosition>& positions,
                                                      const bool source_outputs_available) {
      for (const SqlOrderItem& item : select.syntax().order_by()) {
        const std::size_t key_ordinal = positions.size();
        const BoundExpressionInfo* information = select.find_expression(item.expression.span());
        if (information != nullptr && information->output_ordinal.has_value()) {
          if (*information->output_ordinal >= select.outputs().size()) {
            throw LoweringFailure{
                diagnostic(SqlDiagnosticCode::kExecutionFailure, item.expression.span(),
                           common::StatusCode::kInternal,
                           "Bound ORDER BY alias ordinal is outside the SELECT output")};
          }
          positions.push_back(lower_output(lowerer, select.outputs()[*information->output_ordinal],
                                           source_outputs_available));
        } else {
          positions.push_back(lowerer.output(item.expression));
        }
        sort_keys.push_back({.column_ordinal = key_ordinal,
                             .direction = order_direction(item.direction),
                             .null_placement = order_null_placement(item)});
      }
    };

    std::vector<ColumnOutputPosition> outputs;
    std::size_t aggregate_group_key_count = 0U;
    if (select.aggregate_query()) {
      std::vector<const SqlExpression*> aggregates;
      for (const SqlSelectItem& item : select.syntax().items()) {
        if (const SqlExpression* expression = item.expression(); expression != nullptr)
          collect_aggregates(*expression, aggregates);
      }
      for (const SqlOrderItem& item : select.syntax().order_by())
        collect_aggregates(item.expression, aggregates);
      const bool grouped = !select.syntax().group_by().empty();
      if (aggregates.empty() && !grouped) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                         select.syntax().span(), common::StatusCode::kInternal,
                                         "Bound aggregate query has no aggregate expressions")};
      }

      const auto validate_aggregate = [&select](const SqlExpression& aggregate,
                                                const std::optional<std::size_t> input_ordinal,
                                                const std::size_t output_ordinal) {
        VectorAggregateDefinition definition =
            aggregate_definition(select, aggregate, input_ordinal);
        common::Result<VectorAggregateOutputShape> shape =
            vector_aggregate_output_shape(definition);
        if (!shape.has_value()) {
          const SqlDiagnosticCode code =
              shape.error().code() == common::StatusCode::kResourceExhausted
                  ? SqlDiagnosticCode::kResourceLimit
                  : SqlDiagnosticCode::kUnsupportedSyntax;
          throw LoweringFailure{SqlDiagnostic{code, aggregate.span(), shape.error()}};
        }
        const BoundExpressionInfo& result = bound_expression(select, aggregate);
        if (result.type.value() != shape->type || result.nullable != shape->nullable) {
          throw LoweringFailure{diagnostic(
              SqlDiagnosticCode::kExecutionFailure, aggregate.span(), common::StatusCode::kInternal,
              "Bound aggregate result shape disagrees with the physical kernel")};
        }
        return std::pair{definition, AggregatePhysicalBinding{.expression_span = aggregate.span(),
                                                              .column_ordinal = output_ordinal,
                                                              .type = shape->type,
                                                              .nullable = shape->nullable}};
      };

      std::vector<VectorAggregateDefinition> definitions;
      std::vector<AggregatePhysicalBinding> bindings;
      definitions.reserve(aggregates.size());
      bindings.reserve(aggregates.size());
      std::vector<GroupPhysicalBinding> group_bindings;

      if (grouped) {
        aggregate_group_key_count = select.syntax().group_by().size();
        std::vector<ColumnOutputPosition> prepared_inputs;
        prepared_inputs.reserve(select.syntax().group_by().size() + aggregates.size());
        std::vector<VectorGroupKeyDefinition> keys;
        keys.reserve(select.syntax().group_by().size());
        group_bindings.reserve(select.syntax().group_by().size());
        for (const SqlExpression& group : select.syntax().group_by()) {
          const BoundExpressionInfo& information = bound_expression(select, group);
          const std::size_t ordinal = prepared_inputs.size();
          prepared_inputs.push_back(source_lowerer.output(group));
          keys.push_back({.column_ordinal = ordinal,
                          .type = information.type.value(),
                          .nullable = information.nullable});
          group_bindings.push_back({.expression = std::addressof(group),
                                    .column_ordinal = keys.size() - 1U,
                                    .type = information.type.value(),
                                    .nullable = information.nullable});
        }
        for (const SqlExpression* aggregate : aggregates) {
          if (aggregate->children().size() != 1U) {
            throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                             aggregate->span(), common::StatusCode::kInternal,
                                             "Bound aggregate has invalid arity")};
          }
          const SqlExpression& child = aggregate->children().front();
          std::optional<std::size_t> input_ordinal;
          if (child.kind() != SqlExpressionKind::kStar) {
            input_ordinal = prepared_inputs.size();
            prepared_inputs.push_back(source_lowerer.output(child));
          }
          auto [definition, binding] =
              validate_aggregate(*aggregate, input_ordinal, keys.size() + definitions.size());
          definitions.push_back(definition);
          bindings.push_back(binding);
        }
        stages.emplace_back(ColumnOutputStage{.positions = std::move(prepared_inputs),
                                              .output_limits = limits.output_limits});
        stages.emplace_back(GroupedAggregateStage{.keys = std::move(keys),
                                                  .definitions = std::move(definitions),
                                                  .limits = limits.grouped_aggregate_limits});
      } else {
        bool materialize_inputs = false;
        for (const SqlExpression* aggregate : aggregates) {
          if (aggregate->children().size() != 1U) {
            throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                             aggregate->span(), common::StatusCode::kInternal,
                                             "Bound aggregate has invalid arity")};
          }
          const SqlExpression& child = aggregate->children().front();
          materialize_inputs = materialize_inputs || (child.kind() != SqlExpressionKind::kStar &&
                                                      child.kind() != SqlExpressionKind::kColumn);
        }

        std::vector<ColumnOutputPosition> aggregate_inputs;
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
              if (reference == nullptr) {
                throw LoweringFailure{
                    diagnostic(SqlDiagnosticCode::kExecutionFailure, child.span(),
                               common::StatusCode::kInternal,
                               "Bound direct aggregate input has no physical source column")};
              }
              input_ordinal = source_lowerer.source_column_ordinal(
                  reference->source_ordinal, reference->column_ordinal, child.span());
            }
          }
          auto [definition, binding] =
              validate_aggregate(*aggregate, input_ordinal, definitions.size());
          definitions.push_back(definition);
          bindings.push_back(binding);
        }
        if (materialize_inputs) {
          stages.emplace_back(ColumnOutputStage{.positions = std::move(aggregate_inputs),
                                                .output_limits = limits.output_limits});
        }
        stages.emplace_back(UngroupedAggregateStage{.definitions = std::move(definitions),
                                                    .limits = limits.aggregate_limits});
      }

      ExpressionLowerer result_lowerer{select, limits.expression_limits, bindings, group_bindings,
                                       false};
      outputs = lower_outputs(result_lowerer, false);
      if (ordered) {
        append_order_outputs(result_lowerer, outputs, false);
        for (std::size_t group = 0U; group < aggregate_group_key_count; ++group) {
          const std::size_t key_ordinal = outputs.size();
          outputs.emplace_back(SourceColumnOutputPosition{group});
          sort_keys.push_back({.column_ordinal = key_ordinal,
                               .direction = PhysicalSortDirection::kAscending,
                               .null_placement = ScalarNullPlacement::kLast});
        }
      }
    } else {
      outputs = lower_outputs(source_lowerer, true);
      if (ordered) {
        append_order_outputs(source_lowerer, outputs, true);
        for (const schema::ColumnId key : source_schema.deduplication_key()) {
          const std::optional<std::size_t> ordinal = source_schema.column_ordinal(key);
          if (!ordinal.has_value()) {
            throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                             select.syntax().span(), common::StatusCode::kInternal,
                                             "DEDUP KEY identity has no source schema ordinal")};
          }
          const std::size_t key_ordinal = outputs.size();
          outputs.emplace_back(SourceColumnOutputPosition{*ordinal});
          sort_keys.push_back({.column_ordinal = key_ordinal,
                               .direction = PhysicalSortDirection::kAscending,
                               .null_placement = ScalarNullPlacement::kLast});
        }
        common::Result<VectorRowVersionLayout> suffix =
            vector_row_version_layout(source_column_count);
        if (!suffix.has_value())
          throw LoweringFailure{from_status(select.syntax().span(), suffix.error())};
        for (const std::size_t source_ordinal : {
                 suffix->wal_id_column_ordinal(),
                 suffix->record_sequence_column_ordinal(),
                 suffix->row_ordinal_column_ordinal(),
             }) {
          const std::size_t key_ordinal = outputs.size();
          outputs.emplace_back(SourceColumnOutputPosition{source_ordinal});
          sort_keys.push_back({.column_ordinal = key_ordinal,
                               .direction = PhysicalSortDirection::kAscending,
                               .null_placement = ScalarNullPlacement::kLast});
        }
      }
    }
    stages.emplace_back(
        ColumnOutputStage{.positions = std::move(outputs), .output_limits = limits.output_limits});
    if (ordered) {
      stages.emplace_back(SortStage{.keys = std::move(sort_keys), .limits = limits.sort_limits});
      std::vector<std::size_t> visible_outputs;
      visible_outputs.reserve(select.outputs().size());
      for (std::size_t ordinal = 0U; ordinal < select.outputs().size(); ++ordinal)
        visible_outputs.push_back(ordinal);
      stages.emplace_back(ColumnSubsetStage{.column_ordinals = std::move(visible_outputs)});
    }
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

SqlResult<PhysicalAsofPlan> lower_bound_sql_asof_select(const BoundSqlSelect& select,
                                                        const PhysicalSelectLoweringLimits limits) {
  try {
    if (select.syntax().mode() != SqlSelectMode::kSelect || select.asof_joins().empty() ||
        select.sources().size() != select.asof_joins().size() + 1U ||
        select.syntax().asof_joins().size() != select.asof_joins().size()) {
      return std::unexpected(
          diagnostic(SqlDiagnosticCode::kUnsupportedSyntax, select.syntax().span(),
                     common::StatusCode::kInvalidArgument,
                     "Physical ASOF lowering requires an ordinary bound ASOF SELECT"));
    }
    if (limits.expression_limits.maximum_instructions == 0U ||
        limits.expression_limits.maximum_instructions > kMaximumVectorExpressionInstructions ||
        limits.expression_limits.maximum_retained_configuration_bytes == 0U) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                                        common::StatusCode::kInvalidArgument,
                                        "Physical expression limits are invalid"));
    }
    const bool ordered = !select.syntax().order_by().empty();
    if (ordered && !select.aggregate_query()) {
      for (const BoundSqlSource& source : select.sources()) {
        if (source.schema_ptr()->deduplication_key().empty()) {
          return std::unexpected(diagnostic(
              SqlDiagnosticCode::kUnsupportedSyntax,
              select.syntax().order_by().front().expression.span(),
              common::StatusCode::kInvalidArgument,
              "Joined base-row ORDER BY requires a DEDUP KEY for every source because generated "
              "logical identity is not exposed by vector sources"));
        }
      }
    }

    std::vector<std::vector<PhysicalColumnShape>> source_inputs;
    source_inputs.reserve(select.sources().size());
    for (const BoundSqlSource& source : select.sources())
      source_inputs.push_back(source_shape_with_version(source, select.syntax().span()));

    constexpr std::size_t kUnavailable = std::numeric_limits<std::size_t>::max();
    std::vector<std::size_t> source_offsets(select.sources().size(), kUnavailable);
    std::vector<std::size_t> presence_ordinals(select.sources().size(), kUnavailable);
    source_offsets[0] = 0U;
    std::vector<PhysicalColumnShape> current_shape = source_inputs[0];
    std::vector<PhysicalPipelineStage> pending_left_stages;
    ExpressionLowerer primary_lowerer{select, limits.expression_limits, {},           {},
                                      true,   source_offsets,           current_shape};
    if (select.latest_by().has_value()) {
      const BoundLatestBy& bound_latest = *select.latest_by();
      const SqlLatestBy* syntax_latest = select.syntax().latest_by().has_value()
                                             ? std::addressof(*select.syntax().latest_by())
                                             : nullptr;
      if (syntax_latest == nullptr ||
          !same_span(syntax_latest->timestamp.span(), bound_latest.timestamp_expression_span)) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                         select.syntax().span(), common::StatusCode::kInternal,
                                         "Bound and parsed LATEST BY definitions disagree")};
      }
      std::vector<ColumnOutputPosition> positions;
      positions.reserve(current_shape.size() + 1U);
      for (std::size_t ordinal = 0U; ordinal < current_shape.size(); ++ordinal)
        positions.emplace_back(SourceColumnOutputPosition{ordinal});
      const std::size_t timestamp = positions.size();
      positions.push_back(primary_lowerer.output(syntax_latest->timestamp));
      pending_left_stages.emplace_back(ColumnOutputStage{.positions = std::move(positions),
                                                         .output_limits = limits.output_limits});
      std::vector<std::size_t> physical_keys;
      for (const schema::ColumnId column :
           select.sources()[0].schema_ptr()->physical_ordering_key()) {
        const std::optional<std::size_t> ordinal =
            select.sources()[0].schema_ptr()->column_ordinal(column);
        if (!ordinal.has_value()) {
          throw LoweringFailure{diagnostic(
              SqlDiagnosticCode::kExecutionFailure, syntax_latest->timestamp.span(),
              common::StatusCode::kInternal, "LATEST BY physical key has no schema ordinal")};
        }
        physical_keys.push_back(*ordinal);
      }
      pending_left_stages.emplace_back(
          LatestByStage{.definition =
                            VectorLatestByDefinition{
                                .key_column_ordinals = bound_latest.key_column_ordinals,
                                .timestamp_column_ordinal = timestamp,
                                .physical_ordering_key_ordinals = std::move(physical_keys),
                                .row_version_first_column_ordinal =
                                    select.sources()[0].schema_ptr()->columns().size(),
                            },
                        .limits = limits.latest_by_limits});
      std::vector<std::size_t> retained;
      retained.reserve(current_shape.size());
      for (std::size_t ordinal = 0U; ordinal < current_shape.size(); ++ordinal)
        retained.push_back(ordinal);
      pending_left_stages.emplace_back(ColumnSubsetStage{.column_ordinals = std::move(retained)});
    }

    std::vector<PhysicalAsofPlanJoin> joins;
    joins.reserve(select.asof_joins().size());
    for (std::size_t join_ordinal = 0U; join_ordinal < select.asof_joins().size(); ++join_ordinal) {
      const BoundAsofJoin& bound = select.asof_joins()[join_ordinal];
      const SqlAsofJoin& syntax = select.syntax().asof_joins()[join_ordinal];
      const std::size_t right_source = join_ordinal + 1U;
      if (bound.right_source_ordinal != right_source || bound.left != syntax.left) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                         syntax.condition.span(), common::StatusCode::kInternal,
                                         "Bound and parsed ASOF joins disagree")};
      }
      AsofSyntaxShape syntax_shape;
      collect_asof_syntax(select, syntax.condition, right_source, syntax_shape);
      if (syntax_shape.equality.size() != bound.equality_key_count ||
          syntax_shape.left_time == nullptr || syntax_shape.right_time == nullptr ||
          !same_span(syntax_shape.right_time->span(), bound.right_timestamp_expression_span)) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                         syntax.condition.span(), common::StatusCode::kInternal,
                                         "Bound ASOF metadata and syntax disagree")};
      }

      ExpressionLowerer left_lowerer{select, limits.expression_limits, {},           {},
                                     true,   source_offsets,           current_shape};
      std::vector<std::size_t> right_offsets(select.sources().size(), kUnavailable);
      right_offsets[right_source] = 0U;
      ExpressionLowerer right_lowerer{select,        limits.expression_limits,   {}, {}, true,
                                      right_offsets, source_inputs[right_source]};
      std::vector<ColumnOutputPosition> left_positions;
      std::vector<ColumnOutputPosition> right_positions;
      left_positions.reserve(current_shape.size() + syntax_shape.equality.size() + 1U);
      right_positions.reserve(source_inputs[right_source].size() + syntax_shape.equality.size() +
                              1U);
      for (std::size_t ordinal = 0U; ordinal < current_shape.size(); ++ordinal)
        left_positions.emplace_back(SourceColumnOutputPosition{ordinal});
      for (std::size_t ordinal = 0U; ordinal < source_inputs[right_source].size(); ++ordinal)
        right_positions.emplace_back(SourceColumnOutputPosition{ordinal});
      std::vector<VectorAsofEqualityKey> equality_keys;
      equality_keys.reserve(syntax_shape.equality.size());
      for (const auto& [left, right] : syntax_shape.equality) {
        const schema::LogicalType common = equality_common_type(select, *left, *right);
        const std::size_t left_ordinal = left_positions.size();
        const std::size_t right_ordinal = right_positions.size();
        left_positions.push_back(left_lowerer.output_as(*left, common));
        right_positions.push_back(right_lowerer.output_as(*right, common));
        equality_keys.push_back(
            {.left_column_ordinal = left_ordinal, .right_column_ordinal = right_ordinal});
      }
      const std::size_t left_timestamp = left_positions.size();
      const std::size_t right_timestamp = right_positions.size();
      left_positions.push_back(left_lowerer.output(*syntax_shape.left_time));
      right_positions.push_back(right_lowerer.output(*syntax_shape.right_time));
      pending_left_stages.emplace_back(ColumnOutputStage{.positions = std::move(left_positions),
                                                         .output_limits = limits.output_limits});
      std::vector<PhysicalPipelineStage> right_stages;
      right_stages.emplace_back(ColumnOutputStage{.positions = std::move(right_positions),
                                                  .output_limits = limits.output_limits});
      common::Result<PhysicalPipelinePlan> left_preparation = PhysicalPipelinePlan::create(
          current_shape, std::move(pending_left_stages), limits.plan_limits);
      if (!left_preparation.has_value())
        throw LoweringFailure{from_status(syntax.condition.span(), left_preparation.error())};
      common::Result<PhysicalPipelinePlan> right_preparation = PhysicalPipelinePlan::create(
          source_inputs[right_source], std::move(right_stages), limits.plan_limits);
      if (!right_preparation.has_value())
        throw LoweringFailure{from_status(syntax.condition.span(), right_preparation.error())};

      VectorAsofJoinDefinition definition;
      for (const PhysicalColumnShape& column : left_preparation->output_columns())
        definition.left_input_columns.push_back({.type = column.type, .nullable = column.nullable});
      for (const PhysicalColumnShape& column : right_preparation->output_columns())
        definition.right_input_columns.push_back(
            {.type = column.type, .nullable = column.nullable});
      definition.equality_keys = std::move(equality_keys);
      definition.left_timestamp_column_ordinal = left_timestamp;
      definition.right_timestamp_column_ordinal = right_timestamp;
      for (const schema::ColumnId column :
           select.sources()[right_source].schema_ptr()->physical_ordering_key()) {
        const std::optional<std::size_t> ordinal =
            select.sources()[right_source].schema_ptr()->column_ordinal(column);
        if (!ordinal.has_value()) {
          throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                           syntax.condition.span(), common::StatusCode::kInternal,
                                           "ASOF physical key has no schema ordinal")};
        }
        definition.right_physical_ordering_key_ordinals.push_back(*ordinal);
      }
      definition.right_row_version_first_column_ordinal =
          select.sources()[right_source].schema_ptr()->columns().size();
      definition.left_output_column_ordinals.reserve(current_shape.size());
      for (std::size_t ordinal = 0U; ordinal < current_shape.size(); ++ordinal)
        definition.left_output_column_ordinals.push_back(ordinal);
      definition.right_output_column_ordinals.reserve(source_inputs[right_source].size());
      for (std::size_t ordinal = 0U; ordinal < source_inputs[right_source].size(); ++ordinal)
        definition.right_output_column_ordinals.push_back(ordinal);
      definition.left_outer = bound.left;
      common::Result<std::vector<VectorAsofColumnShape>> joined_shape =
          vector_asof_join_output_shape(definition, limits.asof_join_limits);
      if (!joined_shape.has_value())
        throw LoweringFailure{from_status(syntax.condition.span(), joined_shape.error())};
      const std::size_t right_offset = current_shape.size();
      current_shape.clear();
      current_shape.reserve(joined_shape->size());
      for (const VectorAsofColumnShape& column : *joined_shape)
        current_shape.push_back({.type = column.type, .nullable = column.nullable});
      source_offsets[right_source] = right_offset;
      presence_ordinals[right_source] = current_shape.size() - 1U;
      joins.push_back({.left_preparation = std::move(*left_preparation),
                       .right_preparation = std::move(*right_preparation),
                       .definition = std::move(definition),
                       .limits = limits.asof_join_limits});
      pending_left_stages.clear();
    }

    std::vector<PhysicalPipelineStage> final_stages;
    ExpressionLowerer source_lowerer{select, limits.expression_limits, {},           {},
                                     true,   source_offsets,           current_shape};
    if (const SqlExpression* where = select.syntax().where(); where != nullptr) {
      std::vector<ColumnOutputPosition> predicate_positions;
      predicate_positions.reserve(current_shape.size() + 1U);
      for (std::size_t ordinal = 0U; ordinal < current_shape.size(); ++ordinal)
        predicate_positions.emplace_back(SourceColumnOutputPosition{ordinal});
      predicate_positions.push_back(source_lowerer.output(*where));
      final_stages.emplace_back(ColumnOutputStage{.positions = std::move(predicate_positions),
                                                  .output_limits = limits.output_limits});
      final_stages.emplace_back(BooleanFilterStage{.predicate_column = current_shape.size()});
    }

    const auto lower_output = [&select](ExpressionLowerer& lowerer, const BoundOutputColumn& output,
                                        const bool source_outputs_available) {
      if (source_outputs_available && output.source_ordinal.has_value() &&
          output.column_ordinal.has_value()) {
        return ColumnOutputPosition{SourceColumnOutputPosition{lowerer.source_column_ordinal(
            *output.source_ordinal, *output.column_ordinal, select.syntax().span())}};
      }
      if (!output.expression_span.has_value()) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                         select.syntax().span(), common::StatusCode::kInternal,
                                         "Bound output has no physical expression")};
      }
      const SqlExpression* expression = output_expression(select, *output.expression_span);
      if (expression == nullptr) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                         *output.expression_span, common::StatusCode::kInternal,
                                         "Bound output expression is absent from syntax")};
      }
      return lowerer.output(*expression);
    };
    const auto lower_outputs = [&select, &lower_output](ExpressionLowerer& lowerer,
                                                        const bool source_outputs_available) {
      std::vector<ColumnOutputPosition> outputs;
      outputs.reserve(select.outputs().size());
      for (const BoundOutputColumn& output : select.outputs())
        outputs.push_back(lower_output(lowerer, output, source_outputs_available));
      return outputs;
    };
    std::vector<VectorSortKey> sort_keys;
    const auto append_order_outputs = [&select, &sort_keys,
                                       &lower_output](ExpressionLowerer& lowerer,
                                                      std::vector<ColumnOutputPosition>& positions,
                                                      const bool source_outputs_available) {
      for (const SqlOrderItem& item : select.syntax().order_by()) {
        const std::size_t key_ordinal = positions.size();
        const BoundExpressionInfo* information = select.find_expression(item.expression.span());
        if (information != nullptr && information->output_ordinal.has_value()) {
          positions.push_back(lower_output(lowerer, select.outputs()[*information->output_ordinal],
                                           source_outputs_available));
        } else {
          positions.push_back(lowerer.output(item.expression));
        }
        sort_keys.push_back({.column_ordinal = key_ordinal,
                             .direction = order_direction(item.direction),
                             .null_placement = order_null_placement(item)});
      }
    };

    std::vector<ColumnOutputPosition> outputs;
    std::size_t aggregate_group_key_count = 0U;
    if (select.aggregate_query()) {
      std::vector<const SqlExpression*> aggregates;
      for (const SqlSelectItem& item : select.syntax().items()) {
        if (const SqlExpression* expression = item.expression(); expression != nullptr)
          collect_aggregates(*expression, aggregates);
      }
      for (const SqlOrderItem& item : select.syntax().order_by())
        collect_aggregates(item.expression, aggregates);
      const bool grouped = !select.syntax().group_by().empty();
      if (aggregates.empty() && !grouped) {
        throw LoweringFailure{diagnostic(SqlDiagnosticCode::kExecutionFailure,
                                         select.syntax().span(), common::StatusCode::kInternal,
                                         "Bound aggregate query has no aggregate expressions")};
      }
      const auto validate_aggregate = [&select](
                                          const SqlExpression& aggregate,
                                          const std::optional<std::size_t> input_ordinal,
                                          const std::optional<PhysicalColumnShape> input_shape,
                                          const std::size_t output_ordinal) {
        VectorAggregateDefinition definition =
            aggregate_definition(select, aggregate, input_ordinal);
        if (definition.input.has_value() && input_shape.has_value()) {
          definition.input->type = input_shape->type;
          definition.input->nullable = input_shape->nullable;
        }
        common::Result<VectorAggregateOutputShape> shape =
            vector_aggregate_output_shape(definition);
        if (!shape.has_value())
          throw LoweringFailure{from_status(aggregate.span(), shape.error())};
        return std::pair{definition, AggregatePhysicalBinding{.expression_span = aggregate.span(),
                                                              .column_ordinal = output_ordinal,
                                                              .type = shape->type,
                                                              .nullable = shape->nullable}};
      };
      std::vector<VectorAggregateDefinition> definitions;
      std::vector<AggregatePhysicalBinding> bindings;
      std::vector<GroupPhysicalBinding> group_bindings;
      if (grouped) {
        aggregate_group_key_count = select.syntax().group_by().size();
        std::vector<ColumnOutputPosition> prepared;
        std::vector<VectorGroupKeyDefinition> keys;
        for (const SqlExpression& group : select.syntax().group_by()) {
          const BoundExpressionInfo& information = bound_expression(select, group);
          const std::size_t ordinal = prepared.size();
          ColumnOutputPosition position = source_lowerer.output(group);
          const PhysicalColumnShape group_shape = output_position_shape(position, current_shape);
          prepared.push_back(std::move(position));
          keys.push_back({.column_ordinal = ordinal,
                          .type = group_shape.type,
                          .nullable = group_shape.nullable});
          group_bindings.push_back({.expression = std::addressof(group),
                                    .column_ordinal = keys.size() - 1U,
                                    .type = information.type.value(),
                                    .nullable = keys.back().nullable});
        }
        for (const SqlExpression* aggregate : aggregates) {
          const SqlExpression& child = aggregate->children().front();
          std::optional<std::size_t> input_ordinal;
          std::optional<PhysicalColumnShape> input_shape;
          if (child.kind() != SqlExpressionKind::kStar) {
            input_ordinal = prepared.size();
            ColumnOutputPosition position = source_lowerer.output(child);
            input_shape = output_position_shape(position, current_shape);
            prepared.push_back(std::move(position));
          }
          auto [definition, binding] = validate_aggregate(*aggregate, input_ordinal, input_shape,
                                                          keys.size() + definitions.size());
          definitions.push_back(definition);
          bindings.push_back(binding);
        }
        final_stages.emplace_back(ColumnOutputStage{.positions = std::move(prepared),
                                                    .output_limits = limits.output_limits});
        final_stages.emplace_back(GroupedAggregateStage{.keys = std::move(keys),
                                                        .definitions = std::move(definitions),
                                                        .limits = limits.grouped_aggregate_limits});
      } else {
        std::vector<ColumnOutputPosition> prepared;
        for (const SqlExpression* aggregate : aggregates) {
          const SqlExpression& child = aggregate->children().front();
          std::optional<std::size_t> input_ordinal;
          std::optional<PhysicalColumnShape> input_shape;
          if (child.kind() != SqlExpressionKind::kStar) {
            input_ordinal = prepared.size();
            ColumnOutputPosition position = source_lowerer.output(child);
            input_shape = output_position_shape(position, current_shape);
            prepared.push_back(std::move(position));
          }
          auto [definition, binding] =
              validate_aggregate(*aggregate, input_ordinal, input_shape, definitions.size());
          definitions.push_back(definition);
          bindings.push_back(binding);
        }
        if (!prepared.empty()) {
          final_stages.emplace_back(ColumnOutputStage{.positions = std::move(prepared),
                                                      .output_limits = limits.output_limits});
        }
        final_stages.emplace_back(UngroupedAggregateStage{.definitions = std::move(definitions),
                                                          .limits = limits.aggregate_limits});
      }
      ExpressionLowerer result_lowerer{select, limits.expression_limits, bindings, group_bindings,
                                       false};
      outputs = lower_outputs(result_lowerer, false);
      if (ordered) {
        append_order_outputs(result_lowerer, outputs, false);
        for (std::size_t group = 0U; group < aggregate_group_key_count; ++group) {
          const std::size_t key_ordinal = outputs.size();
          outputs.emplace_back(SourceColumnOutputPosition{group});
          sort_keys.push_back({.column_ordinal = key_ordinal,
                               .direction = PhysicalSortDirection::kAscending,
                               .null_placement = ScalarNullPlacement::kLast});
        }
      }
    } else {
      outputs = lower_outputs(source_lowerer, true);
      if (ordered) {
        append_order_outputs(source_lowerer, outputs, true);
        for (std::size_t source = 0U; source < select.sources().size(); ++source) {
          if (source != 0U) {
            const std::size_t key_ordinal = outputs.size();
            outputs.emplace_back(SourceColumnOutputPosition{presence_ordinals[source]});
            sort_keys.push_back({.column_ordinal = key_ordinal,
                                 .direction = PhysicalSortDirection::kAscending,
                                 .null_placement = ScalarNullPlacement::kLast});
          }
          for (const schema::ColumnId key :
               select.sources()[source].schema_ptr()->deduplication_key()) {
            const std::optional<std::size_t> ordinal =
                select.sources()[source].schema_ptr()->column_ordinal(key);
            if (!ordinal.has_value())
              throw LoweringFailure{diagnostic(
                  SqlDiagnosticCode::kExecutionFailure, select.syntax().span(),
                  common::StatusCode::kInternal, "Joined identity key has no schema ordinal")};
            const std::size_t key_ordinal = outputs.size();
            outputs.emplace_back(SourceColumnOutputPosition{source_offsets[source] + *ordinal});
            sort_keys.push_back({.column_ordinal = key_ordinal,
                                 .direction = PhysicalSortDirection::kAscending,
                                 .null_placement = ScalarNullPlacement::kLast});
          }
        }
        for (std::size_t source = 0U; source < select.sources().size(); ++source) {
          common::Result<VectorRowVersionLayout> suffix = vector_row_version_layout(
              source_offsets[source] + select.sources()[source].schema_ptr()->columns().size());
          if (!suffix.has_value())
            throw LoweringFailure{from_status(select.syntax().span(), suffix.error())};
          for (const std::size_t ordinal :
               {suffix->wal_id_column_ordinal(), suffix->record_sequence_column_ordinal(),
                suffix->row_ordinal_column_ordinal()}) {
            const std::size_t key_ordinal = outputs.size();
            outputs.emplace_back(SourceColumnOutputPosition{ordinal});
            sort_keys.push_back({.column_ordinal = key_ordinal,
                                 .direction = PhysicalSortDirection::kAscending,
                                 .null_placement = ScalarNullPlacement::kLast});
          }
        }
      }
    }
    final_stages.emplace_back(
        ColumnOutputStage{.positions = std::move(outputs), .output_limits = limits.output_limits});
    if (ordered) {
      final_stages.emplace_back(
          SortStage{.keys = std::move(sort_keys), .limits = limits.sort_limits});
      std::vector<std::size_t> visible;
      visible.reserve(select.outputs().size());
      for (std::size_t ordinal = 0U; ordinal < select.outputs().size(); ++ordinal)
        visible.push_back(ordinal);
      final_stages.emplace_back(ColumnSubsetStage{.column_ordinals = std::move(visible)});
    }
    if (select.syntax().limit().has_value())
      final_stages.emplace_back(LimitStage{.maximum_rows = *select.syntax().limit()});
    common::Result<PhysicalPipelinePlan> final_pipeline =
        PhysicalPipelinePlan::create(current_shape, std::move(final_stages), limits.plan_limits);
    if (!final_pipeline.has_value())
      return std::unexpected(from_status(select.syntax().span(), final_pipeline.error()));
    common::Result<PhysicalAsofPlan> plan = PhysicalAsofPlan::create(
        std::move(joins), std::move(*final_pipeline), limits.asof_plan_limits);
    if (!plan.has_value())
      return std::unexpected(from_status(select.syntax().span(), plan.error()));
    return std::move(*plan);
  } catch (LoweringFailure& failure) {
    return std::unexpected(failure.take());
  } catch (const std::bad_alloc&) {
    return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                                      common::StatusCode::kResourceExhausted,
                                      "Physical ASOF lowering allocation failed"));
  } catch (const std::length_error&) {
    return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, select.syntax().span(),
                                      common::StatusCode::kResourceExhausted,
                                      "Physical ASOF lowering exceeds container limits"));
  }
}

} // namespace chronos::query
