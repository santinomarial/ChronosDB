#include "chronos/query/evaluator.hpp"

#include "chronos/common/status.hpp"
#include "chronos/query/literal.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/utf8.hpp"
#include "query/decimal_internal.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::query {
namespace {

struct EvaluationFailure {
  SqlDiagnostic diagnostic;
};

[[nodiscard]] bool same_span(const SourceSpan& left, const SourceSpan& right) noexcept {
  return left == right;
}

[[nodiscard]] bool signed_integer(const schema::LogicalTypeKind kind) noexcept {
  return kind >= schema::LogicalTypeKind::kInt8 && kind <= schema::LogicalTypeKind::kInt64;
}

[[nodiscard]] bool unsigned_integer(const schema::LogicalTypeKind kind) noexcept {
  return kind >= schema::LogicalTypeKind::kUInt8 && kind <= schema::LogicalTypeKind::kUInt64;
}

[[nodiscard]] bool floating(const schema::LogicalTypeKind kind) noexcept {
  return kind == schema::LogicalTypeKind::kFloat32 || kind == schema::LogicalTypeKind::kFloat64;
}

[[nodiscard]] SqlTruthValue sql_not(const SqlTruthValue value) noexcept {
  if (value == SqlTruthValue::kUnknown)
    return value;
  return value == SqlTruthValue::kTrue ? SqlTruthValue::kFalse : SqlTruthValue::kTrue;
}

[[nodiscard]] SqlTruthValue sql_and(const SqlTruthValue left, const SqlTruthValue right) noexcept {
  if (left == SqlTruthValue::kFalse || right == SqlTruthValue::kFalse)
    return SqlTruthValue::kFalse;
  if (left == SqlTruthValue::kUnknown || right == SqlTruthValue::kUnknown)
    return SqlTruthValue::kUnknown;
  return SqlTruthValue::kTrue;
}

[[nodiscard]] SqlTruthValue sql_or(const SqlTruthValue left, const SqlTruthValue right) noexcept {
  if (left == SqlTruthValue::kTrue || right == SqlTruthValue::kTrue)
    return SqlTruthValue::kTrue;
  if (left == SqlTruthValue::kUnknown || right == SqlTruthValue::kUnknown)
    return SqlTruthValue::kUnknown;
  return SqlTruthValue::kFalse;
}

template <typename Value> [[nodiscard]] const Value* stored(const ScalarValue& value) noexcept {
  return std::get_if<Value>(&value.storage());
}

[[nodiscard]] const schema::LogicalType* scalar_type(const ScalarValue& value) noexcept {
  const auto& logical_type = value.type();
  if (!logical_type.has_value())
    return nullptr;
  return std::addressof(*logical_type);
}

[[nodiscard]] std::optional<std::int64_t> checked_add(const std::int64_t left,
                                                      const std::int64_t right) noexcept {
  if ((right > 0 && left > std::numeric_limits<std::int64_t>::max() - right) ||
      (right < 0 && left < std::numeric_limits<std::int64_t>::min() - right))
    return std::nullopt;
  return left + right;
}

[[nodiscard]] std::optional<std::int64_t> checked_subtract(const std::int64_t left,
                                                           const std::int64_t right) noexcept {
  if ((right < 0 && left > std::numeric_limits<std::int64_t>::max() + right) ||
      (right > 0 && left < std::numeric_limits<std::int64_t>::min() + right))
    return std::nullopt;
  return left - right;
}

[[nodiscard]] std::optional<std::int64_t> checked_multiply(const std::int64_t left,
                                                           const std::int64_t right) noexcept {
  if (left == 0 || right == 0)
    return 0;
  if ((left == -1 && right == std::numeric_limits<std::int64_t>::min()) ||
      (right == -1 && left == std::numeric_limits<std::int64_t>::min()))
    return std::nullopt;
  if (left > 0) {
    if ((right > 0 && left > std::numeric_limits<std::int64_t>::max() / right) ||
        (right < 0 && right < std::numeric_limits<std::int64_t>::min() / left))
      return std::nullopt;
  } else if ((right > 0 && left < std::numeric_limits<std::int64_t>::min() / right) ||
             (right < 0 && left < std::numeric_limits<std::int64_t>::max() / right)) {
    return std::nullopt;
  }
  return left * right;
}

class Evaluator {
public:
  Evaluator(const BoundSqlSelect& plan, const ScalarEvaluationContext& context) noexcept
      : plan_(plan), context_(context) {}

  [[nodiscard]] SqlResult<ScalarValue> run(const SqlExpression& expression) {
    if (context_.maximum_recursion == 0U)
      return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, expression.span(),
                                        common::StatusCode::kResourceExhausted,
                                        "Scalar expression recursion limit is zero"));
    try {
      return evaluate(expression, 0U);
    } catch (EvaluationFailure& failure) {
      return std::unexpected(std::move(failure.diagnostic));
    } catch (const std::bad_alloc&) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, expression.span(),
                                        common::StatusCode::kResourceExhausted,
                                        "Scalar expression allocation failed"));
    } catch (const std::length_error&) {
      return std::unexpected(diagnostic(SqlDiagnosticCode::kResourceLimit, expression.span(),
                                        common::StatusCode::kResourceExhausted,
                                        "Scalar expression value exceeds container limits"));
    }
  }

private:
  [[nodiscard]] static SqlDiagnostic diagnostic(const SqlDiagnosticCode code, const SourceSpan span,
                                                const common::StatusCode status,
                                                const std::string_view message) {
    return SqlDiagnostic{code, span, common::Status{status, std::string{message}}};
  }

  [[noreturn]] static void fail(const SourceSpan span, const common::StatusCode status,
                                const std::string_view message) {
    throw EvaluationFailure{
        diagnostic(SqlDiagnosticCode::kExecutionFailure, span, status, message)};
  }

  [[nodiscard]] const schema::LogicalType& result_type(const SqlExpression& expression) const {
    const BoundExpressionInfo* information = plan_.find_expression(expression.span());
    if (information == nullptr || !information->type.has_value())
      fail(expression.span(), common::StatusCode::kInternal,
           "Bound expression does not have a result type");
    return information->type.value();
  }

  [[nodiscard]] ScalarValue typed_null(const SqlExpression& expression) const {
    return ScalarValue::null(result_type(expression));
  }

  [[nodiscard]] static ScalarValue from_result(common::Result<ScalarValue> result,
                                               const SourceSpan span) {
    if (!result.has_value())
      fail(span, result.error().code(), result.error().message());
    return std::move(result.value());
  }

  [[nodiscard]] static SqlTruthValue truth(const ScalarValue& value, const SourceSpan span) {
    if (value.is_null())
      return SqlTruthValue::kUnknown;
    const bool* boolean = stored<bool>(value);
    if (boolean == nullptr)
      fail(span, common::StatusCode::kInternal, "Bound Boolean expression has non-Boolean storage");
    return *boolean ? SqlTruthValue::kTrue : SqlTruthValue::kFalse;
  }

  [[nodiscard]] static ScalarValue boolean(const SqlTruthValue value) {
    if (value == SqlTruthValue::kUnknown)
      return ScalarValue::null(schema::LogicalType::create(schema::LogicalTypeKind::kBool).value());
    return ScalarValue::boolean(value == SqlTruthValue::kTrue).value();
  }

  [[nodiscard]] const ScalarValue* override(const SourceSpan span) const noexcept {
    const auto found = std::ranges::find_if(context_.overrides, [&](const auto& candidate) {
      return same_span(candidate.expression_span, span);
    });
    return found == context_.overrides.end() ? nullptr : found->value;
  }

  [[nodiscard]] ScalarValue evaluate(const SqlExpression& expression, const std::size_t depth) {
    if (depth >= context_.maximum_recursion)
      fail(expression.span(), common::StatusCode::kResourceExhausted,
           "Scalar expression recursion exceeds the limit");
    if (const ScalarValue* replacement = override(expression.span()); replacement != nullptr)
      return *replacement;

    const BoundExpressionInfo* information = plan_.find_expression(expression.span());
    if (information != nullptr && information->output_ordinal.has_value()) {
      const std::size_t ordinal = information->output_ordinal.value();
      if (ordinal >= context_.projected_outputs.size())
        fail(expression.span(), common::StatusCode::kInvalidArgument,
             "ORDER BY output binding is absent from the evaluation context");
      return context_.projected_outputs[ordinal];
    }

    switch (expression.kind()) {
    case SqlExpressionKind::kLiteral:
      return literal(expression);
    case SqlExpressionKind::kColumn:
      return column(expression);
    case SqlExpressionKind::kCast:
      return cast(expression, evaluate(expression.children().front(), depth + 1U));
    case SqlExpressionKind::kUnary:
      return unary(expression, evaluate(expression.children().front(), depth + 1U));
    case SqlExpressionKind::kBinary:
      return binary(expression, depth);
    case SqlExpressionKind::kIsNull: {
      const ScalarValue operand = evaluate(expression.children().front(), depth + 1U);
      const bool is_null = operand.is_null();
      return boolean((expression.operation() == SqlOperator::kIsNull) == is_null
                         ? SqlTruthValue::kTrue
                         : SqlTruthValue::kFalse);
    }
    case SqlExpressionKind::kBetween:
      return between(expression, depth);
    case SqlExpressionKind::kIn:
      return in(expression, depth);
    case SqlExpressionKind::kFunction:
      return function(expression, depth);
    case SqlExpressionKind::kStar:
      fail(expression.span(), common::StatusCode::kInvalidArgument,
           "Star is not a scalar expression");
    }
    fail(expression.span(), common::StatusCode::kInternal, "Unknown scalar expression kind");
  }

  [[nodiscard]] ScalarValue literal(const SqlExpression& expression) const {
    switch (expression.literal_kind()) {
    case SqlLiteralKind::kNull:
      return ScalarValue::untyped_null();
    case SqlLiteralKind::kBoolean:
      return ScalarValue::boolean(expression.text() == "true").value();
    case SqlLiteralKind::kInteger:
      return from_result(
          ScalarValue::signed_value(result_type(expression),
                                    parse_sql_integer_literal(expression.text()).value()),
          expression.span());
    case SqlLiteralKind::kFloat:
      return ScalarValue::float64(parse_sql_float_literal(expression.text()).value()).value();
    case SqlLiteralKind::kString:
      return from_result(ScalarValue::text(result_type(expression), expression.text()),
                         expression.span());
    case SqlLiteralKind::kBinary: {
      std::vector<std::byte> bytes(expression.text().size());
      if (!bytes.empty())
        std::memcpy(bytes.data(), expression.text().data(), bytes.size());
      return ScalarValue::binary(std::move(bytes));
    }
    case SqlLiteralKind::kTimestamp:
      return ScalarValue::signed_value(result_type(expression),
                                       parse_sql_timestamp_ns_literal(expression.text()).value())
          .value();
    case SqlLiteralKind::kDate:
      return ScalarValue::signed_value(result_type(expression),
                                       parse_sql_date_literal(expression.text()).value())
          .value();
    case SqlLiteralKind::kInterval:
      return ScalarValue::signed_value(result_type(expression),
                                       parse_sql_interval_ns_literal(expression.text()).value())
          .value();
    case SqlLiteralKind::kUuid:
      return ScalarValue::uuid(parse_sql_uuid_literal(expression.text()).value());
    }
    fail(expression.span(), common::StatusCode::kInternal, "Unknown SQL literal kind");
  }

  [[nodiscard]] ScalarValue column(const SqlExpression& expression) const {
    const BoundColumnReference* reference = plan_.find_column_reference(expression.span());
    if (reference == nullptr || reference->source_ordinal >= context_.sources.size() ||
        reference->column_ordinal >= context_.sources[reference->source_ordinal].columns.size())
      fail(expression.span(), common::StatusCode::kInvalidArgument,
           "Bound column is absent from the scalar row context");
    return context_.sources[reference->source_ordinal].columns[reference->column_ordinal];
  }

  [[nodiscard]] ScalarValue cast(const SqlExpression& expression,
                                 const ScalarValue& operand) const {
    const schema::LogicalType target = result_type(expression);
    if (operand.is_null())
      return ScalarValue::null(target);
    const schema::LogicalType* source = scalar_type(operand);
    if (source == nullptr)
      fail(expression.span(), common::StatusCode::kInternal, "CAST operand is untyped non-NULL");
    if (*source == target)
      return operand;

    if (target.is_decimal()) {
      common::Result<Decimal128Value> converted = common::make_unexpected(
          common::Status{common::StatusCode::kInvalidArgument,
                         "CAST to DECIMAL does not support the operand type"});
      if (const auto* signed_value = stored<std::int64_t>(operand); signed_value != nullptr)
        converted = detail::decimal_from_signed(*signed_value, target);
      else if (const auto* unsigned_value = stored<std::uint64_t>(operand);
               unsigned_value != nullptr)
        converted = detail::decimal_from_unsigned(*unsigned_value, target);
      else if (const auto* float_value = stored<float>(operand); float_value != nullptr)
        converted = detail::decimal_from_float(*float_value, target);
      else if (const auto* double_value = stored<double>(operand); double_value != nullptr)
        converted = detail::decimal_from_double(*double_value, target);
      else if (const auto* decimal_value = stored<Decimal128Value>(operand);
               decimal_value != nullptr)
        converted = detail::rescale_decimal(*decimal_value, *source, target);
      if (!converted.has_value())
        fail(expression.span(), converted.error().code(), converted.error().message());
      return ScalarValue::decimal(target, *converted).value();
    }

    if (signed_integer(target.kind()) || target.kind() == schema::LogicalTypeKind::kTimestampNs ||
        target.kind() == schema::LogicalTypeKind::kDate) {
      std::int64_t value = 0;
      if (const auto* decimal_value = stored<Decimal128Value>(operand); decimal_value != nullptr) {
        const common::Result<std::int64_t> converted =
            detail::decimal_to_signed(*decimal_value, *source);
        if (!converted.has_value())
          fail(expression.span(), converted.error().code(), converted.error().message());
        value = *converted;
      } else if (const auto* signed_value = stored<std::int64_t>(operand); signed_value != nullptr)
        value = *signed_value;
      else if (const auto* unsigned_value = stored<std::uint64_t>(operand);
               unsigned_value != nullptr &&
               *unsigned_value <=
                   static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
        value = static_cast<std::int64_t>(*unsigned_value);
      else if (const auto* float_value = stored<float>(operand);
               float_value != nullptr && std::isfinite(*float_value) &&
               static_cast<double>(*float_value) >= -9'223'372'036'854'775'808.0 &&
               static_cast<double>(*float_value) < 9'223'372'036'854'775'808.0)
        value = static_cast<std::int64_t>(*float_value);
      else if (const auto* double_value = stored<double>(operand);
               double_value != nullptr && std::isfinite(*double_value) &&
               *double_value >= -9'223'372'036'854'775'808.0 &&
               *double_value < 9'223'372'036'854'775'808.0)
        value = static_cast<std::int64_t>(*double_value);
      else
        fail(expression.span(), common::StatusCode::kOutOfRange,
             "CAST to signed/temporal type is unsupported or out of range");
      if (source->kind() == schema::LogicalTypeKind::kDate &&
          target.kind() == schema::LogicalTypeKind::kTimestampNs) {
        const auto converted = checked_multiply(value, 86'400'000'000'000LL);
        if (!converted.has_value())
          fail(expression.span(), common::StatusCode::kOutOfRange,
               "DATE to TIMESTAMP_NS cast overflows");
        value = *converted;
      } else if (source->kind() == schema::LogicalTypeKind::kTimestampNs &&
                 target.kind() == schema::LogicalTypeKind::kDate) {
        constexpr std::int64_t kDay = 86'400'000'000'000LL;
        value = value / kDay - (value < 0 && value % kDay != 0 ? 1 : 0);
      }
      return from_result(ScalarValue::signed_value(target, value), expression.span());
    }
    if (unsigned_integer(target.kind())) {
      std::uint64_t value = 0U;
      if (const auto* decimal_value = stored<Decimal128Value>(operand); decimal_value != nullptr) {
        const common::Result<std::uint64_t> converted =
            detail::decimal_to_unsigned(*decimal_value, *source);
        if (!converted.has_value())
          fail(expression.span(), converted.error().code(), converted.error().message());
        value = *converted;
      } else if (const auto* unsigned_value = stored<std::uint64_t>(operand);
                 unsigned_value != nullptr)
        value = *unsigned_value;
      else if (const auto* signed_value = stored<std::int64_t>(operand);
               signed_value != nullptr && *signed_value >= 0)
        value = static_cast<std::uint64_t>(*signed_value);
      else if (const auto* float_value = stored<float>(operand);
               float_value != nullptr && std::isfinite(*float_value) && *float_value >= 0.0F &&
               static_cast<double>(*float_value) < 18'446'744'073'709'551'616.0)
        value = static_cast<std::uint64_t>(*float_value);
      else if (const auto* double_value = stored<double>(operand);
               double_value != nullptr && std::isfinite(*double_value) && *double_value >= 0.0 &&
               *double_value < 18'446'744'073'709'551'616.0)
        value = static_cast<std::uint64_t>(*double_value);
      else
        fail(expression.span(), common::StatusCode::kOutOfRange,
             "CAST to unsigned type is unsupported or out of range");
      return from_result(ScalarValue::unsigned_value(target, value), expression.span());
    }
    if (floating(target.kind())) {
      double value = 0.0;
      if (const auto* decimal_value = stored<Decimal128Value>(operand); decimal_value != nullptr) {
        if (target.kind() == schema::LogicalTypeKind::kFloat32) {
          const common::Result<float> converted = detail::decimal_to_float(*decimal_value, *source);
          if (!converted.has_value())
            fail(expression.span(), converted.error().code(), converted.error().message());
          return ScalarValue::float32(*converted).value();
        }
        const common::Result<double> converted = detail::decimal_to_double(*decimal_value, *source);
        if (!converted.has_value())
          fail(expression.span(), converted.error().code(), converted.error().message());
        value = *converted;
      } else if (const auto* signed_value = stored<std::int64_t>(operand); signed_value != nullptr)
        value = static_cast<double>(*signed_value);
      else if (const auto* unsigned_value = stored<std::uint64_t>(operand);
               unsigned_value != nullptr)
        value = static_cast<double>(*unsigned_value);
      else if (const auto* float_value = stored<float>(operand); float_value != nullptr)
        value = *float_value;
      else if (const auto* double_value = stored<double>(operand); double_value != nullptr)
        value = *double_value;
      else
        fail(expression.span(), common::StatusCode::kInvalidArgument,
             "CAST to floating type is unsupported");
      return target.kind() == schema::LogicalTypeKind::kFloat32
                 ? ScalarValue::float32(static_cast<float>(value)).value()
                 : ScalarValue::float64(value).value();
    }
    if (target.kind() == schema::LogicalTypeKind::kString ||
        target.kind() == schema::LogicalTypeKind::kSymbol) {
      const auto* text = stored<std::string>(operand);
      if (text == nullptr)
        fail(expression.span(), common::StatusCode::kInvalidArgument,
             "CAST to STRING/SYMBOL supports only STRING/SYMBOL in SQL v1");
      return ScalarValue::text(target, *text).value();
    }
    fail(expression.span(), common::StatusCode::kInvalidArgument,
         "CAST conversion is not supported in SQL v1");
  }

  [[nodiscard]] ScalarValue unary(const SqlExpression& expression,
                                  const ScalarValue& operand) const {
    if (expression.operation() == SqlOperator::kNot)
      return boolean(sql_not(truth(operand, expression.span())));
    if (operand.is_null())
      return typed_null(expression);
    if (expression.operation() == SqlOperator::kPositive)
      return operand;
    const schema::LogicalType type = result_type(expression);
    if (const auto* value = stored<std::int64_t>(operand); value != nullptr) {
      if (*value == std::numeric_limits<std::int64_t>::min())
        fail(expression.span(), common::StatusCode::kOutOfRange, "Unary negation overflows");
      return from_result(ScalarValue::signed_value(type, -*value), expression.span());
    }
    if (const auto* value = stored<float>(operand); value != nullptr)
      return ScalarValue::float32(-*value).value();
    if (const auto* value = stored<double>(operand); value != nullptr)
      return ScalarValue::float64(-*value).value();
    if (const auto* value = stored<Decimal128Value>(operand); value != nullptr) {
      const common::Result<Decimal128Value> negated = detail::negate_decimal(*value, type);
      if (!negated.has_value())
        fail(expression.span(), negated.error().code(), negated.error().message());
      return ScalarValue::decimal(type, *negated).value();
    }
    fail(expression.span(), common::StatusCode::kInvalidArgument,
         "Unary negation is unsupported for this numeric type");
  }

  [[nodiscard]] ScalarValue binary(const SqlExpression& expression, const std::size_t depth) {
    const ScalarValue left = evaluate(expression.children()[0], depth + 1U);
    if (expression.operation() == SqlOperator::kAnd || expression.operation() == SqlOperator::kOr) {
      const SqlTruthValue lhs = truth(left, expression.children()[0].span());
      if ((expression.operation() == SqlOperator::kAnd && lhs == SqlTruthValue::kFalse) ||
          (expression.operation() == SqlOperator::kOr && lhs == SqlTruthValue::kTrue))
        return boolean(lhs);
      const ScalarValue right = evaluate(expression.children()[1], depth + 1U);
      const SqlTruthValue rhs = truth(right, expression.children()[1].span());
      return boolean(expression.operation() == SqlOperator::kAnd ? sql_and(lhs, rhs)
                                                                 : sql_or(lhs, rhs));
    }
    const ScalarValue right = evaluate(expression.children()[1], depth + 1U);
    if (expression.operation() >= SqlOperator::kEqual &&
        expression.operation() <= SqlOperator::kGreaterEqual)
      return comparison(expression, left, right);
    if (left.is_null() || right.is_null())
      return typed_null(expression);
    return arithmetic(expression, left, right);
  }

  [[nodiscard]] static ScalarValue comparison(const SqlExpression& expression,
                                              const ScalarValue& left, const ScalarValue& right) {
    if (left.is_null() || right.is_null())
      return boolean(SqlTruthValue::kUnknown);
    if (expression.operation() == SqlOperator::kEqual ||
        expression.operation() == SqlOperator::kNotEqual) {
      common::Result<SqlTruthValue> equality = sql_scalar_equal(left, right);
      if (!equality.has_value())
        fail(expression.span(), equality.error().code(), equality.error().message());
      const SqlTruthValue value = expression.operation() == SqlOperator::kNotEqual
                                      ? sql_not(equality.value())
                                      : equality.value();
      return boolean(value);
    }
    const schema::LogicalType* left_type = scalar_type(left);
    const schema::LogicalType* right_type = scalar_type(right);
    if ((left_type != nullptr && floating(left_type->kind()) &&
         ((stored<float>(left) != nullptr && std::isnan(*stored<float>(left))) ||
          (stored<double>(left) != nullptr && std::isnan(*stored<double>(left))))) ||
        (right_type != nullptr && floating(right_type->kind()) &&
         ((stored<float>(right) != nullptr && std::isnan(*stored<float>(right))) ||
          (stored<double>(right) != nullptr && std::isnan(*stored<double>(right))))))
      return boolean(SqlTruthValue::kFalse);
    const common::Result<int> order =
        compare_scalar_values(left, right, ScalarNullPlacement::kLast);
    if (!order.has_value())
      fail(expression.span(), order.error().code(), order.error().message());
    bool result = false;
    switch (expression.operation()) {
    case SqlOperator::kLess:
      result = *order < 0;
      break;
    case SqlOperator::kLessEqual:
      result = *order <= 0;
      break;
    case SqlOperator::kGreater:
      result = *order > 0;
      break;
    case SqlOperator::kGreaterEqual:
      result = *order >= 0;
      break;
    default:
      break;
    }
    return boolean(result ? SqlTruthValue::kTrue : SqlTruthValue::kFalse);
  }

  [[nodiscard]] ScalarValue arithmetic(const SqlExpression& expression, const ScalarValue& left,
                                       const ScalarValue& right) const {
    const schema::LogicalType result = result_type(expression);
    if (const auto* lhs = stored<std::int64_t>(left); lhs != nullptr) {
      const auto* rhs = stored<std::int64_t>(right);
      if (rhs == nullptr)
        fail(expression.span(), common::StatusCode::kInternal,
             "Signed arithmetic storage mismatch");
      std::optional<std::int64_t> value;
      switch (expression.operation()) {
      case SqlOperator::kAdd:
        value = checked_add(*lhs, *rhs);
        break;
      case SqlOperator::kSubtract:
        value = checked_subtract(*lhs, *rhs);
        break;
      case SqlOperator::kMultiply:
        value = checked_multiply(*lhs, *rhs);
        break;
      case SqlOperator::kDivide:
        if (*rhs == 0)
          fail(expression.span(), common::StatusCode::kInvalidArgument, "Integer division by zero");
        if (*lhs == std::numeric_limits<std::int64_t>::min() && *rhs == -1)
          fail(expression.span(), common::StatusCode::kOutOfRange, "Integer division overflows");
        value = *lhs / *rhs;
        break;
      case SqlOperator::kRemainder:
        if (*rhs == 0)
          fail(expression.span(), common::StatusCode::kInvalidArgument,
               "Integer remainder by zero");
        value = *lhs == std::numeric_limits<std::int64_t>::min() && *rhs == -1 ? 0 : *lhs % *rhs;
        break;
      default:
        break;
      }
      if (!value.has_value())
        fail(expression.span(), common::StatusCode::kOutOfRange, "Signed arithmetic overflows");
      return from_result(ScalarValue::signed_value(result, *value), expression.span());
    }
    if (const auto* lhs = stored<std::uint64_t>(left); lhs != nullptr) {
      const auto* rhs = stored<std::uint64_t>(right);
      if (rhs == nullptr)
        fail(expression.span(), common::StatusCode::kInternal,
             "Unsigned arithmetic storage mismatch");
      std::uint64_t value = 0U;
      switch (expression.operation()) {
      case SqlOperator::kAdd:
        if (*rhs > std::numeric_limits<std::uint64_t>::max() - *lhs)
          fail(expression.span(), common::StatusCode::kOutOfRange, "Unsigned addition overflows");
        value = *lhs + *rhs;
        break;
      case SqlOperator::kSubtract:
        if (*lhs < *rhs)
          fail(expression.span(), common::StatusCode::kOutOfRange,
               "Unsigned subtraction underflows");
        value = *lhs - *rhs;
        break;
      case SqlOperator::kMultiply:
        if (*lhs != 0U && *rhs > std::numeric_limits<std::uint64_t>::max() / *lhs)
          fail(expression.span(), common::StatusCode::kOutOfRange,
               "Unsigned multiplication overflows");
        value = *lhs * *rhs;
        break;
      case SqlOperator::kDivide:
      case SqlOperator::kRemainder:
        if (*rhs == 0U)
          fail(expression.span(), common::StatusCode::kInvalidArgument,
               "Unsigned division or remainder by zero");
        value = expression.operation() == SqlOperator::kDivide ? *lhs / *rhs : *lhs % *rhs;
        break;
      default:
        fail(expression.span(), common::StatusCode::kInternal,
             "Unknown unsigned arithmetic operation");
      }
      return from_result(ScalarValue::unsigned_value(result, value), expression.span());
    }
    if (floating(result.kind())) {
      if (result.kind() == schema::LogicalTypeKind::kFloat32) {
        const auto* lhs = stored<float>(left);
        const auto* rhs = stored<float>(right);
        if (lhs == nullptr || rhs == nullptr)
          fail(expression.span(), common::StatusCode::kInternal,
               "FLOAT32 arithmetic storage mismatch");
        float value = 0.0F;
        switch (expression.operation()) {
        case SqlOperator::kAdd:
          value = *lhs + *rhs;
          break;
        case SqlOperator::kSubtract:
          value = *lhs - *rhs;
          break;
        case SqlOperator::kMultiply:
          value = *lhs * *rhs;
          break;
        case SqlOperator::kDivide:
          value = *lhs / *rhs;
          break;
        case SqlOperator::kRemainder:
          value = std::fmod(*lhs, *rhs);
          break;
        default:
          fail(expression.span(), common::StatusCode::kInternal,
               "Unknown FLOAT32 arithmetic operation");
        }
        return ScalarValue::float32(value).value();
      }
      const double lhs =
          stored<float>(left) != nullptr ? *stored<float>(left) : *stored<double>(left);
      const double rhs =
          stored<float>(right) != nullptr ? *stored<float>(right) : *stored<double>(right);
      double value = 0.0;
      switch (expression.operation()) {
      case SqlOperator::kAdd:
        value = lhs + rhs;
        break;
      case SqlOperator::kSubtract:
        value = lhs - rhs;
        break;
      case SqlOperator::kMultiply:
        value = lhs * rhs;
        break;
      case SqlOperator::kDivide:
        value = lhs / rhs;
        break;
      case SqlOperator::kRemainder:
        value = std::fmod(lhs, rhs);
        break;
      default:
        fail(expression.span(), common::StatusCode::kInternal,
             "Unknown floating arithmetic operation");
      }
      return ScalarValue::float64(value).value();
    }
    if (const auto* lhs = stored<Decimal128Value>(left); lhs != nullptr) {
      const auto* rhs = stored<Decimal128Value>(right);
      if (rhs == nullptr)
        fail(expression.span(), common::StatusCode::kInternal,
             "DECIMAL arithmetic storage mismatch");
      detail::DecimalOperation operation = detail::DecimalOperation::kAdd;
      switch (expression.operation()) {
      case SqlOperator::kAdd:
        operation = detail::DecimalOperation::kAdd;
        break;
      case SqlOperator::kSubtract:
        operation = detail::DecimalOperation::kSubtract;
        break;
      case SqlOperator::kMultiply:
        operation = detail::DecimalOperation::kMultiply;
        break;
      case SqlOperator::kDivide:
        operation = detail::DecimalOperation::kDivide;
        break;
      case SqlOperator::kRemainder:
        operation = detail::DecimalOperation::kRemainder;
        break;
      default:
        fail(expression.span(), common::StatusCode::kInternal,
             "Unknown DECIMAL arithmetic operation");
      }
      const common::Result<Decimal128Value> value =
          detail::evaluate_decimal(operation, *lhs, *rhs, result);
      if (!value.has_value())
        fail(expression.span(), value.error().code(), value.error().message());
      return ScalarValue::decimal(result, *value).value();
    }
    fail(expression.span(), common::StatusCode::kInternal,
         "Bound numeric expression has unsupported storage");
  }

  [[nodiscard]] ScalarValue between(const SqlExpression& expression, const std::size_t depth) {
    const ScalarValue value = evaluate(expression.children()[0], depth + 1U);
    const ScalarValue lower = evaluate(expression.children()[1], depth + 1U);
    const ScalarValue upper = evaluate(expression.children()[2], depth + 1U);
    const SqlTruthValue low =
        truth(comparison_expression(value, lower, SqlOperator::kGreaterEqual, expression.span()),
              expression.span());
    const SqlTruthValue high =
        truth(comparison_expression(value, upper, SqlOperator::kLessEqual, expression.span()),
              expression.span());
    SqlTruthValue result = sql_and(low, high);
    if (expression.operation() == SqlOperator::kNotBetween)
      result = sql_not(result);
    return boolean(result);
  }

  [[nodiscard]] static ScalarValue comparison_expression(const ScalarValue& left,
                                                         const ScalarValue& right,
                                                         const SqlOperator operation,
                                                         const SourceSpan span) {
    if (left.is_null() || right.is_null())
      return boolean(SqlTruthValue::kUnknown);
    if ((stored<float>(left) != nullptr && std::isnan(*stored<float>(left))) ||
        (stored<double>(left) != nullptr && std::isnan(*stored<double>(left))) ||
        (stored<float>(right) != nullptr && std::isnan(*stored<float>(right))) ||
        (stored<double>(right) != nullptr && std::isnan(*stored<double>(right))))
      return boolean(SqlTruthValue::kFalse);
    const common::Result<int> order =
        compare_scalar_values(left, right, ScalarNullPlacement::kLast);
    if (!order.has_value())
      fail(span, order.error().code(), order.error().message());
    const bool value = operation == SqlOperator::kGreaterEqual ? *order >= 0 : *order <= 0;
    return boolean(value ? SqlTruthValue::kTrue : SqlTruthValue::kFalse);
  }

  [[nodiscard]] ScalarValue in(const SqlExpression& expression, const std::size_t depth) {
    const ScalarValue searched = evaluate(expression.children().front(), depth + 1U);
    SqlTruthValue result = SqlTruthValue::kFalse;
    for (std::size_t index = 1U; index < expression.children().size(); ++index) {
      const ScalarValue candidate = evaluate(expression.children()[index], depth + 1U);
      const common::Result<SqlTruthValue> equality = sql_scalar_equal(searched, candidate);
      if (!equality.has_value())
        fail(expression.span(), equality.error().code(), equality.error().message());
      if (*equality == SqlTruthValue::kTrue) {
        result = SqlTruthValue::kTrue;
        break;
      }
      if (*equality == SqlTruthValue::kUnknown)
        result = SqlTruthValue::kUnknown;
    }
    if (expression.operation() == SqlOperator::kNotIn)
      result = sql_not(result);
    return boolean(result);
  }

  [[nodiscard]] ScalarValue function(const SqlExpression& expression, const std::size_t depth) {
    if (expression.text() == "coalesce") {
      for (const SqlExpression& child : expression.children()) {
        ScalarValue value = evaluate(child, depth + 1U);
        if (!value.is_null())
          return value;
      }
      return typed_null(expression);
    }
    if (expression.text() == "abs") {
      ScalarValue value = evaluate(expression.children().front(), depth + 1U);
      if (value.is_null())
        return typed_null(expression);
      if (const auto* signed_value = stored<std::int64_t>(value); signed_value != nullptr) {
        if (*signed_value == std::numeric_limits<std::int64_t>::min())
          fail(expression.span(), common::StatusCode::kOutOfRange, "ABS overflows");
        return from_result(ScalarValue::signed_value(result_type(expression), *signed_value < 0
                                                                                  ? -*signed_value
                                                                                  : *signed_value),
                           expression.span());
      }
      if (stored<std::uint64_t>(value) != nullptr)
        return value;
      if (const auto* float_value = stored<float>(value); float_value != nullptr)
        return ScalarValue::float32(std::fabs(*float_value)).value();
      if (const auto* double_value = stored<double>(value); double_value != nullptr)
        return ScalarValue::float64(std::fabs(*double_value)).value();
      if (const auto* decimal_value = stored<Decimal128Value>(value); decimal_value != nullptr) {
        const common::Result<Decimal128Value> absolute =
            detail::absolute_decimal(*decimal_value, result_type(expression));
        if (!absolute.has_value())
          fail(expression.span(), absolute.error().code(), absolute.error().message());
        return ScalarValue::decimal(result_type(expression), *absolute).value();
      }
      fail(expression.span(), common::StatusCode::kInternal,
           "ABS input has unsupported numeric storage");
    }
    if (expression.text() == "lower" || expression.text() == "upper") {
      ScalarValue value = evaluate(expression.children().front(), depth + 1U);
      if (value.is_null())
        return typed_null(expression);
      const auto* input = stored<std::string>(value);
      if (input == nullptr)
        fail(expression.span(), common::StatusCode::kInternal, "Case function input is not text");
      std::string output = *input;
      for (char& character : output) {
        if (expression.text() == "lower" && character >= 'A' && character <= 'Z')
          character = static_cast<char>(character - 'A' + 'a');
        else if (expression.text() == "upper" && character >= 'a' && character <= 'z')
          character = static_cast<char>(character - 'a' + 'A');
      }
      return ScalarValue::text(result_type(expression), std::move(output)).value();
    }
    if (expression.text() == "time_bucket") {
      const ScalarValue interval = evaluate(expression.children()[0], depth + 1U);
      const ScalarValue timestamp = evaluate(expression.children()[1], depth + 1U);
      if (interval.is_null() || timestamp.is_null())
        return typed_null(expression);
      const auto* width = stored<std::int64_t>(interval);
      const auto* point = stored<std::int64_t>(timestamp);
      if (width == nullptr || point == nullptr || *width <= 0)
        fail(expression.span(), common::StatusCode::kInvalidArgument,
             "time_bucket width must be positive nanoseconds");
      std::int64_t bucket = *point / *width;
      if (*point < 0 && *point % *width != 0)
        --bucket;
      const auto start = checked_multiply(bucket, *width);
      if (!start.has_value())
        fail(expression.span(), common::StatusCode::kOutOfRange, "time_bucket result overflows");
      return ScalarValue::signed_value(result_type(expression), *start).value();
    }
    fail(expression.span(), common::StatusCode::kInvalidArgument,
         "Aggregate expression requires a relational override");
  }

  const BoundSqlSelect& plan_;
  const ScalarEvaluationContext& context_;
};

} // namespace

SqlResult<ScalarValue> evaluate_sql_v1_expression(const BoundSqlSelect& plan,
                                                  const SqlExpression& expression,
                                                  const ScalarEvaluationContext& context) {
  return Evaluator{plan, context}.run(expression);
}

SqlResult<SqlTruthValue> evaluate_sql_v1_predicate(const BoundSqlSelect& plan,
                                                   const SqlExpression& expression,
                                                   const ScalarEvaluationContext& context) {
  SqlResult<ScalarValue> value = evaluate_sql_v1_expression(plan, expression, context);
  if (!value.has_value())
    return std::unexpected(value.error());
  if (value->is_null())
    return SqlTruthValue::kUnknown;
  const bool* boolean = std::get_if<bool>(&value->storage());
  if (boolean == nullptr)
    return std::unexpected(SqlDiagnostic{
        SqlDiagnosticCode::kExecutionFailure, expression.span(),
        common::Status{common::StatusCode::kInternal, "Predicate result is not Boolean"}});
  return *boolean ? SqlTruthValue::kTrue : SqlTruthValue::kFalse;
}

} // namespace chronos::query
