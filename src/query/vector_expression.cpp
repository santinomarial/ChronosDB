#include "chronos/query/vector_expression.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"
#include "chronos/query/vector_chunk.hpp"
#include "query/decimal_internal.hpp"
#include "query/vector_expression_internal.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status out_of_range(std::string message) {
  return common::Status{common::StatusCode::kOutOfRange, std::move(message)};
}

[[nodiscard]] common::Status internal(std::string message) {
  return common::Status{common::StatusCode::kInternal, std::move(message)};
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

[[nodiscard]] bool numeric(const schema::LogicalTypeKind kind) noexcept {
  return signed_integer(kind) || unsigned_integer(kind) || floating(kind) ||
         kind == schema::LogicalTypeKind::kDecimal;
}

[[nodiscard]] bool supported_leaf(const schema::LogicalTypeKind kind) noexcept {
  return numeric(kind) || kind == schema::LogicalTypeKind::kBool ||
         kind == schema::LogicalTypeKind::kDate || kind == schema::LogicalTypeKind::kTimestampNs;
}

[[nodiscard]] const schema::LogicalType* scalar_type(const ScalarValue& value) noexcept {
  const std::optional<schema::LogicalType>& type = value.type();
  return type.has_value() ? std::addressof(type.value()) : nullptr;
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

[[nodiscard]] std::optional<schema::LogicalType> common_type(const schema::LogicalType& left,
                                                             const schema::LogicalType& right) {
  if (left == right)
    return left;
  if (signed_integer(left.kind()) && signed_integer(right.kind()))
    return integer_rank(left.kind()) >= integer_rank(right.kind()) ? left : right;
  if (unsigned_integer(left.kind()) && unsigned_integer(right.kind()))
    return integer_rank(left.kind()) >= integer_rank(right.kind()) ? left : right;
  if (floating(left.kind()) && floating(right.kind()))
    return schema::LogicalType::create(schema::LogicalTypeKind::kFloat64).value();
  return std::nullopt;
}

[[nodiscard]] common::Result<void> validate_constant(const ScalarValue& value) {
  const schema::LogicalType* type = scalar_type(value);
  if (type == nullptr)
    return common::make_unexpected(invalid("vector expression constant must be typed"));
  if (!supported_leaf(type->kind())) {
    return common::make_unexpected(
        invalid("vector expression constant type is not supported by this program version"));
  }
  if (value.is_null())
    return {};
  bool valid = false;
  switch (type->kind()) {
  case schema::LogicalTypeKind::kBool:
    valid = std::holds_alternative<bool>(value.storage());
    break;
  case schema::LogicalTypeKind::kInt8:
  case schema::LogicalTypeKind::kInt16:
  case schema::LogicalTypeKind::kInt32:
  case schema::LogicalTypeKind::kInt64:
  case schema::LogicalTypeKind::kDate:
  case schema::LogicalTypeKind::kTimestampNs:
    valid = std::holds_alternative<std::int64_t>(value.storage());
    break;
  case schema::LogicalTypeKind::kUInt8:
  case schema::LogicalTypeKind::kUInt16:
  case schema::LogicalTypeKind::kUInt32:
  case schema::LogicalTypeKind::kUInt64:
    valid = std::holds_alternative<std::uint64_t>(value.storage());
    break;
  case schema::LogicalTypeKind::kFloat32:
    valid = std::holds_alternative<float>(value.storage());
    break;
  case schema::LogicalTypeKind::kFloat64:
    valid = std::holds_alternative<double>(value.storage());
    break;
  case schema::LogicalTypeKind::kDecimal:
    valid = std::holds_alternative<Decimal128Value>(value.storage());
    break;
  case schema::LogicalTypeKind::kSymbol:
  case schema::LogicalTypeKind::kString:
  case schema::LogicalTypeKind::kBinary:
  case schema::LogicalTypeKind::kUuid:
    break;
  }
  if (!valid)
    return common::make_unexpected(internal("vector expression constant storage is inconsistent"));
  return {};
}

[[nodiscard]] common::Result<VectorExpressionShape>
unary_shape(const VectorUnaryExpression& operation, const VectorExpressionShape& operand) {
  switch (operation.operation) {
  case VectorUnaryOperation::kPositive:
    if (!numeric(operand.type.kind()))
      return common::make_unexpected(invalid("unary positive requires a numeric operand"));
    return operand;
  case VectorUnaryOperation::kNegative:
    if (!(signed_integer(operand.type.kind()) || floating(operand.type.kind()) ||
          operand.type.kind() == schema::LogicalTypeKind::kDecimal)) {
      return common::make_unexpected(
          invalid("unary negative requires a signed, floating, or decimal operand"));
    }
    return operand;
  case VectorUnaryOperation::kNot:
    if (operand.type.kind() != schema::LogicalTypeKind::kBool)
      return common::make_unexpected(invalid("logical NOT requires a BOOL operand"));
    return operand;
  case VectorUnaryOperation::kIsNull:
  case VectorUnaryOperation::kIsNotNull:
    return VectorExpressionShape{
        .type = schema::LogicalType::create(schema::LogicalTypeKind::kBool).value(),
        .nullable = false};
  case VectorUnaryOperation::kAbsolute:
    if (!numeric(operand.type.kind()))
      return common::make_unexpected(invalid("ABS requires a numeric operand"));
    return operand;
  }
  return common::make_unexpected(invalid("vector unary operation is invalid"));
}

[[nodiscard]] bool comparison(const VectorBinaryOperation operation) noexcept {
  return operation >= VectorBinaryOperation::kEqual &&
         operation <= VectorBinaryOperation::kGreaterEqual;
}

[[nodiscard]] bool arithmetic(const VectorBinaryOperation operation) noexcept {
  return operation >= VectorBinaryOperation::kAdd && operation <= VectorBinaryOperation::kRemainder;
}

[[nodiscard]] common::Result<VectorExpressionShape>
binary_shape(const VectorBinaryExpression& operation, const VectorExpressionShape& left,
             const VectorExpressionShape& right) {
  const bool nullable = left.nullable || right.nullable;
  if (operation.operation == VectorBinaryOperation::kAnd ||
      operation.operation == VectorBinaryOperation::kOr) {
    if (left.type.kind() != schema::LogicalTypeKind::kBool ||
        right.type.kind() != schema::LogicalTypeKind::kBool) {
      return common::make_unexpected(invalid("logical binary operation requires BOOL operands"));
    }
    return VectorExpressionShape{.type = left.type, .nullable = nullable};
  }
  const std::optional<schema::LogicalType> common = common_type(left.type, right.type);
  if (!common.has_value())
    return common::make_unexpected(invalid("vector binary operands have no permitted common type"));
  if (comparison(operation.operation)) {
    return VectorExpressionShape{
        .type = schema::LogicalType::create(schema::LogicalTypeKind::kBool).value(),
        .nullable = nullable};
  }
  if (!arithmetic(operation.operation) || !numeric(common->kind()))
    return common::make_unexpected(invalid("vector binary operation is invalid for its operands"));
  return VectorExpressionShape{.type = *common, .nullable = nullable};
}

[[nodiscard]] std::optional<std::int64_t> checked_signed_add(const std::int64_t left,
                                                             const std::int64_t right) noexcept {
  if ((right > 0 && left > std::numeric_limits<std::int64_t>::max() - right) ||
      (right < 0 && left < std::numeric_limits<std::int64_t>::min() - right))
    return std::nullopt;
  return left + right;
}

[[nodiscard]] std::optional<std::int64_t>
checked_signed_subtract(const std::int64_t left, const std::int64_t right) noexcept {
  if ((right < 0 && left > std::numeric_limits<std::int64_t>::max() + right) ||
      (right > 0 && left < std::numeric_limits<std::int64_t>::min() + right))
    return std::nullopt;
  return left - right;
}

[[nodiscard]] std::optional<std::int64_t>
checked_signed_multiply(const std::int64_t left, const std::int64_t right) noexcept {
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

[[nodiscard]] SqlTruthValue truth(const ScalarValue& value) {
  if (value.is_null())
    return SqlTruthValue::kUnknown;
  const auto* boolean = std::get_if<bool>(&value.storage());
  return boolean != nullptr && *boolean ? SqlTruthValue::kTrue : SqlTruthValue::kFalse;
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

[[nodiscard]] ScalarValue boolean_value(const SqlTruthValue value) {
  const schema::LogicalType type =
      schema::LogicalType::create(schema::LogicalTypeKind::kBool).value();
  return value == SqlTruthValue::kUnknown
             ? ScalarValue::null(type)
             : ScalarValue::boolean(value == SqlTruthValue::kTrue).value();
}

[[nodiscard]] common::Result<ScalarValue> evaluate_unary(const VectorUnaryOperation operation,
                                                         const ScalarValue& operand,
                                                         const VectorExpressionShape& result) {
  if (operation == VectorUnaryOperation::kIsNull || operation == VectorUnaryOperation::kIsNotNull) {
    const bool is_null = operand.is_null();
    return ScalarValue::boolean(operation == VectorUnaryOperation::kIsNull ? is_null : !is_null);
  }
  if (operation == VectorUnaryOperation::kNot)
    return boolean_value(sql_not(truth(operand)));
  if (operand.is_null())
    return ScalarValue::null(result.type);
  if (operation == VectorUnaryOperation::kPositive)
    return operand;

  if (const auto* value = std::get_if<std::int64_t>(&operand.storage()); value != nullptr) {
    if (*value == std::numeric_limits<std::int64_t>::min())
      return common::make_unexpected(out_of_range("vector unary operation overflows"));
    const std::int64_t computed =
        operation == VectorUnaryOperation::kAbsolute && *value >= 0 ? *value : -*value;
    return ScalarValue::signed_value(result.type, computed);
  }
  if (const auto* value = std::get_if<std::uint64_t>(&operand.storage()); value != nullptr)
    return ScalarValue::unsigned_value(result.type, *value);
  if (const auto* value = std::get_if<float>(&operand.storage()); value != nullptr) {
    return ScalarValue::float32(operation == VectorUnaryOperation::kAbsolute ? std::fabs(*value)
                                                                             : -*value);
  }
  if (const auto* value = std::get_if<double>(&operand.storage()); value != nullptr) {
    return ScalarValue::float64(operation == VectorUnaryOperation::kAbsolute ? std::fabs(*value)
                                                                             : -*value);
  }
  if (const auto* value = std::get_if<Decimal128Value>(&operand.storage()); value != nullptr) {
    const common::Result<Decimal128Value> computed =
        operation == VectorUnaryOperation::kAbsolute ? detail::absolute_decimal(*value, result.type)
                                                     : detail::negate_decimal(*value, result.type);
    if (!computed.has_value())
      return common::make_unexpected(computed.error());
    return ScalarValue::decimal(result.type, *computed);
  }
  return common::make_unexpected(internal("vector unary operand storage is inconsistent"));
}

[[nodiscard]] common::Result<ScalarValue> evaluate_comparison(const VectorBinaryOperation operation,
                                                              const ScalarValue& left,
                                                              const ScalarValue& right) {
  if (left.is_null() || right.is_null())
    return boolean_value(SqlTruthValue::kUnknown);
  if (operation == VectorBinaryOperation::kEqual || operation == VectorBinaryOperation::kNotEqual) {
    common::Result<SqlTruthValue> equal = sql_scalar_equal(left, right);
    if (!equal.has_value())
      return common::make_unexpected(equal.error());
    return boolean_value(operation == VectorBinaryOperation::kNotEqual ? sql_not(*equal) : *equal);
  }
  const auto nan = [](const ScalarValue& value) {
    if (const auto* single = std::get_if<float>(&value.storage()); single != nullptr)
      return std::isnan(*single);
    if (const auto* wide = std::get_if<double>(&value.storage()); wide != nullptr)
      return std::isnan(*wide);
    return false;
  };
  if (nan(left) || nan(right))
    return ScalarValue::boolean(false);
  common::Result<int> order = compare_scalar_values(left, right, ScalarNullPlacement::kLast);
  if (!order.has_value())
    return common::make_unexpected(order.error());
  bool result = false;
  switch (operation) {
  case VectorBinaryOperation::kLess:
    result = *order < 0;
    break;
  case VectorBinaryOperation::kLessEqual:
    result = *order <= 0;
    break;
  case VectorBinaryOperation::kGreater:
    result = *order > 0;
    break;
  case VectorBinaryOperation::kGreaterEqual:
    result = *order >= 0;
    break;
  default:
    return common::make_unexpected(internal("vector comparison operation is inconsistent"));
  }
  return ScalarValue::boolean(result);
}

[[nodiscard]] detail::DecimalOperation decimal_operation(const VectorBinaryOperation operation) {
  switch (operation) {
  case VectorBinaryOperation::kAdd:
    return detail::DecimalOperation::kAdd;
  case VectorBinaryOperation::kSubtract:
    return detail::DecimalOperation::kSubtract;
  case VectorBinaryOperation::kMultiply:
    return detail::DecimalOperation::kMultiply;
  case VectorBinaryOperation::kDivide:
    return detail::DecimalOperation::kDivide;
  case VectorBinaryOperation::kRemainder:
    return detail::DecimalOperation::kRemainder;
  default:
    return detail::DecimalOperation::kAdd;
  }
}

[[nodiscard]] common::Result<ScalarValue> evaluate_arithmetic(const VectorBinaryOperation operation,
                                                              const ScalarValue& left,
                                                              const ScalarValue& right,
                                                              const VectorExpressionShape& result) {
  if (left.is_null() || right.is_null())
    return ScalarValue::null(result.type);
  if (const auto* lhs = std::get_if<std::int64_t>(&left.storage()); lhs != nullptr) {
    const auto* rhs = std::get_if<std::int64_t>(&right.storage());
    if (rhs == nullptr)
      return common::make_unexpected(internal("signed vector arithmetic storage mismatch"));
    std::optional<std::int64_t> value;
    switch (operation) {
    case VectorBinaryOperation::kAdd:
      value = checked_signed_add(*lhs, *rhs);
      break;
    case VectorBinaryOperation::kSubtract:
      value = checked_signed_subtract(*lhs, *rhs);
      break;
    case VectorBinaryOperation::kMultiply:
      value = checked_signed_multiply(*lhs, *rhs);
      break;
    case VectorBinaryOperation::kDivide:
      if (*rhs == 0)
        return common::make_unexpected(invalid("integer division by zero"));
      if (*lhs == std::numeric_limits<std::int64_t>::min() && *rhs == -1)
        return common::make_unexpected(out_of_range("integer division overflows"));
      value = *lhs / *rhs;
      break;
    case VectorBinaryOperation::kRemainder:
      if (*rhs == 0)
        return common::make_unexpected(invalid("integer remainder by zero"));
      value = *lhs == std::numeric_limits<std::int64_t>::min() && *rhs == -1 ? 0 : *lhs % *rhs;
      break;
    default:
      break;
    }
    if (!value.has_value())
      return common::make_unexpected(out_of_range("signed vector arithmetic overflows"));
    return ScalarValue::signed_value(result.type, *value);
  }
  if (const auto* lhs = std::get_if<std::uint64_t>(&left.storage()); lhs != nullptr) {
    const auto* rhs = std::get_if<std::uint64_t>(&right.storage());
    if (rhs == nullptr)
      return common::make_unexpected(internal("unsigned vector arithmetic storage mismatch"));
    std::uint64_t value = 0U;
    switch (operation) {
    case VectorBinaryOperation::kAdd:
      if (*rhs > std::numeric_limits<std::uint64_t>::max() - *lhs)
        return common::make_unexpected(out_of_range("unsigned vector addition overflows"));
      value = *lhs + *rhs;
      break;
    case VectorBinaryOperation::kSubtract:
      if (*lhs < *rhs)
        return common::make_unexpected(out_of_range("unsigned vector subtraction underflows"));
      value = *lhs - *rhs;
      break;
    case VectorBinaryOperation::kMultiply:
      if (*lhs != 0U && *rhs > std::numeric_limits<std::uint64_t>::max() / *lhs)
        return common::make_unexpected(out_of_range("unsigned vector multiplication overflows"));
      value = *lhs * *rhs;
      break;
    case VectorBinaryOperation::kDivide:
    case VectorBinaryOperation::kRemainder:
      if (*rhs == 0U)
        return common::make_unexpected(invalid("unsigned division or remainder by zero"));
      value = operation == VectorBinaryOperation::kDivide ? *lhs / *rhs : *lhs % *rhs;
      break;
    default:
      return common::make_unexpected(internal("unsigned vector operation is inconsistent"));
    }
    return ScalarValue::unsigned_value(result.type, value);
  }
  if (floating(result.type.kind())) {
    if (result.type.kind() == schema::LogicalTypeKind::kFloat32) {
      const auto* lhs = std::get_if<float>(&left.storage());
      const auto* rhs = std::get_if<float>(&right.storage());
      if (lhs == nullptr || rhs == nullptr)
        return common::make_unexpected(internal("FLOAT32 vector arithmetic storage mismatch"));
      float value = 0.0F;
      switch (operation) {
      case VectorBinaryOperation::kAdd:
        value = *lhs + *rhs;
        break;
      case VectorBinaryOperation::kSubtract:
        value = *lhs - *rhs;
        break;
      case VectorBinaryOperation::kMultiply:
        value = *lhs * *rhs;
        break;
      case VectorBinaryOperation::kDivide:
        value = *lhs / *rhs;
        break;
      case VectorBinaryOperation::kRemainder:
        value = std::fmod(*lhs, *rhs);
        break;
      default:
        return common::make_unexpected(internal("FLOAT32 vector operation is inconsistent"));
      }
      return ScalarValue::float32(value);
    }
    const auto as_double = [](const ScalarValue& value) {
      if (const auto* single = std::get_if<float>(&value.storage()); single != nullptr)
        return static_cast<double>(*single);
      return *std::get_if<double>(&value.storage());
    };
    const double lhs = as_double(left);
    const double rhs = as_double(right);
    double value = 0.0;
    switch (operation) {
    case VectorBinaryOperation::kAdd:
      value = lhs + rhs;
      break;
    case VectorBinaryOperation::kSubtract:
      value = lhs - rhs;
      break;
    case VectorBinaryOperation::kMultiply:
      value = lhs * rhs;
      break;
    case VectorBinaryOperation::kDivide:
      value = lhs / rhs;
      break;
    case VectorBinaryOperation::kRemainder:
      value = std::fmod(lhs, rhs);
      break;
    default:
      return common::make_unexpected(internal("FLOAT64 vector operation is inconsistent"));
    }
    return ScalarValue::float64(value);
  }
  const auto* lhs = std::get_if<Decimal128Value>(&left.storage());
  const auto* rhs = std::get_if<Decimal128Value>(&right.storage());
  if (lhs == nullptr || rhs == nullptr)
    return common::make_unexpected(internal("DECIMAL vector arithmetic storage mismatch"));
  common::Result<Decimal128Value> value =
      detail::evaluate_decimal(decimal_operation(operation), *lhs, *rhs, result.type);
  if (!value.has_value())
    return common::make_unexpected(value.error());
  return ScalarValue::decimal(result.type, *value);
}

class RowEvaluator {
public:
  RowEvaluator(const VectorExpression& expression, const VectorChunk& input,
               const std::uint32_t physical_row) noexcept
      : expression_(expression), input_(input), physical_row_(physical_row) {}

  [[nodiscard]] common::Result<ScalarValue> run() {
    return evaluate(expression_.instructions().size() - 1U, 1U);
  }

private:
  [[nodiscard]] common::Result<ScalarValue> remember(const std::size_t index,
                                                     common::Result<ScalarValue> value) {
    if (!value.has_value())
      return common::make_unexpected(value.error());
    values_[index] = *value;
    return *value;
  }

  [[nodiscard]] common::Result<ScalarValue> evaluate(const std::size_t index,
                                                     const std::size_t depth) {
    if (depth > expression_.maximum_depth() || index >= kMaximumVectorExpressionInstructions) {
      return common::make_unexpected(internal("vector expression evaluation depth is invalid"));
    }
    if (const auto* cached = std::get_if<ScalarValue>(&values_[index]); cached != nullptr)
      return *cached;

    const VectorExpressionInstruction& instruction = expression_.instructions()[index];
    if (const auto* source = std::get_if<VectorInputExpression>(&instruction); source != nullptr) {
      const columnar::PhysicalColumnView* column = input_.column(source->input_column_ordinal);
      if (column == nullptr)
        return common::make_unexpected(invalid("vector expression source ordinal is out of range"));
      common::Result<columnar::ColumnCellView> cell = column->cell(physical_row_);
      if (!cell.has_value())
        return common::make_unexpected(cell.error());
      return remember(index, ScalarValue::from_column_cell(source->type, *cell));
    }
    if (const auto* constant = std::get_if<VectorConstantExpression>(&instruction);
        constant != nullptr) {
      return remember(index, constant->value);
    }
    if (const auto* unary = std::get_if<VectorUnaryExpression>(&instruction); unary != nullptr) {
      common::Result<ScalarValue> operand = evaluate(unary->operand_instruction, depth + 1U);
      if (!operand.has_value())
        return common::make_unexpected(operand.error());
      return remember(index, evaluate_unary(unary->operation, *operand,
                                            expression_.instruction_shapes()[index]));
    }
    if (const auto* binary = std::get_if<VectorBinaryExpression>(&instruction); binary != nullptr) {
      common::Result<ScalarValue> left = evaluate(binary->left_instruction, depth + 1U);
      if (!left.has_value())
        return common::make_unexpected(left.error());
      if (binary->operation == VectorBinaryOperation::kAnd ||
          binary->operation == VectorBinaryOperation::kOr) {
        const SqlTruthValue left_truth = truth(*left);
        if ((binary->operation == VectorBinaryOperation::kAnd &&
             left_truth == SqlTruthValue::kFalse) ||
            (binary->operation == VectorBinaryOperation::kOr &&
             left_truth == SqlTruthValue::kTrue)) {
          return remember(index, boolean_value(left_truth));
        }
        common::Result<ScalarValue> right = evaluate(binary->right_instruction, depth + 1U);
        if (!right.has_value())
          return common::make_unexpected(right.error());
        return remember(index, boolean_value(binary->operation == VectorBinaryOperation::kAnd
                                                 ? sql_and(left_truth, truth(*right))
                                                 : sql_or(left_truth, truth(*right))));
      }
      common::Result<ScalarValue> right = evaluate(binary->right_instruction, depth + 1U);
      if (!right.has_value())
        return common::make_unexpected(right.error());
      return remember(index, comparison(binary->operation)
                                 ? evaluate_comparison(binary->operation, *left, *right)
                                 : evaluate_arithmetic(binary->operation, *left, *right,
                                                       expression_.instruction_shapes()[index]));
    }
    return common::make_unexpected(internal("vector expression instruction is invalid"));
  }

  const VectorExpression& expression_;
  const VectorChunk& input_;
  std::uint32_t physical_row_;
  std::array<std::variant<std::monostate, ScalarValue>, kMaximumVectorExpressionInstructions>
      values_{};
};

} // namespace

// The adjacent sizes describe distinct validated properties of one expression program.
// NOLINTBEGIN(bugprone-easily-swappable-parameters)
VectorExpression::VectorExpression(std::vector<VectorExpressionInstruction> instructions,
                                   std::vector<VectorExpressionShape> instruction_shapes,
                                   const std::size_t maximum_depth,
                                   const std::size_t retained_configuration_bytes) noexcept
    : instructions_(std::move(instructions)), instruction_shapes_(std::move(instruction_shapes)),
      maximum_depth_(maximum_depth), retained_configuration_bytes_(retained_configuration_bytes) {}
// NOLINTEND(bugprone-easily-swappable-parameters)

common::Result<VectorExpression>
VectorExpression::create(std::vector<VectorExpressionInstruction> instructions,
                         const VectorExpressionLimits limits) {
  if (limits.maximum_instructions == 0U ||
      limits.maximum_instructions > kMaximumVectorExpressionInstructions ||
      limits.maximum_retained_configuration_bytes == 0U) {
    return common::make_unexpected(invalid("vector expression limits are invalid"));
  }
  if (instructions.empty())
    return common::make_unexpected(invalid("vector expression must contain an instruction"));
  if (instructions.size() > limits.maximum_instructions ||
      instructions.capacity() > limits.maximum_instructions) {
    return common::make_unexpected(exhausted("vector expression instruction limit exceeded"));
  }

  try {
    std::vector<VectorExpressionShape> shapes;
    shapes.reserve(instructions.size());
    std::vector<std::size_t> depths;
    depths.reserve(instructions.size());
    std::size_t maximum_depth = 0U;
    for (std::size_t index = 0U; index < instructions.size(); ++index) {
      const VectorExpressionInstruction& instruction = instructions[index];
      common::Result<VectorExpressionShape> shape =
          common::make_unexpected(invalid("vector expression instruction is invalid"));
      std::size_t depth = 1U;
      if (const auto* source = std::get_if<VectorInputExpression>(&instruction);
          source != nullptr) {
        if (!supported_leaf(source->type.kind())) {
          return common::make_unexpected(
              invalid("vector expression source type is not supported by this program version"));
        }
        shape = VectorExpressionShape{.type = source->type, .nullable = source->nullable};
      } else if (const auto* constant = std::get_if<VectorConstantExpression>(&instruction);
                 constant != nullptr) {
        const common::Result<void> valid = validate_constant(constant->value);
        if (!valid.has_value())
          return common::make_unexpected(valid.error());
        shape = VectorExpressionShape{.type = *scalar_type(constant->value),
                                      .nullable = constant->value.is_null()};
      } else if (const auto* unary = std::get_if<VectorUnaryExpression>(&instruction);
                 unary != nullptr) {
        if (unary->operand_instruction >= index)
          return common::make_unexpected(invalid("vector unary operand must precede its use"));
        shape = unary_shape(*unary, shapes[unary->operand_instruction]);
        depth = depths[unary->operand_instruction] + 1U;
      } else if (const auto* binary = std::get_if<VectorBinaryExpression>(&instruction);
                 binary != nullptr) {
        if (binary->left_instruction >= index || binary->right_instruction >= index) {
          return common::make_unexpected(invalid("vector binary operands must precede their use"));
        }
        shape = binary_shape(*binary, shapes[binary->left_instruction],
                             shapes[binary->right_instruction]);
        depth = std::max(depths[binary->left_instruction], depths[binary->right_instruction]) + 1U;
      }
      if (!shape.has_value())
        return common::make_unexpected(shape.error());
      shapes.push_back(*shape);
      depths.push_back(depth);
      maximum_depth = std::max(maximum_depth, depth);
    }

    const std::optional<std::size_t> instruction_bytes =
        common::checked_multiply(instructions.capacity(), sizeof(VectorExpressionInstruction));
    const std::optional<std::size_t> shape_bytes =
        common::checked_multiply(shapes.capacity(), sizeof(VectorExpressionShape));
    const std::optional<std::size_t> retained =
        instruction_bytes.has_value() && shape_bytes.has_value()
            ? common::checked_add(*instruction_bytes, *shape_bytes)
            : std::nullopt;
    if (!retained.has_value())
      return common::make_unexpected(exhausted("vector expression configuration size overflowed"));
    if (*retained > limits.maximum_retained_configuration_bytes) {
      return common::make_unexpected(
          exhausted("vector expression retained configuration limit exceeded"));
    }
    return VectorExpression{std::move(instructions), std::move(shapes), maximum_depth, *retained};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("vector expression allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("vector expression exceeds container limits"));
  }
}

std::span<const VectorExpressionInstruction> VectorExpression::instructions() const noexcept {
  return instructions_;
}

std::span<const VectorExpressionShape> VectorExpression::instruction_shapes() const noexcept {
  return instruction_shapes_;
}

const VectorExpressionShape& VectorExpression::result_shape() const noexcept {
  return instruction_shapes_.back();
}

std::size_t VectorExpression::maximum_depth() const noexcept {
  return maximum_depth_;
}

std::size_t VectorExpression::retained_configuration_bytes() const noexcept {
  return retained_configuration_bytes_;
}

namespace detail {

common::Result<void> validate_vector_expression_input(const VectorExpression& expression,
                                                      const VectorChunk& input) {
  for (const VectorExpressionInstruction& instruction : expression.instructions()) {
    const auto* source = std::get_if<VectorInputExpression>(&instruction);
    if (source == nullptr)
      continue;
    const columnar::PhysicalColumnView* column = input.column(source->input_column_ordinal);
    if (column == nullptr)
      return common::make_unexpected(invalid("vector expression source ordinal is out of range"));
    if (column->type() != source->type || column->nullable() != source->nullable) {
      return common::make_unexpected(
          invalid("vector expression source physical shape does not match its program"));
    }
  }
  return {};
}

common::Result<ScalarValue> evaluate_vector_expression_row(const VectorExpression& expression,
                                                           const VectorChunk& input,
                                                           const std::uint32_t physical_row) {
  if (physical_row >= input.physical_row_count())
    return common::make_unexpected(invalid("vector expression physical row is out of range"));
  return RowEvaluator{expression, input, physical_row}.run();
}

} // namespace detail
} // namespace chronos::query
