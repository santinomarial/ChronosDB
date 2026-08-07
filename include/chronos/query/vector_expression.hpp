#ifndef CHRONOS_QUERY_VECTOR_EXPRESSION_HPP_
#define CHRONOS_QUERY_VECTOR_EXPRESSION_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/value.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <variant>
#include <vector>

namespace chronos::query {

inline constexpr std::size_t kMaximumVectorExpressionInstructions = 256U;
inline constexpr std::size_t kDefaultVectorExpressionConfigurationByteLimit =
    std::size_t{256U} * 1024U;

enum class VectorUnaryOperation : std::uint8_t {
  kPositive,
  kNegative,
  kNot,
  kIsNull,
  kIsNotNull,
  kAbsolute,
};

enum class VectorBinaryOperation : std::uint8_t {
  kAnd,
  kOr,
  kEqual,
  kNotEqual,
  kLess,
  kLessEqual,
  kGreater,
  kGreaterEqual,
  kAdd,
  kSubtract,
  kMultiply,
  kDivide,
  kRemainder,
};

struct VectorInputExpression {
  std::size_t input_column_ordinal;
  schema::LogicalType type;
  bool nullable;
};

struct VectorConstantExpression {
  ScalarValue value;
};

struct VectorUnaryExpression {
  VectorUnaryOperation operation;
  std::size_t operand_instruction;
};

struct VectorBinaryExpression {
  VectorBinaryOperation operation;
  std::size_t left_instruction;
  std::size_t right_instruction;
};

using VectorExpressionInstruction = std::variant<VectorInputExpression, VectorConstantExpression,
                                                 VectorUnaryExpression, VectorBinaryExpression>;

struct VectorExpressionShape {
  schema::LogicalType type;
  bool nullable;

  friend constexpr bool operator==(const VectorExpressionShape&,
                                   const VectorExpressionShape&) = default;
};

struct VectorExpressionLimits {
  std::size_t maximum_instructions{kMaximumVectorExpressionInstructions};
  std::size_t maximum_retained_configuration_bytes{kDefaultVectorExpressionConfigurationByteLimit};
};

// An immutable, copyable postorder DAG for checked numeric and Boolean physical expressions.
// Successful per-row evaluation uses no heap allocation; diagnostic construction on a failing row
// may allocate. Every operand references an earlier instruction and the final instruction is the
// result. Source leaves retain exact expected physical shapes.
class VectorExpression {
public:
  VectorExpression() = delete;
  VectorExpression(const VectorExpression&) = default;
  VectorExpression& operator=(const VectorExpression&) = default;
  VectorExpression(VectorExpression&&) noexcept = default;
  VectorExpression& operator=(VectorExpression&&) noexcept = default;

  [[nodiscard]] static common::Result<VectorExpression>
  create(std::vector<VectorExpressionInstruction> instructions, VectorExpressionLimits limits = {});

  [[nodiscard]] std::span<const VectorExpressionInstruction> instructions() const noexcept;
  [[nodiscard]] std::span<const VectorExpressionShape> instruction_shapes() const noexcept;
  [[nodiscard]] const VectorExpressionShape& result_shape() const noexcept;
  [[nodiscard]] std::size_t maximum_depth() const noexcept;
  [[nodiscard]] std::size_t retained_configuration_bytes() const noexcept;

private:
  VectorExpression(std::vector<VectorExpressionInstruction> instructions,
                   std::vector<VectorExpressionShape> instruction_shapes, std::size_t maximum_depth,
                   std::size_t retained_configuration_bytes) noexcept;

  std::vector<VectorExpressionInstruction> instructions_;
  std::vector<VectorExpressionShape> instruction_shapes_;
  std::size_t maximum_depth_;
  std::size_t retained_configuration_bytes_;
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_VECTOR_EXPRESSION_HPP_
