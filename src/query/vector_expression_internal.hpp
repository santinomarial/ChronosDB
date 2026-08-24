#ifndef CHRONOS_QUERY_VECTOR_EXPRESSION_INTERNAL_HPP_
#define CHRONOS_QUERY_VECTOR_EXPRESSION_INTERNAL_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/value.hpp"
#include "chronos/query/vector_chunk.hpp"
#include "chronos/query/vector_expression.hpp"

#include <cstdint>
#include <span>

namespace chronos::query::detail {

enum class VariableByteTransform : std::uint8_t { kIdentity, kLowerAscii, kUpperAscii };

[[nodiscard]] constexpr std::byte
transform_variable_byte(const std::byte input, const VariableByteTransform transform) noexcept {
  unsigned char value = std::to_integer<unsigned char>(input);
  if (transform == VariableByteTransform::kLowerAscii && value >= 'A' && value <= 'Z')
    value = static_cast<unsigned char>(value - 'A' + 'a');
  else if (transform == VariableByteTransform::kUpperAscii && value >= 'a' && value <= 'z')
    value = static_cast<unsigned char>(value - 'a' + 'A');
  return static_cast<std::byte>(value);
}

struct BorrowedVariableExpressionValue {
  bool is_null;
  common::ByteView bytes;
  VariableByteTransform transform;
};

// One independently framed canonical input cell. The caller owns the payload and must keep it
// immutable for the synchronous evaluation call. Logical type and declared nullability are carried
// by the corresponding VectorInputExpression instruction.
struct CanonicalVectorExpressionCell {
  bool is_null;
  common::ByteView bytes;
};

[[nodiscard]] common::Result<void>
validate_vector_expression_input(const VectorExpression& expression, const VectorChunk& input);

// Evaluates one physical row with fixed stack storage. The expression program and input chunk must
// outlive the synchronous call; the returned numeric/Boolean ScalarValue owns no variable payload.
[[nodiscard]] common::Result<ScalarValue>
evaluate_vector_expression_row(const VectorExpression& expression, const VectorChunk& input,
                               std::uint32_t physical_row);

// Evaluates one variable-width row without copying its payload. The returned bytes borrow either
// the input chunk or the expression program, both of which must outlive the synchronous use.
[[nodiscard]] common::Result<BorrowedVariableExpressionValue>
evaluate_variable_vector_expression_row(const VectorExpression& expression,
                                        const VectorChunk& input, std::uint32_t physical_row);

// Evaluates over one row of already framed canonical cells, such as decoded Native result cells.
// The fixed-width result owns its scalar storage. Variable bytes borrow the input row or expression
// program and retain a case transform for allocation-free two-pass materialization.
[[nodiscard]] common::Result<ScalarValue>
evaluate_canonical_vector_expression_row(const VectorExpression& expression,
                                         std::span<const CanonicalVectorExpressionCell> input);
[[nodiscard]] common::Result<BorrowedVariableExpressionValue>
evaluate_variable_canonical_vector_expression_row(
    const VectorExpression& expression, std::span<const CanonicalVectorExpressionCell> input);

} // namespace chronos::query::detail

#endif // CHRONOS_QUERY_VECTOR_EXPRESSION_INTERNAL_HPP_
