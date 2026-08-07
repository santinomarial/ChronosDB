#ifndef CHRONOS_QUERY_VECTOR_EXPRESSION_INTERNAL_HPP_
#define CHRONOS_QUERY_VECTOR_EXPRESSION_INTERNAL_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/value.hpp"
#include "chronos/query/vector_chunk.hpp"
#include "chronos/query/vector_expression.hpp"

#include <cstdint>

namespace chronos::query::detail {

[[nodiscard]] common::Result<void>
validate_vector_expression_input(const VectorExpression& expression, const VectorChunk& input);

// Evaluates one physical row with fixed stack storage. The expression program and input chunk must
// outlive the synchronous call; the returned numeric/Boolean ScalarValue owns no variable payload.
[[nodiscard]] common::Result<ScalarValue>
evaluate_vector_expression_row(const VectorExpression& expression, const VectorChunk& input,
                               std::uint32_t physical_row);

} // namespace chronos::query::detail

#endif // CHRONOS_QUERY_VECTOR_EXPRESSION_INTERNAL_HPP_
