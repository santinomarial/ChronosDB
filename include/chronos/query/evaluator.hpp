#ifndef CHRONOS_QUERY_EVALUATOR_HPP_
#define CHRONOS_QUERY_EVALUATOR_HPP_

#include "chronos/query/ast.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/diagnostic.hpp"
#include "chronos/query/value.hpp"

#include <cstddef>
#include <span>

namespace chronos::query {

struct ScalarSourceRow {
  std::span<const ScalarValue> columns;
};

struct ScalarExpressionOverride {
  SourceSpan expression_span;
  const ScalarValue* value{};
};

// Every span and override pointer is borrowed for one synchronous call. sources use the exact
// source and schema-column ordinals recorded by BoundSqlSelect. projected_outputs makes SELECT
// aliases available only where the bound plan explicitly references them (ORDER BY).
struct ScalarEvaluationContext {
  std::span<const ScalarSourceRow> sources;
  std::span<const ScalarValue> projected_outputs;
  std::span<const ScalarExpressionOverride> overrides;
  std::size_t maximum_recursion{256U};
};

[[nodiscard]] SqlResult<ScalarValue>
evaluate_sql_v1_expression(const BoundSqlSelect& plan, const SqlExpression& expression,
                           const ScalarEvaluationContext& context = {});

[[nodiscard]] SqlResult<SqlTruthValue>
evaluate_sql_v1_predicate(const BoundSqlSelect& plan, const SqlExpression& expression,
                          const ScalarEvaluationContext& context = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_EVALUATOR_HPP_
