#ifndef CHRONOS_QUERY_PHYSICAL_LOWERING_HPP_
#define CHRONOS_QUERY_PHYSICAL_LOWERING_HPP_

#include "chronos/query/binder.hpp"
#include "chronos/query/diagnostic.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/query/relational_plan.hpp"
#include "chronos/query/vector_chunk.hpp"
#include "chronos/query/vector_expression.hpp"

#include <cstdint>
#include <span>

namespace chronos::query {

struct PhysicalSelectLoweringLimits {
  VectorExpressionLimits expression_limits{};
  UngroupedAggregateLimits aggregate_limits{};
  GroupedAggregateLimits grouped_aggregate_limits{};
  LatestByLimits latest_by_limits{};
  SortLimits sort_limits{};
  VectorChunkLimits output_limits{};
  PhysicalPipelinePlanLimits plan_limits{};
  PhysicalAsofPlanLimits asof_plan_limits{};
  AsofJoinLimits asof_join_limits{};
};

// Lowers one bound scalar expression against the primary source's exact schema-ordinal input into
// the shared checked vector program. Direct source and literal expressions become one-instruction
// programs so coordinator consumers can retain one uniform immutable execution contract.
[[nodiscard]] SqlResult<VectorExpression>
lower_bound_sql_scalar_expression(const BoundSqlSelect& select, const SqlExpression& expression,
                                  VectorExpressionLimits limits = {});

struct VectorAggregateExpressionBinding {
  SourceSpan expression_span;
  std::uint32_t input_column_ordinal{};
  schema::LogicalType type;
  bool nullable{};
};

// Lowers one post-aggregate expression against exact finalized aggregate columns. Only aggregate
// expressions named by bindings may become inputs; base-table columns fail closed.
[[nodiscard]] SqlResult<VectorExpression> lower_bound_sql_aggregate_scalar_expression(
    const BoundSqlSelect& select, const SqlExpression& expression,
    std::span<const VectorAggregateExpressionBinding> bindings, VectorExpressionLimits limits = {});

// Lowers the executable single-source SELECT or SUBSCRIBE SELECT SQL v1 subset, including global
// and grouped aggregation, into one immutable physical pipeline. Unordered input is the primary
// source's exact schema-ordinal physical shape. Base-row ORDER BY additionally requires the shared
// row-version suffix after those source columns; aggregate ORDER BY does not. LATEST BY also
// requires that suffix, evaluates before WHERE, and uses the schema physical ordering key plus row
// version for exact winner ties. Unsupported relational or scalar features fail with a source-span
// SQL diagnostic; there is no scalar fallback.
[[nodiscard]] SqlResult<PhysicalPipelinePlan>
lower_bound_sql_select(const BoundSqlSelect& select, PhysicalSelectLoweringLimits limits = {});

// Lowers the bound SQL v1 ASOF surface into a checked left-deep physical plan. Every source uses
// the shared row-version suffix. Join-key and timestamp expressions are prepared on the exact side
// where they are bound; WHERE, aggregation, final output, ORDER BY, and LIMIT follow all joins.
[[nodiscard]] SqlResult<PhysicalAsofPlan>
lower_bound_sql_asof_select(const BoundSqlSelect& select, PhysicalSelectLoweringLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_PHYSICAL_LOWERING_HPP_
