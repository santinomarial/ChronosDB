#ifndef CHRONOS_QUERY_PHYSICAL_LOWERING_HPP_
#define CHRONOS_QUERY_PHYSICAL_LOWERING_HPP_

#include "chronos/query/binder.hpp"
#include "chronos/query/diagnostic.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/query/vector_chunk.hpp"
#include "chronos/query/vector_expression.hpp"

namespace chronos::query {

struct PhysicalSelectLoweringLimits {
  VectorExpressionLimits expression_limits{};
  UngroupedAggregateLimits aggregate_limits{};
  GroupedAggregateLimits grouped_aggregate_limits{};
  VectorChunkLimits output_limits{};
  PhysicalPipelinePlanLimits plan_limits{};
};

// Lowers the executable single-source SQL v1 subset, including global and grouped aggregation,
// into one immutable physical pipeline. The input shape is the primary source's exact
// schema-ordinal physical shape. Unsupported relational or scalar features fail with a source-span
// SQL diagnostic; there is no scalar fallback.
[[nodiscard]] SqlResult<PhysicalPipelinePlan>
lower_bound_sql_select(const BoundSqlSelect& select, PhysicalSelectLoweringLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_PHYSICAL_LOWERING_HPP_
