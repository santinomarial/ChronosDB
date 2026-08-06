#ifndef CHRONOS_QUERY_EXPLAIN_HPP_
#define CHRONOS_QUERY_EXPLAIN_HPP_

#include "chronos/query/binder.hpp"
#include "chronos/query/diagnostic.hpp"
#include "chronos/query/executor.hpp"
#include "chronos/query/snapshot.hpp"

#include <string>

namespace chronos::query {

inline constexpr unsigned kSqlV1ExplainFormatVersion = 1U;

struct ScalarExplainAnalyzeResult {
  std::string plan;
  ScalarQueryExecution execution;
};

// Produces the stable line-oriented SQL v1 logical/scalar-physical plan description. It performs
// no snapshot access and accepts SELECT, EXPLAIN, or EXPLAIN ANALYZE bound plans.
[[nodiscard]] SqlResult<std::string> explain_sql_v1_select(const BoundSqlSelect& plan);

// Requires an EXPLAIN ANALYZE plan, executes it once, and returns the same stable plan plus
// deterministic measured work counters and the underlying result used to preserve semantics.
[[nodiscard]] SqlResult<ScalarExplainAnalyzeResult>
execute_sql_v1_explain_analyze(const BoundSqlSelect& plan, const ScalarSnapshotProvider& provider,
                               ScalarQueryLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_EXPLAIN_HPP_
