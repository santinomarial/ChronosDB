#ifndef CHRONOS_QUERY_DISTRIBUTED_SQL_LOWERING_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_SQL_LOWERING_HPP_

#include "chronos/cseg/pruning.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/distributed_vector_plan.hpp"
#include "chronos/query/distributed_vector_result_schema.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace chronos::query {

struct DistributedVectorRowsSqlLoweringLimits {
  std::uint32_t maximum_projection_columns{distributed_vector_plan_format::kMaximumInputColumns};
  std::uint32_t maximum_output_columns{distributed_vector_plan_format::kMaximumOutputColumns};
  std::uint32_t maximum_order_keys{distributed_vector_plan_format::kMaximumOrderKeys};
  std::uint32_t maximum_result_name_bytes{
      distributed_vector_result_schema_format::kMaximumNameLength};
};

// Complete schema-bound row intent for later authority binding. Source projection ordinals are
// unique and preserve first SELECT-output use; row output indices may repeat. ORDER BY and LIMIT
// remain global coordinator semantics. The optional event-time predicate is exact row truth, not
// merely storage-pruning evidence.
struct DistributedVectorRowsSqlPlan {
  schema::TableId table_id;
  schema::SchemaId destination_schema_id;
  std::vector<std::uint32_t> destination_column_ordinals;
  std::optional<cseg::EventTimePredicate> event_time_predicate;
  DistributedVectorPlanIntent intent;
  DistributedVectorResultSchema result_schema;

  friend bool operator==(const DistributedVectorRowsSqlPlan&,
                         const DistributedVectorRowsSqlPlan&) = default;
};

// Lowers the executable distributed row subset: one current table, direct source-column outputs,
// an optional AND-conjunction of event-time/TIMESTAMP comparisons, output-backed ORDER BY, and
// LIMIT. Unsupported local-only SQL fails with its source span; no scalar or relational fallback
// is inferred.
[[nodiscard]] SqlResult<DistributedVectorRowsSqlPlan>
lower_bound_sql_select_to_distributed_vector_rows(
    const BoundSqlSelect& select, DistributedVectorRowsSqlLoweringLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_SQL_LOWERING_HPP_
