#ifndef CHRONOS_QUERY_DISTRIBUTED_SQL_LOWERING_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_SQL_LOWERING_HPP_

#include "chronos/cseg/pruning.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/distributed_vector_plan.hpp"
#include "chronos/query/distributed_vector_result_schema.hpp"
#include "chronos/query/vector_expression.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

namespace chronos::query {

struct DistributedVectorRowsSqlLoweringLimits {
  std::uint32_t maximum_projection_columns{distributed_vector_plan_format::kMaximumInputColumns};
  std::uint32_t maximum_output_columns{distributed_vector_plan_format::kMaximumOutputColumns};
  std::uint32_t maximum_order_keys{distributed_vector_plan_format::kMaximumOrderKeys};
  std::uint32_t maximum_result_name_bytes{
      distributed_vector_result_schema_format::kMaximumNameLength};
  std::size_t maximum_constant_bytes{std::size_t{1024U} * 1024U};
  VectorExpressionLimits expression_limits{};
  std::size_t maximum_expression_configuration_bytes{std::size_t{4U} * 1024U * 1024U};
};

struct DistributedVectorAggregateSqlLoweringLimits {
  std::uint32_t maximum_projection_columns{distributed_vector_plan_format::kMaximumInputColumns};
  std::uint32_t maximum_aggregates{distributed_vector_plan_format::kMaximumAggregates};
  std::uint32_t maximum_result_name_bytes{
      distributed_vector_result_schema_format::kMaximumNameLength};
  VectorExpressionLimits expression_limits{};
  std::size_t maximum_expression_configuration_bytes{std::size_t{4U} * 1024U * 1024U};
};

struct DistributedVectorRowSourceOutput {
  std::uint32_t worker_output_index{};

  friend bool operator==(const DistributedVectorRowSourceOutput&,
                         const DistributedVectorRowSourceOutput&) = default;
};

struct DistributedVectorRowConstantOutput {
  bool is_null{};
  std::vector<std::byte> canonical_value;

  friend bool operator==(const DistributedVectorRowConstantOutput&,
                         const DistributedVectorRowConstantOutput&) = default;
};

struct DistributedVectorRowExpressionOutput {
  VectorExpression expression;

  friend bool operator==(const DistributedVectorRowExpressionOutput&,
                         const DistributedVectorRowExpressionOutput&) = default;
};

struct DistributedVectorRowCoordinatorOrderKey {
  VectorExpression expression;
  PhysicalSortDirection direction{PhysicalSortDirection::kAscending};
  ScalarNullPlacement null_placement{ScalarNullPlacement::kLast};

  friend bool operator==(const DistributedVectorRowCoordinatorOrderKey&,
                         const DistributedVectorRowCoordinatorOrderKey&) = default;
};

using DistributedVectorRowCoordinatorOutput =
    std::variant<DistributedVectorRowSourceOutput, DistributedVectorRowConstantOutput,
                 DistributedVectorRowExpressionOutput>;

struct DistributedVectorRowCoordinatorProjection {
  std::vector<DistributedVectorRowCoordinatorOutput> outputs;
  DistributedVectorResultSchema result_schema;
  std::optional<VectorExpression> predicate;
  std::vector<DistributedVectorRowCoordinatorOrderKey> order_keys;

  friend bool operator==(const DistributedVectorRowCoordinatorProjection&,
                         const DistributedVectorRowCoordinatorProjection&) = default;
};

// Complete schema-bound row intent for later authority binding. Source projection ordinals are
// unique; direct-only plans preserve first use, while row-expression plans preserve complete schema
// order. Row output indices may repeat and may append hidden direct order columns. A nonempty plan
// visibility vector retains the SELECT outputs. ORDER BY and LIMIT remain global coordinator
// semantics. A coordinator projection is present only when source-independent
// expressions must be evaluated or injected. A coordinator predicate is applied after every worker
// stream closes and before global ordering and limit; visible expressions are evaluated afterward.
// Row-dependent programs use source-schema ordinals and therefore cause workers to carry the full
// source row. If any ORDER BY key is computed, every key is evaluated by the coordinator over that
// same full source row before LIMIT. The optional event-time predicate is exact row truth, not
// merely storage-pruning evidence.
struct DistributedVectorRowsSqlPlan {
  schema::TableId table_id;
  schema::SchemaId destination_schema_id;
  std::vector<std::uint32_t> destination_column_ordinals;
  std::optional<cseg::EventTimePredicate> event_time_predicate;
  DistributedVectorPlanIntent intent;
  DistributedVectorResultSchema result_schema;
  std::optional<DistributedVectorRowCoordinatorProjection> coordinator_projection;

  friend bool operator==(const DistributedVectorRowsSqlPlan&,
                         const DistributedVectorRowsSqlPlan&) = default;
};

// Lowers the executable distributed row subset: one current table, direct source-column,
// source-independent scalar, or checked vector-expression outputs; an optional checked Boolean
// WHERE expression (with exact worker-side event-time range specialization), direct or checked
// computed ORDER BY (including hidden helpers), and LIMIT. No scalar or relational fallback is
// inferred.
[[nodiscard]] SqlResult<DistributedVectorRowsSqlPlan>
lower_bound_sql_select_to_distributed_vector_rows(
    const BoundSqlSelect& select, DistributedVectorRowsSqlLoweringLimits limits = {});

// Complete schema-bound ungrouped-aggregate intent for later authority binding. input_rows is the
// exact unlimited identity projection needed by the current replicated mutable-row carrier;
// projection ordinals are unique and preserve first aggregate-input use unless a source-dependent
// coordinator predicate requires the complete source schema in ordinal order. COUNT(*) uses the
// event-time column as a bounded fragment projection anchor when no aggregate or predicate reads a
// source column. The aggregate intent alone carries global LIMIT. A sufficient-state worker may
// consume the same projection without using the transitional row intent.
struct DistributedVectorAggregateSqlPlan {
  DistributedVectorRowsSqlPlan input_rows;
  DistributedVectorPlanIntent intent;
  DistributedVectorResultSchema result_schema;
  std::optional<VectorExpression> coordinator_predicate;

  friend bool operator==(const DistributedVectorAggregateSqlPlan&,
                         const DistributedVectorAggregateSqlPlan&) = default;
};

// Lowers the executable distributed global-aggregate subset: one current table, SELECT outputs
// that are exactly COUNT(*), COUNT, SUM, AVG, MIN, MAX, VAR_POP, or VAR_SAMP over direct source
// columns, an optional checked Boolean WHERE expression (with exact worker-side event-time range
// specialization), and LIMIT. GROUP BY, ORDER BY, computed aggregate inputs/final outputs,
// historical reads, and relational operators fail closed.
[[nodiscard]] SqlResult<DistributedVectorAggregateSqlPlan>
lower_bound_sql_select_to_distributed_vector_aggregate(
    const BoundSqlSelect& select, DistributedVectorAggregateSqlLoweringLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_SQL_LOWERING_HPP_
