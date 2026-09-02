#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_ROWS_FINALIZATION_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_ROWS_FINALIZATION_V2_HPP_

#include "chronos/cluster/distributed_vector_aggregate_finalization_v2.hpp"
#include "chronos/cluster/distributed_vector_query_execution_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/aggregate.hpp"
#include "chronos/query/distributed_vector_plan.hpp"
#include "chronos/query/distributed_vector_result_schema.hpp"
#include "chronos/query/vector_expression.hpp"

#include <cstddef>
#include <cstdint>

namespace chronos::query {
struct DistributedVectorAggregateCoordinatorProjection;
}

namespace chronos::cluster {

inline constexpr std::uint64_t kDefaultDistributedVectorAggregateRowsInputRowsV2 = 1'048'576U;
inline constexpr std::uint64_t kMaximumDistributedVectorAggregateRowsInputRowsV2 = 16'777'216U;
inline constexpr std::size_t kDefaultDistributedVectorAggregateRowsInputBytesV2 =
    std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kDefaultDistributedVectorAggregateRowsWorkingBytesV2 =
    std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorAggregateRowsBytesV2 =
    std::size_t{1024U} * 1024U * 1024U;

struct DistributedVectorAggregateRowsFinalizationLimitsV2 {
  std::uint64_t maximum_input_rows{kDefaultDistributedVectorAggregateRowsInputRowsV2};
  std::size_t maximum_input_messages{query::kMaximumDistributedCoordinatorMessages};
  std::size_t maximum_input_encoded_bytes{kDefaultDistributedVectorAggregateRowsInputBytesV2};
  std::size_t maximum_working_bytes{kDefaultDistributedVectorAggregateRowsWorkingBytesV2};
  std::size_t maximum_query_memory_bytes{
      query::kDefaultDistributedVectorAggregateCoordinatorMemoryBytesV2};
  std::size_t maximum_variable_extremum_bytes{query::kDefaultAggregateExtremumByteLimit};
  network::QueryResultLimits input_batch{};
  DistributedVectorAggregateFinalizationLimitsV2 output{};
};

// Transitional coordinator-side aggregation over a complete all-tablet row execution. The input
// row plan must be an identity projection with no order, visibility, or limit. Every row batch is
// independently schema-validated before its canonical cells enter the shared mergeable aggregate
// kernel. No result is published until every tablet stream is terminal and Native aggregate
// finalization succeeds.
[[nodiscard]] common::Result<DistributedVectorAggregateFinalizedResultV2>
finalize_distributed_vector_aggregate_rows_v2(
    DistributedVectorQueryExecutionResultV2&& input,
    const query::DistributedVectorPlanIntent& aggregate_plan,
    query::DistributedVectorResultSchema&& aggregate_result_schema,
    DistributedVectorAggregateRowsFinalizationLimitsV2 limits = {});

// Applies the checked Boolean predicate to each complete canonical input row before it enters any
// aggregate state. SQL FALSE and NULL discard the row. The expression is synchronously borrowed.
[[nodiscard]] common::Result<DistributedVectorAggregateFinalizedResultV2>
finalize_distributed_vector_aggregate_rows_with_predicate_v2(
    DistributedVectorQueryExecutionResultV2&& input,
    const query::DistributedVectorPlanIntent& aggregate_plan,
    query::DistributedVectorResultSchema&& aggregate_result_schema,
    const query::VectorExpression& predicate,
    DistributedVectorAggregateRowsFinalizationLimitsV2 limits = {});

[[nodiscard]] common::Result<DistributedVectorAggregateFinalizedResultV2>
finalize_distributed_vector_aggregate_rows_with_projection_v2(
    DistributedVectorQueryExecutionResultV2&& input,
    const query::DistributedVectorPlanIntent& aggregate_plan,
    query::DistributedVectorResultSchema&& aggregate_result_schema,
    const query::DistributedVectorAggregateCoordinatorProjection& projection,
    DistributedVectorAggregateRowsFinalizationLimitsV2 limits = {});

[[nodiscard]] common::Result<DistributedVectorAggregateFinalizedResultV2>
finalize_distributed_vector_aggregate_rows_with_predicate_and_projection_v2(
    DistributedVectorQueryExecutionResultV2&& input,
    const query::DistributedVectorPlanIntent& aggregate_plan,
    query::DistributedVectorResultSchema&& aggregate_result_schema,
    const query::VectorExpression& predicate,
    const query::DistributedVectorAggregateCoordinatorProjection& projection,
    DistributedVectorAggregateRowsFinalizationLimitsV2 limits = {});

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_ROWS_FINALIZATION_V2_HPP_
