#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_FINALIZATION_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_FINALIZATION_V2_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/distributed_vector_aggregate_coordinator.hpp"
#include "chronos/query/distributed_vector_plan.hpp"
#include "chronos/query/distributed_vector_result_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kDefaultDistributedVectorAggregateFinalizationWorkingBytesV2 =
    std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kDefaultDistributedVectorAggregateFinalizationOutputBytesV2 =
    std::size_t{16U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorAggregateFinalizationBytesV2 =
    std::size_t{1024U} * 1024U * 1024U;

struct DistributedVectorAggregateFinalizationLimitsV2 {
  std::size_t maximum_working_bytes{kDefaultDistributedVectorAggregateFinalizationWorkingBytesV2};
  std::size_t maximum_output_encoded_bytes{
      kDefaultDistributedVectorAggregateFinalizationOutputBytesV2};
  network::QueryResultLimits output_batch;
};

struct DistributedVectorAggregateFinalizedResultV2 {
  query::DistributedVectorResultSchema result_schema;
  // One Native Protocol v1 QUERY_RESULT payload. LIMIT 0 produces a zero-row schema-bearing batch;
  // every other admitted limit produces the single global aggregate row.
  std::vector<std::byte> encoded_batch;
  std::uint64_t row_count{};
  std::size_t encoded_bytes{};
};

// Revalidates the exact ungrouped plan, definition authority, finalized scalar types, and output
// bounds before moving the schema into one canonical Native Protocol v1 result payload. ORDER BY
// cannot change a single ungrouped row; LIMIT is applied only at this global boundary.
[[nodiscard]] common::Result<DistributedVectorAggregateFinalizedResultV2>
finalize_distributed_vector_aggregate_v2(
    const query::DistributedVectorPlanIntent& plan,
    query::DistributedVectorAggregateQueryResultV2&& input,
    DistributedVectorAggregateFinalizationLimitsV2 limits = {});

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_FINALIZATION_V2_HPP_
