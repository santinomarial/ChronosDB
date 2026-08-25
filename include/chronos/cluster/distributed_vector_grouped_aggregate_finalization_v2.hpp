#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_FINALIZATION_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_FINALIZATION_V2_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_query_execution_v2.hpp"
#include "chronos/cluster/distributed_vector_row_finalization_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/sort.hpp"

#include <cstddef>
#include <cstdint>

namespace chronos::cluster {

struct DistributedVectorGroupedAggregateFinalizationLimitsV2 {
  std::uint64_t maximum_output_rows{kDefaultDistributedVectorRowFinalizationRowsV2};
  std::size_t maximum_output_batches{query::kMaximumDistributedCoordinatorMessages};
  std::size_t maximum_output_encoded_bytes{kDefaultDistributedVectorRowFinalizationOutputBytesV2};
  query::SortLimits sort;
  network::QueryResultLimits output_batch;
};

// Consumes one globally closed grouped sufficient-state execution. Exact pinned plan authority and
// raw key/state result shapes are revalidated before the shared physical sort/limit operators run.
// No Native batch escapes until the complete pipeline succeeds; empty output retains one schema-
// bearing payload.
[[nodiscard]] common::Result<DistributedVectorRowsFinalizedResultV2>
finalize_distributed_vector_grouped_aggregate_v2(
    DistributedVectorGroupedAggregateQueryExecutionV2&& input,
    DistributedVectorGroupedAggregateFinalizationLimitsV2 limits = {});

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_FINALIZATION_V2_HPP_
