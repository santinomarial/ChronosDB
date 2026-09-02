#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_PHYSICAL_ROWS_FINALIZATION_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_PHYSICAL_ROWS_FINALIZATION_V2_HPP_

#include "chronos/cluster/distributed_vector_query_execution_v2.hpp"
#include "chronos/cluster/distributed_vector_row_finalization_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/distributed_vector_result_schema.hpp"
#include "chronos/query/physical_plan.hpp"

#include <cstddef>
#include <cstdint>

namespace chronos::cluster {

struct DistributedVectorPhysicalRowsFinalizationLimitsV2 {
  std::uint64_t maximum_input_rows{kDefaultDistributedVectorRowFinalizationRowsV2};
  std::size_t maximum_input_messages{query::kMaximumDistributedCoordinatorMessages};
  std::size_t maximum_input_encoded_bytes{kDefaultDistributedVectorRowFinalizationInputBytesV2};
  std::size_t maximum_batch_working_bytes{query::kDefaultVectorChunkMemoryLimit};
  std::size_t maximum_query_memory_bytes{std::size_t{64U} * 1024U * 1024U};
  std::uint64_t maximum_output_rows{kDefaultDistributedVectorRowFinalizationRowsV2};
  std::size_t maximum_output_batches{query::kMaximumDistributedCoordinatorMessages};
  std::size_t maximum_output_encoded_bytes{kDefaultDistributedVectorRowFinalizationOutputBytesV2};
  network::QueryResultLimits input_batch{};
  network::QueryResultLimits output_batch{};
};

// Consumes one complete all-tablet identity row stream and runs the borrowed immutable physical
// pipeline synchronously at the coordinator. Input Native batches are revalidated and converted to
// query-accounted canonical chunks one at a time. No output escapes until the entire pipeline has
// completed successfully; empty output retains one schema-bearing Native payload.
[[nodiscard]] common::Result<DistributedVectorRowsFinalizedResultV2>
finalize_distributed_vector_physical_rows_v2(
    DistributedVectorQueryExecutionResultV2&& input,
    const query::PhysicalPipelinePlan& coordinator_pipeline,
    query::DistributedVectorResultSchema&& result_schema,
    DistributedVectorPhysicalRowsFinalizationLimitsV2 limits = {});

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_PHYSICAL_ROWS_FINALIZATION_V2_HPP_
