#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_ROW_FINALIZATION_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_ROW_FINALIZATION_V2_HPP_

#include "chronos/cluster/distributed_vector_query_execution_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/distributed_vector_result_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::query {
struct DistributedVectorRowCoordinatorProjection;
}

namespace chronos::cluster {

inline constexpr std::uint64_t kDefaultDistributedVectorRowFinalizationRowsV2 = 1'048'576U;
inline constexpr std::uint64_t kMaximumDistributedVectorRowFinalizationRowsV2 = 16'777'216U;
inline constexpr std::size_t kDefaultDistributedVectorRowFinalizationInputBytesV2 =
    std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kDefaultDistributedVectorRowFinalizationWorkingBytesV2 =
    std::size_t{512U} * 1024U * 1024U;
inline constexpr std::size_t kDefaultDistributedVectorRowFinalizationOutputBytesV2 =
    std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorRowFinalizationBytesV2 =
    std::size_t{1024U} * 1024U * 1024U;

struct DistributedVectorRowFinalizationLimitsV2 {
  std::uint64_t maximum_input_rows{kDefaultDistributedVectorRowFinalizationRowsV2};
  std::size_t maximum_input_messages{query::kMaximumDistributedCoordinatorMessages};
  std::size_t maximum_input_encoded_bytes{kDefaultDistributedVectorRowFinalizationInputBytesV2};
  std::size_t maximum_working_bytes{kDefaultDistributedVectorRowFinalizationWorkingBytesV2};
  std::size_t maximum_output_batches{query::kMaximumDistributedCoordinatorMessages};
  std::size_t maximum_output_encoded_bytes{kDefaultDistributedVectorRowFinalizationOutputBytesV2};
  network::QueryResultLimits output_batch{};
};

struct DistributedVectorRowsFinalizedResultV2 {
  query::DistributedVectorResultSchema result_schema;
  // Native Protocol v1 QUERY_RESULT payloads. A zero-row result owns exactly one schema-bearing
  // payload. Nonempty payloads preserve final row order across batch boundaries.
  std::vector<std::vector<std::byte>> encoded_batches;
  std::uint64_t row_count{};
  std::size_t encoded_bytes{};
};

// Consumes one all-tablet schema-bound execution result. It independently validates the row-mode
// plan, message-stream closure, native batch schemas, and configured bounds before allocating
// decoded row state. ORDER BY is stable over plan-tablet/message/row input order, NULL placement is
// independent of direction, LIMIT is applied only after the global order is complete, and a
// nonempty row-visibility vector removes helper outputs only while encoding the final batches.
[[nodiscard]] common::Result<DistributedVectorRowsFinalizedResultV2>
finalize_distributed_vector_rows_v2(DistributedVectorQueryExecutionResultV2&& input,
                                    DistributedVectorRowFinalizationLimitsV2 limits = {});

// Applies a checked coordinator-only predicate, computed global order, LIMIT, and
// source/constant/expression projection in SQL order. Worker streams and their wire schema remain
// unchanged; borrowed programs and canonical cells remain valid only for this synchronous call.
[[nodiscard]] common::Result<DistributedVectorRowsFinalizedResultV2>
finalize_distributed_vector_rows_with_projection_v2(
    DistributedVectorQueryExecutionResultV2&& input,
    const query::DistributedVectorRowCoordinatorProjection& projection,
    DistributedVectorRowFinalizationLimitsV2 limits = {});

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_ROW_FINALIZATION_V2_HPP_
