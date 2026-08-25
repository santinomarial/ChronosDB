#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_FINALIZATION_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_FINALIZATION_V2_HPP_

#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_query_execution_v2.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_execution.hpp"
#include "chronos/cluster/distributed_vector_row_finalization_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/sort.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>

namespace chronos::query {
struct DistributedVectorGroupedAggregateCoordinatorProjection;
}

namespace chronos::cluster {

// Owns the validated mutable-query logical identity that authorizes final SQL processing of one
// exact proof-derived shuffle. The shuffle authority is borrowed and outlives this value.
class DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2 {
public:
  DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2() = delete;
  DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2(
      const DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2&) = delete;
  DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2&
  operator=(const DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2&) = delete;
  DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2(
      DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2&&) noexcept = default;
  DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2&
  operator=(DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2&&) noexcept = default;

  [[nodiscard]] static common::Result<
      DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2>
  create(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
         std::span<const query::DistributedMutableVectorFragment> fragments);

  [[nodiscard]] const DistributedVectorGroupedAggregateShuffleAuthority&
  shuffle_authority() const noexcept;
  [[nodiscard]] const query::DistributedVectorPlanIntent& plan() const noexcept;
  [[nodiscard]] const query::DistributedVectorResultSchema& result_schema() const noexcept;
  [[nodiscard]] std::uint32_t input_column_count() const noexcept;

private:
  DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2(
      const DistributedVectorGroupedAggregateShuffleAuthority& authority,
      DistributedMutableVectorQueryLogicalIdentity identity,
      std::uint32_t input_column_count) noexcept;

  std::reference_wrapper<const DistributedVectorGroupedAggregateShuffleAuthority> authority_;
  DistributedMutableVectorQueryLogicalIdentity identity_;
  std::uint32_t input_column_count_{};
};

struct DistributedVectorGroupedAggregateFinalizationLimitsV2 {
  std::uint64_t maximum_output_rows{kDefaultDistributedVectorRowFinalizationRowsV2};
  std::size_t maximum_output_batches{query::kMaximumDistributedCoordinatorMessages};
  std::size_t maximum_output_encoded_bytes{kDefaultDistributedVectorRowFinalizationOutputBytesV2};
  query::SortLimits sort;
  network::QueryResultLimits output_batch;
};

[[nodiscard]] common::Status validate_distributed_vector_grouped_aggregate_finalization_limits_v2(
    const DistributedVectorGroupedAggregateFinalizationLimitsV2& limits) noexcept;

// Drains one globally closed grouped sufficient-state execution. Exact pinned plan authority and
// raw key/state result shapes are revalidated before the shared physical sort/limit operators run.
// No Native batch escapes until the complete pipeline succeeds; empty output retains one schema-
// bearing payload.
[[nodiscard]] common::Result<DistributedVectorRowsFinalizedResultV2>
finalize_distributed_vector_grouped_aggregate_v2(
    DistributedVectorGroupedAggregateQueryExecutionV2& input,
    DistributedVectorGroupedAggregateFinalizationLimitsV2 limits = {});

// Applies checked coordinator expressions to every globally merged raw group before projected
// ORDER BY/LIMIT and Native encoding. The projection is synchronously borrowed.
[[nodiscard]] common::Result<DistributedVectorRowsFinalizedResultV2>
finalize_distributed_vector_grouped_aggregate_with_projection_v2(
    DistributedVectorGroupedAggregateQueryExecutionV2& input,
    const query::DistributedVectorGroupedAggregateCoordinatorProjection& projection,
    DistributedVectorGroupedAggregateFinalizationLimitsV2 limits = {});

// Mutable grouped executions use exact TabletState publication authority rather than a Manifest
// snapshot, but drain through the same raw key/state shape, physical order/projection/limit stages,
// query-memory authority, and atomic Native encoding boundary.
[[nodiscard]] common::Result<DistributedVectorRowsFinalizedResultV2>
finalize_distributed_mutable_vector_grouped_aggregate_v2(
    DistributedMutableVectorGroupedAggregateQueryExecution& input,
    DistributedVectorGroupedAggregateFinalizationLimitsV2 limits = {});

[[nodiscard]] common::Result<DistributedVectorRowsFinalizedResultV2>
finalize_distributed_mutable_vector_grouped_aggregate_with_projection_v2(
    DistributedMutableVectorGroupedAggregateQueryExecution& input,
    const query::DistributedVectorGroupedAggregateCoordinatorProjection& projection,
    DistributedVectorGroupedAggregateFinalizationLimitsV2 limits = {});

[[nodiscard]] common::Result<DistributedVectorRowsFinalizedResultV2>
finalize_distributed_vector_grouped_aggregate_shuffle_v2(
    DistributedVectorGroupedAggregateShuffleResultExecution& input,
    const DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2& authority,
    DistributedVectorGroupedAggregateFinalizationLimitsV2 limits = {});

[[nodiscard]] common::Result<DistributedVectorRowsFinalizedResultV2>
finalize_distributed_vector_grouped_aggregate_shuffle_with_projection_v2(
    DistributedVectorGroupedAggregateShuffleResultExecution& input,
    const DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2& authority,
    const query::DistributedVectorGroupedAggregateCoordinatorProjection& projection,
    DistributedVectorGroupedAggregateFinalizationLimitsV2 limits = {});

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_FINALIZATION_V2_HPP_
