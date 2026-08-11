#ifndef CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_WORKER_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_WORKER_HPP_

#include "chronos/common/result.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/manifest/temporal_part_validation.hpp"
#include "chronos/manifest/temporal_publication.hpp"
#include "chronos/query/distributed.hpp"
#include "chronos/query/distributed_fragment_dispatch.hpp"
#include "chronos/query/temporal_cseg_snapshot.hpp"
#include "chronos/raft/metadata.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <cstdint>
#include <functional>
#include <optional>

namespace chronos::query {

struct DistributedAggregateWorkerLimits {
  manifest::TemporalPartValidationLimits part_validation;
  TemporalManifestCsegResolutionLimits resolution;
};

struct DistributedAggregateWorkerRequest {
  std::reference_wrapper<const DistributedAggregateFragmentDispatch> dispatch;
  std::reference_wrapper<const manifest::ManifestStorage> storage;
  std::reference_wrapper<const manifest::TemporalDatabaseStorageSnapshot> snapshot;
  std::reference_wrapper<const schema::SchemaLineage> lineage;
  std::reference_wrapper<const raft::TabletPlacementMetadata> placement;
  common::Uuid raft_group_id;
  std::uint64_t local_node{};
  std::optional<raft::ReadBarrier> local_linearizable_barrier;
  DistributedAggregateWorkerLimits limits;
};

// Reproves local group/node/placement and exact Manifest v2 authority before any part I/O, loads
// generation-pinned validated temporal CSEGs, resolves current visible winners, applies the pushed
// event-time predicate, and emits one terminal mergeable Float64 aggregate message.
[[nodiscard]] common::Result<ExchangeMessage>
execute_distributed_aggregate_fragment(const DistributedAggregateWorkerRequest& request);

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_WORKER_HPP_
