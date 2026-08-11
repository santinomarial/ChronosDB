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
#include <span>

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

// Synchronous storage seam for proof-gated worker execution. A successful load calls consume()
// exactly once while all views are valid; failures call it zero times. Implementations must return
// only fully validated images for the exact supplied snapshot and identities.
class DistributedTemporalPartBatchConsumer {
public:
  DistributedTemporalPartBatchConsumer() = default;
  DistributedTemporalPartBatchConsumer(const DistributedTemporalPartBatchConsumer&) = delete;
  DistributedTemporalPartBatchConsumer&
  operator=(const DistributedTemporalPartBatchConsumer&) = delete;
  virtual ~DistributedTemporalPartBatchConsumer() = default;

  [[nodiscard]] virtual common::Status
  consume(std::span<const TemporalManifestCsegPartView> parts) = 0;
};

class DistributedTemporalPartBatchLoader {
public:
  DistributedTemporalPartBatchLoader() = default;
  DistributedTemporalPartBatchLoader(const DistributedTemporalPartBatchLoader&) = delete;
  DistributedTemporalPartBatchLoader& operator=(const DistributedTemporalPartBatchLoader&) = delete;
  virtual ~DistributedTemporalPartBatchLoader() = default;

  [[nodiscard]] virtual common::Status
  load(const manifest::TemporalDatabaseStorageSnapshot& snapshot,
       std::span<const cseg::PartId> part_ids,
       std::span<const manifest::TabletSchemaBinding> schema_bindings,
       manifest::TemporalPartValidationLimits validation_limits,
       DistributedTemporalPartBatchConsumer& consumer) const = 0;
};

// Reproves local group/node/placement and exact Manifest v2 authority before any part I/O, loads
// generation-pinned validated temporal CSEGs, resolves current visible winners, applies the pushed
// event-time predicate, and emits one terminal mergeable Float64 aggregate message.
[[nodiscard]] common::Result<ExchangeMessage>
execute_distributed_aggregate_fragment(const DistributedAggregateWorkerRequest& request);

// Uses a caller-supplied validated storage path after all dispatch/placement/snapshot proof gates.
// The request snapshot remains the sole logical authority; the loader only locates its exact bytes.
[[nodiscard]] common::Result<ExchangeMessage>
execute_distributed_aggregate_fragment(const DistributedAggregateWorkerRequest& request,
                                       const DistributedTemporalPartBatchLoader& loader);

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_WORKER_HPP_
