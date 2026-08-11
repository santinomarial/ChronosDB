#ifndef CHRONOS_TIERING_TIERED_DISTRIBUTED_FRAGMENT_WORKER_HPP_
#define CHRONOS_TIERING_TIERED_DISTRIBUTED_FRAGMENT_WORKER_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/distributed_fragment_worker.hpp"
#include "chronos/tiering/object_store.hpp"
#include "chronos/tiering/tiered_part_loader.hpp"
#include "chronos/tiering/tiered_publication.hpp"

#include <functional>

namespace chronos::tiering {

struct TieredDistributedAggregateWorkerRequest {
  // worker.snapshot must be the exact Manifest owner embedded in tiered_snapshot. The worker's
  // local ManifestStorage remains the preferred byte source.
  std::reference_wrapper<const query::DistributedAggregateWorkerRequest> worker;
  std::reference_wrapper<const TieredDatabaseStorageSnapshot> tiered_snapshot;
  std::reference_wrapper<const ObjectStore> remote_store;
  TieredTemporalPartLoadLimits load_limits;
};

// Applies every ordinary worker authority gate before local/remote I/O, then executes from the
// exact aggregate-pinned bytes. Only a missing local final may use the pinned cold route.
[[nodiscard]] common::Result<query::ExchangeMessage> execute_tiered_distributed_aggregate_fragment(
    const TieredDistributedAggregateWorkerRequest& request);

} // namespace chronos::tiering

#endif // CHRONOS_TIERING_TIERED_DISTRIBUTED_FRAGMENT_WORKER_HPP_
