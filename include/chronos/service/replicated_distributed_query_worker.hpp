#ifndef CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_QUERY_WORKER_HPP_
#define CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_QUERY_WORKER_HPP_

#include "chronos/cluster/distributed_query_transport.hpp"
#include "chronos/common/result.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/manifest/temporal_publication.hpp"
#include "chronos/query/distributed_fragment_worker.hpp"
#include "chronos/raft/metadata.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <memory>
#include <optional>

namespace chronos::service {

// Owning, request-local authority retained through synchronous part loading and aggregation.
struct ReplicatedDistributedQueryWorkerContext {
  manifest::TemporalDatabaseStorageSnapshot snapshot;
  std::shared_ptr<const schema::SchemaLineage> lineage;
  raft::TabletPlacementMetadata placement;
  raft::GroupId raft_group_id;
  std::optional<raft::ReadBarrier> local_linearizable_barrier;
};

// Embedding-owned current-authority boundary. Implementations must acquire one coherent context for
// the exact dispatch and provide their own synchronization. Returned values may not be assembled
// from publications observed at different logical instants.
class ReplicatedDistributedQueryWorkerContextProvider {
public:
  ReplicatedDistributedQueryWorkerContextProvider() = default;
  ReplicatedDistributedQueryWorkerContextProvider(
      const ReplicatedDistributedQueryWorkerContextProvider&) = delete;
  ReplicatedDistributedQueryWorkerContextProvider&
  operator=(const ReplicatedDistributedQueryWorkerContextProvider&) = delete;
  virtual ~ReplicatedDistributedQueryWorkerContextProvider() = default;

  [[nodiscard]] virtual common::Result<ReplicatedDistributedQueryWorkerContext>
  acquire(const query::DistributedAggregateFragmentDispatch& dispatch) = 0;
};

struct ReplicatedDistributedQueryWorkerConfig {
  raft::NodeId local_node_id{};
  const manifest::ManifestStorage* storage{};
  ReplicatedDistributedQueryWorkerContextProvider* context_provider{};
  query::DistributedAggregateWorkerLimits limits;
};

// Synchronous receiver service that acquires current local authority for each dispatch and then
// invokes the proof-revalidating real-CSEG worker. The storage and provider must outlive this
// single-threaded service; the provider's owning context outlives all part views for one call.
class ReplicatedDistributedQueryWorker final : public cluster::DistributedQueryWorkerService {
public:
  ReplicatedDistributedQueryWorker() = delete;
  ReplicatedDistributedQueryWorker(const ReplicatedDistributedQueryWorker&) = delete;
  ReplicatedDistributedQueryWorker& operator=(const ReplicatedDistributedQueryWorker&) = delete;
  ReplicatedDistributedQueryWorker(ReplicatedDistributedQueryWorker&&) noexcept = default;
  ReplicatedDistributedQueryWorker&
  operator=(ReplicatedDistributedQueryWorker&&) noexcept = default;
  ~ReplicatedDistributedQueryWorker() override = default;

  [[nodiscard]] static common::Result<ReplicatedDistributedQueryWorker>
  create(ReplicatedDistributedQueryWorkerConfig config);

  [[nodiscard]] common::Result<query::ExchangeMessage>
  execute(const query::DistributedAggregateFragmentDispatch& dispatch) override;

private:
  explicit ReplicatedDistributedQueryWorker(ReplicatedDistributedQueryWorkerConfig config) noexcept;

  ReplicatedDistributedQueryWorkerConfig config_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_QUERY_WORKER_HPP_
