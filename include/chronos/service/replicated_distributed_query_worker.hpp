#ifndef CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_QUERY_WORKER_HPP_
#define CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_QUERY_WORKER_HPP_

#include "chronos/cluster/distributed_grouped_query_transport.hpp"
#include "chronos/cluster/distributed_mutable_vector_query_transport.hpp"
#include "chronos/cluster/distributed_query_transport.hpp"
#include "chronos/cluster/distributed_vector_aggregate_query_transport_v2.hpp"
#include "chronos/cluster/distributed_vector_query_transport_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/manifest/temporal_publication.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/distributed_fragment_worker.hpp"
#include "chronos/raft/metadata.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

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

class ReplicatedDistributedGroupedQueryWorkerContextProvider {
public:
  ReplicatedDistributedGroupedQueryWorkerContextProvider() = default;
  ReplicatedDistributedGroupedQueryWorkerContextProvider(
      const ReplicatedDistributedGroupedQueryWorkerContextProvider&) = delete;
  ReplicatedDistributedGroupedQueryWorkerContextProvider&
  operator=(const ReplicatedDistributedGroupedQueryWorkerContextProvider&) = delete;
  virtual ~ReplicatedDistributedGroupedQueryWorkerContextProvider() = default;

  [[nodiscard]] virtual common::Result<ReplicatedDistributedQueryWorkerContext>
  acquire(const query::DistributedGroupedFloat64FragmentDispatch& dispatch) = 0;
};

struct ReplicatedDistributedGroupedQueryWorkerConfig {
  raft::NodeId local_node_id{};
  const manifest::ManifestStorage* storage{};
  ReplicatedDistributedGroupedQueryWorkerContextProvider* context_provider{};
  query::DistributedAggregateWorkerLimits limits;
};

// Grouped counterpart of the request-local production worker adapter. It acquires one coherent
// owning authority context and invokes the proof-revalidating real-CSEG grouped worker unchanged.
class ReplicatedDistributedGroupedQueryWorker final
    : public cluster::DistributedGroupedQueryWorkerService {
public:
  ReplicatedDistributedGroupedQueryWorker() = delete;
  ReplicatedDistributedGroupedQueryWorker(const ReplicatedDistributedGroupedQueryWorker&) = delete;
  ReplicatedDistributedGroupedQueryWorker&
  operator=(const ReplicatedDistributedGroupedQueryWorker&) = delete;
  ReplicatedDistributedGroupedQueryWorker(ReplicatedDistributedGroupedQueryWorker&&) noexcept =
      default;
  ReplicatedDistributedGroupedQueryWorker&
  operator=(ReplicatedDistributedGroupedQueryWorker&&) noexcept = default;
  ~ReplicatedDistributedGroupedQueryWorker() override = default;

  [[nodiscard]] static common::Result<ReplicatedDistributedGroupedQueryWorker>
  create(ReplicatedDistributedGroupedQueryWorkerConfig config);
  [[nodiscard]] common::Result<query::DistributedGroupedFloat64WorkerResult>
  execute(const query::DistributedGroupedFloat64FragmentDispatch& dispatch) override;

private:
  explicit ReplicatedDistributedGroupedQueryWorker(
      ReplicatedDistributedGroupedQueryWorkerConfig config) noexcept;
  ReplicatedDistributedGroupedQueryWorkerConfig config_;
};

class ReplicatedDistributedVectorQueryWorkerContextProviderV2 {
public:
  ReplicatedDistributedVectorQueryWorkerContextProviderV2() = default;
  ReplicatedDistributedVectorQueryWorkerContextProviderV2(
      const ReplicatedDistributedVectorQueryWorkerContextProviderV2&) = delete;
  ReplicatedDistributedVectorQueryWorkerContextProviderV2&
  operator=(const ReplicatedDistributedVectorQueryWorkerContextProviderV2&) = delete;
  virtual ~ReplicatedDistributedVectorQueryWorkerContextProviderV2() = default;

  [[nodiscard]] virtual common::Result<ReplicatedDistributedQueryWorkerContext>
  acquire(const query::DistributedVectorFragmentDispatchV2& dispatch) = 0;
};

struct ReplicatedDistributedVectorQueryWorkerLimitsV2 {
  query::DistributedVectorRowsWorkerLimitsV2 rows;
  network::QueryResultLimits result;
  std::size_t maximum_messages{1024U};
  std::size_t maximum_total_encoded_bytes{cluster::kDefaultDistributedVectorQueryV2ResponseBytes};
};

struct ReplicatedDistributedVectorQueryWorkerConfigV2 {
  raft::NodeId local_node_id{};
  const manifest::ManifestStorage* storage{};
  ReplicatedDistributedVectorQueryWorkerContextProviderV2* context_provider{};
  ReplicatedDistributedVectorQueryWorkerLimitsV2 limits;
};

// One coherent current mutable publication and its matching schema/Raft authority. The snapshot
// owns every head generation through synchronous execution.
struct ReplicatedDistributedMutableVectorQueryWorkerContext {
  ingest::TabletSnapshot snapshot;
  std::shared_ptr<const schema::SchemaLineage> lineage;
  raft::TabletPlacementMetadata placement;
  raft::GroupId raft_group_id;
  std::optional<raft::ReadBarrier> local_linearizable_barrier;
};

class ReplicatedDistributedMutableVectorQueryWorkerContextProvider {
public:
  ReplicatedDistributedMutableVectorQueryWorkerContextProvider() = default;
  ReplicatedDistributedMutableVectorQueryWorkerContextProvider(
      const ReplicatedDistributedMutableVectorQueryWorkerContextProvider&) = delete;
  ReplicatedDistributedMutableVectorQueryWorkerContextProvider&
  operator=(const ReplicatedDistributedMutableVectorQueryWorkerContextProvider&) = delete;
  ReplicatedDistributedMutableVectorQueryWorkerContextProvider(
      ReplicatedDistributedMutableVectorQueryWorkerContextProvider&&) noexcept = default;
  ReplicatedDistributedMutableVectorQueryWorkerContextProvider&
  operator=(ReplicatedDistributedMutableVectorQueryWorkerContextProvider&&) noexcept = default;
  virtual ~ReplicatedDistributedMutableVectorQueryWorkerContextProvider() = default;

  [[nodiscard]] virtual common::Result<ReplicatedDistributedMutableVectorQueryWorkerContext>
  acquire(const query::DistributedMutableVectorFragment& fragment) = 0;
};

struct ReplicatedDistributedMutableVectorQueryWorkerConfig {
  raft::NodeId local_node_id{};
  ReplicatedDistributedMutableVectorQueryWorkerContextProvider* context_provider{};
  ReplicatedDistributedVectorQueryWorkerLimitsV2 limits;
};

// Request-local production adapter for one proof-bound mutable TabletState fragment. It reacquires
// and pins current publication authority, revalidates it in the query worker, and returns only a
// complete value-owned terminal stream.
class ReplicatedDistributedMutableVectorQueryWorker final
    : public cluster::DistributedMutableVectorQueryWorkerService {
public:
  ReplicatedDistributedMutableVectorQueryWorker() = delete;
  ReplicatedDistributedMutableVectorQueryWorker(
      const ReplicatedDistributedMutableVectorQueryWorker&) = delete;
  ReplicatedDistributedMutableVectorQueryWorker&
  operator=(const ReplicatedDistributedMutableVectorQueryWorker&) = delete;
  ReplicatedDistributedMutableVectorQueryWorker(
      ReplicatedDistributedMutableVectorQueryWorker&&) noexcept = default;
  ReplicatedDistributedMutableVectorQueryWorker&
  operator=(ReplicatedDistributedMutableVectorQueryWorker&&) noexcept = default;
  ~ReplicatedDistributedMutableVectorQueryWorker() override = default;

  [[nodiscard]] static common::Result<ReplicatedDistributedMutableVectorQueryWorker>
  create(ReplicatedDistributedMutableVectorQueryWorkerConfig config);
  [[nodiscard]] common::Result<std::vector<cluster::DistributedVectorResultExchangeMessage>>
  execute(const query::DistributedMutableVectorFragment& fragment) override;

private:
  explicit ReplicatedDistributedMutableVectorQueryWorker(
      ReplicatedDistributedMutableVectorQueryWorkerConfig config) noexcept;
  ReplicatedDistributedMutableVectorQueryWorkerConfig config_;
};

// Request-local production adapter for schema-bound row fragments. It retains one coherent
// Manifest/schema/placement/group context through real-CSEG execution and returns one complete,
// value-owned terminal stream. Aggregate modes belong to the distinct aggregate service below.
class ReplicatedDistributedVectorQueryWorkerV2 final
    : public cluster::DistributedVectorQueryWorkerServiceV2 {
public:
  ReplicatedDistributedVectorQueryWorkerV2() = delete;
  ReplicatedDistributedVectorQueryWorkerV2(const ReplicatedDistributedVectorQueryWorkerV2&) =
      delete;
  ReplicatedDistributedVectorQueryWorkerV2&
  operator=(const ReplicatedDistributedVectorQueryWorkerV2&) = delete;
  ReplicatedDistributedVectorQueryWorkerV2(ReplicatedDistributedVectorQueryWorkerV2&&) noexcept =
      default;
  ReplicatedDistributedVectorQueryWorkerV2&
  operator=(ReplicatedDistributedVectorQueryWorkerV2&&) noexcept = default;
  ~ReplicatedDistributedVectorQueryWorkerV2() override = default;

  [[nodiscard]] static common::Result<ReplicatedDistributedVectorQueryWorkerV2>
  create(ReplicatedDistributedVectorQueryWorkerConfigV2 config);
  [[nodiscard]] common::Result<std::vector<cluster::DistributedVectorResultExchangeMessage>>
  execute(const query::DistributedVectorFragmentDispatchV2& dispatch) override;

private:
  explicit ReplicatedDistributedVectorQueryWorkerV2(
      ReplicatedDistributedVectorQueryWorkerConfigV2 config) noexcept;
  ReplicatedDistributedVectorQueryWorkerConfigV2 config_;
};

struct ReplicatedDistributedVectorAggregateQueryWorkerConfigV2 {
  raft::NodeId local_node_id{};
  const manifest::ManifestStorage* storage{};
  ReplicatedDistributedVectorQueryWorkerContextProviderV2* context_provider{};
  query::DistributedVectorAggregateWorkerLimitsV2 limits;
};

// Production aggregate counterpart. Definition binding and execution each acquire a fresh coherent
// context and enter the same proof-revalidating query boundary; binding never loads CSEG parts.
class ReplicatedDistributedVectorAggregateQueryWorkerV2 final
    : public cluster::DistributedVectorAggregateQueryWorkerServiceV2 {
public:
  ReplicatedDistributedVectorAggregateQueryWorkerV2() = delete;
  ReplicatedDistributedVectorAggregateQueryWorkerV2(
      const ReplicatedDistributedVectorAggregateQueryWorkerV2&) = delete;
  ReplicatedDistributedVectorAggregateQueryWorkerV2&
  operator=(const ReplicatedDistributedVectorAggregateQueryWorkerV2&) = delete;
  ReplicatedDistributedVectorAggregateQueryWorkerV2(
      ReplicatedDistributedVectorAggregateQueryWorkerV2&&) noexcept = default;
  ReplicatedDistributedVectorAggregateQueryWorkerV2&
  operator=(ReplicatedDistributedVectorAggregateQueryWorkerV2&&) noexcept = default;
  ~ReplicatedDistributedVectorAggregateQueryWorkerV2() override = default;

  [[nodiscard]] static common::Result<ReplicatedDistributedVectorAggregateQueryWorkerV2>
  create(ReplicatedDistributedVectorAggregateQueryWorkerConfigV2 config);
  [[nodiscard]] common::Result<std::vector<query::VectorAggregateDefinition>>
  bind_definitions(const query::DistributedVectorFragmentDispatchV2& dispatch) override;
  [[nodiscard]] common::Result<query::DistributedVectorAggregateWorkerResultV2>
  execute(const query::DistributedVectorFragmentDispatchV2& dispatch) override;

private:
  explicit ReplicatedDistributedVectorAggregateQueryWorkerV2(
      ReplicatedDistributedVectorAggregateQueryWorkerConfigV2 config) noexcept;
  ReplicatedDistributedVectorAggregateQueryWorkerConfigV2 config_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_QUERY_WORKER_HPP_
