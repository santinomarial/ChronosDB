#ifndef CHRONOS_SERVICE_REPLICATED_INGEST_DATABASE_HPP_
#define CHRONOS_SERVICE_REPLICATED_INGEST_DATABASE_HPP_

#include "chronos/cluster/distributed_query_tcp_execution.hpp"
#include "chronos/common/result.hpp"
#include "chronos/ingest/raft_tablet_snapshot_storage.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/distributed_fragment_binding.hpp"
#include "chronos/query/distributed_mutable_vector_fragment.hpp"
#include "chronos/query/distributed_sql_lowering.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/query/tablet_state_pipeline.hpp"
#include "chronos/raft/metadata_snapshot_storage.hpp"
#include "chronos/runtime/database_bootstrap.hpp"
#include "chronos/service/replicated_ingest_runtime.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace chronos::service {

struct ReplicatedSingleGroupQueryRoute {
  schema::TableId table_id;
  raft::GroupId group_id;
  std::uint64_t placement_epoch{};
  std::vector<raft::NodeId> replicas;

  friend bool operator==(const ReplicatedSingleGroupQueryRoute&,
                         const ReplicatedSingleGroupQueryRoute&) = default;
};

struct ReplicatedQueryLeaderRoute {
  raft::GroupId group_id;
  raft::NodeId leader_node_id{};
  raft::Term leader_term{};
  std::uint64_t placement_epoch{};

  friend bool operator==(const ReplicatedQueryLeaderRoute&,
                         const ReplicatedQueryLeaderRoute&) = default;
};

struct ReplicatedMutableVectorQueryBinding {
  std::reference_wrapper<const query::DistributedVectorQueryPlan> plan;
  schema::TableId table_id;
  // Canonical unique order by observation.group_id. The metadata-group authority may be present;
  // every selected tablet's immutable group authority must be present.
  std::span<const query::DistributedVectorGroupReadAuthority> group_authorities;
  std::span<const std::uint32_t> destination_column_ordinals;
  std::optional<cseg::EventTimePredicate> event_time_predicate;
  std::reference_wrapper<const query::DistributedVectorResultSchema> result_schema;
};

struct ReplicatedRoutedMutableVectorQuery {
  std::vector<query::DistributedMutableVectorFragment> fragments;
  std::vector<cluster::DistributedQueryNodeRoute> routes;
};

struct ReplicatedMutableVectorRowsSqlBinding {
  common::Uuid query_id;
  std::reference_wrapper<const query::DistributedVectorRowsSqlPlan> sql_plan;
  // Canonical unique group order. Extra metadata authority is ignored; every committed table
  // tablet must have one exact current-leader barrier/observation pair.
  std::span<const query::DistributedVectorGroupReadAuthority> group_authorities;
};

enum class ReplicatedIngestDatabaseStartupStage : std::uint8_t {
  kRootOwnerReady,
  kCatalogRecovered,
  kTabletOwnersPrepared,
  kRuntimeReady,
};

// Borrowed only for the synchronous duration of open_existing(). Callbacks run in order on the
// opening thread after each named owner is complete, receive no partially constructed database,
// and have no return channel into startup status. Blocking a callback directly blocks startup.
class ReplicatedIngestDatabaseStartupObserver {
public:
  virtual ~ReplicatedIngestDatabaseStartupObserver() = default;
  virtual void on_startup_stage(ReplicatedIngestDatabaseStartupStage stage) noexcept = 0;
};

enum class ReplicatedIngestDatabaseShutdownStage : std::uint8_t {
  kCoordinatorReleased,
  kAcceptedWorkDrained,
  kApplicationsStopped,
  kLogClosed,
  kRuntimeStopped,
  kRootReleased,
};

// Borrowed only for one synchronous shutdown(observer) call. Callbacks run in order and are emitted
// at most once; internal drain/application/log callbacks run on the durable worker, while the outer
// ownership callbacks run on the calling thread. They have no return channel into shutdown status.
class ReplicatedIngestDatabaseShutdownObserver {
public:
  virtual ~ReplicatedIngestDatabaseShutdownObserver() = default;
  virtual void on_shutdown_stage(ReplicatedIngestDatabaseShutdownStage stage) noexcept = 0;
};

class ReplicatedQuerySnapshot {
public:
  ReplicatedQuerySnapshot() = delete;
  ~ReplicatedQuerySnapshot();
  ReplicatedQuerySnapshot(const ReplicatedQuerySnapshot&) = delete;
  ReplicatedQuerySnapshot& operator=(const ReplicatedQuerySnapshot&) = delete;
  ReplicatedQuerySnapshot(ReplicatedQuerySnapshot&&) noexcept;
  ReplicatedQuerySnapshot& operator=(ReplicatedQuerySnapshot&&) noexcept;

  // The catalog and all resident tablet publications are retained by this owner and remain valid
  // independently of later Raft application or database shutdown.
  [[nodiscard]] const std::shared_ptr<const query::QueryCatalogSnapshot>& catalog() const noexcept;
  // Returns a borrowed route only when every committed placement for the table maps to one group,
  // one placement epoch, and one replica set. The pointer remains valid for this snapshot's life.
  [[nodiscard]] const ReplicatedSingleGroupQueryRoute*
  single_group_route(const schema::TableId& table_id) const noexcept;
  // Executes only when every committed placement for table_id was resident and successfully
  // pinned at acquisition. Distributed/partial-table reads fail closed rather than return a
  // locally incomplete result.
  [[nodiscard]] common::Result<std::unique_ptr<query::PhysicalOperator>> instantiate_table_pipeline(
      const query::QueryResourceContext& resources, const schema::TableId& table_id,
      const schema::SchemaId& destination_schema_id, const query::PhysicalPipelinePlan& pipeline,
      query::TabletStatePipelineLimits limits = {}) const;
  // Binds every plan-ordered tablet to one pinned committed/applied publication and the exact
  // correlated current-leader authority for its immutable group. This path accepts only
  // leader-linearizable plans and returns no fragments unless the complete set is valid.
  [[nodiscard]] common::Result<std::vector<query::DistributedMutableVectorFragment>>
  bind_linearizable_mutable_vector_fragments(
      const ReplicatedMutableVectorQueryBinding& binding) const;
  // Performs fragment binding and then resolves only those exact serving nodes through the same
  // committed metadata publication. Returned routes own addresses and borrow the supplied TLS
  // contexts, which must outlive the eventual TCP execution.
  [[nodiscard]] common::Result<ReplicatedRoutedMutableVectorQuery>
  bind_and_resolve_linearizable_mutable_vector_query(
      const ReplicatedMutableVectorQueryBinding& binding,
      std::span<const cluster::DistributedQueryNodeTlsContext> tls_contexts,
      cluster::DistributedQueryRouteResolutionLimits limits = {}) const;
  // Constructs the complete canonical table plan from this pinned publication and the correlated
  // current-leader authorities, then binds the schema-lowered SQL product and resolves its routes.
  [[nodiscard]] common::Result<ReplicatedRoutedMutableVectorQuery>
  prepare_linearizable_mutable_vector_rows_query(
      const ReplicatedMutableVectorRowsSqlBinding& binding,
      std::span<const cluster::DistributedQueryNodeTlsContext> tls_contexts,
      cluster::DistributedQueryRouteResolutionLimits limits = {}) const;

private:
  class Impl;
  explicit ReplicatedQuerySnapshot(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
  friend class ReplicatedIngestDatabase;
};

struct ReplicatedIngestDatabaseConfig {
  runtime::DatabaseBootstrapConfig bootstrap;
  std::vector<raft::RaftGroupConfiguration> groups;
  raft::RaftPersistentLogOpenOptions raft_recovery;
  std::optional<raft::MetadataSnapshotStorageConfig> metadata_snapshots;
  std::vector<ingest::RaftTabletSnapshotStorageConfig> tablet_snapshots;
  raft::AsyncDurableMultiRaftLimits runtime_limits;
  ingest::AsyncRaftTabletApplicationLimits application_limits;
  ReplicatedIngestCoordinatorLimits coordinator_limits;
  raft::MetadataLimits metadata_limits;
  raft::MetadataCommandCodecLimits metadata_codec_limits;
  raft::SchemaDefinitionCodecLimits schema_codec_limits;
  ingest::ColumnarAppendDecodeLimits columnar_append_limits;
  ReplicatedIngestDatabaseStartupObserver* startup_observer{};
};

// Recoverable database-root owner for an already provisioned replicated ingest node. External
// configuration supplies exact group membership; committed metadata supplies tablet routing,
// schemas, policies, and local application ownership. Shutdown drains the replicated runtime before
// releasing the database-root lock.
class ReplicatedIngestDatabase {
public:
  ReplicatedIngestDatabase() = delete;
  ~ReplicatedIngestDatabase();
  ReplicatedIngestDatabase(const ReplicatedIngestDatabase&) = delete;
  ReplicatedIngestDatabase& operator=(const ReplicatedIngestDatabase&) = delete;
  ReplicatedIngestDatabase(ReplicatedIngestDatabase&&) noexcept;
  ReplicatedIngestDatabase& operator=(ReplicatedIngestDatabase&&) noexcept;

  [[nodiscard]] static common::Result<ReplicatedIngestDatabase>
  open_existing(ReplicatedIngestDatabaseConfig config);

  [[nodiscard]] const runtime::DatabaseBootstrapDescriptor& bootstrap() const noexcept;
  [[nodiscard]] ReplicatedIngestRuntime* ingest_runtime() noexcept;
  // Acquires an owning local-applied read view. Each tablet publication contains only committed,
  // applied Raft state; the returned vector records a stable per-tablet boundary, not a linearized
  // cross-group read. Tables with any nonresident placement remain bindable but fail execution.
  [[nodiscard]] common::Result<ReplicatedQuerySnapshot> acquire_query_snapshot() const;
  // The barrier vector must contain exactly query_barrier_groups(). Acquisition fails closed
  // unless the immutable metadata catalog and every resident tablet publication cover its
  // corresponding leader-confirmed read index.
  [[nodiscard]] common::Result<ReplicatedQuerySnapshot>
  acquire_query_snapshot(std::span<const raft::GroupReadBarrier> barriers) const;
  [[nodiscard]] std::span<const raft::GroupId> query_barrier_groups() const noexcept;
  // Obtains ordered observations for the complete packaged query-barrier group vector. It returns
  // a redirect only when every group has the same stable remote leader and committed placement is
  // unchanged; all-local leadership returns an empty optional. Split or unknown authority fails.
  [[nodiscard]] common::Result<std::optional<ReplicatedQueryLeaderRoute>>
  resolve_query_leader(const ReplicatedSingleGroupQueryRoute& route);
  [[nodiscard]] bool is_running() const noexcept;
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] common::Status shutdown(ReplicatedIngestDatabaseShutdownObserver& observer);

private:
  [[nodiscard]] common::Status shutdown_with(ReplicatedIngestDatabaseShutdownObserver* observer);
  class Impl;
  explicit ReplicatedIngestDatabase(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_INGEST_DATABASE_HPP_
