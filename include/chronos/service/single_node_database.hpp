#ifndef CHRONOS_SERVICE_SINGLE_NODE_DATABASE_HPP_
#define CHRONOS_SERVICE_SINGLE_NODE_DATABASE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/ingest/columnar_append_executor.hpp"
#include "chronos/ingest/retry_directory.hpp"
#include "chronos/ingest/tablet_state.hpp"
#include "chronos/manifest/publication.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/query/snapshot_pipeline.hpp"
#include "chronos/query/statement_binder.hpp"
#include "chronos/raft/metadata.hpp"
#include "chronos/raft/persistent_log.hpp"
#include "chronos/runtime/database_bootstrap.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/wal/wal_commit_coordinator.hpp"
#include "chronos/wal/wal_recovery.hpp"

#include <cstddef>
#include <memory>
#include <span>
#include <vector>

namespace chronos::service {

struct AppliedSingleNodeColumnarAppend {
  schema::TabletId tablet_id;
  head::HeadCommitPosition position;
  std::shared_ptr<const columnar::OwnedColumnarBatch> batch;
  std::shared_ptr<const ingest::ColumnarAppendRetryOutcome> outcome;
};

// Thread-affine post-apply notification. The observer is borrowed by SingleNodeDatabase and must
// outlive it. It cannot reject an already committed mutation or throw through the write path;
// downstream live overload/failure must terminate or overflow affected subscriptions internally.
class SingleNodeCommittedAppendObserver {
public:
  SingleNodeCommittedAppendObserver() = default;
  SingleNodeCommittedAppendObserver(const SingleNodeCommittedAppendObserver&) = delete;
  SingleNodeCommittedAppendObserver& operator=(const SingleNodeCommittedAppendObserver&) = delete;
  virtual ~SingleNodeCommittedAppendObserver() = default;

  virtual void on_applied(AppliedSingleNodeColumnarAppend append) noexcept = 0;
};

struct SingleNodeDatabaseConfig {
  runtime::DatabaseBootstrapConfig bootstrap;
  wal::WalRecoveryOptions wal_recovery{};
  raft::RaftPersistentLogOpenOptions raft_recovery{};
  wal::WalCommitCoordinatorConfig wal_commit{};
  SingleNodeCommittedAppendObserver* committed_append_observer{};
};

struct NewTableIdentities {
  schema::TableId table_id;
  schema::SchemaId schema_id;
  std::vector<schema::ColumnId> column_ids;
  schema::TabletId tablet_id;
};

struct CreatedSingleNodeTable {
  schema::TableId table_id;
  schema::SchemaId schema_id;
  schema::TabletId tablet_id;
  raft::LogIndex metadata_index{};
  bool resumed_incomplete_creation{};
};

struct SingleNodeAsofSourceBinding {
  schema::TableId table_id;
  schema::SchemaId destination_schema_id;
  query::SnapshotTabletPipelineLimits limits{};
};

struct SingleNodeSubscriptionSnapshotContext {
  const manifest::ManifestStorage* storage{};
  const manifest::DatabaseStoragePublisher* publisher{};
  const schema::SchemaLineage* lineage{};
};

// Recoverable single-process owner for the current WAL-backed single-node product boundary. The
// object is thread-affine except for the independently synchronized WAL coordinator and query-safe
// immutable snapshots returned by TabletState. Metadata Raft, WAL admission, and the database root
// remain exclusively owned until shutdown.
class SingleNodeDatabase {
public:
  SingleNodeDatabase() = delete;
  ~SingleNodeDatabase();

  SingleNodeDatabase(const SingleNodeDatabase&) = delete;
  SingleNodeDatabase& operator=(const SingleNodeDatabase&) = delete;
  SingleNodeDatabase(SingleNodeDatabase&&) noexcept;
  SingleNodeDatabase& operator=(SingleNodeDatabase&&) noexcept;

  [[nodiscard]] static common::Result<SingleNodeDatabase>
  open_or_create(const SingleNodeDatabaseConfig& config);

  [[nodiscard]] const runtime::DatabaseBootstrapDescriptor& bootstrap() const noexcept;
  [[nodiscard]] const raft::MetadataCatalogSnapshot& metadata_catalog() const noexcept;
  [[nodiscard]] const std::shared_ptr<const query::QueryCatalogSnapshot>&
  query_catalog() const noexcept;
  [[nodiscard]] const schema::SchemaLineage*
  find_lineage(const schema::TableId& table_id) const noexcept;
  [[nodiscard]] ingest::TabletState* find_tablet(const schema::TabletId& tablet_id) noexcept;
  [[nodiscard]] const ingest::TabletState*
  find_tablet(const schema::TabletId& tablet_id) const noexcept;
  // Acquires one stable publication for every currently local tablet of table_id in deterministic
  // metadata placement order. The returned pins are independent of later tablet publications.
  [[nodiscard]] common::Result<std::vector<ingest::TabletSnapshot>>
  table_snapshots(const schema::TableId& table_id) const;
  // The sole product-path append boundary. Routing is validated before WAL admission. A newly
  // committed/applied mutation notifies the configured observer exactly once; a matching retry
  // returns its original outcome without another notification.
  [[nodiscard]] common::Result<ingest::ColumnarAppendExecutionResult>
  execute_append(schema::TabletId tablet_id, const ingest::ColumnarAppendExecutionInput& input);
  [[nodiscard]] common::Result<manifest::DatabaseStorageSnapshot> storage_snapshot() const;
  // Borrows the exact storage publication and lineage owners used to execute a subscription's
  // historical half. The database must outlive the returned context and subscription runtime.
  [[nodiscard]] common::Result<SingleNodeSubscriptionSnapshotContext>
  subscription_snapshot_context(const schema::TableId& table_id) const;
  [[nodiscard]] common::Result<std::unique_ptr<query::PhysicalOperator>> instantiate_table_pipeline(
      const query::QueryResourceContext& resources, const schema::TableId& table_id,
      const schema::SchemaId& destination_schema_id, const query::PhysicalPipelinePlan& pipeline,
      query::SnapshotTabletPipelineLimits limits = {}) const;
  [[nodiscard]] common::Result<std::unique_ptr<query::PhysicalOperator>>
  instantiate_asof_pipeline(const query::QueryResourceContext& resources,
                            std::span<const SingleNodeAsofSourceBinding> sources,
                            const query::PhysicalAsofPlan& plan) const;

  // Synchronously drains every ready per-tablet sealed-head queue through durable CSEG/Manifest
  // publication. Returns the number of completed replacements; no work is also successful.
  [[nodiscard]] common::Result<std::size_t> flush_ready_heads();

  // Publishes one initial schema, complete policy, and local placement through exact-retained
  // metadata Raft proposals. A matching incomplete schema prefix is resumed using its durable
  // identities; the table becomes routable only after all three records are applied.
  [[nodiscard]] common::Result<CreatedSingleNodeTable>
  create_table(const query::BoundSqlCreateTable& statement, NewTableIdentities identities,
               std::uint64_t retry_retention_positions);

  // Stops WAL admission and synchronizes its final LOCAL_SYNC group, then closes metadata Raft and
  // finally releases the database-root lock. Repeated calls are safe.
  [[nodiscard]] common::Status shutdown();

private:
  void shutdown_noexcept() noexcept;
  [[nodiscard]] common::Status checkpoint_flushed_wal();
  class Impl;
  explicit SingleNodeDatabase(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_SINGLE_NODE_DATABASE_HPP_
