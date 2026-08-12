#ifndef CHRONOS_SERVICE_SINGLE_NODE_DATABASE_HPP_
#define CHRONOS_SERVICE_SINGLE_NODE_DATABASE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/ingest/retry_directory.hpp"
#include "chronos/ingest/tablet_state.hpp"
#include "chronos/manifest/publication.hpp"
#include "chronos/query/catalog.hpp"
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

struct SingleNodeDatabaseConfig {
  runtime::DatabaseBootstrapConfig bootstrap;
  wal::WalRecoveryOptions wal_recovery{};
  raft::RaftPersistentLogOpenOptions raft_recovery{};
  wal::WalCommitCoordinatorConfig wal_commit{};
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
  open_or_create(SingleNodeDatabaseConfig config);

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
  [[nodiscard]] ingest::RetryDirectory& retry_directory() noexcept;
  [[nodiscard]] wal::WalCommitCoordinator& wal_coordinator() noexcept;
  [[nodiscard]] common::Result<manifest::DatabaseStorageSnapshot> storage_snapshot() const;

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
  class Impl;
  explicit SingleNodeDatabase(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_SINGLE_NODE_DATABASE_HPP_
