#ifndef CHRONOS_SERVICE_REPLICATED_INGEST_DATABASE_HPP_
#define CHRONOS_SERVICE_REPLICATED_INGEST_DATABASE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/raft_tablet_snapshot_storage.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/query/tablet_state_pipeline.hpp"
#include "chronos/raft/metadata_snapshot_storage.hpp"
#include "chronos/runtime/database_bootstrap.hpp"
#include "chronos/service/replicated_ingest_runtime.hpp"

#include <memory>
#include <optional>
#include <vector>

namespace chronos::service {

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
  // Executes only when every committed placement for table_id was resident and successfully
  // pinned at acquisition. Distributed/partial-table reads fail closed rather than return a
  // locally incomplete result.
  [[nodiscard]] common::Result<std::unique_ptr<query::PhysicalOperator>> instantiate_table_pipeline(
      const query::QueryResourceContext& resources, const schema::TableId& table_id,
      const schema::SchemaId& destination_schema_id, const query::PhysicalPipelinePlan& pipeline,
      query::TabletStatePipelineLimits limits = {}) const;

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
  [[nodiscard]] bool is_running() const noexcept;
  [[nodiscard]] common::Status shutdown();

private:
  class Impl;
  explicit ReplicatedIngestDatabase(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_INGEST_DATABASE_HPP_
