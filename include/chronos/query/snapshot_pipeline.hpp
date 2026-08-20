#ifndef CHRONOS_QUERY_SNAPSHOT_PIPELINE_HPP_
#define CHRONOS_QUERY_SNAPSHOT_PIPELINE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/query/database_cseg_scan.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/physical_optimizer.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/query/relational_plan.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/query/tablet_state_pipeline.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <cstddef>
#include <functional>
#include <memory>
#include <span>
#include <vector>

namespace chronos::query {

inline constexpr std::size_t kDefaultSnapshotMultiTabletLimit = 256U;

struct SnapshotTabletPipelineLimits {
  SnapshotCsegPartScanPlanLimits planning{};
  manifest::ReferencedPartValidationLimits validation{};
  SnapshotTabletScanLimits scan{};
};

// One SQL source binding for a checked multi-source ASOF plan. The lineage is borrowed only for
// instantiation; returned operators own or pin all state needed by later pulls.
struct SnapshotTabletSourceBinding {
  schema::TabletId target_tablet;
  std::reference_wrapper<const schema::SchemaLineage> lineage;
  schema::SchemaId destination_schema_id;
  SnapshotTabletPipelineLimits limits{};
};

// One whole-table SQL source binding for a checked multi-source ASOF plan. Tablet identifiers are
// borrowed only during instantiation and must form a nonempty canonical vector. The returned
// operator owns every constructed source and does not retain this binding or its tablet span.
struct SnapshotTableSourceBinding {
  std::span<const schema::TabletId> target_tablets;
  std::reference_wrapper<const schema::SchemaLineage> lineage;
  schema::SchemaId destination_schema_id;
  SnapshotTabletPipelineLimits limits{};
};

// One canonical tablet source beneath a mixed-authority historical plan. A null Raft snapshot
// selects the supplied aggregate Manifest/WAL publication; a nonnull pointer selects that pinned
// immutable Raft tablet publication. The pointer is borrowed only during construction.
struct MixedSnapshotTabletSourceBinding {
  schema::TabletId tablet_id;
  const ingest::TabletSnapshot* raft_snapshot{};
};

// Instantiates a checked unary physical pipeline over one exact append-only tablet snapshot.
// The pipeline input must be every destination-schema column in ordinal order, optionally followed
// by the shared row-version suffix. Suffix mode is inferred from that checked shape and applied
// uniformly to durable and mutable sources. SQL lowering remains separate so SqlDiagnostic spans
// are never collapsed into storage Status values at this boundary.
[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
instantiate_snapshot_tablet_pipeline(
    const QueryResourceContext& resources, const manifest::ManifestStorage& storage,
    const manifest::DatabaseStorageSnapshot& snapshot, const schema::TabletId& target_tablet,
    const schema::SchemaLineage& lineage, schema::SchemaId destination_schema_id,
    const PhysicalPipelinePlan& pipeline, SnapshotTabletPipelineLimits limits = {});

// Instantiates one physical pipeline over the concatenated raw scans of a canonical tablet vector
// from one aggregate database epoch. Unary SQL stages run once above the combined source, so global
// aggregate, sort, latest, and limit semantics are not evaluated independently per tablet.
[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
instantiate_snapshot_tablets_pipeline(const QueryResourceContext& resources,
                                      const manifest::ManifestStorage& storage,
                                      const manifest::DatabaseStorageSnapshot& snapshot,
                                      std::span<const schema::TabletId> target_tablets,
                                      const schema::SchemaLineage& lineage,
                                      schema::SchemaId destination_schema_id,
                                      const PhysicalPipelinePlan& pipeline,
                                      SnapshotTabletPipelineLimits limits = {});

// Instantiates one physical pipeline over an interleaved canonical vector of WAL and Raft raw
// tablet sources. Both authority kinds must be present. The WAL subset shares one exact aggregate
// publication reservation, while each Raft source retains its own immutable tablet publication.
[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
instantiate_mixed_snapshot_tablets_pipeline(
    const QueryResourceContext& resources, const manifest::ManifestStorage& storage,
    const manifest::DatabaseStorageSnapshot& wal_snapshot,
    std::span<const MixedSnapshotTabletSourceBinding> sources, const schema::SchemaLineage& lineage,
    schema::SchemaId destination_schema_id, const PhysicalPipelinePlan& pipeline,
    SnapshotTabletPipelineLimits wal_limits = {}, TabletStatePipelineLimits raft_limits = {});

[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
instantiate_optimized_snapshot_tablet_pipeline(
    const QueryResourceContext& resources, const manifest::ManifestStorage& storage,
    const manifest::DatabaseStorageSnapshot& snapshot, const schema::TabletId& target_tablet,
    const schema::SchemaLineage& lineage, schema::SchemaId destination_schema_id,
    const OptimizedPhysicalPipelinePlan& pipeline,
    std::vector<ExternalSortExecutionTarget> external_sort_targets = {},
    SnapshotTabletPipelineLimits limits = {});

// Instantiates every SQL source of a checked left-deep ASOF plan from the same exact aggregate
// database epoch. Bindings are in SQL source order and must match the plan's exact per-source input
// shapes. A failure destroys all partially created sources and releases their query credit.
[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>> instantiate_snapshot_asof_plan(
    const QueryResourceContext& resources, const manifest::ManifestStorage& storage,
    const manifest::DatabaseStorageSnapshot& snapshot,
    std::span<const SnapshotTabletSourceBinding> sources, const PhysicalAsofPlan& plan);

// Instantiates every SQL source over all of its canonical local tablets while retaining one shared
// aggregate database epoch. Each table source is concatenated below its ASOF preparation pipeline,
// so join and final aggregate/sort/limit semantics run once over the complete source.
[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
instantiate_snapshot_tables_asof_plan(const QueryResourceContext& resources,
                                      const manifest::ManifestStorage& storage,
                                      const manifest::DatabaseStorageSnapshot& snapshot,
                                      std::span<const SnapshotTableSourceBinding> sources,
                                      const PhysicalAsofPlan& plan);

} // namespace chronos::query

#endif // CHRONOS_QUERY_SNAPSHOT_PIPELINE_HPP_
