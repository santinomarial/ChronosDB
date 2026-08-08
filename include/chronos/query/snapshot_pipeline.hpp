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
#include "chronos/schema/identity.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <functional>
#include <memory>
#include <span>
#include <vector>

namespace chronos::query {

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

} // namespace chronos::query

#endif // CHRONOS_QUERY_SNAPSHOT_PIPELINE_HPP_
