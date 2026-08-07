#ifndef CHRONOS_QUERY_SNAPSHOT_PIPELINE_HPP_
#define CHRONOS_QUERY_SNAPSHOT_PIPELINE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/query/database_cseg_scan.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <memory>

namespace chronos::query {

struct SnapshotTabletPipelineLimits {
  SnapshotCsegPartScanPlanLimits planning{};
  manifest::ReferencedPartValidationLimits validation{};
  SnapshotTabletScanLimits scan{};
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

} // namespace chronos::query

#endif // CHRONOS_QUERY_SNAPSHOT_PIPELINE_HPP_
