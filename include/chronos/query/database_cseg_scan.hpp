#ifndef CHRONOS_QUERY_DATABASE_CSEG_SCAN_HPP_
#define CHRONOS_QUERY_DATABASE_CSEG_SCAN_HPP_

#include "chronos/common/result.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/query/cseg_scan.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::query {

// Converts one storage-validated, snapshot-bound image into the generic immutable CSEG pin. The
// image's aggregate publication token and complete conservative charge follow every pin copy.
[[nodiscard]] common::Result<CsegPartPin>
pin_snapshot_cseg_part(std::shared_ptr<const manifest::SnapshotPartImage> image);

// Creates one single-part scan only after the snapshot descriptor, retained lineage, destination
// schema, and target tablet agree. Page-level binding/integrity remains the CSEG reader's job.
[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>> create_snapshot_cseg_scan(
    const QueryResourceContext& resources, std::shared_ptr<const manifest::SnapshotPartImage> image,
    const schema::SchemaLineage& lineage, schema::SchemaId destination_schema_id,
    const schema::TabletId& target_tablet, std::vector<std::uint32_t> destination_column_ordinals,
    CsegScanLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_DATABASE_CSEG_SCAN_HPP_
