#ifndef CHRONOS_QUERY_TABLET_STATE_PIPELINE_HPP_
#define CHRONOS_QUERY_TABLET_STATE_PIPELINE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/tablet_state.hpp"
#include "chronos/query/head_scan.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <cstddef>
#include <memory>
#include <span>

namespace chronos::query {

struct TabletStatePipelineLimits {
  HeadScanLimits scan{};
  std::size_t maximum_source_configuration_bytes{std::size_t{8U} * 1024U * 1024U};
};

// Creates the checked raw source beneath a physical pipeline. This is the composition seam for
// callers that must place one global plan above tablet publications from more than one authority.
// Snapshots are consumed only during construction; the returned scans retain their own pins.
[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>> create_tablet_states_source(
    const QueryResourceContext& resources, std::span<const ingest::TabletSnapshot> snapshots,
    const schema::SchemaLineage& lineage, schema::SchemaId destination_schema_id,
    std::span<const PhysicalColumnShape> input_shape, TabletStatePipelineLimits limits = {});

// Instantiates one checked physical SQL pipeline over a stable set of TabletState publications for
// the same table. Every sealed/active generation from every tablet is placed below one shared
// physical pipeline, preserving table-wide aggregate/sort/latest/limit semantics. Tablet IDs must
// be nonnil and unique.
[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>> instantiate_tablet_states_pipeline(
    const QueryResourceContext& resources, std::span<const ingest::TabletSnapshot> snapshots,
    const schema::SchemaLineage& lineage, schema::SchemaId destination_schema_id,
    const PhysicalPipelinePlan& pipeline, TabletStatePipelineLimits limits = {});

// Instantiates one checked physical SQL pipeline over a stable TabletState publication. Sealed
// generations are scanned in retained order and the active generation last; global pipeline stages
// are instantiated once above that serial source, preserving aggregate/sort/latest/limit semantics.
[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>> instantiate_tablet_state_pipeline(
    const QueryResourceContext& resources, ingest::TabletSnapshot snapshot,
    const schema::SchemaLineage& lineage, schema::SchemaId destination_schema_id,
    const PhysicalPipelinePlan& pipeline, TabletStatePipelineLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_TABLET_STATE_PIPELINE_HPP_
