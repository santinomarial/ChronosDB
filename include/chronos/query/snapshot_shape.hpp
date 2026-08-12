#ifndef CHRONOS_QUERY_SNAPSHOT_SHAPE_HPP_
#define CHRONOS_QUERY_SNAPSHOT_SHAPE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/query/row_version.hpp"
#include "chronos/schema/table_schema.hpp"

#include <span>

namespace chronos::query {

// Validates the canonical vector source shape shared by mutable-head and durable CSEG snapshots.
// User columns must be in destination-schema order; the only accepted extension is the fixed
// non-null row-version suffix. The returned mode configures both source kinds identically.
[[nodiscard]] common::Result<RowVersionScanMode>
validate_snapshot_pipeline_input_shape(std::span<const PhysicalColumnShape> input,
                                       const schema::TableSchema& destination_schema);

} // namespace chronos::query

#endif // CHRONOS_QUERY_SNAPSHOT_SHAPE_HPP_
