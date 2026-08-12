#include "chronos/query/snapshot_shape.hpp"

namespace {
using ValidateShapeFunction = chronos::common::Result<chronos::query::RowVersionScanMode> (*)(
    std::span<const chronos::query::PhysicalColumnShape>, const chronos::schema::TableSchema&);
[[maybe_unused]] ValidateShapeFunction validate_shape =
    &chronos::query::validate_snapshot_pipeline_input_shape;
} // namespace
