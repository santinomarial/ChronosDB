#include "chronos/query/snapshot_pipeline.hpp"

#include "chronos/common/status.hpp"
#include "chronos/query/row_version.hpp"
#include "chronos/schema/table_schema.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Result<RowVersionScanMode>
validate_input_shape(const PhysicalPipelinePlan& pipeline,
                     const schema::TableSchema& destination_schema) {
  const std::span<const PhysicalColumnShape> input = pipeline.input_columns();
  const std::span<const schema::ColumnDefinition> columns = destination_schema.columns();
  common::Result<std::size_t> appended_count =
      scan_output_column_count(columns.size(), RowVersionScanMode::kAppend);
  if (!appended_count.has_value())
    return common::make_unexpected(appended_count.error());
  if (input.size() != columns.size() && input.size() != *appended_count) {
    return common::make_unexpected(invalid(
        "snapshot pipeline input must be the destination schema with an optional row-version "
        "suffix"));
  }
  for (std::size_t ordinal = 0U; ordinal < columns.size(); ++ordinal) {
    const PhysicalColumnShape expected{.type = columns[ordinal].type(),
                                       .nullable = columns[ordinal].nullable()};
    if (input[ordinal] != expected) {
      return common::make_unexpected(
          invalid("snapshot pipeline user-column shape disagrees with the destination schema"));
    }
  }
  if (input.size() == columns.size())
    return RowVersionScanMode::kOmit;

  constexpr std::array<VectorRowVersionColumnKind, kVectorRowVersionColumnCount> kSuffixKinds{
      VectorRowVersionColumnKind::kWalId, VectorRowVersionColumnKind::kRecordSequence,
      VectorRowVersionColumnKind::kRowOrdinal, VectorRowVersionColumnKind::kOperation};
  for (std::size_t suffix = 0U; suffix < kSuffixKinds.size(); ++suffix) {
    common::Result<schema::LogicalType> expected_type =
        vector_row_version_column_type(kSuffixKinds[suffix]);
    if (!expected_type.has_value())
      return common::make_unexpected(expected_type.error());
    const PhysicalColumnShape& actual = input[columns.size() + suffix];
    if (actual.nullable || actual.type != *expected_type) {
      return common::make_unexpected(
          invalid("snapshot pipeline row-version suffix has an invalid physical shape"));
    }
  }
  return RowVersionScanMode::kAppend;
}

} // namespace

common::Result<std::unique_ptr<PhysicalOperator>> instantiate_snapshot_tablet_pipeline(
    const QueryResourceContext& resources, const manifest::ManifestStorage& storage,
    const manifest::DatabaseStorageSnapshot& snapshot, const schema::TabletId& target_tablet,
    const schema::SchemaLineage& lineage, const schema::SchemaId destination_schema_id,
    const PhysicalPipelinePlan& pipeline, SnapshotTabletPipelineLimits limits) {
  const std::shared_ptr<const schema::TableSchema> destination_schema =
      lineage.find(destination_schema_id);
  if (destination_schema == nullptr) {
    return common::make_unexpected(
        invalid("snapshot pipeline destination schema is absent from its lineage"));
  }
  if (destination_schema->table_id() != lineage.table_id()) {
    return common::make_unexpected(
        invalid("snapshot pipeline destination schema disagrees with its lineage"));
  }
  common::Result<RowVersionScanMode> row_version_mode =
      validate_input_shape(pipeline, *destination_schema);
  if (!row_version_mode.has_value())
    return common::make_unexpected(row_version_mode.error());
  limits.scan.cseg.row_version_columns = *row_version_mode;
  limits.scan.head.row_version_columns = *row_version_mode;

  common::Result<SnapshotCsegPartScanPlan> scan_plan =
      plan_snapshot_cseg_part_scan(snapshot, target_tablet, std::nullopt, limits.planning);
  if (!scan_plan.has_value())
    return common::make_unexpected(scan_plan.error());
  if (scan_plan->table_id() != destination_schema->table_id()) {
    return common::make_unexpected(
        invalid("snapshot pipeline destination schema disagrees with the target tablet"));
  }
  common::Result<std::vector<std::shared_ptr<const manifest::SnapshotPartImage>>> images =
      load_snapshot_cseg_part_scan_images(storage, snapshot, *scan_plan, lineage,
                                          limits.validation);
  if (!images.has_value())
    return common::make_unexpected(images.error());

  try {
    std::vector<std::uint32_t> ordinals;
    ordinals.reserve(destination_schema->columns().size());
    for (std::size_t ordinal = 0U; ordinal < destination_schema->columns().size(); ++ordinal)
      ordinals.push_back(static_cast<std::uint32_t>(ordinal));
    common::Result<std::unique_ptr<PhysicalOperator>> source =
        create_snapshot_tablet_scan(resources, snapshot, *scan_plan, std::move(*images), lineage,
                                    destination_schema_id, ordinals, limits.scan);
    if (!source.has_value())
      return common::make_unexpected(source.error());
    return pipeline.instantiate(std::move(*source));
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("snapshot pipeline allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("snapshot pipeline exceeds container limits"));
  }
}

} // namespace chronos::query
