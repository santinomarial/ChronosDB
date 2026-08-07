#include "chronos/query/database_cseg_scan.hpp"

#include "chronos/common/status.hpp"

#include <memory>
#include <string>
#include <utility>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

} // namespace

common::Result<CsegPartPin>
pin_snapshot_cseg_part(std::shared_ptr<const manifest::SnapshotPartImage> image) {
  if (image == nullptr) {
    return common::make_unexpected(invalid("snapshot CSEG pin requires an owning part image"));
  }
  if (image->descriptor().file_length != image->bytes().size()) {
    return common::make_unexpected(
        invalid("snapshot CSEG image length disagrees with its selected descriptor"));
  }
  const common::ByteView bytes = image->bytes();
  const std::size_t retained = image->retained_buffer_bytes();
  std::shared_ptr<const void> owner = std::move(image);
  return CsegPartPin::create(std::move(owner), bytes, retained);
}

common::Result<std::unique_ptr<PhysicalOperator>> create_snapshot_cseg_scan(
    const QueryResourceContext& resources, std::shared_ptr<const manifest::SnapshotPartImage> image,
    const schema::SchemaLineage& lineage, const schema::SchemaId destination_schema_id,
    const schema::TabletId& target_tablet, std::vector<std::uint32_t> destination_column_ordinals,
    const CsegScanLimits limits) {
  if (image == nullptr) {
    return common::make_unexpected(invalid("snapshot CSEG scan requires an owning part image"));
  }
  const manifest::PartDescriptor& descriptor = image->descriptor();
  if (descriptor.tablet_id != target_tablet) {
    return common::make_unexpected(
        invalid("snapshot CSEG descriptor disagrees with the target tablet"));
  }
  const std::shared_ptr<const schema::TableSchema> source_schema =
      lineage.find(descriptor.schema_id);
  const std::shared_ptr<const schema::TableSchema> destination_schema =
      lineage.find(destination_schema_id);
  if (source_schema == nullptr || destination_schema == nullptr) {
    return common::make_unexpected(
        invalid("snapshot CSEG scan schemas are not retained in the supplied lineage"));
  }
  if (source_schema->table_id() != descriptor.table_id ||
      source_schema->version() != descriptor.schema_version ||
      destination_schema->table_id() != descriptor.table_id) {
    return common::make_unexpected(
        invalid("snapshot CSEG descriptor disagrees with its retained schema lineage"));
  }
  common::Result<CsegPartPin> part = pin_snapshot_cseg_part(std::move(image));
  if (!part.has_value())
    return common::make_unexpected(part.error());
  return CsegScanOperator::create(resources, std::move(*part), lineage, destination_schema_id,
                                  target_tablet, std::move(destination_column_ordinals), limits);
}

} // namespace chronos::query
