#include "chronos/query/database_cseg_scan.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"
#include "chronos/manifest/publication.hpp"

#include <algorithm>
#include <array>
#include <bitset>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <optional>
#include <span>
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

[[nodiscard]] common::Status not_found(std::string message) {
  return common::Status{common::StatusCode::kNotFound, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return common::Status{common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Result<std::size_t> add(const std::size_t left, const std::size_t right,
                                              const char* const message) {
  const std::optional<std::size_t> value = common::checked_add(left, right);
  if (!value.has_value())
    return common::make_unexpected(exhausted(message));
  return *value;
}

[[nodiscard]] common::Result<std::size_t>
bytes_for(const std::size_t count, const std::size_t width, const char* const message) {
  const std::optional<std::size_t> value = common::checked_multiply(count, width);
  if (!value.has_value())
    return common::make_unexpected(exhausted(message));
  return *value;
}

[[nodiscard]] const manifest::TabletDescriptor*
find_durable_tablet(const manifest::DatabaseStorageSnapshot& snapshot,
                    const schema::TabletId& tablet_id) noexcept {
  const auto found = std::ranges::lower_bound(snapshot.durable_tablets(), tablet_id, {},
                                              &manifest::TabletDescriptor::tablet_id);
  return found != snapshot.durable_tablets().end() && found->tablet_id == tablet_id ? &*found
                                                                                    : nullptr;
}

[[nodiscard]] common::Result<std::span<const manifest::PartDescriptor>>
tablet_parts(const manifest::DatabaseStorageSnapshot& snapshot,
             const manifest::TabletDescriptor& tablet) {
  if (tablet.first_part_index > snapshot.parts().size() ||
      tablet.part_count > snapshot.parts().size() - tablet.first_part_index) {
    return common::make_unexpected(
        corruption("snapshot durable tablet part range is outside its Manifest"));
  }
  return snapshot.parts().subspan(static_cast<std::size_t>(tablet.first_part_index),
                                  static_cast<std::size_t>(tablet.part_count));
}

[[nodiscard]] common::Status
validate_plan_snapshot(const manifest::DatabaseStorageSnapshot& snapshot,
                       const SnapshotCsegPartScanPlan& plan) {
  if (plan.database_id() != snapshot.database_id() || plan.wal_id() != snapshot.wal_id() ||
      plan.snapshot_generation() != snapshot.generation()) {
    return invalid("snapshot CSEG part-scan plan belongs to another database epoch");
  }
  const manifest::TabletDescriptor* tablet = find_durable_tablet(snapshot, plan.tablet_id());
  if (tablet == nullptr || tablet->table_id != plan.table_id())
    return invalid("snapshot CSEG part-scan plan disagrees with its durable tablet");
  common::Result<std::span<const manifest::PartDescriptor>> parts = tablet_parts(snapshot, *tablet);
  if (!parts.has_value())
    return parts.error();
  std::size_t selected = 0U;
  for (const manifest::PartDescriptor& part : *parts) {
    common::Result<bool> may_match = cseg::cseg_event_time_range_may_match(
        part.minimum_event_time, part.maximum_event_time, plan.event_time_predicate());
    if (!may_match.has_value())
      return may_match.error();
    if (*may_match) {
      if (selected >= plan.selected_part_ids().size() ||
          plan.selected_part_ids()[selected] != part.part_id) {
        return invalid("snapshot CSEG part-scan plan no longer matches its Manifest range");
      }
      ++selected;
    }
  }
  return selected == plan.selected_part_ids().size()
             ? common::Status::ok()
             : invalid("snapshot CSEG part-scan plan omits selected Manifest work");
}

[[nodiscard]] common::Status
validate_projection_request(const schema::TableSchema& destination_schema,
                            const std::vector<std::uint32_t>& destination_column_ordinals,
                            const std::optional<cseg::EventTimePredicate>& predicate,
                            const CsegScanLimits limits) {
  if (limits.chunk.maximum_rows == 0U || limits.chunk.maximum_columns == 0U ||
      limits.chunk.maximum_buffer_bytes == 0U || limits.chunk.maximum_retained_buffer_bytes == 0U) {
    return invalid("snapshot CSEG part-scan chunk limits must be nonzero");
  }
  if (destination_column_ordinals.size() > limits.reader.max_projected_columns ||
      destination_column_ordinals.size() > limits.chunk.maximum_columns) {
    return exhausted("snapshot CSEG part-scan projection exceeds configured limits");
  }
  if (predicate.has_value() && (limits.pruning.max_granules == 0U ||
                                limits.pruning.max_granules > cseg::format::kMaximumGranuleCount)) {
    return invalid("snapshot CSEG part-scan pruning limit is outside the v1 format domain");
  }
  std::bitset<schema::kMaximumSchemaColumnCount> seen;
  for (const std::uint32_t ordinal : destination_column_ordinals) {
    if (ordinal >= destination_schema.columns().size())
      return invalid("snapshot CSEG part-scan projection ordinal is outside the schema");
    if (seen[ordinal])
      return invalid("snapshot CSEG part-scan projection ordinals are not unique");
    seen[ordinal] = true;
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_image(const SnapshotCsegPartScanPlan& plan,
                                            const manifest::SnapshotPartImage& image,
                                            const cseg::PartId& expected_part_id) {
  if (image.database_id() != plan.database_id() || image.wal_id() != plan.wal_id() ||
      image.snapshot_generation() != plan.snapshot_generation()) {
    return invalid("snapshot CSEG part-scan image belongs to another database epoch");
  }
  if (image.descriptor().part_id != expected_part_id ||
      image.descriptor().table_id != plan.table_id() ||
      image.descriptor().tablet_id != plan.tablet_id()) {
    return invalid("snapshot CSEG part-scan image disagrees with its planned identity");
  }
  return common::Status::ok();
}

class SequentialSnapshotCsegScan final : public PhysicalOperator {
public:
  SequentialSnapshotCsegScan(std::vector<std::unique_ptr<PhysicalOperator>> children,
                             QueryMemoryReservation reservation) noexcept
      : children_(std::move(children)), reservation_(std::move(reservation)) {}

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override {
    if (ended_)
      return PhysicalOperatorStep::end();
    common::Result<void> active = resources.check_cancelled();
    if (!active.has_value())
      return common::make_unexpected(active.error());
    if (!resources.owns(reservation_)) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(invalid("snapshot CSEG part scan belongs to another query"));
    }
    while (next_child_ < children_.size()) {
      common::Result<PhysicalOperatorStep> step = children_[next_child_]->next(resources);
      if (!step.has_value()) {
        static_cast<void>(resources.request_cancel());
        return common::make_unexpected(step.error());
      }
      if (step->kind() == PhysicalOperatorStepKind::kEnd) {
        children_[next_child_].reset();
        ++next_child_;
        continue;
      }
      if (step->chunk() == nullptr || !step->chunk()->belongs_to(resources)) {
        static_cast<void>(resources.request_cancel());
        return common::make_unexpected(
            invalid("snapshot CSEG part scan received a foreign or missing chunk"));
      }
      return step;
    }
    std::vector<std::unique_ptr<PhysicalOperator>>{}.swap(children_);
    reservation_.release();
    ended_ = true;
    return PhysicalOperatorStep::end();
  }

private:
  std::vector<std::unique_ptr<PhysicalOperator>> children_;
  QueryMemoryReservation reservation_;
  std::size_t next_child_{};
  bool ended_{};
};

[[nodiscard]] common::Result<std::size_t> sequential_source_charge(const std::size_t child_count) {
  constexpr std::size_t fixed_objects = sizeof(SequentialSnapshotCsegScan) + 256U;
  common::Result<std::size_t> child_slots =
      bytes_for(child_count, sizeof(std::unique_ptr<PhysicalOperator>) + 256U,
                "snapshot CSEG child-source accounting overflowed");
  if (!child_slots.has_value())
    return child_slots;
  common::Result<std::size_t> total =
      add(fixed_objects, *child_slots, "snapshot CSEG source accounting overflowed");
  if (!total.has_value())
    return total;
  common::Result<std::size_t> overhead =
      bytes_for(child_count + 3U, 64U, "snapshot CSEG allocation accounting overflowed");
  return overhead.has_value() ? add(*total, *overhead, "snapshot CSEG source accounting overflowed")
                              : overhead;
}

} // namespace

SnapshotCsegPartScanPlan::SnapshotCsegPartScanPlan(
    const manifest::DatabaseId database_id, const wal::WalId wal_id,
    const std::uint64_t snapshot_generation, const schema::TableId table_id,
    const schema::TabletId tablet_id, std::optional<cseg::EventTimePredicate> event_time_predicate,
    std::vector<cseg::PartId> selected_part_ids, const Metrics metrics) noexcept
    : database_id_(database_id), wal_id_(wal_id), snapshot_generation_(snapshot_generation),
      table_id_(table_id), tablet_id_(tablet_id), event_time_predicate_(event_time_predicate),
      selected_part_ids_(std::move(selected_part_ids)),
      skipped_part_count_(metrics.skipped_part_count), selected_rows_(metrics.selected_rows),
      skipped_rows_(metrics.skipped_rows),
      retained_configuration_bytes_(metrics.retained_configuration_bytes) {}

const manifest::DatabaseId& SnapshotCsegPartScanPlan::database_id() const noexcept {
  return database_id_;
}

const wal::WalId& SnapshotCsegPartScanPlan::wal_id() const noexcept {
  return wal_id_;
}

std::uint64_t SnapshotCsegPartScanPlan::snapshot_generation() const noexcept {
  return snapshot_generation_;
}

const schema::TableId& SnapshotCsegPartScanPlan::table_id() const noexcept {
  return table_id_;
}

const schema::TabletId& SnapshotCsegPartScanPlan::tablet_id() const noexcept {
  return tablet_id_;
}

const std::optional<cseg::EventTimePredicate>&
SnapshotCsegPartScanPlan::event_time_predicate() const noexcept {
  return event_time_predicate_;
}

std::span<const cseg::PartId> SnapshotCsegPartScanPlan::selected_part_ids() const noexcept {
  return selected_part_ids_;
}

std::uint32_t SnapshotCsegPartScanPlan::selected_part_count() const noexcept {
  return static_cast<std::uint32_t>(selected_part_ids_.size());
}

std::uint32_t SnapshotCsegPartScanPlan::skipped_part_count() const noexcept {
  return skipped_part_count_;
}

std::uint64_t SnapshotCsegPartScanPlan::selected_rows() const noexcept {
  return selected_rows_;
}

std::uint64_t SnapshotCsegPartScanPlan::skipped_rows() const noexcept {
  return skipped_rows_;
}

std::size_t SnapshotCsegPartScanPlan::retained_configuration_bytes() const noexcept {
  return retained_configuration_bytes_;
}

common::Result<SnapshotCsegPartScanPlan>
plan_snapshot_cseg_part_scan(const manifest::DatabaseStorageSnapshot& snapshot,
                             const schema::TabletId& target_tablet,
                             const std::optional<cseg::EventTimePredicate>& event_time_predicate,
                             const SnapshotCsegPartScanPlanLimits limits) {
  if (limits.maximum_parts == 0U || limits.maximum_selected_parts == 0U ||
      limits.maximum_retained_configuration_bytes == 0U) {
    return common::make_unexpected(invalid("snapshot CSEG part-scan limits must be nonzero"));
  }
  const manifest::TabletDescriptor* tablet = find_durable_tablet(snapshot, target_tablet);
  if (tablet == nullptr)
    return common::make_unexpected(not_found("snapshot has no durable target tablet"));
  if (tablet->part_count > limits.maximum_parts)
    return common::make_unexpected(exhausted("snapshot CSEG part count exceeds its plan limit"));
  common::Result<std::span<const manifest::PartDescriptor>> parts = tablet_parts(snapshot, *tablet);
  if (!parts.has_value())
    return common::make_unexpected(parts.error());

  std::size_t selected_count = 0U;
  std::uint64_t selected_rows = 0U;
  std::uint64_t skipped_rows = 0U;
  for (const manifest::PartDescriptor& part : *parts) {
    common::Result<bool> may_match = cseg::cseg_event_time_range_may_match(
        part.minimum_event_time, part.maximum_event_time, event_time_predicate);
    if (!may_match.has_value())
      return common::make_unexpected(may_match.error());
    if (*may_match) {
      ++selected_count;
      const std::optional<std::uint64_t> next = common::checked_add(selected_rows, part.row_count);
      if (!next.has_value())
        return common::make_unexpected(exhausted("snapshot CSEG selected rows overflow"));
      selected_rows = *next;
    } else {
      const std::optional<std::uint64_t> next = common::checked_add(skipped_rows, part.row_count);
      if (!next.has_value())
        return common::make_unexpected(exhausted("snapshot CSEG skipped rows overflow"));
      skipped_rows = *next;
    }
  }
  if (selected_count > limits.maximum_selected_parts)
    return common::make_unexpected(exhausted("snapshot CSEG selection exceeds its part limit"));
  common::Result<std::size_t> selected_bytes =
      bytes_for(selected_count, sizeof(cseg::PartId),
                "snapshot CSEG plan configuration accounting overflowed");
  common::Result<std::size_t> requested_retained =
      selected_bytes.has_value() ? add(sizeof(SnapshotCsegPartScanPlan) + 64U, *selected_bytes,
                                       "snapshot CSEG plan configuration accounting overflowed")
                                 : selected_bytes;
  if (!requested_retained.has_value())
    return common::make_unexpected(requested_retained.error());
  if (*requested_retained > limits.maximum_retained_configuration_bytes)
    return common::make_unexpected(
        exhausted("snapshot CSEG plan exceeds its retained configuration limit"));

  try {
    std::vector<cseg::PartId> selected_part_ids;
    selected_part_ids.reserve(selected_count);
    for (const manifest::PartDescriptor& part : *parts) {
      common::Result<bool> may_match = cseg::cseg_event_time_range_may_match(
          part.minimum_event_time, part.maximum_event_time, event_time_predicate);
      if (!may_match.has_value())
        return common::make_unexpected(may_match.error());
      if (*may_match)
        selected_part_ids.push_back(part.part_id);
    }
    common::Result<std::size_t> capacity_bytes =
        bytes_for(selected_part_ids.capacity(), sizeof(cseg::PartId),
                  "snapshot CSEG plan configuration accounting overflowed");
    common::Result<std::size_t> retained =
        capacity_bytes.has_value() ? add(sizeof(SnapshotCsegPartScanPlan) + 64U, *capacity_bytes,
                                         "snapshot CSEG plan configuration accounting overflowed")
                                   : capacity_bytes;
    if (!retained.has_value())
      return common::make_unexpected(retained.error());
    if (*retained > limits.maximum_retained_configuration_bytes)
      return common::make_unexpected(
          exhausted("snapshot CSEG plan exceeds its retained configuration limit"));
    const std::uint32_t skipped =
        static_cast<std::uint32_t>(parts->size() - selected_part_ids.size());
    return SnapshotCsegPartScanPlan{
        snapshot.database_id(),
        snapshot.wal_id(),
        snapshot.generation(),
        tablet->table_id,
        tablet->tablet_id,
        event_time_predicate,
        std::move(selected_part_ids),
        SnapshotCsegPartScanPlan::Metrics{.skipped_part_count = skipped,
                                          .selected_rows = selected_rows,
                                          .skipped_rows = skipped_rows,
                                          .retained_configuration_bytes = *retained}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("cannot allocate snapshot CSEG part-scan plan"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("snapshot CSEG part-scan plan exceeds container limits"));
  }
}

common::Result<std::vector<std::shared_ptr<const manifest::SnapshotPartImage>>>
load_snapshot_cseg_part_scan_images(
    const manifest::ManifestStorage& storage, const manifest::DatabaseStorageSnapshot& snapshot,
    const SnapshotCsegPartScanPlan& plan, const schema::SchemaLineage& lineage,
    const manifest::ReferencedPartValidationLimits validation_limits) {
  common::Status plan_status = validate_plan_snapshot(snapshot, plan);
  if (!plan_status.is_ok())
    return common::make_unexpected(std::move(plan_status));
  if (plan.selected_part_ids().empty())
    return std::vector<std::shared_ptr<const manifest::SnapshotPartImage>>{};
  const std::array bindings{
      manifest::TabletSchemaBinding{.tablet_id = plan.tablet_id(), .lineage = std::cref(lineage)}};
  common::Result<std::vector<manifest::SnapshotPartImage>> loaded =
      storage.load_snapshot_part_images(snapshot, plan.selected_part_ids(), bindings,
                                        validation_limits);
  if (!loaded.has_value())
    return common::make_unexpected(loaded.error());
  try {
    std::vector<std::shared_ptr<const manifest::SnapshotPartImage>> images;
    images.reserve(loaded->size());
    for (std::size_t index = 0U; index < loaded->size(); ++index) {
      common::Status image_status =
          validate_image(plan, (*loaded)[index], plan.selected_part_ids()[index]);
      if (!image_status.is_ok())
        return common::make_unexpected(std::move(image_status));
      images.push_back(
          std::make_shared<const manifest::SnapshotPartImage>(std::move((*loaded)[index])));
    }
    return images;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("cannot allocate snapshot CSEG shared images"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("snapshot CSEG shared images exceed container limits"));
  }
}

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

common::Result<std::unique_ptr<PhysicalOperator>> create_snapshot_cseg_part_scan(
    const QueryResourceContext& resources, const SnapshotCsegPartScanPlan& plan,
    std::vector<std::shared_ptr<const manifest::SnapshotPartImage>> images,
    const schema::SchemaLineage& lineage, const schema::SchemaId destination_schema_id,
    const std::vector<std::uint32_t>& destination_column_ordinals, const CsegScanLimits limits) {
  const std::shared_ptr<const schema::TableSchema> destination_schema =
      lineage.find(destination_schema_id);
  if (destination_schema == nullptr)
    return common::make_unexpected(not_found("snapshot CSEG destination schema is not retained"));
  if (destination_schema->table_id() != plan.table_id())
    return common::make_unexpected(
        invalid("snapshot CSEG destination schema belongs to another table"));
  common::Status projection = validate_projection_request(
      *destination_schema, destination_column_ordinals, plan.event_time_predicate(), limits);
  if (!projection.is_ok())
    return common::make_unexpected(std::move(projection));
  if (images.size() != plan.selected_part_ids().size())
    return common::make_unexpected(
        invalid("snapshot CSEG images do not exactly cover the planned parts"));
  for (std::size_t index = 0U; index < images.size(); ++index) {
    if (images[index] == nullptr)
      return common::make_unexpected(invalid("snapshot CSEG part-scan image is null"));
    common::Status image_status =
        validate_image(plan, *images[index], plan.selected_part_ids()[index]);
    if (!image_status.is_ok())
      return common::make_unexpected(std::move(image_status));
    const std::shared_ptr<const schema::TableSchema> source_schema =
        lineage.find(images[index]->descriptor().schema_id);
    if (source_schema == nullptr || source_schema->table_id() != plan.table_id() ||
        source_schema->version() != images[index]->descriptor().schema_version) {
      return common::make_unexpected(
          invalid("snapshot CSEG image has no exact retained source schema"));
    }
  }

  common::Result<std::size_t> charge = sequential_source_charge(images.size());
  if (!charge.has_value())
    return common::make_unexpected(charge.error());
  common::Result<QueryMemoryReservation> reservation = resources.reserve(*charge);
  if (!reservation.has_value())
    return common::make_unexpected(reservation.error());
  try {
    std::vector<std::unique_ptr<PhysicalOperator>> children;
    children.reserve(images.size());
    const std::optional<cseg::EventTimePredicate>& predicate = plan.event_time_predicate();
    for (std::shared_ptr<const manifest::SnapshotPartImage>& image : images) {
      common::Result<CsegPartPin> part = pin_snapshot_cseg_part(std::move(image));
      if (!part.has_value())
        return common::make_unexpected(part.error());
      std::vector<std::uint32_t> child_ordinals = destination_column_ordinals;
      if (predicate.has_value()) {
        common::Result<std::unique_ptr<PhysicalOperator>> child =
            CsegScanOperator::create_event_time_pruned(
                resources, std::move(*part), lineage, destination_schema_id, plan.tablet_id(),
                std::move(child_ordinals), predicate.value(), limits);
        if (!child.has_value())
          return common::make_unexpected(child.error());
        children.push_back(std::move(*child));
      } else {
        common::Result<std::unique_ptr<PhysicalOperator>> child =
            CsegScanOperator::create(resources, std::move(*part), lineage, destination_schema_id,
                                     plan.tablet_id(), std::move(child_ordinals), limits);
        if (!child.has_value())
          return common::make_unexpected(child.error());
        children.push_back(std::move(*child));
      }
    }
    return std::unique_ptr<PhysicalOperator>{
        new SequentialSnapshotCsegScan{std::move(children), std::move(*reservation)}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("snapshot CSEG source allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("snapshot CSEG source exceeds container limits"));
  }
}

} // namespace chronos::query
