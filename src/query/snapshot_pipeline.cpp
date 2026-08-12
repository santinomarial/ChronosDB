#include "chronos/query/snapshot_pipeline.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"
#include "chronos/manifest/publication.hpp"
#include "chronos/query/snapshot_shape.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
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

class SequentialTabletSources final : public PhysicalOperator {
public:
  SequentialTabletSources(std::vector<std::unique_ptr<PhysicalOperator>> sources,
                          QuerySharedMemoryReservation publication,
                          QueryMemoryReservation reservation) noexcept
      : sources_(std::move(sources)), publication_(std::move(publication)),
        reservation_(std::move(reservation)) {}

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override {
    if (ended_)
      return PhysicalOperatorStep::end();
    auto active = resources.check_cancelled();
    if (!active.has_value())
      return common::make_unexpected(active.error());
    if (!resources.owns(publication_) || !resources.owns(reservation_)) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(
          invalid("multi-tablet snapshot source belongs to another query"));
    }
    while (next_source_ < sources_.size()) {
      auto step = sources_[next_source_]->next(resources);
      if (!step.has_value()) {
        static_cast<void>(resources.request_cancel());
        return common::make_unexpected(step.error());
      }
      if (step->kind() == PhysicalOperatorStepKind::kEnd) {
        sources_[next_source_].reset();
        ++next_source_;
        continue;
      }
      if (step->chunk() == nullptr || !step->chunk()->belongs_to(resources)) {
        static_cast<void>(resources.request_cancel());
        return common::make_unexpected(
            invalid("multi-tablet snapshot source returned a foreign or missing chunk"));
      }
      return step;
    }
    std::vector<std::unique_ptr<PhysicalOperator>>{}.swap(sources_);
    publication_.reset();
    reservation_.release();
    ended_ = true;
    return PhysicalOperatorStep::end();
  }

private:
  std::vector<std::unique_ptr<PhysicalOperator>> sources_;
  QuerySharedMemoryReservation publication_;
  QueryMemoryReservation reservation_;
  std::size_t next_source_{};
  bool ended_{};
};

[[nodiscard]] common::Result<std::size_t>
sequential_tablet_source_charge(const std::size_t source_count) {
  auto slots =
      common::checked_multiply(source_count, sizeof(std::unique_ptr<PhysicalOperator>) + 256U);
  if (!slots.has_value())
    return common::make_unexpected(exhausted("multi-tablet source accounting overflows"));
  auto charge = common::checked_add(sizeof(SequentialTabletSources) + 256U, *slots);
  if (!charge.has_value())
    return common::make_unexpected(exhausted("multi-tablet source accounting overflows"));
  return *charge;
}

[[nodiscard]] common::Status
validate_tablet_vector(const std::span<const schema::TabletId> tablets) {
  if (tablets.empty() || tablets.size() > kDefaultSnapshotMultiTabletLimit)
    return invalid("multi-tablet snapshot source count is invalid");
  for (std::size_t index = 0U; index < tablets.size(); ++index) {
    if (tablets[index].uuid().is_nil() || (index != 0U && tablets[index - 1U] >= tablets[index]))
      return invalid("multi-tablet snapshot vector is not canonical");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>> create_snapshot_tablet_source(
    const QueryResourceContext& resources, const manifest::ManifestStorage& storage,
    const manifest::DatabaseStorageSnapshot& snapshot, const schema::TabletId& target_tablet,
    const schema::SchemaLineage& lineage, const schema::SchemaId destination_schema_id,
    const std::span<const PhysicalColumnShape> input_shape,
    QuerySharedMemoryReservation publication_reservation, SnapshotTabletPipelineLimits limits) {
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
      validate_snapshot_pipeline_input_shape(input_shape, *destination_schema);
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

  std::vector<std::uint32_t> ordinals;
  ordinals.reserve(destination_schema->columns().size());
  for (std::size_t ordinal = 0U; ordinal < destination_schema->columns().size(); ++ordinal)
    ordinals.push_back(static_cast<std::uint32_t>(ordinal));
  if (publication_reservation.is_valid()) {
    if (!resources.owns(publication_reservation) ||
        publication_reservation.bytes() != snapshot.retained_buffer_bytes()) {
      return common::make_unexpected(
          invalid("snapshot pipeline shared publication reservation is invalid"));
    }
  } else {
    common::Result<QuerySharedMemoryReservation> shared =
        resources.reserve_shared(snapshot.retained_buffer_bytes());
    if (!shared.has_value())
      return common::make_unexpected(shared.error());
    publication_reservation = std::move(*shared);
  }
  return create_snapshot_tablet_scan_with_shared_publication(
      resources, std::move(publication_reservation), snapshot, *scan_plan, std::move(*images),
      lineage, destination_schema_id, ordinals, limits.scan);
}

[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>> create_snapshot_tablets_source(
    const QueryResourceContext& resources, const manifest::ManifestStorage& storage,
    const manifest::DatabaseStorageSnapshot& snapshot,
    const std::span<const schema::TabletId> target_tablets, const schema::SchemaLineage& lineage,
    const schema::SchemaId destination_schema_id,
    const std::span<const PhysicalColumnShape> input_shape,
    QuerySharedMemoryReservation publication_reservation,
    const SnapshotTabletPipelineLimits limits) {
  common::Status canonical = validate_tablet_vector(target_tablets);
  if (!canonical.is_ok())
    return common::make_unexpected(std::move(canonical));
  if (target_tablets.size() == 1U) {
    return create_snapshot_tablet_source(resources, storage, snapshot, target_tablets.front(),
                                         lineage, destination_schema_id, input_shape,
                                         std::move(publication_reservation), limits);
  }
  auto charge = sequential_tablet_source_charge(target_tablets.size());
  if (!charge.has_value())
    return common::make_unexpected(charge.error());
  auto reservation = resources.reserve(*charge);
  if (!reservation.has_value())
    return common::make_unexpected(reservation.error());
  std::vector<std::unique_ptr<PhysicalOperator>> sources;
  sources.reserve(target_tablets.size());
  for (const schema::TabletId& tablet : target_tablets) {
    auto source = create_snapshot_tablet_source(resources, storage, snapshot, tablet, lineage,
                                                destination_schema_id, input_shape,
                                                publication_reservation, limits);
    if (!source.has_value())
      return common::make_unexpected(source.error());
    sources.push_back(std::move(*source));
  }
  std::unique_ptr<PhysicalOperator> combined{new SequentialTabletSources{
      std::move(sources), std::move(publication_reservation), std::move(*reservation)}};
  return combined;
}

} // namespace

common::Result<std::unique_ptr<PhysicalOperator>> instantiate_snapshot_tablet_pipeline(
    const QueryResourceContext& resources, const manifest::ManifestStorage& storage,
    const manifest::DatabaseStorageSnapshot& snapshot, const schema::TabletId& target_tablet,
    const schema::SchemaLineage& lineage, const schema::SchemaId destination_schema_id,
    const PhysicalPipelinePlan& pipeline, SnapshotTabletPipelineLimits limits) {
  try {
    common::Result<std::unique_ptr<PhysicalOperator>> source = create_snapshot_tablet_source(
        resources, storage, snapshot, target_tablet, lineage, destination_schema_id,
        pipeline.input_columns(), QuerySharedMemoryReservation{}, limits);
    if (!source.has_value())
      return common::make_unexpected(source.error());
    return pipeline.instantiate(std::move(*source));
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("snapshot pipeline allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("snapshot pipeline exceeds container limits"));
  }
}

common::Result<std::unique_ptr<PhysicalOperator>> instantiate_snapshot_tablets_pipeline(
    const QueryResourceContext& resources, const manifest::ManifestStorage& storage,
    const manifest::DatabaseStorageSnapshot& snapshot,
    const std::span<const schema::TabletId> target_tablets, const schema::SchemaLineage& lineage,
    const schema::SchemaId destination_schema_id, const PhysicalPipelinePlan& pipeline,
    const SnapshotTabletPipelineLimits limits) {
  common::Status canonical = validate_tablet_vector(target_tablets);
  if (!canonical.is_ok())
    return common::make_unexpected(std::move(canonical));
  try {
    auto publication = resources.reserve_shared(snapshot.retained_buffer_bytes());
    if (!publication.has_value())
      return common::make_unexpected(publication.error());
    auto combined = create_snapshot_tablets_source(
        resources, storage, snapshot, target_tablets, lineage, destination_schema_id,
        pipeline.input_columns(), std::move(*publication), limits);
    if (!combined.has_value())
      return common::make_unexpected(combined.error());
    return pipeline.instantiate(std::move(*combined));
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("multi-tablet snapshot allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("multi-tablet snapshot exceeds container limits"));
  }
}

common::Result<std::unique_ptr<PhysicalOperator>> instantiate_optimized_snapshot_tablet_pipeline(
    const QueryResourceContext& resources, const manifest::ManifestStorage& storage,
    const manifest::DatabaseStorageSnapshot& snapshot, const schema::TabletId& target_tablet,
    const schema::SchemaLineage& lineage, const schema::SchemaId destination_schema_id,
    const OptimizedPhysicalPipelinePlan& pipeline,
    std::vector<ExternalSortExecutionTarget> external_sort_targets,
    SnapshotTabletPipelineLimits limits) {
  if (pipeline.source_task_count() != 1U) {
    return common::make_unexpected(
        invalid("optimized snapshot tablet pipeline requires exactly one complete source"));
  }
  try {
    common::Result<std::unique_ptr<PhysicalOperator>> source = create_snapshot_tablet_source(
        resources, storage, snapshot, target_tablet, lineage, destination_schema_id,
        pipeline.pipeline().input_columns(), QuerySharedMemoryReservation{}, limits);
    if (!source.has_value())
      return common::make_unexpected(source.error());
    std::vector<std::unique_ptr<PhysicalOperator>> sources;
    sources.reserve(1U);
    sources.push_back(std::move(*source));
    return pipeline.instantiate(resources, std::move(sources), std::move(external_sort_targets));
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("optimized snapshot pipeline instantiation allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("optimized snapshot pipeline instantiation exceeds container limits"));
  }
}

common::Result<std::unique_ptr<PhysicalOperator>> instantiate_snapshot_asof_plan(
    const QueryResourceContext& resources, const manifest::ManifestStorage& storage,
    const manifest::DatabaseStorageSnapshot& snapshot,
    const std::span<const SnapshotTabletSourceBinding> sources, const PhysicalAsofPlan& plan) {
  if (sources.size() != plan.source_count())
    return common::make_unexpected(invalid("snapshot ASOF source count mismatch"));
  try {
    std::vector<SnapshotTableSourceBinding> tables;
    tables.reserve(sources.size());
    for (const SnapshotTabletSourceBinding& source : sources) {
      tables.push_back({.target_tablets = std::span{&source.target_tablet, std::size_t{1U}},
                        .lineage = source.lineage,
                        .destination_schema_id = source.destination_schema_id,
                        .limits = source.limits});
    }
    return instantiate_snapshot_tables_asof_plan(resources, storage, snapshot, tables, plan);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("snapshot ASOF plan allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("snapshot ASOF plan exceeds container limits"));
  }
}

common::Result<std::unique_ptr<PhysicalOperator>> instantiate_snapshot_tables_asof_plan(
    const QueryResourceContext& resources, const manifest::ManifestStorage& storage,
    const manifest::DatabaseStorageSnapshot& snapshot,
    const std::span<const SnapshotTableSourceBinding> sources, const PhysicalAsofPlan& plan) {
  if (sources.size() != plan.source_count())
    return common::make_unexpected(invalid("snapshot ASOF source count mismatch"));
  try {
    common::Result<QuerySharedMemoryReservation> publication_reservation =
        resources.reserve_shared(snapshot.retained_buffer_bytes());
    if (!publication_reservation.has_value())
      return common::make_unexpected(publication_reservation.error());
    std::vector<std::unique_ptr<PhysicalOperator>> operators;
    operators.reserve(sources.size());
    const std::span<const PhysicalAsofPlanJoin> joins = plan.joins();
    for (std::size_t ordinal = 0U; ordinal < sources.size(); ++ordinal) {
      const SnapshotTableSourceBinding& source = sources[ordinal];
      const std::span<const PhysicalColumnShape> expected =
          ordinal == 0U ? joins.front().left_preparation.input_columns()
                        : joins[ordinal - 1U].right_preparation.input_columns();
      common::Result<std::unique_ptr<PhysicalOperator>> created = create_snapshot_tablets_source(
          resources, storage, snapshot, source.target_tablets, source.lineage.get(),
          source.destination_schema_id, expected, *publication_reservation, source.limits);
      if (!created.has_value())
        return common::make_unexpected(created.error());
      operators.push_back(std::move(*created));
    }
    return plan.instantiate(std::move(operators));
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("snapshot ASOF plan allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("snapshot ASOF plan exceeds container limits"));
  }
}

} // namespace chronos::query
