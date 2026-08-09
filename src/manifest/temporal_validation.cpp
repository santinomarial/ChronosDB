#include "chronos/manifest/temporal_validation.hpp"

#include "chronos/common/checked_math.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>

namespace chronos::manifest {
namespace {

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

[[nodiscard]] const TabletSchemaBinding*
find_binding(const std::span<const TabletSchemaBinding> bindings,
             const schema::TabletId& tablet_id) noexcept {
  const auto found =
      std::ranges::lower_bound(bindings, tablet_id, {}, [](const TabletSchemaBinding& binding) {
        return binding.tablet_id;
      });
  return found != bindings.end() && found->tablet_id == tablet_id ? &*found : nullptr;
}

[[nodiscard]] const TemporalTabletDescriptor*
find_tablet(const std::span<const TemporalTabletDescriptor> tablets,
            const schema::TabletId& tablet_id) noexcept {
  const auto found =
      std::ranges::lower_bound(tablets, tablet_id, {}, [](const TemporalTabletDescriptor& tablet) {
        return tablet.tablet_id;
      });
  return found != tablets.end() && found->tablet_id == tablet_id ? &*found : nullptr;
}

[[nodiscard]] const TemporalPartDescriptor* find_part(const DecodedTemporalManifestView& manifest,
                                                      const TemporalTabletDescriptor& tablet,
                                                      const cseg::PartId& part_id) noexcept {
  const std::span<const TemporalPartDescriptor> range =
      manifest.parts().subspan(static_cast<std::size_t>(tablet.first_part_index),
                               static_cast<std::size_t>(tablet.part_count));
  const auto found = std::ranges::lower_bound(
      range, part_id, {}, [](const TemporalPartDescriptor& part) { return part.part_id; });
  return found != range.end() && found->part_id == part_id ? &*found : nullptr;
}

[[nodiscard]] bool retry_less(const TemporalRetryDescriptor& left,
                              const TemporalRetryDescriptor& right) noexcept {
  return left.client_id != right.client_id ? left.client_id < right.client_id
                                           : left.client_batch_id < right.client_batch_id;
}

[[nodiscard]] const TemporalRetryDescriptor*
find_retry(const std::span<const TemporalRetryDescriptor> retries,
           const TemporalRetryDescriptor& retry) noexcept {
  const auto found = std::ranges::lower_bound(retries, retry, retry_less);
  return found != retries.end() && found->client_id == retry.client_id &&
                 found->client_batch_id == retry.client_batch_id
             ? &*found
             : nullptr;
}

[[nodiscard]] bool checkpoint_less(const WalCheckpoint& left, const WalCheckpoint& right) noexcept {
  return left.segment_number != right.segment_number ? left.segment_number < right.segment_number
                                                     : left.byte_offset < right.byte_offset;
}

[[nodiscard]] common::Status validate_tablet_schema(const DecodedTemporalManifestView& manifest,
                                                    const TemporalTabletDescriptor& tablet,
                                                    const schema::SchemaLineage& lineage) {
  if (lineage.table_id() != tablet.table_id) {
    return invalid("Manifest v2 tablet table identity does not bind to its schema lineage");
  }
  const std::shared_ptr<const schema::TableSchema> recovery =
      lineage.find(tablet.recovery_schema_id);
  if (!recovery || recovery->version() != tablet.recovery_schema_version) {
    return invalid("Manifest v2 recovery schema is absent from its retained lineage");
  }
  for (std::uint64_t local = 0U; local < tablet.part_count; ++local) {
    const TemporalPartDescriptor& part =
        manifest.parts()[static_cast<std::size_t>(tablet.first_part_index + local)];
    const std::shared_ptr<const schema::TableSchema> part_schema = lineage.find(part.schema_id);
    if (!part_schema || part_schema->version() != part.schema_version) {
      return invalid("Manifest v2 part schema is absent from its retained tablet lineage");
    }
    const common::Result<schema::SchemaProjection> projection = lineage.projection(
        {.ancestor_schema_id = part.schema_id, .descendant_schema_id = tablet.recovery_schema_id});
    if (!projection.has_value()) {
      return invalid("Manifest v2 part schema is not an ancestor of its recovery schema");
    }
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_bindings(const DecodedTemporalManifestView& manifest,
                                               const std::span<const TabletSchemaBinding> bindings,
                                               const bool require_exact) {
  if (require_exact && bindings.size() != manifest.tablets().size()) {
    return invalid("Manifest v2 schema bindings do not exactly cover its tablets");
  }
  for (std::size_t index = 1U; index < bindings.size(); ++index) {
    if (!(bindings[index - 1U].tablet_id < bindings[index].tablet_id)) {
      return invalid("Manifest v2 schema bindings are not strictly sorted");
    }
  }
  for (std::size_t index = 0U; index < manifest.tablets().size(); ++index) {
    const TemporalTabletDescriptor& tablet = manifest.tablets()[index];
    const TabletSchemaBinding* binding =
        require_exact ? &bindings[index] : find_binding(bindings, tablet.tablet_id);
    if (binding == nullptr || binding->tablet_id != tablet.tablet_id) {
      return invalid("Manifest v2 tablet is missing its retained schema binding");
    }
    const common::Status tablet_status =
        validate_tablet_schema(manifest, tablet, binding->lineage.get());
    if (!tablet_status.is_ok()) {
      return tablet_status;
    }
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status
validate_wal_checkpoint_transition(const std::optional<TemporalWalReclaimCheckpoint>& predecessor,
                                   const std::optional<TemporalWalReclaimCheckpoint>& next) {
  if (!predecessor.has_value()) {
    return common::Status::ok();
  }
  if (!next.has_value() || predecessor->wal_id != next->wal_id) {
    return invalid("Manifest v2 transition removed or changed its WAL reclaim lineage");
  }
  const WalCheckpoint& old_coordinate = predecessor->coordinate;
  const WalCheckpoint& new_coordinate = next->coordinate;
  if (new_coordinate.record_sequence < old_coordinate.record_sequence ||
      checkpoint_less(new_coordinate, old_coordinate) ||
      (new_coordinate.record_sequence == old_coordinate.record_sequence &&
       new_coordinate != old_coordinate) ||
      (new_coordinate.record_sequence > old_coordinate.record_sequence &&
       !checkpoint_less(old_coordinate, new_coordinate))) {
    return invalid("Manifest v2 transition moved its WAL reclaim checkpoint backward or sideways");
  }
  return common::Status::ok();
}

} // namespace

common::Status
validate_manifest_v2_temporal_schema_binding(const DecodedTemporalManifestView& manifest,
                                             const std::span<const TabletSchemaBinding> bindings) {
  return validate_bindings(manifest, bindings, true);
}

common::Status
validate_manifest_v2_temporal_transition(const DecodedTemporalManifestView& predecessor,
                                         const DecodedTemporalManifestView& next,
                                         const std::span<const TabletSchemaBinding> bindings) {
  common::Status binding = validate_bindings(next, bindings, true);
  if (!binding.is_ok()) {
    return binding;
  }
  binding = validate_bindings(predecessor, bindings, false);
  if (!binding.is_ok()) {
    return binding;
  }
  if (predecessor.database_id() != next.database_id()) {
    return invalid("Manifest v2 transition changed the database identity");
  }
  const std::optional<std::uint64_t> expected_generation =
      common::checked_add(predecessor.generation(), std::uint64_t{1U});
  if (!expected_generation.has_value() || next.generation() != *expected_generation ||
      next.previous_generation() != predecessor.generation()) {
    return invalid("Manifest v2 transition does not advance exactly one generation");
  }
  common::Status checkpoint = validate_wal_checkpoint_transition(
      predecessor.wal_reclaim_checkpoint(), next.wal_reclaim_checkpoint());
  if (!checkpoint.is_ok()) {
    return checkpoint;
  }

  for (const TemporalTabletDescriptor& old_tablet : predecessor.tablets()) {
    const TemporalTabletDescriptor* new_tablet = find_tablet(next.tablets(), old_tablet.tablet_id);
    if (new_tablet == nullptr) {
      return invalid("Manifest v2 transition removed a tablet");
    }
    if (new_tablet->table_id != old_tablet.table_id ||
        new_tablet->commit_source != old_tablet.commit_source ||
        new_tablet->source_id != old_tablet.source_id ||
        new_tablet->durable_position < old_tablet.durable_position ||
        new_tablet->reclaim_position < old_tablet.reclaim_position) {
      return invalid("Manifest v2 transition changed lineage or regressed a tablet boundary");
    }
    const TabletSchemaBinding* schema_binding = find_binding(bindings, old_tablet.tablet_id);
    const common::Result<schema::SchemaProjection> projection =
        schema_binding->lineage.get().projection(
            {.ancestor_schema_id = old_tablet.recovery_schema_id,
             .descendant_schema_id = new_tablet->recovery_schema_id});
    if (!projection.has_value()) {
      return invalid("Manifest v2 transition regressed a tablet recovery schema");
    }
    for (std::uint64_t local = 0U; local < old_tablet.part_count; ++local) {
      const TemporalPartDescriptor& old_part =
          predecessor.parts()[static_cast<std::size_t>(old_tablet.first_part_index + local)];
      const TemporalPartDescriptor* new_part = find_part(next, *new_tablet, old_part.part_id);
      if (new_part == nullptr || *new_part != old_part) {
        return invalid("Manifest v2 add-only transition removed or changed temporal history");
      }
    }
    for (std::uint64_t local = 0U; local < new_tablet->part_count; ++local) {
      const TemporalPartDescriptor& new_part =
          next.parts()[static_cast<std::size_t>(new_tablet->first_part_index + local)];
      if (find_part(predecessor, old_tablet, new_part.part_id) == nullptr &&
          new_part.minimum_commit_position <= old_tablet.durable_position) {
        return invalid("Manifest v2 transition introduced fresh history below a durable boundary");
      }
    }
  }
  for (const TemporalRetryDescriptor& old_retry : predecessor.retries()) {
    const TemporalRetryDescriptor* new_retry = find_retry(next.retries(), old_retry);
    if (new_retry == nullptr || *new_retry != old_retry) {
      return invalid("Manifest v2 transition removed or changed a protected retry outcome");
    }
  }
  for (const TemporalRetryDescriptor& new_retry : next.retries()) {
    if (find_retry(predecessor.retries(), new_retry) != nullptr) {
      continue;
    }
    const TemporalTabletDescriptor* old_tablet =
        find_tablet(predecessor.tablets(), new_retry.tablet_id);
    if (old_tablet != nullptr && new_retry.commit_position <= old_tablet->durable_position) {
      return invalid("Manifest v2 transition introduced a retry below a durable tablet boundary");
    }
  }
  return common::Status::ok();
}

} // namespace chronos::manifest
