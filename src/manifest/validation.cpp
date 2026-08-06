#include "chronos/manifest/validation.hpp"

#include "chronos/common/checked_math.hpp"

#include <algorithm>
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

[[nodiscard]] const TabletDescriptor* find_tablet(const std::span<const TabletDescriptor> tablets,
                                                  const schema::TabletId& tablet_id) noexcept {
  const auto found = std::ranges::lower_bound(
      tablets, tablet_id, {}, [](const TabletDescriptor& tablet) { return tablet.tablet_id; });
  return found != tablets.end() && found->tablet_id == tablet_id ? &*found : nullptr;
}

[[nodiscard]] common::Status validate_tablet_schema(const DecodedManifestView& manifest,
                                                    const TabletDescriptor& tablet,
                                                    const schema::SchemaLineage& lineage) {
  if (lineage.table_id() != tablet.table_id) {
    return invalid("Manifest tablet table identity does not bind to its schema lineage");
  }
  const std::shared_ptr<const schema::TableSchema> recovery_schema =
      lineage.find(tablet.recovery_schema_id);
  if (!recovery_schema || recovery_schema->version() != tablet.recovery_schema_version) {
    return invalid(
        "Manifest tablet recovery schema identity or version is absent from its lineage");
  }
  for (std::uint64_t local_index = 0U; local_index < tablet.part_count; ++local_index) {
    const PartDescriptor& part =
        manifest.parts()[static_cast<std::size_t>(tablet.first_part_index + local_index)];
    const std::shared_ptr<const schema::TableSchema> part_schema = lineage.find(part.schema_id);
    if (!part_schema || part_schema->version() != part.schema_version) {
      return invalid("Manifest part schema identity or version is absent from its tablet lineage");
    }
    const common::Result<schema::SchemaProjection> projection = lineage.projection(
        {.ancestor_schema_id = part.schema_id, .descendant_schema_id = tablet.recovery_schema_id});
    if (!projection.has_value()) {
      return invalid("Manifest part schema is not an ancestor of its tablet recovery schema");
    }
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_bindings(const DecodedManifestView& manifest,
                                               const std::span<const TabletSchemaBinding> bindings,
                                               const bool require_exact) {
  if (require_exact && bindings.size() != manifest.tablets().size()) {
    return invalid("Manifest schema bindings do not exactly cover the tablet array");
  }
  for (std::size_t index = 0U; index < bindings.size(); ++index) {
    if (index != 0U && !(bindings[index - 1U].tablet_id < bindings[index].tablet_id)) {
      return invalid("Manifest schema bindings are not strictly sorted by tablet identity");
    }
  }
  for (std::size_t index = 0U; index < manifest.tablets().size(); ++index) {
    const TabletDescriptor& tablet = manifest.tablets()[index];
    const TabletSchemaBinding* binding =
        require_exact ? &bindings[index] : find_binding(bindings, tablet.tablet_id);
    if (binding == nullptr || binding->tablet_id != tablet.tablet_id) {
      return invalid("Manifest tablet is missing its exact schema binding");
    }
    const common::Status schema_status =
        validate_tablet_schema(manifest, tablet, binding->lineage.get());
    if (!schema_status.is_ok()) {
      return schema_status;
    }
  }
  return common::Status::ok();
}

[[nodiscard]] bool checkpoint_less(const WalCheckpoint& left, const WalCheckpoint& right) noexcept {
  if (left.segment_number != right.segment_number) {
    return left.segment_number < right.segment_number;
  }
  return left.byte_offset < right.byte_offset;
}

[[nodiscard]] const PartDescriptor* find_part(const DecodedManifestView& manifest,
                                              const TabletDescriptor& tablet,
                                              const cseg::PartId& part_id) noexcept {
  const std::span<const PartDescriptor> range =
      manifest.parts().subspan(static_cast<std::size_t>(tablet.first_part_index),
                               static_cast<std::size_t>(tablet.part_count));
  const auto found = std::ranges::lower_bound(
      range, part_id, {}, [](const PartDescriptor& part) { return part.part_id; });
  return found != range.end() && found->part_id == part_id ? &*found : nullptr;
}

[[nodiscard]] bool retry_less(const RetryDescriptor& left, const RetryDescriptor& right) {
  if (left.client_id != right.client_id) {
    return left.client_id < right.client_id;
  }
  return left.client_batch_id < right.client_batch_id;
}

[[nodiscard]] const RetryDescriptor* find_retry(const std::span<const RetryDescriptor> retries,
                                                const RetryDescriptor& retry) noexcept {
  const auto found = std::ranges::lower_bound(retries, retry, retry_less);
  return found != retries.end() && found->client_id == retry.client_id &&
                 found->client_batch_id == retry.client_batch_id
             ? &*found
             : nullptr;
}

} // namespace

common::Status
validate_manifest_v1_schema_binding(const DecodedManifestView& manifest,
                                    const std::span<const TabletSchemaBinding> bindings) {
  return validate_bindings(manifest, bindings, true);
}

common::Status
validate_manifest_v1_transition(const DecodedManifestView& predecessor,
                                const DecodedManifestView& next,
                                const std::span<const TabletSchemaBinding> bindings) {
  common::Status binding_status = validate_bindings(next, bindings, true);
  if (!binding_status.is_ok()) {
    return binding_status;
  }
  binding_status = validate_bindings(predecessor, bindings, false);
  if (!binding_status.is_ok()) {
    return binding_status;
  }
  if (predecessor.database_id() != next.database_id() || predecessor.wal_id() != next.wal_id()) {
    return invalid("Manifest transition changed the database or WAL identity");
  }
  const auto expected_generation = common::checked_add(predecessor.generation(), std::uint64_t{1U});
  if (!expected_generation.has_value() || next.generation() != *expected_generation ||
      next.previous_generation() != predecessor.generation()) {
    return invalid("Manifest transition does not advance exactly one generation");
  }
  const WalCheckpoint& old_checkpoint = predecessor.reclaim_checkpoint();
  const WalCheckpoint& new_checkpoint = next.reclaim_checkpoint();
  if (new_checkpoint.record_sequence < old_checkpoint.record_sequence ||
      checkpoint_less(new_checkpoint, old_checkpoint) ||
      (new_checkpoint.record_sequence == old_checkpoint.record_sequence &&
       new_checkpoint != old_checkpoint) ||
      (new_checkpoint.record_sequence > old_checkpoint.record_sequence &&
       !checkpoint_less(old_checkpoint, new_checkpoint))) {
    return invalid("Manifest transition moved the global reclaim checkpoint backward or sideways");
  }

  for (const TabletDescriptor& old_tablet : predecessor.tablets()) {
    const TabletDescriptor* new_tablet = find_tablet(next.tablets(), old_tablet.tablet_id);
    if (new_tablet == nullptr) {
      return invalid("Manifest transition removed a tablet");
    }
    if (new_tablet->table_id != old_tablet.table_id ||
        new_tablet->durable_record_sequence < old_tablet.durable_record_sequence) {
      return invalid("Manifest transition changed a tablet table or moved its boundary backward");
    }
    const TabletSchemaBinding* binding = find_binding(bindings, old_tablet.tablet_id);
    const common::Result<schema::SchemaProjection> projection =
        binding->lineage.get().projection({.ancestor_schema_id = old_tablet.recovery_schema_id,
                                           .descendant_schema_id = new_tablet->recovery_schema_id});
    if (!projection.has_value()) {
      return invalid("Manifest transition moved a tablet outside its retained schema lineage");
    }
    for (std::uint64_t local_index = 0U; local_index < old_tablet.part_count; ++local_index) {
      const PartDescriptor& old_part =
          predecessor.parts()[static_cast<std::size_t>(old_tablet.first_part_index + local_index)];
      const PartDescriptor* new_part = find_part(next, *new_tablet, old_part.part_id);
      if (new_part == nullptr || *new_part != old_part) {
        return invalid("Manifest Phase 6 transition removed or replaced an installed part");
      }
    }
  }

  for (const RetryDescriptor& old_retry : predecessor.retries()) {
    const RetryDescriptor* new_retry = find_retry(next.retries(), old_retry);
    if (new_retry == nullptr || *new_retry != old_retry) {
      return invalid("Manifest Phase 6 transition removed or changed a protected retry outcome");
    }
  }
  return common::Status::ok();
}

} // namespace chronos::manifest
