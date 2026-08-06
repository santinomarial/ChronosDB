#include "chronos/manifest/validation.hpp"

#include "chronos/common/checked_math.hpp"

#include <algorithm>
#include <cstddef>
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

[[nodiscard]] const PartDescriptor* find_part_globally(const DecodedManifestView& manifest,
                                                       const cseg::PartId& part_id) noexcept {
  const auto found = std::ranges::find(manifest.parts(), part_id, &PartDescriptor::part_id);
  return found != manifest.parts().end() ? &*found : nullptr;
}

[[nodiscard]] bool same_tablet_state(const TabletDescriptor& left,
                                     const TabletDescriptor& right) noexcept {
  return left.table_id == right.table_id && left.tablet_id == right.tablet_id &&
         left.recovery_schema_id == right.recovery_schema_id &&
         left.recovery_schema_version == right.recovery_schema_version &&
         left.durable_record_sequence == right.durable_record_sequence &&
         left.durable_row_count == right.durable_row_count;
}

[[nodiscard]] bool strictly_sorted(const std::span<const cseg::PartId> ids) noexcept {
  for (std::size_t index = 1U; index < ids.size(); ++index) {
    if (!(ids[index - 1U] < ids[index])) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] bool contains_id(const std::span<const cseg::PartId> ids,
                               const cseg::PartId& id) noexcept {
  return std::ranges::binary_search(ids, id);
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

common::Status
validate_manifest_v1_compaction_transition(const DecodedManifestView& predecessor,
                                           const DecodedManifestView& next,
                                           const std::span<const TabletSchemaBinding> bindings,
                                           const ManifestCompactionReplacement& replacement) {
  common::Status binding_status = validate_bindings(predecessor, bindings, true);
  if (!binding_status.is_ok()) {
    return binding_status;
  }
  binding_status = validate_bindings(next, bindings, true);
  if (!binding_status.is_ok()) {
    return binding_status;
  }
  if (replacement.input_part_ids.empty() || replacement.output_part_ids.empty() ||
      !strictly_sorted(replacement.input_part_ids) ||
      !strictly_sorted(replacement.output_part_ids)) {
    return invalid("Manifest compaction part identities must be nonempty and strictly sorted");
  }
  if (predecessor.database_id() != next.database_id() || predecessor.wal_id() != next.wal_id()) {
    return invalid("Manifest compaction changed the database or WAL identity");
  }
  const auto expected_generation = common::checked_add(predecessor.generation(), std::uint64_t{1U});
  if (!expected_generation.has_value() || next.generation() != *expected_generation ||
      next.previous_generation() != predecessor.generation()) {
    return invalid("Manifest compaction does not advance exactly one generation");
  }
  if (predecessor.reclaim_checkpoint() != next.reclaim_checkpoint()) {
    return invalid("Manifest compaction changed the reclaim checkpoint");
  }
  if (predecessor.tablets().size() != next.tablets().size()) {
    return invalid("Manifest compaction changed the tablet set");
  }
  if (!std::ranges::equal(predecessor.retries(), next.retries())) {
    return invalid("Manifest compaction changed protected retry state");
  }

  const TabletDescriptor* old_target = find_tablet(predecessor.tablets(), replacement.tablet_id);
  const TabletDescriptor* new_target = find_tablet(next.tablets(), replacement.tablet_id);
  if (old_target == nullptr || new_target == nullptr) {
    return invalid("Manifest compaction target tablet is absent from a generation");
  }

  for (std::size_t index = 0U; index < predecessor.tablets().size(); ++index) {
    const TabletDescriptor& old_tablet = predecessor.tablets()[index];
    const TabletDescriptor& new_tablet = next.tablets()[index];
    if (!same_tablet_state(old_tablet, new_tablet)) {
      return invalid("Manifest compaction changed durable tablet state");
    }
    if (old_tablet.tablet_id == replacement.tablet_id) {
      continue;
    }
    if (old_tablet.part_count != new_tablet.part_count) {
      return invalid("Manifest compaction changed an unrelated tablet part set");
    }
    for (std::uint64_t local_index = 0U; local_index < old_tablet.part_count; ++local_index) {
      const PartDescriptor& old_part =
          predecessor.parts()[static_cast<std::size_t>(old_tablet.first_part_index + local_index)];
      const PartDescriptor& new_part =
          next.parts()[static_cast<std::size_t>(new_tablet.first_part_index + local_index)];
      if (old_part != new_part) {
        return invalid("Manifest compaction replaced an unrelated part");
      }
    }
  }

  const PartDescriptor* first_input =
      find_part(predecessor, *old_target, replacement.input_part_ids.front());
  if (first_input == nullptr) {
    return invalid("Manifest compaction input is absent from the target tablet");
  }
  for (const cseg::PartId& input_id : replacement.input_part_ids) {
    const PartDescriptor* input = find_part(predecessor, *old_target, input_id);
    if (input == nullptr || input->schema_id != first_input->schema_id ||
        input->schema_version != first_input->schema_version) {
      return invalid("Manifest compaction inputs do not share one target schema");
    }
    if (find_part(next, *new_target, input_id) != nullptr) {
      return invalid("Manifest compaction retained an authorized input identity");
    }
  }
  for (const cseg::PartId& output_id : replacement.output_part_ids) {
    if (find_part_globally(predecessor, output_id) != nullptr) {
      return invalid("Manifest compaction output identity is not fresh");
    }
    const PartDescriptor* output = find_part(next, *new_target, output_id);
    if (output == nullptr || output->schema_id != first_input->schema_id ||
        output->schema_version != first_input->schema_version) {
      return invalid("Manifest compaction output is absent or uses a different schema");
    }
  }

  for (std::uint64_t local_index = 0U; local_index < old_target->part_count; ++local_index) {
    const PartDescriptor& old_part =
        predecessor.parts()[static_cast<std::size_t>(old_target->first_part_index + local_index)];
    const PartDescriptor* new_part = find_part(next, *new_target, old_part.part_id);
    if (contains_id(replacement.input_part_ids, old_part.part_id)) {
      if (new_part != nullptr) {
        return invalid("Manifest compaction did not remove exactly its authorized inputs");
      }
    } else if (new_part == nullptr || *new_part != old_part) {
      return invalid("Manifest compaction removed or changed an unauthorized target part");
    }
  }
  for (std::uint64_t local_index = 0U; local_index < new_target->part_count; ++local_index) {
    const PartDescriptor& new_part =
        next.parts()[static_cast<std::size_t>(new_target->first_part_index + local_index)];
    const PartDescriptor* old_part = find_part(predecessor, *old_target, new_part.part_id);
    if (old_part == nullptr && !contains_id(replacement.output_part_ids, new_part.part_id)) {
      return invalid("Manifest compaction added an unauthorized target part");
    }
  }
  return common::Status::ok();
}

} // namespace chronos::manifest
