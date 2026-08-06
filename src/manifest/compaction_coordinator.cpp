#include "chronos/manifest/compaction_coordinator.hpp"

#include "chronos/common/checked_math.hpp"

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return common::Status{common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status unavailable(std::string message) {
  return common::Status{common::StatusCode::kUnavailable, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status with_context(std::string context, const common::Status& cause) {
  context.append(": ");
  context.append(cause.to_string());
  return common::Status{cause.code(), std::move(context)};
}

void saturating_add(std::uint64_t& value, const std::uint64_t increment) noexcept {
  const std::uint64_t maximum = std::numeric_limits<std::uint64_t>::max();
  value = value > maximum - increment ? maximum : value + increment;
}

[[nodiscard]] const PartDescriptor* find_part(const std::span<const PartDescriptor> parts,
                                              const cseg::PartId& part_id) noexcept {
  const auto found = std::ranges::find(parts, part_id, &PartDescriptor::part_id);
  return found == parts.end() ? nullptr : &*found;
}

[[nodiscard]] const TabletSchemaBinding*
find_binding(const std::span<const TabletSchemaBinding> bindings,
             const schema::TabletId& tablet_id) noexcept {
  const auto found =
      std::ranges::lower_bound(bindings, tablet_id, {}, &TabletSchemaBinding::tablet_id);
  return found != bindings.end() && found->tablet_id == tablet_id ? &*found : nullptr;
}

} // namespace

AppendOnlyCompactionCoordinator::AppendOnlyCompactionCoordinator(
    ManifestStorage& storage, DatabaseStoragePublisher& publisher) noexcept
    : storage_(&storage), publisher_(&publisher) {}

common::Result<AppendOnlyCompactionCoordinator>
AppendOnlyCompactionCoordinator::create(ManifestStorage& storage,
                                        DatabaseStoragePublisher& publisher) {
  if (!storage.is_usable() || !publisher.is_usable()) {
    return common::make_unexpected(
        unavailable("append-only compaction dependencies are not usable"));
  }
  return AppendOnlyCompactionCoordinator{storage, publisher};
}

common::Result<AppendOnlyCompactionCompletion>
AppendOnlyCompactionCoordinator::compact(const AppendOnlyCompactionOperation& operation) {
  saturating_add(metrics_.attempts, 1U);
  if (!is_usable()) {
    saturating_add(metrics_.failures, 1U);
    return common::make_unexpected(unavailable("append-only compaction coordinator failed closed"));
  }
  if (operation.input_part_ids.empty() || operation.part_nonce.is_nil() ||
      operation.manifest_nonce.is_nil()) {
    saturating_add(metrics_.failures, 1U);
    return common::make_unexpected(
        invalid("append-only compaction identities and nonces must be nonempty"));
  }

  bool manifest_is_durable = false;
  auto fail = [&](common::Status status) -> common::Result<AppendOnlyCompactionCompletion> {
    saturating_add(metrics_.failures, 1U);
    if (manifest_is_durable) {
      metrics_.failed = true;
      poison_status_ = status;
    }
    return common::make_unexpected(std::move(status));
  };

  try {
    common::Result<DatabaseStorageSnapshot> published = publisher_->snapshot();
    if (!published.has_value()) {
      return fail(with_context("acquire compaction publication", published.error()));
    }
    common::Result<LoadedManifestGeneration> loaded = storage_->load_selected_manifest(
        {.expected_database_id = published->database_id(),
         .expected_wal_id = published->wal_id(),
         .schema_bindings = operation.schema_bindings,
         .decode_limits = operation.manifest_decode_limits,
         .part_validation_limits = operation.part_validation_limits});
    if (!loaded.has_value()) {
      return fail(with_context("load selected Manifest for compaction", loaded.error()));
    }
    auto selected = std::make_shared<const LoadedManifestGeneration>(std::move(*loaded));
    const std::optional<std::uint64_t> expected_next =
        common::checked_add(published->generation(), std::uint64_t{1U});
    bool resumed = false;
    std::uint64_t row_count = 0U;

    const std::array output_ids{operation.output_part_id};
    const ManifestCompactionReplacement replacement{.tablet_id = operation.tablet_id,
                                                    .input_part_ids = operation.input_part_ids,
                                                    .output_part_ids = output_ids};
    if (selected->generation() == published->generation()) {
      common::Result<std::vector<LoadedPartImage>> owned_inputs =
          storage_->load_selected_part_images(*selected, operation.input_part_ids,
                                              operation.schema_bindings,
                                              operation.part_validation_limits);
      if (!owned_inputs.has_value()) {
        return fail(with_context("load authoritative compaction inputs", owned_inputs.error()));
      }
      std::vector<CompactionPartImage> input_images;
      input_images.reserve(owned_inputs->size());
      for (const LoadedPartImage& image : *owned_inputs) {
        input_images.push_back({.part_id = image.descriptor().part_id, .bytes = image.bytes()});
      }
      const PartDescriptor& first = owned_inputs->front().descriptor();
      const TabletSchemaBinding* binding = nullptr;
      binding = find_binding(operation.schema_bindings, first.tablet_id);
      const std::shared_ptr<const schema::TableSchema> schema_value =
          binding == nullptr ? nullptr : binding->lineage.get().find(first.schema_id);
      if (schema_value == nullptr || first.tablet_id != operation.tablet_id) {
        return fail(invalid("compaction inputs have no exact target schema binding"));
      }
      common::Result<EncodedCompactionPart> merged =
          merge_append_only_cseg_v1({.inputs = input_images,
                                     .schema = std::cref(*schema_value),
                                     .tablet_id = operation.tablet_id,
                                     .wal_id = selected->wal_id(),
                                     .output_part_id = operation.output_part_id,
                                     .compression = operation.compression,
                                     .limits = operation.compaction_limits});
      if (!merged.has_value()) {
        return fail(with_context("merge append-only CSEG inputs", merged.error()));
      }
      row_count = merged->descriptor.row_count;
      const common::Result<InstalledPart> installed_part =
          storage_->install_part({.encoded_part = std::cref(merged->encoded_part),
                                  .descriptor = merged->descriptor,
                                  .wal_id = merged->wal_id,
                                  .schema = std::cref(*schema_value),
                                  .nonce = operation.part_nonce,
                                  .validation_limits = operation.part_validation_limits});
      if (!installed_part.has_value()) {
        return fail(with_context("install compacted CSEG output", installed_part.error()));
      }
      ManifestDecodeResult predecessor =
          decode_manifest_v1_exact(selected->encoded_bytes(), operation.manifest_decode_limits);
      if (!predecessor.has_value()) {
        return fail(corruption("selected compaction predecessor no longer decodes"));
      }
      common::Result<EncodedManifest> candidate = build_manifest_v1_for_append_only_compaction(
          {.predecessor = *predecessor,
           .inputs = input_images,
           .output = std::cref(*merged),
           .schema = std::cref(*schema_value),
           .schema_bindings = operation.schema_bindings,
           .equivalence_limits = operation.compaction_limits.equivalence,
           .part_validation_limits = operation.part_validation_limits});
      if (!candidate.has_value()) {
        return fail(with_context("build compacted Manifest successor", candidate.error()));
      }
      const common::Result<InstalledManifest> installed_manifest = storage_->install_manifest(
          {.encoded_manifest = std::cref(*candidate),
           .schema_bindings = operation.schema_bindings,
           .nonce = operation.manifest_nonce,
           .decode_limits = operation.manifest_decode_limits,
           .part_validation_limits = operation.part_validation_limits,
           .compaction_replacement = &replacement,
           .compaction_equivalence_limits = operation.compaction_limits.equivalence});
      if (!installed_manifest.has_value()) {
        return fail(
            with_context("install compacted Manifest successor", installed_manifest.error()));
      }
      manifest_is_durable = true;
      saturating_add(metrics_.input_parts, operation.input_part_ids.size());
      saturating_add(metrics_.compacted_rows, row_count);
      saturating_add(metrics_.output_bytes, merged->encoded_part.size());
      loaded = storage_->load_selected_manifest(
          {.expected_database_id = published->database_id(),
           .expected_wal_id = published->wal_id(),
           .schema_bindings = operation.schema_bindings,
           .decode_limits = operation.manifest_decode_limits,
           .part_validation_limits = operation.part_validation_limits});
      if (!loaded.has_value()) {
        return fail(with_context("reload compacted Manifest successor", loaded.error()));
      }
      selected = std::make_shared<const LoadedManifestGeneration>(std::move(*loaded));
    } else if (expected_next.has_value() && selected->generation() == *expected_next) {
      manifest_is_durable = true;
      ManifestDecodeResult predecessor =
          decode_manifest_v1_exact(published->manifest_bytes(), operation.manifest_decode_limits);
      ManifestDecodeResult successor =
          decode_manifest_v1_exact(selected->encoded_bytes(), operation.manifest_decode_limits);
      if (!predecessor.has_value() || !successor.has_value()) {
        return fail(corruption("resumed compaction Manifest no longer decodes"));
      }
      common::Status transition = validate_manifest_v1_compaction_transition(
          *predecessor, *successor, operation.schema_bindings, replacement);
      if (!transition.is_ok()) {
        return fail(std::move(transition));
      }
      resumed = true;
    } else {
      manifest_is_durable = selected->generation() > published->generation();
      return fail(corruption("selected Manifest cannot resume this compaction operation"));
    }

    const PartDescriptor* output = nullptr;
    output = find_part(selected->parts(), operation.output_part_id);
    if (output == nullptr) {
      return fail(corruption("compaction successor omits its exact output part"));
    }
    row_count = output->row_count;
    common::Result<DatabaseStorageSnapshot> next =
        publisher_->publish_compaction_manifest({.selected_manifest = selected,
                                                 .schema_bindings = operation.schema_bindings,
                                                 .replacement = replacement});
    if (!next.has_value()) {
      manifest_is_durable = true;
      return fail(with_context("publish compacted Manifest successor", next.error()));
    }
    saturating_add(metrics_.completed, 1U);
    if (resumed) {
      saturating_add(metrics_.resumed_durable_manifests, 1U);
    }
    return AppendOnlyCompactionCompletion{.output_part_id = operation.output_part_id,
                                          .manifest_generation = selected->generation(),
                                          .row_count = row_count,
                                          .resumed_durable_manifest = resumed};
  } catch (const std::bad_alloc&) {
    return fail(exhausted("append-only compaction coordinator allocation failed"));
  } catch (const std::length_error&) {
    return fail(exhausted("append-only compaction coordinator exceeds container limits"));
  }
}

bool AppendOnlyCompactionCoordinator::is_usable() const noexcept {
  return storage_ != nullptr && publisher_ != nullptr && !metrics_.failed &&
         storage_->is_usable() && publisher_->is_usable();
}

common::Status AppendOnlyCompactionCoordinator::poison_status() const {
  if (metrics_.failed) {
    return poison_status_;
  }
  if (storage_ != nullptr && !storage_->is_usable()) {
    return storage_->poison_status();
  }
  if (publisher_ != nullptr && !publisher_->is_usable()) {
    return publisher_->poison_status();
  }
  return common::Status::ok();
}

AppendOnlyCompactionCoordinatorMetrics AppendOnlyCompactionCoordinator::metrics() const noexcept {
  return metrics_;
}

} // namespace chronos::manifest
