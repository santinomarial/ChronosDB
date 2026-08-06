#include "chronos/manifest/sealed_head_flush_coordinator.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/generation_builder.hpp"
#include "chronos/manifest/sealed_head_flush.hpp"

#include <algorithm>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
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
  const auto found = std::ranges::lower_bound(parts, part_id, {}, &PartDescriptor::part_id);
  return found == parts.end() || found->part_id != part_id ? nullptr : &*found;
}

[[nodiscard]] const ingest::SealedGenerationRetirementReceipt*
find_receipt(const DatabaseStorageSnapshot& snapshot, const head::HeadSnapshot& sealed,
             const PartDescriptor& part, const wal::WalId& wal_id) noexcept {
  const auto found = std::ranges::find_if(
      snapshot.retirement_receipts(),
      [&](const ingest::SealedGenerationRetirementReceipt& receipt) {
        return receipt.table_id() == sealed.table_id() &&
               receipt.tablet_id() == sealed.tablet_id() &&
               receipt.schema_id() == sealed.schema_ptr()->schema_id() &&
               receipt.schema_version() == sealed.schema_ptr()->version() &&
               receipt.head_generation() == sealed.generation() &&
               receipt.row_count() == sealed.row_count() && receipt.wal_id() == wal_id &&
               receipt.minimum_record_sequence() == part.minimum_record_sequence &&
               receipt.maximum_record_sequence() == part.maximum_record_sequence;
      });
  return found == snapshot.retirement_receipts().end() ? nullptr : &*found;
}

} // namespace

SealedHeadFlushCoordinator::SealedHeadFlushCoordinator(
    std::shared_ptr<ingest::SealedHeadFlushQueue> queue, ManifestStorage& storage,
    DatabaseStoragePublisher& publisher) noexcept
    : queue_(std::move(queue)), storage_(&storage), publisher_(&publisher) {}

common::Result<SealedHeadFlushCoordinator>
SealedHeadFlushCoordinator::create(std::shared_ptr<ingest::SealedHeadFlushQueue> queue,
                                   ManifestStorage& storage, DatabaseStoragePublisher& publisher) {
  if (queue == nullptr) {
    return common::make_unexpected(invalid("sealed-head flush coordinator requires a queue"));
  }
  if (!storage.is_usable()) {
    return common::make_unexpected(unavailable("Manifest storage is not usable"));
  }
  if (!publisher.is_usable()) {
    return common::make_unexpected(unavailable("database storage publisher is not usable"));
  }
  return SealedHeadFlushCoordinator{std::move(queue), storage, publisher};
}

common::Result<std::optional<SealedHeadFlushCompletion>>
SealedHeadFlushCoordinator::try_flush_one(ingest::TabletState& tablet,
                                          const SealedHeadFlushOperation& operation) {
  saturating_add(metrics_.attempts, 1U);
  if (!is_usable()) {
    saturating_add(metrics_.failures, 1U);
    return common::make_unexpected(unavailable("sealed-head flush coordinator is failed closed"));
  }
  if (operation.part_nonce.is_nil() || operation.manifest_nonce.is_nil()) {
    saturating_add(metrics_.failures, 1U);
    return common::make_unexpected(invalid("sealed-head flush nonces must be nonzero"));
  }

  common::Result<std::optional<ingest::SealedHeadFlushWork>> acquired = queue_->try_acquire();
  if (!acquired.has_value()) {
    saturating_add(metrics_.failures, 1U);
    return common::make_unexpected(acquired.error());
  }
  if (!acquired->has_value()) {
    saturating_add(metrics_.empty_polls, 1U);
    return std::optional<SealedHeadFlushCompletion>{};
  }

  ingest::SealedHeadFlushWork work = std::move(**acquired);
  const head::HeadSnapshot* const sealed = work.snapshot();
  if (sealed == nullptr) {
    saturating_add(metrics_.failures, 1U);
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "acquired flush work has no snapshot"});
  }
  const std::uint32_t sealed_row_count = sealed->row_count();
  const std::uint64_t work_sequence = work.sequence();

  bool manifest_is_durable = false;
  auto fail =
      [&](common::Status status) -> common::Result<std::optional<SealedHeadFlushCompletion>> {
    saturating_add(metrics_.failures, 1U);
    if (manifest_is_durable) {
      static_cast<void>(tablet.fail_closed());
      poison_status_ = status;
      metrics_.failed = true;
    }
    return common::make_unexpected(std::move(status));
  };

  try {
    const common::Result<ingest::TabletSnapshot> tablet_snapshot = tablet.snapshot();
    if (!tablet_snapshot.has_value()) {
      return fail(
          with_context("acquire tablet state for sealed-head flush", tablet_snapshot.error()));
    }
    if (tablet_snapshot->table_id() != sealed->table_id() ||
        tablet_snapshot->tablet_id() != sealed->tablet_id()) {
      return fail(invalid("flush work does not belong to the supplied tablet owner"));
    }

    const common::Result<EncodedSealedHeadPart> encoded = encode_sealed_head_v1(
        {.snapshot = *sealed, .part_id = operation.part_id, .compression = operation.compression});
    if (!encoded.has_value()) {
      return fail(with_context("encode sealed head", encoded.error()));
    }

    common::Result<DatabaseStorageSnapshot> published = publisher_->snapshot();
    if (!published.has_value()) {
      return fail(with_context("acquire database storage publication", published.error()));
    }
    const common::Result<ManifestNamespaceSnapshot> namespace_snapshot = storage_->scan_namespace();
    if (!namespace_snapshot.has_value()) {
      return fail(with_context("scan Manifest namespace before flush", namespace_snapshot.error()));
    }
    if (namespace_snapshot->generations.empty()) {
      return fail(common::Status{common::StatusCode::kCorruption,
                                 "Manifest namespace has no selected generation"});
    }
    manifest_is_durable = namespace_snapshot->generations.back() != published->generation();
    std::vector<TabletSchemaBinding> predecessor_bindings;
    if (namespace_snapshot->generations.back() == published->generation()) {
      predecessor_bindings.reserve(published->durable_tablets().size());
      for (const TabletDescriptor& descriptor : published->durable_tablets()) {
        const auto binding = std::ranges::find(operation.schema_bindings, descriptor.tablet_id,
                                               &TabletSchemaBinding::tablet_id);
        if (binding == operation.schema_bindings.end()) {
          return fail(invalid("flush schema bindings omit a predecessor tablet"));
        }
        predecessor_bindings.push_back(*binding);
      }
    }
    const std::span<const TabletSchemaBinding> selected_bindings =
        namespace_snapshot->generations.back() == published->generation()
            ? std::span<const TabletSchemaBinding>{predecessor_bindings}
            : operation.schema_bindings;
    common::Result<LoadedManifestGeneration> loaded = storage_->load_selected_manifest(
        {.expected_database_id = published->database_id(),
         .expected_wal_id = published->wal_id(),
         .schema_bindings = selected_bindings,
         .decode_limits = operation.manifest_decode_limits,
         .part_validation_limits = operation.part_validation_limits});
    if (!loaded.has_value()) {
      return fail(with_context("load selected Manifest before flush", loaded.error()));
    }
    auto selected = std::make_shared<const LoadedManifestGeneration>(std::move(*loaded));
    const PartDescriptor* selected_part = find_part(selected->parts(), operation.part_id);

    bool resumed = false;
    const std::optional<std::uint64_t> next_generation =
        common::checked_add(published->generation(), std::uint64_t{1U});
    if (selected->generation() == published->generation()) {
      if (selected_part != nullptr) {
        manifest_is_durable = true;
        resumed = true;
      }
    } else if (next_generation.has_value() && selected->generation() == *next_generation &&
               selected_part != nullptr) {
      manifest_is_durable = true;
      resumed = true;
    } else {
      manifest_is_durable = selected->generation() > published->generation();
      return fail(common::Status{
          common::StatusCode::kCorruption,
          "selected and published Manifest generations cannot resume this queued flush"});
    }

    if (selected_part != nullptr && *selected_part != encoded->descriptor) {
      manifest_is_durable = true;
      return fail(common::Status{common::StatusCode::kCorruption,
                                 "selected flush part disagrees with the queued sealed head"});
    }

    if (!resumed) {
      const common::Result<DatabaseStorageSnapshot> refreshed =
          publisher_->publish_tablet_snapshot(*tablet_snapshot);
      if (!refreshed.has_value()) {
        return fail(with_context("publish current tablet before durable flush", refreshed.error()));
      }
      published = *refreshed;

      const common::Result<InstalledPart> installed_part =
          storage_->install_part({.encoded_part = std::cref(encoded->encoded_part),
                                  .descriptor = encoded->descriptor,
                                  .wal_id = encoded->wal_id,
                                  .schema = std::cref(*sealed->schema_ptr()),
                                  .nonce = operation.part_nonce,
                                  .validation_limits = operation.part_validation_limits});
      if (!installed_part.has_value()) {
        return fail(with_context("install sealed-head CSEG part", installed_part.error()));
      }

      ManifestDecodeResult predecessor =
          decode_manifest_v1_exact(selected->encoded_bytes(), operation.manifest_decode_limits);
      if (!predecessor.has_value()) {
        return fail(common::Status{common::StatusCode::kCorruption,
                                   "selected predecessor failed exact decode during flush"});
      }
      common::Result<EncodedManifest> candidate = build_manifest_v1_for_sealed_head(
          {.predecessor = *predecessor,
           .sealed_part = *encoded,
           .new_retries = operation.new_retries,
           .schema_bindings = operation.schema_bindings,
           .part_validation_limits = operation.part_validation_limits});
      if (!candidate.has_value()) {
        return fail(with_context("build sealed-head Manifest successor", candidate.error()));
      }
      const common::Result<InstalledManifest> installed_manifest =
          storage_->install_manifest({.encoded_manifest = std::cref(*candidate),
                                      .schema_bindings = operation.schema_bindings,
                                      .nonce = operation.manifest_nonce,
                                      .decode_limits = operation.manifest_decode_limits,
                                      .part_validation_limits = operation.part_validation_limits});
      if (!installed_manifest.has_value()) {
        return fail(
            with_context("install sealed-head Manifest successor", installed_manifest.error()));
      }
      manifest_is_durable = true;
      loaded = storage_->load_selected_manifest(
          {.expected_database_id = published->database_id(),
           .expected_wal_id = published->wal_id(),
           .schema_bindings = operation.schema_bindings,
           .decode_limits = operation.manifest_decode_limits,
           .part_validation_limits = operation.part_validation_limits});
      if (!loaded.has_value()) {
        return fail(with_context("reload durable Manifest successor", loaded.error()));
      }
      selected = std::make_shared<const LoadedManifestGeneration>(std::move(*loaded));
      selected_part = find_part(selected->parts(), operation.part_id);
      if (selected_part == nullptr || *selected_part != encoded->descriptor) {
        return fail(common::Status{common::StatusCode::kCorruption,
                                   "reloaded Manifest omits the installed sealed-head part"});
      }
    }

    if (selected->generation() > published->generation()) {
      const SealedHeadReplacement replacement{.tablet_id = sealed->tablet_id(),
                                              .head_generation = sealed->generation(),
                                              .replacement_part_id = operation.part_id};
      const common::Result<DatabaseStorageSnapshot> replacement_publication =
          publisher_->publish_manifest(
              {.selected_manifest = selected, .replacements = {&replacement, 1U}});
      if (!replacement_publication.has_value()) {
        return fail(with_context("publish durable sealed-head replacement",
                                 replacement_publication.error()));
      }
      published = *replacement_publication;
    }

    const ingest::SealedGenerationRetirementReceipt* const receipt =
        find_receipt(*published, *sealed, *selected_part, encoded->wal_id);
    if (receipt == nullptr) {
      return fail(
          common::Status{common::StatusCode::kCorruption,
                         "published sealed-head replacement has no exact retirement receipt"});
    }
    const common::Result<ingest::TabletSnapshot> retired =
        tablet.retire_sealed_generation(*receipt);
    if (!retired.has_value()) {
      return fail(with_context("retire published sealed generation", retired.error()));
    }
    const common::Status completed = work.complete(*receipt);
    if (!completed.is_ok()) {
      return fail(with_context("complete sealed-head flush queue work", completed));
    }

    saturating_add(metrics_.completed, 1U);
    saturating_add(metrics_.encoded_rows, sealed_row_count);
    saturating_add(metrics_.encoded_bytes, encoded->encoded_part.size());
    if (resumed) {
      saturating_add(metrics_.resumed_durable_manifests, 1U);
    }
    return std::optional<SealedHeadFlushCompletion>{
        SealedHeadFlushCompletion{.part_id = operation.part_id,
                                  .manifest_generation = selected->generation(),
                                  .queue_sequence = work_sequence,
                                  .row_count = sealed_row_count,
                                  .resumed_durable_manifest = resumed}};
  } catch (const std::bad_alloc&) {
    return fail(exhausted("sealed-head flush coordinator allocation failed"));
  } catch (const std::length_error&) {
    return fail(exhausted("sealed-head flush coordinator exceeds container limits"));
  }
}

bool SealedHeadFlushCoordinator::is_usable() const noexcept {
  return queue_ != nullptr && storage_ != nullptr && publisher_ != nullptr && !metrics_.failed &&
         storage_->is_usable() && publisher_->is_usable();
}

common::Status SealedHeadFlushCoordinator::poison_status() const {
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

SealedHeadFlushCoordinatorMetrics SealedHeadFlushCoordinator::metrics() const noexcept {
  return metrics_;
}

} // namespace chronos::manifest
