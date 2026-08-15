#include "chronos/tiering/tiered_reclamation.hpp"

#include "chronos/ingest/sha256.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/tiering/tiered_part_loader.hpp"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <memory>
#include <ranges>
#include <utility>

namespace chronos::tiering {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] bool same_cold_authority(const DecodedColdLocationManifest& left,
                                       const DecodedColdLocationManifest& right) noexcept {
  return left.generation() == right.generation() &&
         left.previous_generation() == right.previous_generation() &&
         left.base_manifest_generation() == right.base_manifest_generation() &&
         left.database_id() == right.database_id() &&
         left.object_store_id() == right.object_store_id() &&
         left.encoded_size() == right.encoded_size() &&
         std::ranges::equal(left.locations(), right.locations());
}

[[nodiscard]] const manifest::TemporalTabletDescriptor*
find_tablet(const std::span<const manifest::TemporalTabletDescriptor> tablets,
            const schema::TabletId& tablet_id) noexcept {
  const auto found =
      std::ranges::find(tablets, tablet_id, &manifest::TemporalTabletDescriptor::tablet_id);
  return found == tablets.end() ? nullptr : std::addressof(*found);
}

[[nodiscard]] const manifest::TabletSchemaBinding*
find_binding(const std::span<const manifest::TabletSchemaBinding> bindings,
             const schema::TabletId& tablet_id) noexcept {
  const auto found =
      std::ranges::find(bindings, tablet_id, &manifest::TabletSchemaBinding::tablet_id);
  return found == bindings.end() ? nullptr : std::addressof(*found);
}

} // namespace

TieredLocalPartReclamationProof::TieredLocalPartReclamationProof(
    TieredPairCommitRecord record, TieredDatabaseStorageSnapshot snapshot,
    std::vector<manifest::TemporalPartDescriptor> parts,
    std::vector<std::weak_ptr<const detail::TieredDatabaseStorageEpoch>> reader_pins) noexcept
    : record_(record), snapshot_(std::move(snapshot)), parts_(std::move(parts)),
      reader_pins_(std::move(reader_pins)) {}

std::uint64_t TieredLocalPartReclamationProof::pair_generation() const noexcept {
  return record_.generation;
}

std::uint64_t TieredLocalPartReclamationProof::manifest_generation() const noexcept {
  return snapshot_.manifest_generation();
}

std::span<const manifest::TemporalPartDescriptor>
TieredLocalPartReclamationProof::parts() const noexcept {
  return parts_;
}

bool TieredLocalPartReclamationProof::is_pinned() const noexcept {
  return std::ranges::any_of(reader_pins_, [](const auto& pin) { return !pin.expired(); });
}

TieredRemoteObjectReclamationProof::TieredRemoteObjectReclamationProof(
    TieredPairCommitRecord record, TieredDatabaseStorageSnapshot snapshot,
    std::vector<ColdPartLocationDescriptor> locations,
    std::vector<std::weak_ptr<const detail::TieredDatabaseStorageEpoch>> reader_pins) noexcept
    : record_(record), snapshot_(std::move(snapshot)), locations_(std::move(locations)),
      reader_pins_(std::move(reader_pins)) {}

std::uint64_t TieredRemoteObjectReclamationProof::pair_generation() const noexcept {
  return record_.generation;
}

std::uint64_t TieredRemoteObjectReclamationProof::manifest_generation() const noexcept {
  return snapshot_.manifest_generation();
}

std::span<const ColdPartLocationDescriptor>
TieredRemoteObjectReclamationProof::locations() const noexcept {
  return locations_;
}

bool TieredRemoteObjectReclamationProof::is_pinned() const noexcept {
  return std::ranges::any_of(reader_pins_, [](const auto& pin) { return !pin.expired(); });
}

common::Result<TieredLocalPartReclamationReport> TieredLocalPartReclamationCoordinator::reclaim(
    const TieredLocalPartReclamationProof& proof, const TieredPairCommitStorage& pair_storage,
    manifest::ManifestStorage& manifest_storage, const ObjectStore& remote_store,
    const std::span<const manifest::TabletSchemaBinding> schema_bindings,
    const TieredLocalPartReclamationLimits limits) {
  TieredLocalPartReclamationReport report{
      .outcome = manifest::PartReclamationOutcome::kPending,
      .pair_generation = proof.record_.generation,
      .manifest_generation = proof.snapshot_.manifest_generation(),
      .candidate_parts = static_cast<std::uint64_t>(proof.parts_.size()),
  };
  if (proof.parts_.empty())
    return common::make_unexpected(invalid("tiered local reclamation proof is empty"));
  if (proof.is_pinned())
    return report;

  auto selected_pair = pair_storage.load_selected_record();
  if (!selected_pair.has_value())
    return common::make_unexpected(selected_pair.error());
  if (*selected_pair != proof.record_) {
    return common::make_unexpected(
        invalid("tiered local reclamation pair is no longer the selected durable authority"));
  }
  for (const manifest::TemporalPartDescriptor& descriptor : proof.parts_) {
    const ColdPartLocationDescriptor* location =
        proof.snapshot_.find_cold_location(descriptor.part_id);
    const manifest::TemporalTabletDescriptor* owner =
        find_tablet(proof.snapshot_.manifest_snapshot().tablets(), descriptor.tablet_id);
    const manifest::TabletSchemaBinding* binding =
        find_binding(schema_bindings, descriptor.tablet_id);
    const std::shared_ptr<const schema::TableSchema> schema_value =
        binding == nullptr ? nullptr : binding->lineage.get().find(descriptor.schema_id);
    if (location == nullptr || owner == nullptr || schema_value == nullptr) {
      return common::make_unexpected(
          invalid("tiered local reclamation lost its exact route, tablet, or schema"));
    }
    auto remote = load_validated_remote_temporal_part_image(
        remote_store, *location, descriptor, *owner, *schema_value, limits.part_validation);
    if (!remote.has_value())
      return common::make_unexpected(remote.error());
    ++report.remote_parts_validated;
  }

  auto selected_owner = proof.snapshot_.manifest_snapshot().selected_manifest();
  if (selected_owner == nullptr)
    return common::make_unexpected(corruption("tiered reclamation Manifest owner disappeared"));
  manifest::TieredLocalPartReclamationAuthority authority{selected_owner, proof.parts_};
  auto reclaimed = manifest_storage.reclaim_tiered_local_temporal_parts(
      {.authority = std::cref(authority),
       .decode_limits = limits.manifest_decode,
       .part_validation_limits = limits.part_validation});
  if (!reclaimed.has_value())
    return common::make_unexpected(reclaimed.error());
  report.outcome = reclaimed->outcome;
  report.removed_parts = reclaimed->removed_parts;
  report.removed_bytes = reclaimed->removed_bytes;
  report.already_absent_parts = reclaimed->already_absent_parts;
  report.directory_syncs = reclaimed->directory_syncs;
  return report;
}

common::Result<TieredRemoteObjectReclamationReport>
TieredRemoteObjectReclamationCoordinator::reclaim(const TieredRemoteObjectReclamationProof& proof,
                                                  const TieredPairCommitStorage& pair_storage,
                                                  ObjectStore& remote_store) {
  TieredRemoteObjectReclamationReport report{
      .outcome = manifest::PartReclamationOutcome::kPending,
      .pair_generation = proof.record_.generation,
      .manifest_generation = proof.snapshot_.manifest_generation(),
      .candidate_objects = static_cast<std::uint64_t>(proof.locations_.size()),
  };
  if (proof.locations_.empty())
    return common::make_unexpected(invalid("tiered remote reclamation proof is empty"));
  if (proof.is_pinned())
    return report;

  auto selected_pair = pair_storage.load_selected_record();
  if (!selected_pair.has_value())
    return common::make_unexpected(selected_pair.error());
  if (*selected_pair != proof.record_) {
    return common::make_unexpected(
        invalid("tiered remote reclamation pair is no longer the selected durable authority"));
  }
  const LoadedColdLocationManifest* current_cold = proof.snapshot_.cold_manifest();
  if (current_cold == nullptr)
    return common::make_unexpected(corruption("tiered remote reclamation cold owner disappeared"));
  const auto current_parts = proof.snapshot_.manifest_snapshot().parts();
  const auto current_locations = current_cold->manifest().locations();
  for (const ColdPartLocationDescriptor& retired : proof.locations_) {
    if (retired.file_length > std::numeric_limits<std::size_t>::max()) {
      return common::make_unexpected(
          invalid("tiered remote reclamation object is not addressable on this host"));
    }
    if (std::ranges::find(current_parts, retired.part_id,
                          &manifest::TemporalPartDescriptor::part_id) != current_parts.end() ||
        std::ranges::find(current_locations, retired.part_id,
                          &ColdPartLocationDescriptor::part_id) != current_locations.end() ||
        std::ranges::find(current_locations, retired.object_key,
                          &ColdPartLocationDescriptor::object_key) != current_locations.end()) {
      return common::make_unexpected(
          invalid("tiered remote reclamation candidate returned to current authority"));
    }
    auto metadata = remote_store.stat(retired.object_key);
    if (!metadata.has_value()) {
      if (metadata.error().code() == common::StatusCode::kNotFound)
        continue;
      return common::make_unexpected(metadata.error());
    }
    if (metadata->key != retired.object_key ||
        metadata->size != static_cast<std::size_t>(retired.file_length) ||
        metadata->checksum != retired.content_sha256) {
      return common::make_unexpected(
          corruption("tiered remote reclamation object metadata differs"));
    }
    ++report.metadata_validated;
  }

  for (const ColdPartLocationDescriptor& retired : proof.locations_) {
    auto removed = remote_store.remove_if_exact(
        retired.object_key, static_cast<std::size_t>(retired.file_length), retired.content_sha256);
    if (!removed.has_value())
      return common::make_unexpected(removed.error());
    if (removed->removed == removed->already_absent) {
      return common::make_unexpected(
          corruption("object store returned an invalid exact-deletion outcome"));
    }
    if (removed->removed)
      ++report.removed_objects;
    if (removed->already_absent)
      ++report.already_absent_objects;
  }
  report.outcome = manifest::PartReclamationOutcome::kReclaimed;
  return report;
}

common::Result<TieredRestartRemoteGarbageReport>
TieredRestartRemoteGarbageCoordinator::reclaim_unreachable(
    const RecoveredTieredPair& recovered_pair, const TieredPairCommitStorage& pair_storage,
    manifest::ManifestStorage& manifest_storage, const ColdLocationManifestStorage& cold_storage,
    const std::span<const TieredRestartManifestBinding> manifest_bindings,
    ObjectStore& remote_store, const TieredRestartRemoteGarbageLimits limits) {
  TieredRestartRemoteGarbageReport report{
      .pair_generation = recovered_pair.record.generation,
      .manifest_generation = recovered_pair.record.manifest_generation,
  };
  if (limits.maximum_cold_generations == 0U || limits.maximum_objects == 0U) {
    return common::make_unexpected(invalid("tiered restart garbage request is invalid"));
  }
  for (std::size_t index = 0U; index < manifest_bindings.size(); ++index) {
    if (manifest_bindings[index].manifest_generation == 0U ||
        (index > 0U && manifest_bindings[index - 1U].manifest_generation >=
                           manifest_bindings[index].manifest_generation)) {
      return common::make_unexpected(
          invalid("tiered restart garbage Manifest bindings are not strictly sorted"));
    }
  }
  auto selected_pair = pair_storage.load_selected_record();
  if (!selected_pair.has_value())
    return common::make_unexpected(selected_pair.error());
  if (!selected_pair->has_value() || **selected_pair != recovered_pair.record) {
    return common::make_unexpected(
        invalid("tiered restart garbage pair is not selected durable authority"));
  }
  const auto& current_snapshot = recovered_pair.manifest_snapshot;
  if (current_snapshot.database_id() != recovered_pair.record.database_id ||
      current_snapshot.generation() != recovered_pair.record.manifest_generation ||
      current_snapshot.manifest_bytes().size() != recovered_pair.record.manifest_length) {
    return common::make_unexpected(
        corruption("tiered restart garbage recovered Manifest owner differs"));
  }
  auto current_manifest_digest = ingest::sha256(current_snapshot.manifest_bytes());
  if (!current_manifest_digest.has_value() ||
      *current_manifest_digest != recovered_pair.record.manifest_sha256) {
    return common::make_unexpected(
        corruption("tiered restart garbage recovered Manifest digest differs"));
  }
  auto current_manifest = manifest::decode_manifest_v2_temporal_exact(
      current_snapshot.manifest_bytes(), limits.manifest_decode);
  if (!current_manifest.has_value())
    return common::make_unexpected(current_manifest.error().status());

  if (recovered_pair.record.cold_generation == 0U) {
    if (recovered_pair.cold_manifest != nullptr)
      return common::make_unexpected(
          corruption("tiered restart garbage has unexpected cold authority"));
    return report;
  }
  if (manifest_bindings.empty()) {
    return common::make_unexpected(
        invalid("tiered restart garbage lacks historical Manifest bindings"));
  }
  if (recovered_pair.cold_manifest == nullptr) {
    return common::make_unexpected(
        corruption("tiered restart garbage recovered cold owner disappeared"));
  }
  if (recovered_pair.record.cold_generation > limits.maximum_cold_generations ||
      recovered_pair.record.cold_generation > std::numeric_limits<std::size_t>::max()) {
    return common::make_unexpected(
        exhausted("tiered restart garbage cold history exceeds configured limits"));
  }
  const LoadedColdLocationManifest& selected_cold = *recovered_pair.cold_manifest;
  if (selected_cold.manifest().generation() != recovered_pair.record.cold_generation ||
      selected_cold.manifest().database_id() != recovered_pair.record.database_id ||
      selected_cold.manifest().object_store_id() != recovered_pair.record.object_store_id ||
      selected_cold.encoded_bytes().size() != recovered_pair.record.cold_length) {
    return common::make_unexpected(
        corruption("tiered restart garbage recovered cold authority differs"));
  }
  auto current_cold_digest = ingest::sha256(selected_cold.encoded_bytes());
  if (!current_cold_digest.has_value() ||
      *current_cold_digest != recovered_pair.record.cold_sha256) {
    return common::make_unexpected(
        corruption("tiered restart garbage recovered cold digest differs"));
  }
  common::Status binding =
      validate_cold_location_manifest_binding(selected_cold.manifest(), *current_manifest);
  if (!binding.is_ok())
    return common::make_unexpected(std::move(binding));

  try {
    std::vector<ColdPartLocationDescriptor> candidates;
    std::optional<DecodedColdLocationManifest> predecessor;
    const auto current_parts = current_snapshot.parts();
    const auto current_locations = selected_cold.manifest().locations();
    for (std::uint64_t generation = 1U; generation <= recovered_pair.record.cold_generation;
         ++generation) {
      auto cold = cold_storage.load_generation_metadata(generation);
      if (!cold.has_value())
        return common::make_unexpected(cold.error());
      const auto historical_binding =
          std::ranges::lower_bound(manifest_bindings, cold->base_manifest_generation(), {},
                                   &TieredRestartManifestBinding::manifest_generation);
      if (historical_binding == manifest_bindings.end() ||
          historical_binding->manifest_generation != cold->base_manifest_generation()) {
        return common::make_unexpected(
            invalid("tiered restart garbage lacks an exact historical Manifest binding"));
      }
      const manifest::TemporalManifestLoadRequest historical_request{
          .expected_database_id = recovered_pair.record.database_id,
          .schema_bindings = historical_binding->schema_bindings,
          .source_bindings = historical_binding->source_bindings,
          .decode_limits = limits.manifest_decode,
          .part_validation_limits = {}};
      auto base_metadata = manifest_storage.load_temporal_manifest_metadata(
          cold->base_manifest_generation(), historical_request);
      if (!base_metadata.has_value())
        return common::make_unexpected(base_metadata.error());
      auto base_manifest = manifest::decode_manifest_v2_temporal_exact(
          base_metadata->encoded_bytes(), limits.manifest_decode);
      if (!base_manifest.has_value())
        return common::make_unexpected(base_manifest.error().status());
      binding = validate_cold_location_manifest_binding(*cold, *base_manifest);
      if (!binding.is_ok())
        return common::make_unexpected(std::move(binding));
      if (predecessor.has_value()) {
        common::Status transition =
            validate_cold_location_manifest_transition(*predecessor, *cold, *base_manifest);
        if (!transition.is_ok())
          return common::make_unexpected(std::move(transition));
      }
      ++report.cold_generations_validated;
      if (generation == recovered_pair.record.cold_generation) {
        if (!same_cold_authority(*cold, selected_cold.manifest())) {
          return common::make_unexpected(
              corruption("tiered restart garbage selected cold metadata differs"));
        }
        break;
      }

      for (const ColdPartLocationDescriptor& historical : cold->locations()) {
        const auto current_part = std::ranges::find(current_parts, historical.part_id,
                                                    &manifest::TemporalPartDescriptor::part_id);
        const auto current_route = std::ranges::lower_bound(
            current_locations, historical.part_id, {}, &ColdPartLocationDescriptor::part_id);
        const auto current_key = std::ranges::find(current_locations, historical.object_key,
                                                   &ColdPartLocationDescriptor::object_key);
        if (current_route != current_locations.end() &&
            current_route->part_id == historical.part_id) {
          if (*current_route != historical)
            return common::make_unexpected(
                corruption("tiered restart garbage historical route changed identity"));
          continue;
        }
        if (current_key != current_locations.end()) {
          return common::make_unexpected(
              corruption("tiered restart garbage historical object key was reused"));
        }
        if (current_part != current_parts.end())
          continue;

        const auto same_part =
            std::ranges::find(candidates, historical.part_id, &ColdPartLocationDescriptor::part_id);
        if (same_part != candidates.end()) {
          if (*same_part != historical)
            return common::make_unexpected(
                corruption("tiered restart garbage retired route changed identity"));
          continue;
        }
        if (std::ranges::find(candidates, historical.object_key,
                              &ColdPartLocationDescriptor::object_key) != candidates.end()) {
          return common::make_unexpected(
              corruption("tiered restart garbage retired object key is duplicated"));
        }
        if (candidates.size() >= limits.maximum_objects)
          return common::make_unexpected(
              exhausted("tiered restart garbage objects exceed configured limits"));
        candidates.push_back(historical);
      }
      predecessor = std::move(*cold);
    }
    std::ranges::sort(candidates, {}, &ColdPartLocationDescriptor::part_id);
    report.candidate_objects = static_cast<std::uint64_t>(candidates.size());

    for (const ColdPartLocationDescriptor& candidate : candidates) {
      if (candidate.file_length > std::numeric_limits<std::size_t>::max()) {
        return common::make_unexpected(
            invalid("tiered restart garbage object is not addressable on this host"));
      }
      auto metadata = remote_store.stat(candidate.object_key);
      if (!metadata.has_value()) {
        if (metadata.error().code() == common::StatusCode::kNotFound)
          continue;
        return common::make_unexpected(metadata.error());
      }
      if (metadata->key != candidate.object_key ||
          metadata->size != static_cast<std::size_t>(candidate.file_length) ||
          metadata->checksum != candidate.content_sha256) {
        return common::make_unexpected(
            corruption("tiered restart garbage object metadata differs"));
      }
      ++report.metadata_validated;
    }

    selected_pair = pair_storage.load_selected_record();
    if (!selected_pair.has_value())
      return common::make_unexpected(selected_pair.error());
    if (!selected_pair->has_value() || **selected_pair != recovered_pair.record) {
      return common::make_unexpected(
          invalid("tiered restart garbage pair changed before deletion"));
    }
    for (const ColdPartLocationDescriptor& candidate : candidates) {
      auto removed = remote_store.remove_if_exact(candidate.object_key,
                                                  static_cast<std::size_t>(candidate.file_length),
                                                  candidate.content_sha256);
      if (!removed.has_value())
        return common::make_unexpected(removed.error());
      if (removed->removed == removed->already_absent) {
        return common::make_unexpected(
            corruption("object store returned an invalid exact-deletion outcome"));
      }
      if (removed->removed)
        ++report.removed_objects;
      if (removed->already_absent)
        ++report.already_absent_objects;
    }
    return report;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("tiered restart garbage discovery allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("tiered restart garbage discovery exceeded limits"));
  }
}

} // namespace chronos::tiering
