#include "chronos/tiering/tiered_reclamation.hpp"

#include "chronos/tiering/tiered_part_loader.hpp"

#include <algorithm>
#include <cstddef>
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
    : record_(std::move(record)), snapshot_(std::move(snapshot)), parts_(std::move(parts)),
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
  if (!selected_pair->has_value() || **selected_pair != proof.record_) {
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

} // namespace chronos::tiering
