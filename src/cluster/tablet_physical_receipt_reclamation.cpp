#include "chronos/cluster/tablet_physical_receipt_reclamation.hpp"

#include "chronos/manifest/raft_tablet_physical_snapshot.hpp"
#include "chronos/manifest/temporal_codec.hpp"

#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>

namespace chronos::cluster {
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

} // namespace

common::Result<TabletPhysicalReceiptReclamationReport>
reclaim_tablet_physical_part_receipt(TabletPhysicalPartChunkStorage& receipt,
                                     const manifest::TemporalDatabaseStorageSnapshot& destination,
                                     const TabletPhysicalMovementReadinessReport& readiness) {
  try {
    auto session = receipt.transfer_session();
    if (!session.has_value())
      return common::make_unexpected(session.error());
    const auto& application = readiness.application_snapshot;
    const auto& movement = readiness.movement;
    if (movement.phase != raft::TabletMovementPhase::kReady ||
        movement.tablet_id != session->tablet_id ||
        movement.placement_epoch != session->placement_epoch ||
        movement.source_node != session->source_node ||
        movement.target_node != session->target_node || application.table_id != session->table_id ||
        application.tablet_id != session->tablet_id || application.group_id != session->group_id ||
        application.raft_snapshot.manifest_generation != session->manifest_generation ||
        readiness.destination_manifest_generation != destination.generation()) {
      return common::make_unexpected(
          invalid("physical receipt reclamation authority differs from its transfer session"));
    }

    auto decoded = manifest::decode_manifest_v2_temporal_exact(destination.manifest_bytes());
    if (!decoded.has_value())
      return common::make_unexpected(decoded.error().status());
    auto physical = manifest::build_raft_tablet_physical_snapshot(
        *decoded, application.group_id, application.tablet_id,
        application.raft_snapshot.last_included_index);
    if (!physical.has_value())
      return common::make_unexpected(physical.error());
    const ingest::Sha256Digest expected{application.raft_snapshot.part_set_checksum};
    if (physical->part_set_checksum() != expected ||
        physical->part_set_checksum() != readiness.part_set_checksum) {
      return common::make_unexpected(
          corruption("physical receipt reclamation part-set authority changed"));
    }

    const auto tablet = std::ranges::find(destination.tablets(), session->tablet_id,
                                          &manifest::TemporalTabletDescriptor::tablet_id);
    if (tablet == destination.tablets().end() ||
        tablet->first_part_index > destination.parts().size() ||
        tablet->part_count > destination.parts().size() - tablet->first_part_index) {
      return common::make_unexpected(
          corruption("physical receipt reclamation tablet part range is unavailable"));
    }
    const auto first = static_cast<std::size_t>(tablet->first_part_index);
    const auto count = static_cast<std::size_t>(tablet->part_count);
    const auto parts = destination.parts().subspan(first, count);
    const auto part =
        std::ranges::find(parts, session->part_id, &manifest::TemporalPartDescriptor::part_id);
    if (part == parts.end() || part->table_id != session->table_id ||
        part->tablet_id != session->tablet_id || part->source_id != session->group_id ||
        part->commit_source != manifest::ManifestCommitSource::kRaft ||
        part->file_length != session->total_bytes ||
        part->content_sha256 != session->content_sha256) {
      return common::make_unexpected(
          corruption("published destination does not own the physical receipt part"));
    }

    auto reclaimed = receipt.reclaim();
    if (!reclaimed.has_value())
      return common::make_unexpected(reclaimed.error());
    return TabletPhysicalReceiptReclamationReport{
        .receipt = std::move(*reclaimed),
        .destination_manifest_generation = destination.generation(),
        .part_set_checksum = physical->part_set_checksum()};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("physical receipt reclamation allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("physical receipt reclamation exceeded limits"));
  }
}

} // namespace chronos::cluster
