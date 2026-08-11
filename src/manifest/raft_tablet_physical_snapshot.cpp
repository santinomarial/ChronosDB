#include "chronos/manifest/raft_tablet_physical_snapshot.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/manifest/format.hpp"
#include "chronos/manifest/temporal_format.hpp"
#include "chronos/manifest/temporal_validation.hpp"

#include <algorithm>
#include <iterator>
#include <limits>
#include <new>
#include <optional>
#include <ranges>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::manifest {
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

[[nodiscard]] bool same_source(const common::Uuid& source, const raft::GroupId& group_id) noexcept {
  return source == group_id;
}

[[nodiscard]] common::Result<ingest::Sha256Digest>
checksum_part_descriptors(const common::ByteView bytes, const std::size_t part_count) {
  const std::size_t parts_offset =
      format::kFileHeaderLength + temporal_format::kTabletDescriptorLength;
  if (part_count > (std::numeric_limits<std::size_t>::max() - parts_offset) /
                       temporal_format::kPartDescriptorLength) {
    return common::make_unexpected(exhausted("Raft tablet physical part table overflows"));
  }
  const std::size_t part_bytes = part_count * temporal_format::kPartDescriptorLength;
  if (parts_offset > bytes.size() || part_bytes > bytes.size() - parts_offset) {
    return common::make_unexpected(corruption("Raft tablet physical part table is truncated"));
  }
  return ingest::sha256(bytes.subspan(parts_offset, part_bytes));
}

[[nodiscard]] common::Status decode_status(const ManifestDecodeError& error) {
  return error.status();
}

[[nodiscard]] bool retry_less(const TemporalRetryDescriptor& left,
                              const TemporalRetryDescriptor& right) noexcept {
  return left.client_id != right.client_id ? left.client_id < right.client_id
                                           : left.client_batch_id < right.client_batch_id;
}

} // namespace

EncodedRaftTabletPhysicalSnapshot::EncodedRaftTabletPhysicalSnapshot(
    EncodedTemporalManifest manifest, ingest::Sha256Digest part_set_checksum) noexcept
    : manifest_(std::move(manifest)), part_set_checksum_(part_set_checksum) {}

common::ByteView EncodedRaftTabletPhysicalSnapshot::bytes() const noexcept {
  return manifest_.bytes();
}

const ingest::Sha256Digest& EncodedRaftTabletPhysicalSnapshot::part_set_checksum() const noexcept {
  return part_set_checksum_;
}

common::Result<EncodedRaftTabletPhysicalSnapshot> build_raft_tablet_physical_snapshot(
    const DecodedTemporalManifestView& selected, const raft::GroupId& group_id,
    const schema::TabletId& tablet_id, const raft::LogIndex applied_position) {
  if (group_id.is_nil() || tablet_id.uuid().is_nil() || applied_position == 0U) {
    return common::make_unexpected(invalid("Raft tablet physical snapshot identity is invalid"));
  }
  try {
    const auto tablet =
        std::ranges::find(selected.tablets(), tablet_id, &TemporalTabletDescriptor::tablet_id);
    if (tablet == selected.tablets().end()) {
      return common::make_unexpected(invalid("Raft tablet is absent from selected Manifest v2"));
    }
    if (tablet->commit_source != ManifestCommitSource::kRaft ||
        !same_source(tablet->source_id, group_id) || tablet->durable_position != applied_position) {
      return common::make_unexpected(
          invalid("Raft tablet physical snapshot source or applied boundary differs"));
    }
    if (tablet->first_part_index > selected.parts().size() ||
        tablet->part_count > selected.parts().size() - tablet->first_part_index) {
      return common::make_unexpected(
          corruption("selected Manifest v2 tablet part range is invalid"));
    }

    TemporalTabletDescriptor projected_tablet = *tablet;
    projected_tablet.first_part_index = 0U;
    const auto first = static_cast<std::size_t>(tablet->first_part_index);
    const auto count = static_cast<std::size_t>(tablet->part_count);
    const std::span<const TemporalPartDescriptor> selected_parts =
        selected.parts().subspan(first, count);
    std::vector<TemporalPartDescriptor> parts(selected_parts.begin(), selected_parts.end());
    std::vector<TemporalRetryDescriptor> retries;
    for (const TemporalRetryDescriptor& retry : selected.retries()) {
      if (retry.tablet_id == tablet_id) {
        retries.push_back(retry);
      }
    }
    const std::array<TemporalTabletDescriptor, 1U> tablets{projected_tablet};
    auto manifest = encode_manifest_v2_temporal({.generation = selected.generation(),
                                                 .database_id = selected.database_id(),
                                                 .wal_reclaim_checkpoint = std::nullopt,
                                                 .tablets = tablets,
                                                 .parts = parts,
                                                 .retries = retries});
    if (!manifest.has_value()) {
      return common::make_unexpected(manifest.error());
    }
    auto checksum = checksum_part_descriptors(manifest->bytes(), parts.size());
    if (!checksum.has_value()) {
      return common::make_unexpected(checksum.error());
    }
    return EncodedRaftTabletPhysicalSnapshot{std::move(*manifest), *checksum};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft tablet physical snapshot allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Raft tablet physical snapshot exceeded limits"));
  }
}

common::Result<RaftTabletPhysicalSnapshotReport> validate_raft_tablet_physical_snapshot(
    const common::ByteView bytes, const raft::GroupId& group_id, const schema::TableId& table_id,
    const schema::TabletId& tablet_id, const raft::SnapshotMetadata& raft_snapshot,
    const ManifestDecodeLimits limits) {
  if (group_id.is_nil() || table_id.uuid().is_nil() || tablet_id.uuid().is_nil() ||
      raft_snapshot.last_included_index == 0U || raft_snapshot.last_included_term == 0U ||
      raft_snapshot.manifest_generation == 0U) {
    return common::make_unexpected(invalid("Raft tablet physical validation identity is invalid"));
  }
  try {
    auto decoded = decode_manifest_v2_temporal_exact(bytes, limits);
    if (!decoded.has_value()) {
      return common::make_unexpected(decode_status(decoded.error()));
    }
    if (decoded->wal_reclaim_checkpoint().has_value() || decoded->tablets().size() != 1U) {
      return common::make_unexpected(
          corruption("Raft tablet physical snapshot is not a one-tablet projection"));
    }
    const TemporalTabletDescriptor& tablet = decoded->tablets().front();
    if (tablet.table_id != table_id || tablet.tablet_id != tablet_id ||
        tablet.commit_source != ManifestCommitSource::kRaft ||
        !same_source(tablet.source_id, group_id) || tablet.first_part_index != 0U ||
        tablet.part_count != decoded->parts().size() ||
        tablet.durable_position != raft_snapshot.last_included_index ||
        decoded->generation() != raft_snapshot.manifest_generation) {
      return common::make_unexpected(
          corruption("Raft tablet physical snapshot authority differs from Raft metadata"));
    }
    if (std::ranges::any_of(decoded->retries(), [&](const TemporalRetryDescriptor& retry) {
          return retry.tablet_id != tablet_id;
        })) {
      return common::make_unexpected(
          corruption("Raft tablet physical snapshot contains a foreign retry outcome"));
    }
    auto checksum = checksum_part_descriptors(bytes, decoded->parts().size());
    if (!checksum.has_value()) {
      return common::make_unexpected(checksum.error());
    }
    if (!std::ranges::equal(checksum->bytes(), raft_snapshot.part_set_checksum)) {
      return common::make_unexpected(
          corruption("Raft tablet physical snapshot part-set checksum differs"));
    }
    return RaftTabletPhysicalSnapshotReport{.database_id = decoded->database_id(),
                                            .group_id = group_id,
                                            .table_id = table_id,
                                            .tablet_id = tablet_id,
                                            .manifest_generation = decoded->generation(),
                                            .applied_position = tablet.durable_position,
                                            .part_count = decoded->parts().size(),
                                            .retry_count = decoded->retries().size(),
                                            .part_set_checksum = *checksum};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft tablet physical validation allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Raft tablet physical validation exceeded limits"));
  }
}

common::Result<EncodedTemporalManifest>
build_raft_tablet_destination_manifest(const DecodedTemporalManifestView& destination,
                                       const RaftTabletDestinationManifestRequest& request) {
  auto authority = validate_raft_tablet_physical_snapshot(
      request.physical_snapshot, request.group_id, request.table_id, request.tablet_id,
      request.raft_snapshot.get(), request.decode_limits);
  if (!authority.has_value())
    return common::make_unexpected(authority.error());
  try {
    auto projected =
        decode_manifest_v2_temporal_exact(request.physical_snapshot, request.decode_limits);
    if (!projected.has_value())
      return common::make_unexpected(decode_status(projected.error()));
    if (projected->database_id() != destination.database_id()) {
      return common::make_unexpected(
          invalid("Raft physical snapshot belongs to a different destination database"));
    }
    if (std::ranges::any_of(destination.tablets(), [&](const TemporalTabletDescriptor& tablet) {
          return tablet.tablet_id == request.tablet_id;
        })) {
      return common::make_unexpected(
          invalid("Raft physical snapshot tablet already exists in the destination Manifest"));
    }
    const std::optional<std::uint64_t> generation =
        common::checked_add(destination.generation(), std::uint64_t{1U});
    if (!generation.has_value())
      return common::make_unexpected(exhausted("destination Manifest generation overflowed"));

    std::vector<TemporalTabletDescriptor> tablets(destination.tablets().begin(),
                                                  destination.tablets().end());
    tablets.push_back(projected->tablets().front());
    std::ranges::sort(tablets, {}, &TemporalTabletDescriptor::tablet_id);
    std::vector<TemporalPartDescriptor> parts;
    parts.reserve(destination.parts().size() + projected->parts().size());
    for (TemporalTabletDescriptor& tablet : tablets) {
      const bool incoming = tablet.tablet_id == request.tablet_id;
      const DecodedTemporalManifestView& source = incoming ? *projected : destination;
      const auto source_tablet = std::ranges::find(source.tablets(), tablet.tablet_id,
                                                   &TemporalTabletDescriptor::tablet_id);
      if (source_tablet == source.tablets().end())
        return common::make_unexpected(corruption("destination tablet became inaccessible"));
      const std::size_t first = static_cast<std::size_t>(source_tablet->first_part_index);
      const std::size_t count = static_cast<std::size_t>(source_tablet->part_count);
      tablet.first_part_index = parts.size();
      parts.insert(parts.end(), source.parts().begin() + static_cast<std::ptrdiff_t>(first),
                   source.parts().begin() + static_cast<std::ptrdiff_t>(first + count));
    }
    std::vector<TemporalRetryDescriptor> retries(destination.retries().begin(),
                                                 destination.retries().end());
    retries.insert(retries.end(), projected->retries().begin(), projected->retries().end());
    std::ranges::sort(retries, retry_less);

    auto encoded =
        encode_manifest_v2_temporal({.generation = *generation,
                                     .database_id = destination.database_id(),
                                     .wal_reclaim_checkpoint = destination.wal_reclaim_checkpoint(),
                                     .tablets = tablets,
                                     .parts = parts,
                                     .retries = retries});
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    auto candidate = decode_manifest_v2_temporal_exact(encoded->bytes(), request.decode_limits);
    if (!candidate.has_value())
      return common::make_unexpected(decode_status(candidate.error()));
    common::Status transition =
        validate_manifest_v2_temporal_transition(destination, *candidate, request.schema_bindings);
    if (!transition.is_ok())
      return common::make_unexpected(std::move(transition));
    return encoded;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("Raft destination Manifest composition allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("Raft destination Manifest composition exceeded limits"));
  }
}

common::Result<BuiltRaftTabletSourceRetirementManifest>
build_raft_tablet_source_retirement_manifest(const DecodedTemporalManifestView& source,
                                             const RaftTabletSourceRetirementRequest& request) {
  const raft::TabletMovementRecord& movement = request.completed_movement.get();
  const raft::TabletPlacementMetadata& placement = request.committed_placement.get();
  const common::Status movement_status = raft::validate_tablet_movement_record(movement);
  if (!movement_status.is_ok())
    return common::make_unexpected(movement_status);
  if (request.group_id.is_nil() || request.table_id.uuid().is_nil() ||
      request.tablet_id.uuid().is_nil() || request.source_node == 0U ||
      movement.phase != raft::TabletMovementPhase::kComplete ||
      movement.tablet_id != request.tablet_id || movement.source_node != request.source_node ||
      movement.placement_epoch != placement.placement_epoch ||
      movement.voting_replicas != placement.replicas || placement.table_id != request.table_id ||
      placement.tablet_id != request.tablet_id ||
      std::ranges::find(placement.replicas, request.source_node) != placement.replicas.end() ||
      !std::binary_search(placement.replicas.begin(), placement.replicas.end(),
                          movement.target_node) ||
      (placement.leader_hint.has_value() &&
       !std::binary_search(placement.replicas.begin(), placement.replicas.end(),
                           *placement.leader_hint))) {
    return common::make_unexpected(
        invalid("Raft tablet source retirement authority is incomplete or inconsistent"));
  }
  try {
    const auto retired = std::ranges::find(source.tablets(), request.tablet_id,
                                           &TemporalTabletDescriptor::tablet_id);
    if (retired == source.tablets().end() || retired->table_id != request.table_id ||
        retired->commit_source != ManifestCommitSource::kRaft ||
        !same_source(retired->source_id, request.group_id) ||
        retired->first_part_index > source.parts().size() ||
        retired->part_count > source.parts().size() - retired->first_part_index) {
      return common::make_unexpected(
          invalid("Raft tablet source retirement does not match the selected Manifest"));
    }
    const std::optional<std::uint64_t> generation =
        common::checked_add(source.generation(), std::uint64_t{1U});
    if (!generation.has_value())
      return common::make_unexpected(exhausted("source retirement Manifest generation overflowed"));

    const auto retired_first = static_cast<std::size_t>(retired->first_part_index);
    const auto retired_count = static_cast<std::size_t>(retired->part_count);
    std::vector<TemporalPartDescriptor> retired_parts(
        source.parts().begin() + static_cast<std::ptrdiff_t>(retired_first),
        source.parts().begin() + static_cast<std::ptrdiff_t>(retired_first + retired_count));
    std::vector<TemporalTabletDescriptor> tablets;
    std::vector<TemporalPartDescriptor> parts;
    tablets.reserve(source.tablets().size() - 1U);
    parts.reserve(source.parts().size() - retired_count);
    for (const TemporalTabletDescriptor& current : source.tablets()) {
      if (current.tablet_id == request.tablet_id)
        continue;
      if (current.first_part_index > source.parts().size() ||
          current.part_count > source.parts().size() - current.first_part_index) {
        return common::make_unexpected(corruption("source Manifest tablet part range is invalid"));
      }
      TemporalTabletDescriptor retained = current;
      retained.first_part_index = parts.size();
      const auto first = static_cast<std::size_t>(current.first_part_index);
      const auto count = static_cast<std::size_t>(current.part_count);
      parts.insert(parts.end(), source.parts().begin() + static_cast<std::ptrdiff_t>(first),
                   source.parts().begin() + static_cast<std::ptrdiff_t>(first + count));
      tablets.push_back(std::move(retained));
    }
    std::vector<TemporalRetryDescriptor> retries;
    retries.reserve(source.retries().size());
    std::ranges::copy_if(
        source.retries(), std::back_inserter(retries),
        [&](const TemporalRetryDescriptor& retry) { return retry.tablet_id != request.tablet_id; });
    auto encoded =
        encode_manifest_v2_temporal({.generation = *generation,
                                     .database_id = source.database_id(),
                                     .wal_reclaim_checkpoint = source.wal_reclaim_checkpoint(),
                                     .tablets = tablets,
                                     .parts = parts,
                                     .retries = retries});
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    return BuiltRaftTabletSourceRetirementManifest{.manifest = std::move(*encoded),
                                                   .predecessor_generation = source.generation(),
                                                   .retired_parts = std::move(retired_parts)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft tablet source retirement allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Raft tablet source retirement exceeded limits"));
  }
}

} // namespace chronos::manifest
