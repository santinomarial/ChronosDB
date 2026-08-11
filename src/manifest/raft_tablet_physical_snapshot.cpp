#include "chronos/manifest/raft_tablet_physical_snapshot.hpp"

#include "chronos/manifest/format.hpp"
#include "chronos/manifest/temporal_format.hpp"

#include <algorithm>
#include <limits>
#include <new>
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

} // namespace chronos::manifest
