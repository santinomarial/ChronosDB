#include "chronos/ingest/tablet_movement_snapshot_handoff.hpp"

#include <algorithm>
#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>

namespace chronos::ingest {
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

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

[[nodiscard]] bool phase_allows_first_install(const raft::TabletMovementPhase phase) noexcept {
  return phase == raft::TabletMovementPhase::kCatchingUp;
}

[[nodiscard]] bool phase_requires_prior_install(const raft::TabletMovementPhase phase) noexcept {
  return phase == raft::TabletMovementPhase::kReady ||
         phase == raft::TabletMovementPhase::kTargetPromoted ||
         phase == raft::TabletMovementPhase::kComplete;
}

[[nodiscard]] bool phase_retains_source_voters(const raft::TabletMovementPhase phase) noexcept {
  return phase == raft::TabletMovementPhase::kCatchingUp ||
         phase == raft::TabletMovementPhase::kReady;
}

} // namespace

common::Result<TabletMovementSnapshotHandoffReport>
install_recovered_tablet_movement_snapshot(const raft::RecoveredTabletMovementGeneration& recovered,
                                           const schema::TableId expected_table_id,
                                           RaftTabletSnapshotStorage& snapshot_storage,
                                           const RaftTabletSnapshotCodecLimits codec_limits) {
  if (recovered.checkpoint_generation == 0U || expected_table_id.uuid().is_nil())
    return common::make_unexpected(invalid("tablet movement snapshot handoff identity is invalid"));

  try {
    const raft::TabletMovementRecord record = recovered.movement.record();
    if (!phase_allows_first_install(record.phase) && !phase_requires_prior_install(record.phase)) {
      return common::make_unexpected(
          unavailable("tablet movement snapshot is not complete and installable"));
    }
    const common::ByteView transferred = recovered.movement.received_snapshot();
    if (transferred.size() != record.snapshot.total_bytes)
      return common::make_unexpected(corruption("completed movement snapshot length changed"));

    auto decoded = decode_raft_tablet_application_snapshot_v1(transferred, codec_limits);
    if (!decoded.has_value())
      return common::make_unexpected(decoded.error());
    if (decoded->table_id != expected_table_id || decoded->tablet_id != record.tablet_id ||
        decoded->raft_snapshot.manifest_generation != record.snapshot.manifest_generation ||
        decoded->raft_snapshot.last_included_index != record.snapshot.applied_index ||
        decoded->raft_snapshot.last_included_term != record.snapshot.applied_term) {
      return common::make_unexpected(
          corruption("movement transfer metadata differs from its RTAS snapshot"));
    }

    auto canonical = encode_raft_tablet_application_snapshot_v1(*decoded, codec_limits);
    if (!canonical.has_value())
      return common::make_unexpected(canonical.error());
    if (!std::ranges::equal(*canonical, transferred))
      return common::make_unexpected(corruption("transferred RTAS bytes are not canonical"));
    if (phase_retains_source_voters(record.phase) &&
        decoded->raft_snapshot.voters != record.voting_replicas) {
      return common::make_unexpected(
          corruption("movement source voters differ from the transferred RTAS snapshot"));
    }

    InstalledRaftTabletSnapshot installation;
    if (phase_allows_first_install(record.phase)) {
      auto installed = snapshot_storage.install(*decoded);
      if (!installed.has_value())
        return common::make_unexpected(installed.error());
      installation = std::move(*installed);
    } else {
      auto installed = snapshot_storage.load(decoded->raft_snapshot.last_included_index);
      if (!installed.has_value()) {
        if (installed.error().code() == common::StatusCode::kNotFound) {
          return common::make_unexpected(
              corruption("advanced movement has no durable RTAS snapshot"));
        }
        return common::make_unexpected(installed.error());
      }
      if (installed->snapshot != *decoded || installed->bytes != *canonical) {
        return common::make_unexpected(
            corruption("promoted movement RTAS snapshot differs from transferred bytes"));
      }
      installation = InstalledRaftTabletSnapshot{.last_included_index =
                                                     decoded->raft_snapshot.last_included_index,
                                                 .file_name = std::move(installed->file_name),
                                                 .already_present = true};
    }
    return TabletMovementSnapshotHandoffReport{.checkpoint_generation =
                                                   recovered.checkpoint_generation,
                                               .group_id = decoded->group_id,
                                               .table_id = decoded->table_id,
                                               .tablet_id = decoded->tablet_id,
                                               .raft_snapshot = decoded->raft_snapshot,
                                               .installation = std::move(installation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("tablet movement handoff allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("tablet movement handoff exceeded container limits"));
  }
}

} // namespace chronos::ingest
