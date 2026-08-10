#include "chronos/raft/tablet_movement_checkpoint_recovery.hpp"

#include <utility>
#include <variant>

namespace chronos::raft {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status unsupported(const char* message) {
  return {common::StatusCode::kNotSupported, message};
}

[[nodiscard]] common::Result<TabletMovement>
recover_reference(const TabletMovementCheckpointReference& reference,
                  const TabletMovementSnapshotChunkStorage& chunk_storage,
                  const TabletMovementLimits limits, const bool recovering_durable_reference) {
  auto expected_session = tablet_movement_snapshot_session(reference, limits);
  if (!expected_session.has_value())
    return common::make_unexpected(expected_session.error());
  auto actual_session = chunk_storage.session();
  if (!actual_session.has_value())
    return common::make_unexpected(actual_session.error());
  if (*actual_session != *expected_session)
    return common::make_unexpected(invalid("movement checkpoint and chunk sessions differ"));
  auto durable_bytes = chunk_storage.received_bytes();
  if (!durable_bytes.has_value())
    return common::make_unexpected(durable_bytes.error());
  if (*durable_bytes < reference.record.received_bytes) {
    return common::make_unexpected(
        recovering_durable_reference
            ? corruption("durable movement checkpoint references a missing chunk prefix")
            : unavailable("movement checkpoint references a non-durable chunk prefix"));
  }
  auto prefix = chunk_storage.load_prefix_through(reference.record.received_bytes);
  if (!prefix.has_value()) {
    if (recovering_durable_reference &&
        prefix.error().code() == common::StatusCode::kInvalidArgument) {
      return common::make_unexpected(
          corruption("durable movement checkpoint is not an installed chunk boundary"));
    }
    return common::make_unexpected(prefix.error());
  }
  return TabletMovement::recover(reference.record, std::move(*prefix), limits);
}

[[nodiscard]] common::Result<RecoveredTabletMovementGeneration>
recover_impl(const LoadedTabletMovementCheckpointGeneration& loaded,
             const TabletMovementSnapshotChunkStorage* const chunk_storage,
             const TabletMovementLimits limits) {
  if (const auto* checkpoint =
          std::get_if<TabletMovementCheckpointGeneration>(&loaded.generation)) {
    auto movement = TabletMovement::recover(checkpoint->checkpoint.record,
                                            checkpoint->checkpoint.received_snapshot, limits);
    if (!movement.has_value())
      return common::make_unexpected(movement.error());
    return RecoveredTabletMovementGeneration{checkpoint->checkpoint_generation, false,
                                             std::move(*movement)};
  }
  const auto& reference = std::get<TabletMovementCheckpointReferenceGeneration>(loaded.generation);
  if (chunk_storage == nullptr) {
    return common::make_unexpected(
        unsupported("movement checkpoint recovery requires external prefix storage"));
  }
  auto movement = recover_reference(reference.reference, *chunk_storage, limits, true);
  if (!movement.has_value())
    return common::make_unexpected(movement.error());
  return RecoveredTabletMovementGeneration{reference.checkpoint_generation, true,
                                           std::move(*movement)};
}

} // namespace

common::Result<InstalledTabletMovementCheckpoint> install_verified_tablet_movement_reference(
    TabletMovementCheckpointStorage& checkpoint_storage,
    const TabletMovementSnapshotChunkStorage& chunk_storage,
    const TabletMovementCheckpointReferenceGeneration& generation,
    const TabletMovementLimits limits) {
  auto verified = recover_reference(generation.reference, chunk_storage, limits, false);
  if (!verified.has_value())
    return common::make_unexpected(verified.error());
  return checkpoint_storage.install_reference(generation);
}

common::Result<RecoveredTabletMovementGeneration>
recover_tablet_movement_generation(const LoadedTabletMovementCheckpointGeneration& generation,
                                   const TabletMovementLimits limits) {
  return recover_impl(generation, nullptr, limits);
}

common::Result<RecoveredTabletMovementGeneration>
recover_tablet_movement_generation(const LoadedTabletMovementCheckpointGeneration& generation,
                                   const TabletMovementSnapshotChunkStorage& chunk_storage,
                                   const TabletMovementLimits limits) {
  return recover_impl(generation, &chunk_storage, limits);
}

} // namespace chronos::raft
