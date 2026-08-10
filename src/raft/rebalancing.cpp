#include "chronos/raft/rebalancing.hpp"

#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] bool valid_limits(const TabletMovementLimits& limits) {
  return limits.maximum_snapshot_bytes > 0U && limits.maximum_chunk_bytes > 0U &&
         limits.maximum_chunk_bytes <= limits.maximum_snapshot_bytes &&
         limits.maximum_replicas >= 2U;
}

} // namespace

class TabletMovement::Impl {
public:
  Impl(TabletMovementRecord value, TabletMovementLimits configured)
      : state(std::move(value)), limits(configured) {}
  TabletMovementRecord state;
  TabletMovementLimits limits;
  std::vector<std::byte> snapshot_bytes;
};

TabletMovement::TabletMovement(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}
TabletMovement::~TabletMovement() = default;
TabletMovement::TabletMovement(TabletMovement&&) noexcept = default;
TabletMovement& TabletMovement::operator=(TabletMovement&&) noexcept = default;

common::Result<TabletMovement>
TabletMovement::begin(schema::TabletId tablet_id, const std::uint64_t placement_epoch,
                      const NodeId source_node, const NodeId target_node,
                      std::vector<NodeId> voting_replicas, const TabletMovementLimits limits) {
  std::ranges::sort(voting_replicas);
  if (tablet_id.uuid().is_nil() || placement_epoch == 0U || source_node == 0U ||
      target_node == 0U || source_node == target_node || !valid_limits(limits) ||
      voting_replicas.empty() || voting_replicas.size() >= limits.maximum_replicas ||
      !std::binary_search(voting_replicas.begin(), voting_replicas.end(), source_node) ||
      std::binary_search(voting_replicas.begin(), voting_replicas.end(), target_node) ||
      voting_replicas.front() == 0U ||
      std::adjacent_find(voting_replicas.begin(), voting_replicas.end()) != voting_replicas.end()) {
    return common::make_unexpected(
        invalid("tablet movement identity, membership, or limits invalid"));
  }
  TabletMovementRecord record{tablet_id,
                              placement_epoch,
                              source_node,
                              target_node,
                              std::move(voting_replicas),
                              {target_node},
                              TabletMovementPhase::kAddingTarget,
                              {},
                              0U};
  return TabletMovement{std::make_unique<Impl>(std::move(record), limits)};
}

common::Result<TabletMovement> TabletMovement::recover(TabletMovementRecord record,
                                                       std::vector<std::byte> received_snapshot,
                                                       const TabletMovementLimits limits) {
  const common::Status validated =
      validate_tablet_movement_state(record, received_snapshot, limits);
  if (!validated.is_ok())
    return common::make_unexpected(validated);
  auto impl = std::make_unique<Impl>(std::move(record), limits);
  impl->snapshot_bytes = std::move(received_snapshot);
  return TabletMovement{std::move(impl)};
}

common::Status TabletMovement::begin_snapshot(const SnapshotTransferMetadata metadata) {
  if (impl_->state.phase != TabletMovementPhase::kAddingTarget ||
      metadata.manifest_generation == 0U || metadata.applied_index == 0U ||
      metadata.applied_term == 0U || metadata.total_bytes == 0U ||
      metadata.total_bytes > impl_->limits.maximum_snapshot_bytes) {
    return invalid("snapshot transfer metadata or movement phase is invalid");
  }
  impl_->state.snapshot = metadata;
  impl_->state.phase = TabletMovementPhase::kTransferringSnapshot;
  impl_->state.received_bytes = 0U;
  impl_->snapshot_bytes.clear();
  impl_->snapshot_bytes.reserve(metadata.total_bytes);
  return common::Status::ok();
}

common::Status TabletMovement::accept_snapshot_chunk(const std::size_t offset,
                                                     const common::ByteView bytes,
                                                     const std::uint32_t chunk_crc32c) {
  if (impl_->state.phase != TabletMovementPhase::kTransferringSnapshot || bytes.empty() ||
      bytes.size() > impl_->limits.maximum_chunk_bytes ||
      offset > impl_->state.snapshot.total_bytes ||
      bytes.size() > impl_->state.snapshot.total_bytes - offset ||
      common::crc32c(bytes) != chunk_crc32c) {
    return invalid("snapshot chunk phase, bounds, or checksum is invalid");
  }
  if (offset < impl_->state.received_bytes) {
    if (bytes.size() > impl_->state.received_bytes - offset ||
        !std::equal(bytes.begin(), bytes.end(),
                    impl_->snapshot_bytes.begin() + static_cast<std::ptrdiff_t>(offset))) {
      return common::Status{common::StatusCode::kCorruption,
                            "snapshot chunk retry differs from retained bytes"};
    }
    return common::Status::ok();
  }
  if (offset != impl_->state.received_bytes) {
    return common::Status{common::StatusCode::kUnavailable,
                          "snapshot chunk creates a transfer gap"};
  }
  impl_->snapshot_bytes.insert(impl_->snapshot_bytes.end(), bytes.begin(), bytes.end());
  impl_->state.received_bytes = impl_->snapshot_bytes.size();
  return common::Status::ok();
}

common::Status TabletMovement::finish_snapshot() {
  if (impl_->state.phase != TabletMovementPhase::kTransferringSnapshot ||
      impl_->snapshot_bytes.size() != impl_->state.snapshot.total_bytes) {
    return common::Status{common::StatusCode::kUnavailable, "snapshot transfer is not complete"};
  }
  if (common::crc32c(impl_->snapshot_bytes) != impl_->state.snapshot.content_crc32c) {
    return common::Status{common::StatusCode::kCorruption,
                          "snapshot content checksum does not match metadata"};
  }
  impl_->state.phase = TabletMovementPhase::kCatchingUp;
  return common::Status::ok();
}

common::Status TabletMovement::mark_caught_up(const LogIndex target_applied_index) {
  if (impl_->state.phase != TabletMovementPhase::kCatchingUp ||
      target_applied_index < impl_->state.snapshot.applied_index) {
    return common::Status{common::StatusCode::kUnavailable,
                          "target has not caught up through the snapshot boundary"};
  }
  impl_->state.phase = TabletMovementPhase::kReady;
  return common::Status::ok();
}

common::Status TabletMovement::promote_target(const std::uint64_t expected_epoch,
                                              const std::uint64_t new_epoch) {
  if (impl_->state.phase != TabletMovementPhase::kReady ||
      expected_epoch != impl_->state.placement_epoch || new_epoch != expected_epoch + 1U) {
    return invalid("target promotion phase or placement epoch is stale");
  }
  impl_->state.voting_replicas.push_back(impl_->state.target_node);
  std::ranges::sort(impl_->state.voting_replicas);
  impl_->state.learners.clear();
  impl_->state.placement_epoch = new_epoch;
  impl_->state.phase = TabletMovementPhase::kTargetPromoted;
  return common::Status::ok();
}

common::Status TabletMovement::remove_source(const std::uint64_t expected_epoch,
                                             const std::uint64_t new_epoch) {
  if (impl_->state.phase != TabletMovementPhase::kTargetPromoted ||
      expected_epoch != impl_->state.placement_epoch || new_epoch != expected_epoch + 1U) {
    return invalid("source removal phase or placement epoch is stale");
  }
  const auto source = std::ranges::find(impl_->state.voting_replicas, impl_->state.source_node);
  if (source == impl_->state.voting_replicas.end() ||
      !std::binary_search(impl_->state.voting_replicas.begin(), impl_->state.voting_replicas.end(),
                          impl_->state.target_node)) {
    return common::Status{common::StatusCode::kInternal,
                          "tablet movement membership lost source or target"};
  }
  impl_->state.voting_replicas.erase(source);
  impl_->state.placement_epoch = new_epoch;
  impl_->state.phase = TabletMovementPhase::kComplete;
  return common::Status::ok();
}

common::Status TabletMovement::restart_snapshot_transfer() {
  if (impl_->state.phase != TabletMovementPhase::kTransferringSnapshot) {
    return invalid("only an active snapshot transfer can restart");
  }
  impl_->snapshot_bytes.clear();
  impl_->state.received_bytes = 0U;
  return common::Status::ok();
}

TabletMovementRecord TabletMovement::record() const {
  return impl_->state;
}
common::ByteView TabletMovement::received_snapshot() const noexcept {
  return impl_->snapshot_bytes;
}

common::Status validate_tablet_movement_record(const TabletMovementRecord& record,
                                               const TabletMovementLimits limits) {
  if (!valid_limits(limits) || record.tablet_id.uuid().is_nil() || record.placement_epoch == 0U ||
      record.source_node == 0U || record.target_node == 0U ||
      record.source_node == record.target_node || record.voting_replicas.empty() ||
      record.voting_replicas.size() > limits.maximum_replicas ||
      record.voting_replicas.front() == 0U || !std::ranges::is_sorted(record.voting_replicas) ||
      std::adjacent_find(record.voting_replicas.begin(), record.voting_replicas.end()) !=
          record.voting_replicas.end() ||
      !std::ranges::is_sorted(record.learners) ||
      std::adjacent_find(record.learners.begin(), record.learners.end()) != record.learners.end() ||
      record.snapshot.total_bytes > limits.maximum_snapshot_bytes) {
    return invalid("tablet movement checkpoint identity, bounds, or canonical order is invalid");
  }

  const bool source_voter = std::binary_search(record.voting_replicas.begin(),
                                               record.voting_replicas.end(), record.source_node);
  const bool target_voter = std::binary_search(record.voting_replicas.begin(),
                                               record.voting_replicas.end(), record.target_node);
  const bool target_only_learner =
      record.learners.size() == 1U && record.learners.front() == record.target_node;
  const bool empty_snapshot =
      record.snapshot.manifest_generation == 0U && record.snapshot.applied_index == 0U &&
      record.snapshot.applied_term == 0U && record.snapshot.total_bytes == 0U &&
      record.snapshot.content_crc32c == 0U && record.received_bytes == 0U;
  const bool valid_snapshot =
      record.snapshot.manifest_generation != 0U && record.snapshot.applied_index != 0U &&
      record.snapshot.applied_term != 0U && record.snapshot.total_bytes != 0U &&
      record.received_bytes <= record.snapshot.total_bytes;

  switch (record.phase) {
  case TabletMovementPhase::kAddingTarget:
    return source_voter && !target_voter && target_only_learner && empty_snapshot &&
                   record.voting_replicas.size() < limits.maximum_replicas
               ? common::Status::ok()
               : invalid("adding-target movement checkpoint is inconsistent");
  case TabletMovementPhase::kTransferringSnapshot:
    return source_voter && !target_voter && target_only_learner && valid_snapshot &&
                   record.voting_replicas.size() < limits.maximum_replicas
               ? common::Status::ok()
               : invalid("transferring movement checkpoint is inconsistent");
  case TabletMovementPhase::kCatchingUp:
  case TabletMovementPhase::kReady:
    if (!source_voter || target_voter || !target_only_learner || !valid_snapshot ||
        record.voting_replicas.size() >= limits.maximum_replicas ||
        record.received_bytes != record.snapshot.total_bytes) {
      return invalid("caught-up movement checkpoint is inconsistent");
    }
    break;
  case TabletMovementPhase::kTargetPromoted:
    if (!source_voter || !target_voter || !record.learners.empty() || !valid_snapshot ||
        record.received_bytes != record.snapshot.total_bytes) {
      return invalid("promoted movement checkpoint is inconsistent");
    }
    break;
  case TabletMovementPhase::kComplete:
    if (source_voter || !target_voter || !record.learners.empty() || !valid_snapshot ||
        record.received_bytes != record.snapshot.total_bytes) {
      return invalid("complete movement checkpoint is inconsistent");
    }
    break;
  default:
    return invalid("tablet movement checkpoint phase is unknown");
  }
  return common::Status::ok();
}

common::Status validate_tablet_movement_state(const TabletMovementRecord& record,
                                              const common::ByteView received_snapshot,
                                              const TabletMovementLimits limits) {
  common::Status structural = validate_tablet_movement_record(record, limits);
  if (!structural.is_ok())
    return structural;
  if (record.received_bytes != received_snapshot.size())
    return invalid("tablet movement checkpoint received length differs from owned bytes");
  if (record.phase == TabletMovementPhase::kAddingTarget ||
      record.phase == TabletMovementPhase::kTransferringSnapshot) {
    return common::Status::ok();
  }
  return common::crc32c(received_snapshot) == record.snapshot.content_crc32c
             ? common::Status::ok()
             : common::Status{common::StatusCode::kCorruption,
                              "tablet movement checkpoint snapshot checksum differs"};
}

} // namespace chronos::raft
