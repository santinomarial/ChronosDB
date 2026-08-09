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

common::Result<TabletMovement> TabletMovement::begin(
    schema::TabletId tablet_id, const std::uint64_t placement_epoch, const NodeId source_node,
    const NodeId target_node, std::vector<NodeId> voting_replicas,
    const TabletMovementLimits limits) {
  std::ranges::sort(voting_replicas);
  if (tablet_id.uuid().is_nil() || placement_epoch == 0U || source_node == 0U || target_node == 0U ||
      source_node == target_node || limits.maximum_snapshot_bytes == 0U ||
      limits.maximum_chunk_bytes == 0U || limits.maximum_replicas < 2U || voting_replicas.empty() ||
      voting_replicas.size() >= limits.maximum_replicas ||
      !std::binary_search(voting_replicas.begin(), voting_replicas.end(), source_node) ||
      std::binary_search(voting_replicas.begin(), voting_replicas.end(), target_node) ||
      voting_replicas.front() == 0U ||
      std::adjacent_find(voting_replicas.begin(), voting_replicas.end()) != voting_replicas.end()) {
    return common::make_unexpected(invalid("tablet movement identity, membership, or limits invalid"));
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

common::Status TabletMovement::begin_snapshot(const SnapshotTransferMetadata metadata) {
  if (impl_->state.phase != TabletMovementPhase::kAddingTarget || metadata.manifest_generation == 0U ||
      metadata.applied_index == 0U || metadata.applied_term == 0U || metadata.total_bytes == 0U ||
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
        !std::equal(bytes.begin(), bytes.end(), impl_->snapshot_bytes.begin() +
                                                   static_cast<std::ptrdiff_t>(offset))) {
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
    return common::Status{common::StatusCode::kUnavailable,
                          "snapshot transfer is not complete"};
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
      !std::binary_search(impl_->state.voting_replicas.begin(),
                          impl_->state.voting_replicas.end(), impl_->state.target_node)) {
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

TabletMovementRecord TabletMovement::record() const { return impl_->state; }
common::ByteView TabletMovement::received_snapshot() const noexcept { return impl_->snapshot_bytes; }

} // namespace chronos::raft
