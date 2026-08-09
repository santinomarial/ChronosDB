#ifndef CHRONOS_RAFT_REBALANCING_HPP_
#define CHRONOS_RAFT_REBALANCING_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/raft/types.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::raft {

enum class TabletMovementPhase : std::uint8_t {
  kAddingTarget = 1,
  kTransferringSnapshot = 2,
  kCatchingUp = 3,
  kReady = 4,
  kTargetPromoted = 5,
  kComplete = 6,
};

struct TabletMovementLimits {
  std::size_t maximum_snapshot_bytes{1U << 30U};
  std::size_t maximum_chunk_bytes{4U * 1024U * 1024U};
  std::size_t maximum_replicas{9U};
};

struct SnapshotTransferMetadata {
  std::uint64_t manifest_generation{};
  LogIndex applied_index{};
  Term applied_term{};
  std::size_t total_bytes{};
  std::uint32_t content_crc32c{};
};

struct TabletMovementRecord {
  schema::TabletId tablet_id;
  std::uint64_t placement_epoch{};
  NodeId source_node{};
  NodeId target_node{};
  std::vector<NodeId> voting_replicas;
  std::vector<NodeId> learners;
  TabletMovementPhase phase{TabletMovementPhase::kAddingTarget};
  SnapshotTransferMetadata snapshot;
  std::size_t received_bytes{};
};

// Single-owner add-before-remove tablet movement. Snapshot chunks are sequential, checksummed, and
// idempotent on exact retry. Promotion is impossible until the target has installed the complete
// snapshot and caught up through its covered Raft index.
class TabletMovement {
public:
  TabletMovement() = delete;
  ~TabletMovement();
  TabletMovement(const TabletMovement&) = delete;
  TabletMovement& operator=(const TabletMovement&) = delete;
  TabletMovement(TabletMovement&&) noexcept;
  TabletMovement& operator=(TabletMovement&&) noexcept;

  [[nodiscard]] static common::Result<TabletMovement>
  begin(schema::TabletId tablet_id, std::uint64_t placement_epoch, NodeId source_node,
        NodeId target_node, std::vector<NodeId> voting_replicas, TabletMovementLimits limits = {});

  [[nodiscard]] common::Status begin_snapshot(SnapshotTransferMetadata metadata);
  [[nodiscard]] common::Status accept_snapshot_chunk(std::size_t offset, common::ByteView bytes,
                                                     std::uint32_t chunk_crc32c);
  [[nodiscard]] common::Status finish_snapshot();
  [[nodiscard]] common::Status mark_caught_up(LogIndex target_applied_index);
  [[nodiscard]] common::Status promote_target(std::uint64_t expected_epoch,
                                              std::uint64_t new_epoch);
  [[nodiscard]] common::Status remove_source(std::uint64_t expected_epoch, std::uint64_t new_epoch);
  [[nodiscard]] common::Status restart_snapshot_transfer();

  [[nodiscard]] TabletMovementRecord record() const;
  [[nodiscard]] common::ByteView received_snapshot() const noexcept;

private:
  class Impl;
  explicit TabletMovement(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_REBALANCING_HPP_
