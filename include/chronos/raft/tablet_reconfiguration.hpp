#ifndef CHRONOS_RAFT_TABLET_RECONFIGURATION_HPP_
#define CHRONOS_RAFT_TABLET_RECONFIGURATION_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/metadata.hpp"
#include "chronos/raft/rebalancing.hpp"
#include "chronos/schema/identity.hpp"

#include <cstdint>
#include <memory>
#include <optional>

namespace chronos::raft {

enum class TabletReconfigurationActionKind : std::uint8_t {
  kBeginJointMembership = 1,
  kFinalizeJointMembership = 2,
  kPublishPlacement = 3,
};

struct TabletReconfigurationActionId {
  schema::TabletId tablet_id;
  std::uint64_t movement_epoch{};
  TabletReconfigurationActionKind kind{TabletReconfigurationActionKind::kBeginJointMembership};

  friend bool operator==(const TabletReconfigurationActionId&,
                         const TabletReconfigurationActionId&) = default;
};

struct TabletReconfigurationAction {
  TabletReconfigurationActionId id;
  TabletReconfigurationActionKind kind{TabletReconfigurationActionKind::kBeginJointMembership};
  DurableRaftRequest request;
};

// Deterministic bridge between learner-first TabletMovement, the tablet's joint-consensus group,
// and committed metadata placement. The caller executes at most one returned action, observes its
// committed/application result, then reconciles again. Local movement phase advances only after
// both authoritative state machines expose the expected stable configuration.
class TabletReconfigurationCoordinator {
public:
  TabletReconfigurationCoordinator() = delete;
  ~TabletReconfigurationCoordinator();
  TabletReconfigurationCoordinator(const TabletReconfigurationCoordinator&) = delete;
  TabletReconfigurationCoordinator& operator=(const TabletReconfigurationCoordinator&) = delete;
  TabletReconfigurationCoordinator(TabletReconfigurationCoordinator&&) noexcept;
  TabletReconfigurationCoordinator& operator=(TabletReconfigurationCoordinator&&) noexcept;

  // Recovery may resume from ready, target-promoted, or complete; earlier transfer phases belong
  // to the snapshot-transfer owner.
  [[nodiscard]] static common::Result<TabletReconfigurationCoordinator>
  create(GroupId tablet_group_id, GroupId metadata_group_id, schema::TableId table_id,
         TabletMovement movement, std::optional<NodeId> leader_hint = std::nullopt);

  [[nodiscard]] common::Result<std::optional<TabletReconfigurationAction>>
  reconcile(const RaftNode& tablet_group, const MetadataStateMachine& metadata);

  [[nodiscard]] TabletMovementRecord record() const;

private:
  class Impl;
  explicit TabletReconfigurationCoordinator(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_TABLET_RECONFIGURATION_HPP_
