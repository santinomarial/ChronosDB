#ifndef CHRONOS_RAFT_DURABLE_TABLET_RECONFIGURATION_HPP_
#define CHRONOS_RAFT_DURABLE_TABLET_RECONFIGURATION_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/tablet_movement_checkpoint_recovery.hpp"
#include "chronos/raft/tablet_reconfiguration.hpp"
#include "chronos/raft/tablet_reconfiguration_action_ledger.hpp"

#include <optional>

namespace chronos::raft {

struct DurableTabletReconfigurationResult {
  std::optional<TabletReconfigurationAction> action;
  std::optional<InstalledTabletMovementCheckpoint> installed_checkpoint;
};

struct PreparedTabletReconfigurationDispatch {
  TabletReconfigurationAction action;
  PreparedTabletReconfigurationAction preparation;
};

struct PreparedDurableTabletReconfigurationResult {
  std::optional<PreparedTabletReconfigurationDispatch> dispatch;
  std::optional<InstalledTabletMovementCheckpoint> installed_checkpoint;
};

// Reconciles against a disposable movement candidate. When authoritative Raft/metadata state
// advances movement phase, the next checkpoint is installed before the candidate is adopted by the
// recovered live owner. This overload can persist only self-contained generations.
[[nodiscard]] common::Result<DurableTabletReconfigurationResult>
reconcile_durable_tablet_reconfiguration(RecoveredTabletMovementGeneration& recovered,
                                         GroupId tablet_group_id, GroupId metadata_group_id,
                                         schema::TableId table_id, const RaftNode& tablet_group,
                                         const MetadataStateMachine& metadata,
                                         TabletMovementCheckpointStorage& checkpoint_storage,
                                         std::optional<NodeId> leader_hint = std::nullopt,
                                         TabletMovementLimits limits = {});

// Production-facing composition: an emitted action is returned only after the exact action is
// durable in the tablet-bound pre-dispatch ledger. A phase checkpoint may already be installed if
// ledger preparation fails; retry then reconstructs and prepares the same deterministic action.
[[nodiscard]] common::Result<PreparedDurableTabletReconfigurationResult>
reconcile_and_prepare_durable_tablet_reconfiguration(
    RecoveredTabletMovementGeneration& recovered, GroupId tablet_group_id,
    GroupId metadata_group_id, schema::TableId table_id, const RaftNode& tablet_group,
    const MetadataStateMachine& metadata, TabletMovementCheckpointStorage& checkpoint_storage,
    TabletReconfigurationActionLedger& action_ledger,
    std::optional<NodeId> leader_hint = std::nullopt, TabletMovementLimits limits = {});

[[nodiscard]] common::Result<PreparedDurableTabletReconfigurationResult>
reconcile_and_prepare_durable_tablet_reconfiguration(
    RecoveredTabletMovementGeneration& recovered, GroupId tablet_group_id,
    GroupId metadata_group_id, schema::TableId table_id, const RaftNode& tablet_group,
    const MetadataStateMachine& metadata, TabletMovementCheckpointStorage& checkpoint_storage,
    const TabletMovementSnapshotChunkStorage& chunk_storage,
    TabletReconfigurationActionLedger& action_ledger,
    std::optional<NodeId> leader_hint = std::nullopt, TabletMovementLimits limits = {});

// External-prefix generations retain their representation and exact chunk-session validation.
[[nodiscard]] common::Result<DurableTabletReconfigurationResult>
reconcile_durable_tablet_reconfiguration(RecoveredTabletMovementGeneration& recovered,
                                         GroupId tablet_group_id, GroupId metadata_group_id,
                                         schema::TableId table_id, const RaftNode& tablet_group,
                                         const MetadataStateMachine& metadata,
                                         TabletMovementCheckpointStorage& checkpoint_storage,
                                         const TabletMovementSnapshotChunkStorage& chunk_storage,
                                         std::optional<NodeId> leader_hint = std::nullopt,
                                         TabletMovementLimits limits = {});

} // namespace chronos::raft

#endif // CHRONOS_RAFT_DURABLE_TABLET_RECONFIGURATION_HPP_
