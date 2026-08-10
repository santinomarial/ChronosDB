#ifndef CHRONOS_RAFT_DURABLE_TABLET_RECONFIGURATION_HPP_
#define CHRONOS_RAFT_DURABLE_TABLET_RECONFIGURATION_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/async_durable_runtime.hpp"
#include "chronos/raft/tablet_movement_checkpoint_recovery.hpp"
#include "chronos/raft/tablet_reconfiguration.hpp"
#include "chronos/raft/tablet_reconfiguration_action_ledger.hpp"

#include <optional>

namespace chronos::raft {

namespace detail {
class PreparedTabletReconfigurationDispatchFactory;
}

struct DurableTabletReconfigurationResult {
  std::optional<TabletReconfigurationAction> action;
  std::optional<InstalledTabletMovementCheckpoint> installed_checkpoint;
};

class PreparedTabletReconfigurationDispatch {
public:
  PreparedTabletReconfigurationDispatch() = delete;
  ~PreparedTabletReconfigurationDispatch() = default;
  PreparedTabletReconfigurationDispatch(const PreparedTabletReconfigurationDispatch&) = delete;
  PreparedTabletReconfigurationDispatch&
  operator=(const PreparedTabletReconfigurationDispatch&) = delete;
  PreparedTabletReconfigurationDispatch(PreparedTabletReconfigurationDispatch&& other) noexcept;
  PreparedTabletReconfigurationDispatch&
  operator=(PreparedTabletReconfigurationDispatch&& other) noexcept;

  [[nodiscard]] bool is_valid() const noexcept;
  [[nodiscard]] const TabletReconfigurationAction& action() const noexcept;
  [[nodiscard]] const PreparedTabletReconfigurationAction& preparation() const noexcept;

private:
  PreparedTabletReconfigurationDispatch(TabletReconfigurationAction action,
                                        PreparedTabletReconfigurationAction preparation) noexcept;
  TabletReconfigurationAction action_;
  PreparedTabletReconfigurationAction preparation_;
  bool valid_{true};
  friend class detail::PreparedTabletReconfigurationDispatchFactory;
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

// Async-owner composition. The owning observation must name the configured tablet group and is
// semantically validated before it can drive the same durable checkpoint/prepare transition.
[[nodiscard]] common::Result<PreparedDurableTabletReconfigurationResult>
reconcile_and_prepare_durable_tablet_reconfiguration(
    RecoveredTabletMovementGeneration& recovered, GroupId tablet_group_id,
    GroupId metadata_group_id, schema::TableId table_id, const RaftGroupObservation& tablet_group,
    const MetadataStateMachine& metadata, TabletMovementCheckpointStorage& checkpoint_storage,
    TabletReconfigurationActionLedger& action_ledger,
    std::optional<NodeId> leader_hint = std::nullopt, TabletMovementLimits limits = {});

// Executes one sealed, ledger-prepared action on a node-local synchronous durable runtime. Any
// returned transition, including outbound messages, is released only after the runtime's existing
// persist-and-sync boundary. Commit/application observation and remote routing remain external.
[[nodiscard]] common::Result<DurableRaftResult>
execute_local_prepared_tablet_reconfiguration(const PreparedTabletReconfigurationDispatch& dispatch,
                                              DurableMultiRaftRuntime& runtime);

// Nonblocking admission to the asynchronous single-owner runtime. Rejection leaves the sealed
// capability reusable. The returned completion releases results only after durable execution; a
// reactor must poll it or hand it to a non-reactor continuation rather than block in wait().
[[nodiscard]] common::Result<AsyncDurableRaftCompletion>
try_submit_local_prepared_tablet_reconfiguration(
    const PreparedTabletReconfigurationDispatch& dispatch, AsyncDurableMultiRaftRuntime& runtime);

[[nodiscard]] common::Result<PreparedDurableTabletReconfigurationResult>
reconcile_and_prepare_durable_tablet_reconfiguration(
    RecoveredTabletMovementGeneration& recovered, GroupId tablet_group_id,
    GroupId metadata_group_id, schema::TableId table_id, const RaftNode& tablet_group,
    const MetadataStateMachine& metadata, TabletMovementCheckpointStorage& checkpoint_storage,
    const TabletMovementSnapshotChunkStorage& chunk_storage,
    TabletReconfigurationActionLedger& action_ledger,
    std::optional<NodeId> leader_hint = std::nullopt, TabletMovementLimits limits = {});

[[nodiscard]] common::Result<PreparedDurableTabletReconfigurationResult>
reconcile_and_prepare_durable_tablet_reconfiguration(
    RecoveredTabletMovementGeneration& recovered, GroupId tablet_group_id,
    GroupId metadata_group_id, schema::TableId table_id, const RaftGroupObservation& tablet_group,
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
