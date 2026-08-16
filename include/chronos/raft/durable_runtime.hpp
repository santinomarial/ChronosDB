#ifndef CHRONOS_RAFT_DURABLE_RUNTIME_HPP_
#define CHRONOS_RAFT_DURABLE_RUNTIME_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/multi_raft.hpp"
#include "chronos/raft/persistent_log.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::raft {

struct RaftGroupConfiguration {
  GroupId group_id;
  std::vector<NodeId> voters;
};

struct StartElectionOperation {};
struct ReceiveOperation {
  NodeId source{};
  Message message;
};
struct ProposeOperation {
  std::uint8_t type{};
  std::vector<std::byte> payload;
};
// Explicit exact-byte retry semantics for commands whose logical identity is already carried by
// their canonical payload. A retained current-term or committed exact entry suppresses re-append.
struct ProposeExactRetainedOperation {
  std::uint8_t type{};
  std::vector<std::byte> payload;
};
struct CommitCurrentTermOperation {};
struct ObserveGroupOperation {};
struct BeginMembershipChangeOperation {
  std::vector<NodeId> new_voters;
};
struct FinalizeMembershipChangeOperation {};
struct CompleteSnapshotInstallOperation {
  NodeId source{};
  SnapshotMetadata snapshot;
  bool installed{};
};
struct CompactSnapshotOperation {
  SnapshotMetadata snapshot;
};
struct HeartbeatOperation {};
struct BeginReadBarrierOperation {};
struct MarkAppliedOperation {
  LogIndex index{};
};

using DurableRaftOperation =
    std::variant<StartElectionOperation, ReceiveOperation, ProposeOperation,
                 ProposeExactRetainedOperation, CommitCurrentTermOperation, ObserveGroupOperation,
                 BeginMembershipChangeOperation, FinalizeMembershipChangeOperation,
                 CompleteSnapshotInstallOperation, CompactSnapshotOperation, HeartbeatOperation,
                 BeginReadBarrierOperation, MarkAppliedOperation>;

struct DurableRaftRequest {
  DurableRaftRequest(GroupId configured_group_id, DurableRaftOperation configured_operation,
                     std::optional<Term> configured_required_leader_term = std::nullopt)
      : group_id(configured_group_id), operation(std::move(configured_operation)),
        required_leader_term(configured_required_leader_term) {}

  GroupId group_id;
  DurableRaftOperation operation;
  // When present, the single owner checks this precondition immediately before dispatching the
  // operation. A role or term mismatch returns UNAVAILABLE without mutating the group. This is the
  // atomic admission boundary for work routed to a particular observed leader term.
  std::optional<Term> required_leader_term;
};

// Bounded owning copy of the group state needed by routing and reconciliation. It deliberately
// excludes retained log payloads and pending outbound messages.
struct RaftGroupObservation {
  GroupId group_id;
  NodeId node_id{};
  Role role{Role::kFollower};
  Term current_term{};
  std::optional<NodeId> leader_id;
  LogIndex last_log_index{};
  LogIndex commit_index{};
  LogIndex applied_index{};
  LogIndex snapshot_index{};
  std::vector<NodeId> voters;
  std::vector<NodeId> committed_voters;
  std::vector<NodeId> joint_old_voters;
  std::vector<NodeId> joint_new_voters;
  bool joint_membership_active{};
  bool joint_membership_can_finalize{};
  bool final_membership_pending{};

  friend bool operator==(const RaftGroupObservation&, const RaftGroupObservation&) = default;
};

struct DurableRaftResult {
  common::Status status;
  std::optional<MultiRaftTransition> transition;
  std::optional<RaftGroupObservation> observation;
};

struct DurableMultiRaftLimits {
  std::size_t maximum_batch_operations{1024U};
  std::size_t maximum_batch_outbound{65'536U};
  MultiRaftLimits runtime;
};

// Proof available only on the current leader after a locally synchronized commit transition.
// Under DurableMultiRaftRuntime's persist-before-response contract, commit implies that the stable
// majority, or both joint majorities, durably stored the entry. This is a storage-durability proof;
// callers must separately require state-machine application before returning a query-visible write
// acknowledgement.
struct QuorumSyncReceipt {
  GroupId group_id;
  NodeId leader_node_id{};
  Term leader_term{};
  LogIndex log_index{};
  Term entry_term{};
  std::uint64_t local_durable_physical_sequence{};

  friend bool operator==(const QuorumSyncReceipt&, const QuorumSyncReceipt&) = default;
};

// Single-thread-affine composition of deterministic Multi-Raft and the shared physical log. A
// successful execute_batch() returns outbound messages only after every persistent transition in
// that batch has been appended and covered by one successful local synchronization.
class DurableMultiRaftRuntime {
public:
  DurableMultiRaftRuntime() = delete;
  ~DurableMultiRaftRuntime();
  DurableMultiRaftRuntime(const DurableMultiRaftRuntime&) = delete;
  DurableMultiRaftRuntime& operator=(const DurableMultiRaftRuntime&) = delete;
  DurableMultiRaftRuntime(DurableMultiRaftRuntime&&) noexcept;
  DurableMultiRaftRuntime& operator=(DurableMultiRaftRuntime&&) noexcept;

  [[nodiscard]] static common::Result<DurableMultiRaftRuntime>
  create_new(NodeId local_node_id, const RaftPersistentLogConfig& log_config,
             std::vector<RaftGroupConfiguration> groups, DurableMultiRaftLimits limits = {});
  [[nodiscard]] static common::Result<DurableMultiRaftRuntime>
  open_existing(NodeId local_node_id, const RaftPersistentLogConfig& log_config,
                const RaftPersistentLogOpenOptions& open_options,
                std::vector<RaftGroupConfiguration> groups, DurableMultiRaftLimits limits = {});

  [[nodiscard]] common::Result<std::vector<DurableRaftResult>>
  execute_batch(std::vector<DurableRaftRequest> requests);

  [[nodiscard]] common::Result<RaftGroupObservation> observe_group(const GroupId& group_id) const;
  [[nodiscard]] const RaftNode* find_group(const GroupId& group_id) const noexcept;
  [[nodiscard]] common::Result<QuorumSyncReceipt> prove_quorum_sync(const GroupId& group_id,
                                                                    LogIndex index) const;
  // Persists a complete full-state checkpoint for all groups and only then reclaims shared-log
  // segments that precede it.
  [[nodiscard]] common::Result<RaftPersistentLogReclamation> checkpoint_and_reclaim();
  [[nodiscard]] RaftPhysicalPosition written_position() const noexcept;
  [[nodiscard]] std::uint64_t durable_physical_sequence() const noexcept;
  [[nodiscard]] bool failed() const noexcept;
  [[nodiscard]] common::Status failure_status() const;
  [[nodiscard]] common::Status close();

private:
  class Impl;
  explicit DurableMultiRaftRuntime(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_DURABLE_RUNTIME_HPP_
