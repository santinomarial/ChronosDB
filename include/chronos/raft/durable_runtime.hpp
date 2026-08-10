#ifndef CHRONOS_RAFT_DURABLE_RUNTIME_HPP_
#define CHRONOS_RAFT_DURABLE_RUNTIME_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/multi_raft.hpp"
#include "chronos/raft/persistent_log.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
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
                 BeginMembershipChangeOperation, FinalizeMembershipChangeOperation,
                 CompleteSnapshotInstallOperation, CompactSnapshotOperation, HeartbeatOperation,
                 BeginReadBarrierOperation, MarkAppliedOperation>;

struct DurableRaftRequest {
  GroupId group_id;
  DurableRaftOperation operation;
};

struct DurableRaftResult {
  common::Status status;
  std::optional<MultiRaftTransition> transition;
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

  [[nodiscard]] const RaftNode* find_group(const GroupId& group_id) const noexcept;
  [[nodiscard]] common::Result<QuorumSyncReceipt> prove_quorum_sync(const GroupId& group_id,
                                                                    LogIndex index) const;
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
