#ifndef CHRONOS_RAFT_NODE_HPP_
#define CHRONOS_RAFT_NODE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/types.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace chronos::raft {

struct RaftLimits {
  std::size_t maximum_voters{31U};
  std::size_t maximum_log_entries{1U << 20U};
  std::size_t maximum_entry_bytes{std::size_t{16U} * 1024U * 1024U};
  std::size_t maximum_append_entries{1024U};
};

class RaftNode {
public:
  RaftNode() = delete;
  ~RaftNode();
  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;
  RaftNode(RaftNode&&) noexcept;
  RaftNode& operator=(RaftNode&&) noexcept;

  [[nodiscard]] static common::Result<RaftNode> create(NodeId node_id, std::vector<NodeId> voters,
                                                       PersistentState persistent = {},
                                                       RaftLimits limits = {});

  // Called by the timer runtime after its randomized election deadline. The core itself owns no
  // clock or random source.
  [[nodiscard]] common::Result<Transition> start_election();
  [[nodiscard]] common::Result<Transition> receive(NodeId source, Message message);
  [[nodiscard]] common::Result<Transition> propose(std::uint8_t type,
                                                   std::vector<std::byte> payload);
  [[nodiscard]] common::Result<Transition> propose_exact_retained(std::uint8_t type,
                                                                  std::vector<std::byte> payload);
  [[nodiscard]] common::Result<Transition> commit_current_term();
  [[nodiscard]] common::Result<Transition> begin_membership_change(std::vector<NodeId> new_voters);
  [[nodiscard]] common::Result<Transition> finalize_membership_change();
  // Exactly one external installation may be pending. Duplicate requests coalesce; a competing
  // request receives a negative response without replacing the original completion identity.
  [[nodiscard]] common::Result<Transition>
  complete_snapshot_install(NodeId source, SnapshotMetadata snapshot, bool installed);
  // Local compaction is unavailable while an externally owned snapshot installation is pending.
  // The caller must complete or reject that installation before creating another snapshot identity.
  [[nodiscard]] common::Result<Transition> compact_snapshot(SnapshotMetadata snapshot);
  [[nodiscard]] common::Result<Transition> heartbeat();
  // Starts one bounded current-term quorum probe. The leader must first have committed an entry in
  // its current term. Only one read barrier may be pending; completion is reported by receive().
  [[nodiscard]] common::Result<Transition> begin_read_barrier();

  // Advancing application state is itself persistent state. The returned transition must cross the
  // same persistence boundary as term, vote, log, and commit changes.
  [[nodiscard]] common::Result<Transition> mark_applied(LogIndex index);
  [[nodiscard]] std::span<const LogEntry> committed_unapplied() const noexcept;

  [[nodiscard]] NodeId node_id() const noexcept;
  [[nodiscard]] Role role() const noexcept;
  [[nodiscard]] Term current_term() const noexcept;
  [[nodiscard]] std::optional<NodeId> leader_id() const noexcept;
  [[nodiscard]] LogIndex last_log_index() const noexcept;
  [[nodiscard]] LogIndex commit_index() const noexcept;
  [[nodiscard]] LogIndex applied_index() const noexcept;
  [[nodiscard]] std::span<const NodeId> voters() const noexcept;
  [[nodiscard]] std::span<const NodeId> committed_voters() const noexcept;
  [[nodiscard]] std::span<const NodeId> joint_old_voters() const noexcept;
  [[nodiscard]] std::span<const NodeId> joint_new_voters() const noexcept;
  [[nodiscard]] bool joint_membership_active() const noexcept;
  [[nodiscard]] bool joint_membership_can_finalize() const noexcept;
  [[nodiscard]] bool final_membership_pending() const noexcept;
  [[nodiscard]] const PersistentState& persistent_state() const noexcept;

private:
  class Impl;
  explicit RaftNode(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_NODE_HPP_
