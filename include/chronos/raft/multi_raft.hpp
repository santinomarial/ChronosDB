#ifndef CHRONOS_RAFT_MULTI_RAFT_HPP_
#define CHRONOS_RAFT_MULTI_RAFT_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/raft/node.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::raft {

using GroupId = common::Uuid;

struct GroupOutboundMessage {
  GroupId group_id;
  NodeId source{};
  OutboundMessage outbound;
};

struct GroupPersistentState {
  GroupId group_id;
  std::uint64_t physical_sequence{};
  PersistentState state;

  friend bool operator==(const GroupPersistentState&, const GroupPersistentState&) = default;
};

struct MultiRaftTransition {
  std::optional<GroupPersistentState> persistence;
  std::vector<GroupOutboundMessage> outbound;
  std::optional<LogIndex> advanced_commit_index;
};

struct MultiRaftLimits {
  std::size_t maximum_groups{4096U};
  std::size_t maximum_queued_outbound{65'536U};
  RaftLimits raft;
};

// One node-local, single-worker owner for many deterministic groups. Timers, transport, and disk
// are external shared services. A returned persistence state must be appended to the shared
// physical log before any associated outbound message is released.
class MultiRaftRuntime {
public:
  MultiRaftRuntime() = delete;
  ~MultiRaftRuntime();
  MultiRaftRuntime(const MultiRaftRuntime&) = delete;
  MultiRaftRuntime& operator=(const MultiRaftRuntime&) = delete;
  MultiRaftRuntime(MultiRaftRuntime&&) noexcept;
  MultiRaftRuntime& operator=(MultiRaftRuntime&&) noexcept;

  [[nodiscard]] static common::Result<MultiRaftRuntime> create(NodeId local_node_id,
                                                               MultiRaftLimits limits = {});

  [[nodiscard]] common::Status add_group(GroupId group_id, std::vector<NodeId> voters,
                                         PersistentState persistent = {},
                                         std::uint64_t recovered_physical_sequence = 0U);
  [[nodiscard]] common::Status remove_group(const GroupId& group_id);

  [[nodiscard]] common::Result<MultiRaftTransition> start_election(const GroupId& group_id);
  [[nodiscard]] common::Result<MultiRaftTransition> receive(const GroupId& group_id, NodeId source,
                                                            Message message);
  [[nodiscard]] common::Result<MultiRaftTransition>
  propose(const GroupId& group_id, std::uint8_t type, std::vector<std::byte> payload);
  [[nodiscard]] common::Result<MultiRaftTransition>
  begin_membership_change(const GroupId& group_id, std::vector<NodeId> new_voters);
  [[nodiscard]] common::Result<MultiRaftTransition>
  finalize_membership_change(const GroupId& group_id);
  [[nodiscard]] common::Result<MultiRaftTransition> heartbeat(const GroupId& group_id);
  [[nodiscard]] common::Result<MultiRaftTransition> mark_applied(const GroupId& group_id,
                                                                 LogIndex index);

  [[nodiscard]] const RaftNode* find_group(const GroupId& group_id) const noexcept;
  [[nodiscard]] std::size_t group_count() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  class Impl;
  explicit MultiRaftRuntime(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_MULTI_RAFT_HPP_
