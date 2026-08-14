#include "chronos/raft/multi_raft.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <new>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}

} // namespace

class MultiRaftRuntime::Impl {
public:
  struct GroupState {
    RaftNode node;
  };

  Impl(const NodeId id, const MultiRaftLimits configured) : local_node_id(id), limits(configured) {}

  [[nodiscard]] common::Result<MultiRaftTransition> wrap(const GroupId& group_id,
                                                         common::Result<Transition> transition) {
    if (!transition.has_value()) {
      return common::make_unexpected(transition.error());
    }
    if (transition->outbound.size() > limits.maximum_queued_outbound) {
      failed_state = true;
      return common::make_unexpected(
          common::Status{common::StatusCode::kResourceExhausted,
                         "one Multi-Raft transition exceeds the bounded outbound batch"});
    }
    auto group = groups.find(group_id);
    if (group == groups.end()) {
      failed_state = true;
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "Multi-Raft group disappeared"});
    }
    Transition core = std::move(*transition);
    MultiRaftTransition output;
    output.advanced_commit_index = core.advanced_commit_index;
    std::optional<PendingSnapshotInstall> snapshot_install = std::move(core.snapshot_install);
    if (snapshot_install.has_value()) {
      output.snapshot_install = GroupSnapshotInstall{group_id, std::move(snapshot_install).value()};
    }
    const std::optional<ReadBarrier> read_barrier = core.read_barrier_ready;
    if (read_barrier.has_value()) {
      output.read_barrier_ready = GroupReadBarrier{group_id, read_barrier.value()};
    }
    std::optional<PersistentState> persistent_state = std::move(core.persistent_state);
    if (persistent_state.has_value()) {
      if (physical_sequence == std::numeric_limits<std::uint64_t>::max()) {
        failed_state = true;
        return common::make_unexpected(
            common::Status{common::StatusCode::kOutOfRange,
                           "Multi-Raft physical persistence sequence is exhausted"});
      }
      ++physical_sequence;
      output.persistence =
          GroupPersistentState{group_id, physical_sequence, std::move(persistent_state).value()};
    }
    output.outbound.reserve(core.outbound.size());
    for (OutboundMessage& message : core.outbound) {
      output.outbound.push_back(GroupOutboundMessage{group_id, local_node_id, std::move(message)});
    }
    return output;
  }

  NodeId local_node_id{};
  MultiRaftLimits limits;
  std::map<GroupId, GroupState> groups;
  std::uint64_t physical_sequence{};
  bool failed_state{};
};

MultiRaftRuntime::MultiRaftRuntime(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}
MultiRaftRuntime::~MultiRaftRuntime() = default;
MultiRaftRuntime::MultiRaftRuntime(MultiRaftRuntime&&) noexcept = default;
MultiRaftRuntime& MultiRaftRuntime::operator=(MultiRaftRuntime&&) noexcept = default;

common::Result<MultiRaftRuntime> MultiRaftRuntime::create(const NodeId local_node_id,
                                                          const MultiRaftLimits limits) {
  if (local_node_id == 0U || limits.maximum_groups == 0U || limits.maximum_queued_outbound == 0U) {
    return common::make_unexpected(invalid("Multi-Raft node identity or limits are invalid"));
  }
  return MultiRaftRuntime{std::make_unique<Impl>(local_node_id, limits)};
}

common::Status MultiRaftRuntime::add_group(const GroupId group_id, std::vector<NodeId> voters,
                                           PersistentState persistent,
                                           const std::uint64_t recovered_physical_sequence) {
  if (impl_->failed_state) {
    return common::Status{common::StatusCode::kUnavailable, "Multi-Raft runtime has failed closed"};
  }
  if (group_id.is_nil()) {
    return invalid("Multi-Raft group identity must be nonzero");
  }
  if (impl_->groups.contains(group_id)) {
    return common::Status{common::StatusCode::kAlreadyExists, "Multi-Raft group already exists"};
  }
  if (impl_->groups.size() >= impl_->limits.maximum_groups) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "Multi-Raft group capacity is exhausted"};
  }
  auto node = RaftNode::create(impl_->local_node_id, std::move(voters), std::move(persistent),
                               impl_->limits.raft);
  if (!node.has_value()) {
    return node.error();
  }
  impl_->physical_sequence = std::max(impl_->physical_sequence, recovered_physical_sequence);
  impl_->groups.emplace(group_id, Impl::GroupState{std::move(*node)});
  return common::Status::ok();
}

common::Status MultiRaftRuntime::remove_group(const GroupId& group_id) {
  if (impl_->failed_state) {
    return common::Status{common::StatusCode::kUnavailable, "Multi-Raft runtime has failed closed"};
  }
  if (impl_->groups.erase(group_id) == 0U) {
    return common::Status{common::StatusCode::kNotFound, "Multi-Raft group does not exist"};
  }
  return common::Status::ok();
}

common::Result<MultiRaftTransition> MultiRaftRuntime::start_election(const GroupId& group_id) {
  if (impl_->failed_state) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "Multi-Raft runtime has failed closed"});
  }
  const auto group = impl_->groups.find(group_id);
  if (group == impl_->groups.end()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "Multi-Raft group does not exist"});
  }
  return impl_->wrap(group_id, group->second.node.start_election());
}

common::Result<MultiRaftTransition>
MultiRaftRuntime::receive(const GroupId& group_id, const NodeId source, Message message) {
  if (impl_->failed_state) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "Multi-Raft runtime has failed closed"});
  }
  const auto group = impl_->groups.find(group_id);
  if (group == impl_->groups.end()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "Multi-Raft group does not exist"});
  }
  return impl_->wrap(group_id, group->second.node.receive(source, std::move(message)));
}

common::Result<MultiRaftTransition> MultiRaftRuntime::propose(const GroupId& group_id,
                                                              const std::uint8_t type,
                                                              std::vector<std::byte> payload) {
  if (impl_->failed_state) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "Multi-Raft runtime has failed closed"});
  }
  const auto group = impl_->groups.find(group_id);
  if (group == impl_->groups.end()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "Multi-Raft group does not exist"});
  }
  return impl_->wrap(group_id, group->second.node.propose(type, std::move(payload)));
}

common::Result<MultiRaftTransition>
MultiRaftRuntime::propose_exact_retained(const GroupId& group_id, const std::uint8_t type,
                                         std::vector<std::byte> payload) {
  if (impl_->failed_state) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "Multi-Raft runtime has failed closed"});
  }
  const auto group = impl_->groups.find(group_id);
  if (group == impl_->groups.end()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "Multi-Raft group does not exist"});
  }
  return impl_->wrap(group_id, group->second.node.propose_exact_retained(type, std::move(payload)));
}

common::Result<MultiRaftTransition> MultiRaftRuntime::commit_current_term(const GroupId& group_id) {
  if (impl_->failed_state) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "Multi-Raft runtime has failed closed"});
  }
  const auto group = impl_->groups.find(group_id);
  if (group == impl_->groups.end()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "Multi-Raft group does not exist"});
  }
  return impl_->wrap(group_id, group->second.node.commit_current_term());
}

common::Result<MultiRaftTransition>
MultiRaftRuntime::begin_membership_change(const GroupId& group_id, std::vector<NodeId> new_voters) {
  if (impl_->failed_state) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "Multi-Raft runtime has failed closed"});
  }
  const auto group = impl_->groups.find(group_id);
  if (group == impl_->groups.end()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "Multi-Raft group does not exist"});
  }
  return impl_->wrap(group_id, group->second.node.begin_membership_change(std::move(new_voters)));
}

common::Result<MultiRaftTransition>
MultiRaftRuntime::finalize_membership_change(const GroupId& group_id) {
  if (impl_->failed_state) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "Multi-Raft runtime has failed closed"});
  }
  const auto group = impl_->groups.find(group_id);
  if (group == impl_->groups.end()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "Multi-Raft group does not exist"});
  }
  return impl_->wrap(group_id, group->second.node.finalize_membership_change());
}

common::Result<MultiRaftTransition>
MultiRaftRuntime::complete_snapshot_install(const GroupId& group_id, const NodeId source,
                                            SnapshotMetadata snapshot, const bool installed) {
  if (impl_->failed_state) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "Multi-Raft runtime has failed closed"});
  }
  const auto group = impl_->groups.find(group_id);
  if (group == impl_->groups.end()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "Multi-Raft group does not exist"});
  }
  return impl_->wrap(group_id, group->second.node.complete_snapshot_install(
                                   source, std::move(snapshot), installed));
}

common::Result<MultiRaftTransition> MultiRaftRuntime::compact_snapshot(const GroupId& group_id,
                                                                       SnapshotMetadata snapshot) {
  if (impl_->failed_state) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "Multi-Raft runtime has failed closed"});
  }
  const auto group = impl_->groups.find(group_id);
  if (group == impl_->groups.end()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "Multi-Raft group does not exist"});
  }
  return impl_->wrap(group_id, group->second.node.compact_snapshot(std::move(snapshot)));
}

common::Result<MultiRaftTransition> MultiRaftRuntime::heartbeat(const GroupId& group_id) {
  if (impl_->failed_state) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "Multi-Raft runtime has failed closed"});
  }
  const auto group = impl_->groups.find(group_id);
  if (group == impl_->groups.end()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "Multi-Raft group does not exist"});
  }
  return impl_->wrap(group_id, group->second.node.heartbeat());
}

common::Result<MultiRaftTransition> MultiRaftRuntime::begin_read_barrier(const GroupId& group_id) {
  if (impl_->failed_state) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "Multi-Raft runtime has failed closed"});
  }
  const auto group = impl_->groups.find(group_id);
  if (group == impl_->groups.end()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "Multi-Raft group does not exist"});
  }
  return impl_->wrap(group_id, group->second.node.begin_read_barrier());
}

common::Result<MultiRaftTransition> MultiRaftRuntime::mark_applied(const GroupId& group_id,
                                                                   const LogIndex index) {
  if (impl_->failed_state) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "Multi-Raft runtime has failed closed"});
  }
  const auto group = impl_->groups.find(group_id);
  if (group == impl_->groups.end()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "Multi-Raft group does not exist"});
  }
  return impl_->wrap(group_id, group->second.node.mark_applied(index));
}

common::Result<std::vector<GroupPersistentState>>
MultiRaftRuntime::create_persistence_checkpoint() {
  if (impl_->failed_state) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "Multi-Raft runtime has failed closed"});
  }
  if (impl_->groups.empty()) {
    return common::make_unexpected(
        invalid("Multi-Raft persistence checkpoint requires at least one group"));
  }
  if (impl_->groups.size() > std::numeric_limits<std::uint64_t>::max() - impl_->physical_sequence) {
    impl_->failed_state = true;
    return common::make_unexpected(common::Status{
        common::StatusCode::kOutOfRange, "Multi-Raft physical persistence sequence is exhausted"});
  }
  try {
    std::vector<GroupPersistentState> checkpoint;
    checkpoint.reserve(impl_->groups.size());
    std::uint64_t sequence = impl_->physical_sequence;
    for (const auto& [group_id, group] : impl_->groups) {
      checkpoint.push_back(
          GroupPersistentState{group_id, ++sequence, group.node.persistent_state()});
    }
    impl_->physical_sequence = sequence;
    return checkpoint;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "Multi-Raft checkpoint allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kResourceExhausted, "Multi-Raft checkpoint exceeds container limits"});
  }
}

const RaftNode* MultiRaftRuntime::find_group(const GroupId& group_id) const noexcept {
  const auto group = impl_->groups.find(group_id);
  return group == impl_->groups.end() ? nullptr : &group->second.node;
}

std::size_t MultiRaftRuntime::group_count() const noexcept {
  return impl_->groups.size();
}
bool MultiRaftRuntime::failed() const noexcept {
  return impl_->failed_state;
}

} // namespace chronos::raft
