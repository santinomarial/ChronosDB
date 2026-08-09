#include "chronos/raft/multi_raft.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
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
    MultiRaftTransition output;
    output.advanced_commit_index = transition->advanced_commit_index;
    if (transition->persistent_state.has_value()) {
      if (physical_sequence == std::numeric_limits<std::uint64_t>::max()) {
        failed_state = true;
        return common::make_unexpected(
            common::Status{common::StatusCode::kOutOfRange,
                           "Multi-Raft physical persistence sequence is exhausted"});
      }
      ++physical_sequence;
      output.persistence = GroupPersistentState{group_id, physical_sequence,
                                                std::move(*transition->persistent_state)};
    }
    output.outbound.reserve(transition->outbound.size());
    for (OutboundMessage& message : transition->outbound) {
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

common::Status MultiRaftRuntime::mark_applied(const GroupId& group_id, const LogIndex index) {
  if (impl_->failed_state) {
    return common::Status{common::StatusCode::kUnavailable, "Multi-Raft runtime has failed closed"};
  }
  const auto group = impl_->groups.find(group_id);
  if (group == impl_->groups.end()) {
    return common::Status{common::StatusCode::kNotFound, "Multi-Raft group does not exist"};
  }
  return group->second.node.mark_applied(index);
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
