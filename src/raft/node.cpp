#include "chronos/raft/node.hpp"

#include "chronos/raft/membership.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::raft {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return common::Status{common::StatusCode::kCorruption, message};
}

struct JointConfiguration {
  std::vector<NodeId> old_voters;
  std::vector<NodeId> new_voters;
  LogIndex joint_index{};
  bool final_pending{};
};

struct DerivedMembership {
  std::vector<NodeId> committed_voters;
  std::vector<NodeId> active_voters;
  std::optional<JointConfiguration> joint;
};

struct PendingReadBarrier {
  Term term{};
  std::uint64_t context{};
  LogIndex read_index{};
  std::vector<NodeId> old_voters;
  std::vector<NodeId> new_voters;
  std::set<NodeId> acknowledgements;
};

[[nodiscard]] std::vector<NodeId> voter_union(const std::vector<NodeId>& first,
                                              const std::vector<NodeId>& second) {
  std::vector<NodeId> result;
  result.reserve(first.size() + second.size());
  std::ranges::set_union(first, second, std::back_inserter(result));
  return result;
}

[[nodiscard]] bool valid_voters(const std::vector<NodeId>& voters, const std::size_t maximum) {
  return !voters.empty() && voters.size() <= maximum && voters.front() != 0U &&
         std::ranges::is_sorted(voters) &&
         std::adjacent_find(voters.begin(), voters.end()) == voters.end();
}

[[nodiscard]] bool valid_snapshot(const SnapshotMetadata& snapshot,
                                  const std::size_t maximum_voters) {
  return snapshot.last_included_index != 0U && snapshot.last_included_term != 0U &&
         snapshot.manifest_generation != 0U &&
         snapshot.configuration_index <= snapshot.last_included_index &&
         valid_voters(snapshot.voters, maximum_voters);
}

[[nodiscard]] common::Result<DerivedMembership>
derive_membership(const std::vector<NodeId>& base_voters, const std::span<const LogEntry> log,
                  const LogIndex commit_index, const std::size_t maximum_voters) {
  DerivedMembership derived{.committed_voters = base_voters, .active_voters = base_voters};
  for (const LogEntry& entry : log) {
    if (!is_membership_entry_type(entry.type))
      continue;
    auto decoded = decode_membership_command_v1(entry.payload, maximum_voters);
    if (!decoded.has_value())
      return common::make_unexpected(decoded.error());
    if (entry.type == kJointMembershipEntryType) {
      const auto* joint = std::get_if<JointMembershipCommand>(&*decoded);
      if (joint == nullptr || derived.joint.has_value() ||
          joint->old_voters != derived.committed_voters) {
        return common::make_unexpected(
            corruption("Raft log has an invalid joint-membership transition"));
      }
      std::vector<NodeId> active = voter_union(joint->old_voters, joint->new_voters);
      if (active.size() > maximum_voters) {
        return common::make_unexpected(
            invalid("Raft joint membership exceeds configured voter capacity"));
      }
      derived.active_voters = std::move(active);
      derived.joint = JointConfiguration{joint->old_voters, joint->new_voters, entry.index, false};
      continue;
    }
    const auto* final = std::get_if<FinalMembershipCommand>(&*decoded);
    if (final == nullptr || !derived.joint.has_value() || derived.joint->final_pending ||
        final->joint_index != derived.joint->joint_index ||
        final->new_voters != derived.joint->new_voters ||
        derived.joint->joint_index > commit_index) {
      return common::make_unexpected(
          corruption("Raft log has an invalid final-membership transition"));
    }
    if (entry.index <= commit_index) {
      derived.committed_voters = final->new_voters;
      derived.active_voters = final->new_voters;
      derived.joint.reset();
    } else {
      derived.joint->final_pending = true;
    }
  }
  return derived;
}

} // namespace

class RaftNode::Impl {
public:
  Impl(const NodeId id_value, std::vector<NodeId> base_voter_values, DerivedMembership membership,
       PersistentState state_value, const RaftLimits limits_value)
      : id(id_value), base_voters(std::move(base_voter_values)),
        committed_voters(std::move(membership.committed_voters)),
        voters(std::move(membership.active_voters)), joint(std::move(membership.joint)),
        state(std::move(state_value)), limits(limits_value) {}

  [[nodiscard]] LogIndex last_index() const noexcept {
    return state.log.empty() ? state.snapshot.last_included_index : state.log.back().index;
  }

  [[nodiscard]] Term last_term() const noexcept {
    return state.log.empty() ? state.snapshot.last_included_term : state.log.back().term;
  }

  [[nodiscard]] std::optional<Term> term_at(const LogIndex index) const noexcept {
    if (index == 0U) {
      return Term{0U};
    }
    if (index == state.snapshot.last_included_index) {
      return state.snapshot.last_included_term;
    }
    if (index <= state.snapshot.last_included_index || index > last_index()) {
      return std::nullopt;
    }
    const std::size_t offset =
        static_cast<std::size_t>(index - state.snapshot.last_included_index - 1U);
    return state.log[offset].term;
  }

  [[nodiscard]] std::size_t offset_for(const LogIndex index) const noexcept {
    return static_cast<std::size_t>(index - state.snapshot.last_included_index - 1U);
  }

  [[nodiscard]] bool voter(const NodeId node) const noexcept {
    return std::binary_search(voters.begin(), voters.end(), node);
  }

  [[nodiscard]] static std::size_t majority(const std::vector<NodeId>& members) noexcept {
    return members.size() / 2U + 1U;
  }

  template <typename Predicate>
  [[nodiscard]] static bool has_majority(const std::vector<NodeId>& members,
                                         Predicate&& predicate) {
    return static_cast<std::size_t>(std::ranges::count_if(members, predicate)) >= majority(members);
  }

  [[nodiscard]] bool vote_quorum() const {
    const auto voted = [&](const NodeId node) { return votes.contains(node); };
    return joint.has_value()
               ? has_majority(joint->old_voters, voted) && has_majority(joint->new_voters, voted)
               : has_majority(committed_voters, voted);
  }

  [[nodiscard]] bool replication_quorum(const LogIndex index) const {
    const auto replicated = [&](const NodeId node) {
      const auto found = match_index.find(node);
      return found != match_index.end() && found->second >= index;
    };
    return joint.has_value() ? has_majority(joint->old_voters, replicated) &&
                                   has_majority(joint->new_voters, replicated)
                             : has_majority(committed_voters, replicated);
  }

  [[nodiscard]] bool read_barrier_quorum() const {
    if (!pending_read_barrier.has_value())
      return false;
    const auto acknowledged = [&](const NodeId node) {
      return pending_read_barrier->acknowledgements.contains(node);
    };
    return has_majority(pending_read_barrier->old_voters, acknowledged) &&
           (pending_read_barrier->new_voters.empty() ||
            has_majority(pending_read_barrier->new_voters, acknowledged));
  }

  void finish_read_barrier(Transition& transition) {
    if (!read_barrier_quorum())
      return;
    transition.read_barrier_ready =
        ReadBarrier{pending_read_barrier->term, pending_read_barrier->context,
                    pending_read_barrier->read_index};
    pending_read_barrier.reset();
  }

  void install_membership(DerivedMembership membership) {
    committed_voters = std::move(membership.committed_voters);
    voters = std::move(membership.active_voters);
    joint = std::move(membership.joint);
    if (role != Role::kLeader)
      return;
    for (auto iterator = next_index.begin(); iterator != next_index.end();) {
      if (!voter(iterator->first)) {
        match_index.erase(iterator->first);
        iterator = next_index.erase(iterator);
      } else {
        ++iterator;
      }
    }
    for (const NodeId member : voters) {
      if (!next_index.contains(member)) {
        next_index.emplace(member, member == id ? last_index() + 1U : 1U);
        match_index.emplace(member, member == id ? last_index() : 0U);
      }
    }
  }

  void step_down_if_removed() {
    if (role == Role::kLeader && !voter(id))
      become_follower(state.current_term, std::nullopt);
  }

  void become_follower(const Term term, const std::optional<NodeId> leader) {
    role = Role::kFollower;
    leader_id = leader;
    votes.clear();
    next_index.clear();
    match_index.clear();
    pending_read_barrier.reset();
    if (term > state.current_term) {
      state.current_term = term;
      state.voted_for.reset();
    }
  }

  [[nodiscard]] Message replication_for(const NodeId peer) const {
    const LogIndex next = next_index.at(peer);
    if (next <= state.snapshot.last_included_index)
      return InstallSnapshotRequest{state.current_term, id, state.snapshot};
    const LogIndex previous = next - 1U;
    const Term previous_term = term_at(previous).value_or(0U);
    std::vector<LogEntry> entries;
    if (next <= last_index()) {
      const std::size_t begin = offset_for(next);
      const std::size_t count = std::min(limits.maximum_append_entries, state.log.size() - begin);
      entries.insert(entries.end(), state.log.begin() + static_cast<std::ptrdiff_t>(begin),
                     state.log.begin() + static_cast<std::ptrdiff_t>(begin + count));
    }
    return AppendEntriesRequest{state.current_term, id, previous, previous_term, std::move(entries),
                                state.commit_index};
  }

  void append_to_all(Transition& transition) const {
    for (const NodeId peer : voters) {
      if (peer != id) {
        transition.outbound.push_back(OutboundMessage{peer, replication_for(peer)});
      }
    }
  }

  void initialize_leader(Transition& transition) {
    role = Role::kLeader;
    leader_id = id;
    next_index.clear();
    match_index.clear();
    pending_read_barrier.reset();
    for (const NodeId peer : voters) {
      next_index.emplace(peer, last_index() + 1U);
      match_index.emplace(peer, peer == id ? last_index() : 0U);
    }
    append_to_all(transition);
  }

  [[nodiscard]] bool candidate_log_is_current(const LogIndex index,
                                              const Term term) const noexcept {
    return term > last_term() || (term == last_term() && index >= last_index());
  }

  [[nodiscard]] common::Result<bool> advance_commit(Transition& transition) {
    for (LogIndex candidate = last_index(); candidate > state.commit_index; --candidate) {
      if (term_at(candidate) != state.current_term) {
        continue;
      }
      if (replication_quorum(candidate)) {
        auto membership =
            derive_membership(base_voters, state.log, candidate, limits.maximum_voters);
        if (!membership.has_value())
          return common::make_unexpected(membership.error());
        state.commit_index = candidate;
        install_membership(std::move(*membership));
        transition.advanced_commit_index = candidate;
        return true;
      }
    }
    return false;
  }

  NodeId id{};
  std::vector<NodeId> base_voters;
  std::vector<NodeId> committed_voters;
  std::vector<NodeId> voters;
  std::optional<JointConfiguration> joint;
  PersistentState state;
  RaftLimits limits;
  Role role{Role::kFollower};
  std::optional<NodeId> leader_id;
  std::set<NodeId> votes;
  std::map<NodeId, LogIndex> next_index;
  std::map<NodeId, LogIndex> match_index;
  std::optional<InstallSnapshotRequest> pending_snapshot;
  std::optional<PendingReadBarrier> pending_read_barrier;
  std::uint64_t next_read_context{1U};
};

RaftNode::RaftNode(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}
RaftNode::~RaftNode() = default;
RaftNode::RaftNode(RaftNode&&) noexcept = default;
RaftNode& RaftNode::operator=(RaftNode&&) noexcept = default;

common::Result<RaftNode> RaftNode::create(const NodeId node_id, std::vector<NodeId> voters,
                                          PersistentState persistent, const RaftLimits limits) {
  if (node_id == 0U || voters.empty() || voters.size() > limits.maximum_voters ||
      limits.maximum_voters == 0U || limits.maximum_log_entries == 0U ||
      limits.maximum_entry_bytes == 0U || limits.maximum_append_entries == 0U) {
    return common::make_unexpected(invalid("Raft identity, membership, or limits are invalid"));
  }
  std::ranges::sort(voters);
  if (voters.front() == 0U || std::adjacent_find(voters.begin(), voters.end()) != voters.end()) {
    return common::make_unexpected(invalid("Raft voters must be unique and nonzero"));
  }
  if (persistent.snapshot.last_included_index == 0U &&
      persistent.snapshot.last_included_term != 0U) {
    return common::make_unexpected(invalid("Raft snapshot zero index must have zero term"));
  }
  if (persistent.snapshot.last_included_index == 0U &&
      (persistent.snapshot.configuration_index != 0U || !persistent.snapshot.voters.empty())) {
    return common::make_unexpected(invalid("Raft empty snapshot has a membership checkpoint"));
  }
  if (persistent.snapshot.last_included_index == std::numeric_limits<LogIndex>::max()) {
    return common::make_unexpected(
        invalid("Raft maximum log index is reserved for exhaustion detection"));
  }
  LogIndex expected = persistent.snapshot.last_included_index + 1U;
  for (const LogEntry& entry : persistent.log) {
    if (entry.index != expected || entry.index == std::numeric_limits<LogIndex>::max() ||
        entry.term == 0U || entry.term > persistent.current_term || entry.type == 0U ||
        entry.payload.size() > limits.maximum_entry_bytes) {
      return common::make_unexpected(invalid("Raft persistent log is not contiguous or bounded"));
    }
    if (expected == std::numeric_limits<LogIndex>::max() && &entry != &persistent.log.back()) {
      return common::make_unexpected(invalid("Raft persistent log index overflows"));
    }
    ++expected;
  }
  if (persistent.log.size() > limits.maximum_log_entries) {
    return common::make_unexpected(invalid("Raft persistent log exceeds configured capacity"));
  }
  const LogIndex last = persistent.log.empty() ? persistent.snapshot.last_included_index
                                               : persistent.log.back().index;
  std::vector<NodeId> base_voters = voters;
  if (persistent.snapshot.last_included_index != 0U) {
    if (persistent.snapshot.voters.empty()) {
      persistent.snapshot.voters = voters;
    } else {
      base_voters = persistent.snapshot.voters;
    }
    if (!valid_snapshot(persistent.snapshot, limits.maximum_voters)) {
      return common::make_unexpected(invalid("Raft snapshot membership checkpoint is invalid"));
    }
  }
  auto membership = derive_membership(base_voters, persistent.log, persistent.commit_index,
                                      limits.maximum_voters);
  if (!membership.has_value()) {
    return common::make_unexpected(membership.error());
  }
  if (persistent.commit_index < persistent.snapshot.last_included_index ||
      persistent.commit_index > last ||
      persistent.applied_index < persistent.snapshot.last_included_index ||
      persistent.applied_index > persistent.commit_index ||
      (persistent.voted_for.has_value() && *persistent.voted_for == 0U)) {
    return common::make_unexpected(invalid("Raft persistent commit, apply, or vote state invalid"));
  }
  return RaftNode{std::make_unique<Impl>(node_id, std::move(base_voters), std::move(*membership),
                                         std::move(persistent), limits)};
}

common::Result<Transition> RaftNode::start_election() {
  if (!impl_->voter(impl_->id)) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kUnavailable, "nonvoting Raft member cannot start an election"});
  }
  if (impl_->state.current_term == std::numeric_limits<Term>::max() ||
      impl_->last_index() == std::numeric_limits<LogIndex>::max()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kOutOfRange, "Raft term or log index is exhausted"});
  }
  ++impl_->state.current_term;
  impl_->role = Role::kCandidate;
  impl_->leader_id.reset();
  impl_->state.voted_for = impl_->id;
  impl_->votes = {impl_->id};
  impl_->pending_read_barrier.reset();
  Transition transition;
  if (impl_->vote_quorum()) {
    impl_->initialize_leader(transition);
  } else {
    const RequestVoteRequest request{impl_->state.current_term, impl_->id, impl_->last_index(),
                                     impl_->last_term()};
    for (const NodeId peer : impl_->voters) {
      if (peer != impl_->id) {
        transition.outbound.push_back(OutboundMessage{peer, request});
      }
    }
  }
  transition.persistent_state = impl_->state;
  return transition;
}

common::Result<Transition> RaftNode::receive(const NodeId source, Message message) {
  const bool replication_request = std::holds_alternative<AppendEntriesRequest>(message) ||
                                   std::holds_alternative<InstallSnapshotRequest>(message) ||
                                   std::holds_alternative<ReadBarrierRequest>(message);
  if (source == 0U || source == impl_->id || (!impl_->voter(source) && !replication_request)) {
    return common::make_unexpected(
        invalid("Raft message source is invalid or not an active voter"));
  }

  // Validate every fallible, message-local condition before observing a newer term. Otherwise a
  // hostile higher-term message could change current_term and then return an error without giving
  // the runtime the persistence state required by the persist-before-send contract.
  std::optional<DerivedMembership> validated_membership;
  const common::Status validation = std::visit(
      [&](const auto& value) -> common::Status {
        using T = std::remove_cvref_t<decltype(value)>;
        if constexpr (std::is_same_v<T, RequestVoteRequest>) {
          return value.candidate_id == source
                     ? common::Status::ok()
                     : invalid("RequestVote candidate does not match message source");
        } else if constexpr (std::is_same_v<T, AppendEntriesRequest>) {
          if (value.term == 0U || value.leader_id != source ||
              value.entries.size() > impl_->limits.maximum_append_entries) {
            return invalid("AppendEntries leader identity or batch bound is invalid");
          }
          if (value.previous_log_index == std::numeric_limits<LogIndex>::max()) {
            return invalid("AppendEntries log index overflows");
          }
          LogIndex expected = value.previous_log_index;
          for (const LogEntry& entry : value.entries) {
            if (expected == std::numeric_limits<LogIndex>::max()) {
              return invalid("AppendEntries log index overflows");
            }
            ++expected;
            if (entry.index != expected || entry.index == std::numeric_limits<LogIndex>::max() ||
                entry.term == 0U || entry.term > value.term || entry.type == 0U ||
                entry.payload.size() > impl_->limits.maximum_entry_bytes) {
              return invalid("AppendEntries contains an invalid log entry");
            }
          }
          if (value.term < impl_->state.current_term) {
            return common::Status::ok();
          }
          const auto previous_term = impl_->term_at(value.previous_log_index);
          if (!previous_term.has_value() || *previous_term != value.previous_log_term) {
            return common::Status::ok();
          }
          std::optional<std::size_t> first_conflict;
          for (std::size_t index = 0U; index < value.entries.size(); ++index) {
            const LogEntry& entry = value.entries[index];
            const auto local_term = impl_->term_at(entry.index);
            if (local_term.has_value() && *local_term == entry.term) {
              const LogEntry& local = impl_->state.log[impl_->offset_for(entry.index)];
              if (local != entry) {
                return common::Status{common::StatusCode::kCorruption,
                                      "matching Raft term and index have different entry bytes"};
              }
              continue;
            }
            if (local_term.has_value() && entry.index <= impl_->state.commit_index) {
              return common::Status{common::StatusCode::kCorruption,
                                    "leader attempted to overwrite a committed Raft entry"};
            }
            first_conflict = index;
            break;
          }
          if (first_conflict.has_value()) {
            const LogIndex conflict_index = value.entries[*first_conflict].index;
            const std::size_t retained = conflict_index <= impl_->last_index()
                                             ? impl_->offset_for(conflict_index)
                                             : impl_->state.log.size();
            if (retained + (value.entries.size() - *first_conflict) >
                impl_->limits.maximum_log_entries) {
              return common::Status{common::StatusCode::kResourceExhausted,
                                    "Raft log capacity is exhausted"};
            }
          }
          std::vector<LogEntry> candidate_log = impl_->state.log;
          for (const LogEntry& entry : value.entries) {
            const auto local = std::ranges::find(candidate_log, entry.index, &LogEntry::index);
            if (local != candidate_log.end() && local->term != entry.term) {
              candidate_log.erase(local, candidate_log.end());
            }
            if (std::ranges::find(candidate_log, entry.index, &LogEntry::index) ==
                candidate_log.end()) {
              candidate_log.push_back(entry);
            }
          }
          const LogIndex candidate_last = candidate_log.empty()
                                              ? impl_->state.snapshot.last_included_index
                                              : candidate_log.back().index;
          const LogIndex prospective_commit =
              std::max(impl_->state.commit_index, std::min(value.leader_commit, candidate_last));
          auto membership = derive_membership(impl_->base_voters, candidate_log, prospective_commit,
                                              impl_->limits.maximum_voters);
          if (!membership.has_value())
            return membership.error();
          validated_membership = std::move(*membership);
        } else if constexpr (std::is_same_v<T, AppendEntriesResponse>) {
          if (value.success && value.match_index > impl_->last_index()) {
            return invalid("AppendEntries response exceeds the local log");
          }
        } else if constexpr (std::is_same_v<T, InstallSnapshotRequest>) {
          if (value.term == 0U || value.leader_id != source ||
              !valid_snapshot(value.snapshot, impl_->limits.maximum_voters)) {
            return invalid("InstallSnapshot identity or metadata is invalid");
          }
        } else if constexpr (std::is_same_v<T, InstallSnapshotResponse>) {
          if (value.success && value.last_included_index > impl_->last_index())
            return invalid("InstallSnapshot response exceeds the local log");
        } else if constexpr (std::is_same_v<T, ReadBarrierRequest>) {
          if (value.term == 0U || value.leader_id != source || value.context == 0U)
            return invalid("read-barrier request identity or context is invalid");
        } else if constexpr (std::is_same_v<T, ReadBarrierResponse>) {
          if (value.context == 0U)
            return invalid("read-barrier response context is invalid");
        }
        return common::Status::ok();
      },
      message);
  if (!validation.is_ok()) {
    return common::make_unexpected(validation);
  }

  Transition transition;
  bool persistence_changed = false;

  const auto message_term = std::visit([](const auto& value) { return value.term; }, message);
  if (message_term > impl_->state.current_term) {
    impl_->become_follower(message_term, std::nullopt);
    persistence_changed = true;
  }

  auto status = std::visit(
      [&](auto&& value) -> common::Status {
        using T = std::remove_cvref_t<decltype(value)>;
        if constexpr (std::is_same_v<T, RequestVoteRequest>) {
          bool granted = false;
          if (impl_->voter(impl_->id) && value.term == impl_->state.current_term &&
              (!impl_->state.voted_for.has_value() || *impl_->state.voted_for == source) &&
              impl_->candidate_log_is_current(value.last_log_index, value.last_log_term)) {
            impl_->state.voted_for = source;
            impl_->role = Role::kFollower;
            impl_->leader_id.reset();
            granted = true;
            persistence_changed = true;
          }
          transition.outbound.push_back(
              OutboundMessage{source, RequestVoteResponse{impl_->state.current_term, granted}});
          return common::Status::ok();
        } else if constexpr (std::is_same_v<T, RequestVoteResponse>) {
          if (value.term < impl_->state.current_term || impl_->role != Role::kCandidate ||
              value.term != impl_->state.current_term) {
            return common::Status::ok();
          }
          if (value.granted) {
            impl_->votes.insert(source);
            if (impl_->vote_quorum()) {
              impl_->initialize_leader(transition);
            }
          }
          return common::Status::ok();
        } else if constexpr (std::is_same_v<T, AppendEntriesRequest>) {
          if (value.term < impl_->state.current_term) {
            transition.outbound.push_back(OutboundMessage{
                source, AppendEntriesResponse{impl_->state.current_term, false, impl_->last_index(),
                                              std::nullopt, impl_->last_index() + 1U}});
            return common::Status::ok();
          }
          impl_->become_follower(value.term, source);
          const auto previous_term = impl_->term_at(value.previous_log_index);
          if (!previous_term.has_value() || *previous_term != value.previous_log_term) {
            std::optional<Term> conflict_term;
            LogIndex conflict_index = impl_->last_index() + 1U;
            if (previous_term.has_value()) {
              conflict_term = *previous_term;
              conflict_index = value.previous_log_index;
              while (conflict_index > impl_->state.snapshot.last_included_index + 1U &&
                     impl_->term_at(conflict_index - 1U) == conflict_term) {
                --conflict_index;
              }
            }
            transition.outbound.push_back(OutboundMessage{
                source, AppendEntriesResponse{impl_->state.current_term, false, impl_->last_index(),
                                              conflict_term, conflict_index}});
            return common::Status::ok();
          }

          for (const LogEntry& entry : value.entries) {
            const auto local_term = impl_->term_at(entry.index);
            if (local_term.has_value() && *local_term != entry.term) {
              impl_->state.log.erase(impl_->state.log.begin() + static_cast<std::ptrdiff_t>(
                                                                    impl_->offset_for(entry.index)),
                                     impl_->state.log.end());
              persistence_changed = true;
            }
            if (!impl_->term_at(entry.index).has_value()) {
              impl_->state.log.push_back(entry);
              persistence_changed = true;
            }
          }
          const LogIndex accepted =
              value.entries.empty() ? value.previous_log_index : value.entries.back().index;
          const LogIndex new_commit = std::min(value.leader_commit, impl_->last_index());
          if (new_commit > impl_->state.commit_index) {
            impl_->state.commit_index = new_commit;
            transition.advanced_commit_index = new_commit;
            persistence_changed = true;
          }
          if (!validated_membership.has_value())
            return corruption("validated Raft membership state is unavailable");
          impl_->install_membership(std::move(*validated_membership));
          transition.outbound.push_back(
              OutboundMessage{source, AppendEntriesResponse{impl_->state.current_term, true,
                                                            accepted, std::nullopt, 0U}});
          return common::Status::ok();
        } else if constexpr (std::is_same_v<T, AppendEntriesResponse>) {
          if (value.term < impl_->state.current_term || impl_->role != Role::kLeader ||
              value.term != impl_->state.current_term) {
            return common::Status::ok();
          }
          if (value.success) {
            impl_->match_index[source] = std::max(impl_->match_index[source], value.match_index);
            impl_->next_index[source] = impl_->match_index[source] + 1U;
            auto advanced = impl_->advance_commit(transition);
            if (!advanced.has_value())
              return advanced.error();
            if (*advanced) {
              persistence_changed = true;
              impl_->append_to_all(transition);
              impl_->step_down_if_removed();
            } else if (impl_->next_index[source] <= impl_->last_index()) {
              transition.outbound.push_back(
                  OutboundMessage{source, impl_->replication_for(source)});
            }
          } else {
            LogIndex next = value.conflict_index == 0U ? 1U : value.conflict_index;
            if (value.conflict_term.has_value()) {
              for (auto iterator = impl_->state.log.rbegin(); iterator != impl_->state.log.rend();
                   ++iterator) {
                if (iterator->term == *value.conflict_term) {
                  next = iterator->index + 1U;
                  break;
                }
              }
            }
            impl_->next_index[source] =
                std::max<LogIndex>(1U, std::min(next, impl_->last_index() + 1U));
            transition.outbound.push_back(OutboundMessage{source, impl_->replication_for(source)});
          }
          return common::Status::ok();
        } else if constexpr (std::is_same_v<T, InstallSnapshotRequest>) {
          if (value.term < impl_->state.current_term) {
            transition.outbound.push_back(OutboundMessage{
                source, InstallSnapshotResponse{impl_->state.current_term, false,
                                                impl_->state.snapshot.last_included_index}});
            return common::Status::ok();
          }
          impl_->become_follower(value.term, source);
          if (value.snapshot.last_included_index <= impl_->state.snapshot.last_included_index) {
            transition.outbound.push_back(OutboundMessage{
                source, InstallSnapshotResponse{impl_->state.current_term, true,
                                                impl_->state.snapshot.last_included_index}});
            return common::Status::ok();
          }
          impl_->pending_snapshot = value;
          transition.snapshot_install = PendingSnapshotInstall{source, value.snapshot};
          return common::Status::ok();
        } else if constexpr (std::is_same_v<T, InstallSnapshotResponse>) {
          if (value.term < impl_->state.current_term || impl_->role != Role::kLeader ||
              value.term != impl_->state.current_term) {
            return common::Status::ok();
          }
          if (value.success) {
            impl_->match_index[source] =
                std::max(impl_->match_index[source], value.last_included_index);
            impl_->next_index[source] = impl_->match_index[source] + 1U;
          }
          transition.outbound.push_back(OutboundMessage{source, impl_->replication_for(source)});
          return common::Status::ok();
        } else if constexpr (std::is_same_v<T, ReadBarrierRequest>) {
          const bool accepted = value.term == impl_->state.current_term;
          if (accepted)
            impl_->become_follower(value.term, source);
          transition.outbound.push_back(OutboundMessage{
              source, ReadBarrierResponse{impl_->state.current_term, value.context, accepted}});
          return common::Status::ok();
        } else {
          if (value.term < impl_->state.current_term || impl_->role != Role::kLeader ||
              value.term != impl_->state.current_term || !value.accepted ||
              !impl_->pending_read_barrier.has_value() ||
              value.context != impl_->pending_read_barrier->context ||
              value.term != impl_->pending_read_barrier->term) {
            return common::Status::ok();
          }
          impl_->pending_read_barrier->acknowledgements.insert(source);
          impl_->finish_read_barrier(transition);
          return common::Status::ok();
        }
      },
      std::move(message));
  if (!status.is_ok()) {
    return common::make_unexpected(status);
  }
  if (persistence_changed) {
    transition.persistent_state = impl_->state;
  }
  return transition;
}

common::Result<Transition> RaftNode::propose(const std::uint8_t type,
                                             std::vector<std::byte> payload) {
  if (impl_->role != Role::kLeader) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "Raft proposal requires current leader"});
  }
  if (type == 0U || is_membership_entry_type(type) ||
      payload.size() > impl_->limits.maximum_entry_bytes ||
      impl_->state.log.size() >= impl_->limits.maximum_log_entries ||
      impl_->last_index() >= std::numeric_limits<LogIndex>::max() - 1U) {
    return common::make_unexpected(invalid("Raft proposal type, size, or log bound invalid"));
  }
  const LogIndex index = impl_->last_index() + 1U;
  impl_->state.log.push_back(LogEntry{index, impl_->state.current_term, type, std::move(payload)});
  impl_->match_index[impl_->id] = index;
  impl_->next_index[impl_->id] = index + 1U;
  Transition transition;
  auto advanced = impl_->advance_commit(transition);
  if (!advanced.has_value())
    return common::make_unexpected(advanced.error());
  impl_->append_to_all(transition);
  transition.persistent_state = impl_->state;
  return transition;
}

common::Result<Transition> RaftNode::begin_membership_change(std::vector<NodeId> new_voters) {
  if (impl_->role != Role::kLeader) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kUnavailable, "membership change requires current Raft leader"});
  }
  if (impl_->pending_read_barrier.has_value()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable,
                       "membership change cannot start while a Raft read barrier is pending"});
  }
  std::ranges::sort(new_voters);
  if (impl_->joint.has_value() || !valid_voters(new_voters, impl_->limits.maximum_voters) ||
      new_voters == impl_->committed_voters ||
      voter_union(impl_->committed_voters, new_voters).size() > impl_->limits.maximum_voters ||
      impl_->state.log.size() >= impl_->limits.maximum_log_entries ||
      impl_->last_index() >= std::numeric_limits<LogIndex>::max() - 1U) {
    return common::make_unexpected(
        invalid("Raft membership change is invalid or another change is active"));
  }
  auto payload = encode_membership_command_v1(
      JointMembershipCommand{impl_->committed_voters, std::move(new_voters)},
      impl_->limits.maximum_voters);
  if (!payload.has_value())
    return common::make_unexpected(payload.error());
  if (payload->size() > impl_->limits.maximum_entry_bytes)
    return common::make_unexpected(invalid("Raft membership command exceeds entry size limit"));

  const LogIndex index = impl_->last_index() + 1U;
  impl_->state.log.push_back(
      LogEntry{index, impl_->state.current_term, kJointMembershipEntryType, std::move(*payload)});
  auto membership = derive_membership(impl_->base_voters, impl_->state.log,
                                      impl_->state.commit_index, impl_->limits.maximum_voters);
  if (!membership.has_value())
    return common::make_unexpected(membership.error());
  impl_->install_membership(std::move(*membership));
  impl_->match_index[impl_->id] = index;
  impl_->next_index[impl_->id] = index + 1U;

  Transition transition;
  auto advanced = impl_->advance_commit(transition);
  if (!advanced.has_value())
    return common::make_unexpected(advanced.error());
  impl_->append_to_all(transition);
  impl_->step_down_if_removed();
  transition.persistent_state = impl_->state;
  return transition;
}

common::Result<Transition> RaftNode::finalize_membership_change() {
  if (impl_->role != Role::kLeader) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kUnavailable, "membership change requires current Raft leader"});
  }
  if (impl_->pending_read_barrier.has_value()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable,
                       "membership change cannot finalize while a Raft read barrier is pending"});
  }
  if (!impl_->joint.has_value() || impl_->joint->final_pending ||
      impl_->joint->joint_index > impl_->state.commit_index ||
      impl_->state.log.size() >= impl_->limits.maximum_log_entries ||
      impl_->last_index() >= std::numeric_limits<LogIndex>::max() - 1U) {
    return common::make_unexpected(
        invalid("joint membership must commit before it can be finalized"));
  }
  auto payload = encode_membership_command_v1(
      FinalMembershipCommand{impl_->joint->joint_index, impl_->joint->new_voters},
      impl_->limits.maximum_voters);
  if (!payload.has_value())
    return common::make_unexpected(payload.error());
  if (payload->size() > impl_->limits.maximum_entry_bytes)
    return common::make_unexpected(invalid("Raft membership command exceeds entry size limit"));

  const LogIndex index = impl_->last_index() + 1U;
  impl_->state.log.push_back(
      LogEntry{index, impl_->state.current_term, kFinalMembershipEntryType, std::move(*payload)});
  auto membership = derive_membership(impl_->base_voters, impl_->state.log,
                                      impl_->state.commit_index, impl_->limits.maximum_voters);
  if (!membership.has_value())
    return common::make_unexpected(membership.error());
  impl_->install_membership(std::move(*membership));
  impl_->match_index[impl_->id] = index;
  impl_->next_index[impl_->id] = index + 1U;

  Transition transition;
  auto advanced = impl_->advance_commit(transition);
  if (!advanced.has_value())
    return common::make_unexpected(advanced.error());
  impl_->append_to_all(transition);
  impl_->step_down_if_removed();
  transition.persistent_state = impl_->state;
  return transition;
}

common::Result<Transition> RaftNode::complete_snapshot_install(const NodeId source,
                                                               SnapshotMetadata snapshot,
                                                               const bool installed) {
  if (!impl_->pending_snapshot.has_value() || source != impl_->pending_snapshot->leader_id ||
      snapshot != impl_->pending_snapshot->snapshot) {
    return common::make_unexpected(invalid("snapshot completion does not match pending install"));
  }
  const Term request_term = impl_->pending_snapshot->term;
  impl_->pending_snapshot.reset();
  Transition transition;
  if (!installed || request_term != impl_->state.current_term) {
    transition.outbound.push_back(OutboundMessage{
        source, InstallSnapshotResponse{impl_->state.current_term, false,
                                        impl_->state.snapshot.last_included_index}});
    return transition;
  }

  std::vector<LogEntry> retained;
  const auto local_term = impl_->term_at(snapshot.last_included_index);
  if (snapshot.last_included_index < impl_->state.commit_index &&
      (!local_term.has_value() || *local_term != snapshot.last_included_term)) {
    return common::make_unexpected(
        corruption("installed snapshot conflicts with the local committed prefix"));
  }
  if (local_term.has_value() && *local_term == snapshot.last_included_term &&
      snapshot.last_included_index < impl_->last_index()) {
    const std::size_t first = impl_->offset_for(snapshot.last_included_index) + 1U;
    retained.assign(impl_->state.log.begin() + static_cast<std::ptrdiff_t>(first),
                    impl_->state.log.end());
  }
  const LogIndex new_commit = std::max(impl_->state.commit_index, snapshot.last_included_index);
  auto membership =
      derive_membership(snapshot.voters, retained, new_commit, impl_->limits.maximum_voters);
  if (!membership.has_value())
    return common::make_unexpected(membership.error());

  impl_->state.snapshot = std::move(snapshot);
  impl_->state.log = std::move(retained);
  impl_->state.commit_index = new_commit;
  impl_->state.applied_index =
      std::max(impl_->state.applied_index, impl_->state.snapshot.last_included_index);
  impl_->base_voters = impl_->state.snapshot.voters;
  impl_->install_membership(std::move(*membership));
  transition.persistent_state = impl_->state;
  transition.advanced_commit_index = impl_->state.commit_index;
  transition.outbound.push_back(
      OutboundMessage{source, InstallSnapshotResponse{impl_->state.current_term, true,
                                                      impl_->state.snapshot.last_included_index}});
  return transition;
}

common::Result<Transition> RaftNode::compact_snapshot(SnapshotMetadata snapshot) {
  if (impl_->joint.has_value() ||
      snapshot.last_included_index <= impl_->state.snapshot.last_included_index ||
      snapshot.last_included_index > impl_->state.applied_index ||
      snapshot.last_included_term == 0U || snapshot.manifest_generation == 0U ||
      impl_->term_at(snapshot.last_included_index) != snapshot.last_included_term) {
    return common::make_unexpected(
        invalid("Raft snapshot must cover an applied stable-configuration prefix"));
  }
  snapshot.voters = impl_->committed_voters;
  snapshot.configuration_index = impl_->state.snapshot.configuration_index;
  for (const LogEntry& entry : impl_->state.log) {
    if (entry.index > snapshot.last_included_index)
      break;
    if (entry.type == kFinalMembershipEntryType)
      snapshot.configuration_index = entry.index;
  }
  if (!valid_snapshot(snapshot, impl_->limits.maximum_voters))
    return common::make_unexpected(invalid("Raft snapshot metadata is invalid"));
  const std::size_t retained_offset = impl_->offset_for(snapshot.last_included_index) + 1U;
  impl_->state.log.erase(impl_->state.log.begin(),
                         impl_->state.log.begin() + static_cast<std::ptrdiff_t>(retained_offset));
  impl_->state.snapshot = std::move(snapshot);
  impl_->base_voters = impl_->state.snapshot.voters;
  Transition transition;
  transition.persistent_state = impl_->state;
  return transition;
}

common::Result<Transition> RaftNode::heartbeat() {
  if (impl_->role != Role::kLeader) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "Raft heartbeat requires current leader"});
  }
  Transition transition;
  impl_->append_to_all(transition);
  return transition;
}

common::Result<Transition> RaftNode::begin_read_barrier() {
  if (impl_->role != Role::kLeader) {
    return common::make_unexpected(common::Status{common::StatusCode::kUnavailable,
                                                  "Raft read barrier requires current leader"});
  }
  if (impl_->pending_read_barrier.has_value()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "a Raft read barrier is already pending"});
  }
  if (impl_->state.commit_index == 0U ||
      impl_->term_at(impl_->state.commit_index) != impl_->state.current_term) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable,
                       "Raft leader must commit a current-term entry before confirming reads"});
  }
  if (impl_->next_read_context == 0U) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kOutOfRange, "Raft read-barrier context is exhausted"});
  }

  const std::uint64_t context = impl_->next_read_context;
  impl_->next_read_context =
      context == std::numeric_limits<std::uint64_t>::max() ? 0U : context + 1U;
  std::vector<NodeId> new_voters;
  std::vector<NodeId> old_voters = impl_->committed_voters;
  if (impl_->joint.has_value()) {
    old_voters = impl_->joint->old_voters;
    new_voters = impl_->joint->new_voters;
  }
  impl_->pending_read_barrier = PendingReadBarrier{impl_->state.current_term, context,
                                                   impl_->state.commit_index, std::move(old_voters),
                                                   std::move(new_voters),     {impl_->id}};
  Transition transition;
  impl_->finish_read_barrier(transition);
  if (transition.read_barrier_ready.has_value())
    return transition;
  const ReadBarrierRequest request{impl_->state.current_term, impl_->id, context};
  for (const NodeId peer : impl_->voters) {
    if (peer != impl_->id)
      transition.outbound.push_back(OutboundMessage{peer, request});
  }
  return transition;
}

common::Result<Transition> RaftNode::mark_applied(const LogIndex index) {
  if (index < impl_->state.applied_index || index > impl_->state.commit_index) {
    return common::make_unexpected(
        invalid("Raft applied index must advance within the committed prefix"));
  }
  Transition transition;
  if (index == impl_->state.applied_index)
    return transition;
  impl_->state.applied_index = index;
  transition.persistent_state = impl_->state;
  return transition;
}

std::span<const LogEntry> RaftNode::committed_unapplied() const noexcept {
  if (impl_->state.applied_index >= impl_->state.commit_index) {
    return {};
  }
  const LogIndex first = impl_->state.applied_index + 1U;
  const std::size_t begin = impl_->offset_for(first);
  const std::size_t count =
      static_cast<std::size_t>(impl_->state.commit_index - impl_->state.applied_index);
  return std::span<const LogEntry>{impl_->state.log}.subspan(begin, count);
}

NodeId RaftNode::node_id() const noexcept {
  return impl_->id;
}
Role RaftNode::role() const noexcept {
  return impl_->role;
}
Term RaftNode::current_term() const noexcept {
  return impl_->state.current_term;
}
std::optional<NodeId> RaftNode::leader_id() const noexcept {
  return impl_->leader_id;
}
LogIndex RaftNode::last_log_index() const noexcept {
  return impl_->last_index();
}
LogIndex RaftNode::commit_index() const noexcept {
  return impl_->state.commit_index;
}
LogIndex RaftNode::applied_index() const noexcept {
  return impl_->state.applied_index;
}
std::span<const NodeId> RaftNode::voters() const noexcept {
  return impl_->voters;
}
bool RaftNode::joint_membership_active() const noexcept {
  return impl_->joint.has_value();
}
const PersistentState& RaftNode::persistent_state() const noexcept {
  return impl_->state;
}

} // namespace chronos::raft
