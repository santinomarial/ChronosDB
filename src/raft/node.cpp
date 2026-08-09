#include "chronos/raft/node.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
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

} // namespace

class RaftNode::Impl {
public:
  Impl(const NodeId id_value, std::vector<NodeId> voter_values, PersistentState state_value,
       const RaftLimits limits_value)
      : id(id_value), voters(std::move(voter_values)), state(std::move(state_value)),
        limits(limits_value) {}

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

  [[nodiscard]] std::size_t majority() const noexcept {
    return voters.size() / 2U + 1U;
  }

  void become_follower(const Term term, const std::optional<NodeId> leader) {
    role = Role::kFollower;
    leader_id = leader;
    votes.clear();
    next_index.clear();
    match_index.clear();
    if (term > state.current_term) {
      state.current_term = term;
      state.voted_for.reset();
    }
  }

  [[nodiscard]] AppendEntriesRequest append_for(const NodeId peer) const {
    const LogIndex next = next_index.at(peer);
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
        transition.outbound.push_back(OutboundMessage{peer, append_for(peer)});
      }
    }
  }

  void initialize_leader(Transition& transition) {
    role = Role::kLeader;
    leader_id = id;
    next_index.clear();
    match_index.clear();
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

  [[nodiscard]] bool advance_commit(Transition& transition) {
    for (LogIndex candidate = last_index(); candidate > state.commit_index; --candidate) {
      if (term_at(candidate) != state.current_term) {
        continue;
      }
      std::size_t replicated = 0U;
      for (const NodeId peer : voters) {
        const auto found = match_index.find(peer);
        if (found != match_index.end() && found->second >= candidate) {
          ++replicated;
        }
      }
      if (replicated >= majority()) {
        state.commit_index = candidate;
        transition.advanced_commit_index = candidate;
        return true;
      }
    }
    return false;
  }

  NodeId id{};
  std::vector<NodeId> voters;
  PersistentState state;
  RaftLimits limits;
  Role role{Role::kFollower};
  std::optional<NodeId> leader_id;
  std::set<NodeId> votes;
  std::map<NodeId, LogIndex> next_index;
  std::map<NodeId, LogIndex> match_index;
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
  if (!std::binary_search(voters.begin(), voters.end(), node_id) || voters.front() == 0U ||
      std::adjacent_find(voters.begin(), voters.end()) != voters.end()) {
    return common::make_unexpected(
        invalid("Raft voters must be unique, nonzero, and include self"));
  }
  if (persistent.snapshot.last_included_index == 0U &&
      persistent.snapshot.last_included_term != 0U) {
    return common::make_unexpected(invalid("Raft snapshot zero index must have zero term"));
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
  if (persistent.commit_index < persistent.snapshot.last_included_index ||
      persistent.commit_index > last ||
      persistent.applied_index < persistent.snapshot.last_included_index ||
      persistent.applied_index > persistent.commit_index ||
      (persistent.voted_for.has_value() &&
       !std::binary_search(voters.begin(), voters.end(), *persistent.voted_for))) {
    return common::make_unexpected(invalid("Raft persistent commit, apply, or vote state invalid"));
  }
  return RaftNode{
      std::make_unique<Impl>(node_id, std::move(voters), std::move(persistent), limits)};
}

common::Result<Transition> RaftNode::start_election() {
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
  Transition transition;
  if (impl_->majority() == 1U) {
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
  if (!impl_->voter(source) || source == impl_->id) {
    return common::make_unexpected(invalid("Raft message source is not a remote voter"));
  }

  // Validate every fallible, message-local condition before observing a newer term. Otherwise a
  // hostile higher-term message could change current_term and then return an error without giving
  // the runtime the persistence state required by the persist-before-send contract.
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
        } else if constexpr (std::is_same_v<T, AppendEntriesResponse>) {
          if (value.success && value.match_index > impl_->last_index()) {
            return invalid("AppendEntries response exceeds the local log");
          }
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
          if (value.term == impl_->state.current_term &&
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
            if (impl_->votes.size() >= impl_->majority()) {
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
          transition.outbound.push_back(
              OutboundMessage{source, AppendEntriesResponse{impl_->state.current_term, true,
                                                            accepted, std::nullopt, 0U}});
          return common::Status::ok();
        } else {
          if (value.term < impl_->state.current_term || impl_->role != Role::kLeader ||
              value.term != impl_->state.current_term) {
            return common::Status::ok();
          }
          if (value.success) {
            impl_->match_index[source] = std::max(impl_->match_index[source], value.match_index);
            impl_->next_index[source] = impl_->match_index[source] + 1U;
            if (impl_->advance_commit(transition)) {
              persistence_changed = true;
              impl_->append_to_all(transition);
            } else if (impl_->next_index[source] <= impl_->last_index()) {
              transition.outbound.push_back(OutboundMessage{source, impl_->append_for(source)});
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
            transition.outbound.push_back(OutboundMessage{source, impl_->append_for(source)});
          }
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
  if (type == 0U || payload.size() > impl_->limits.maximum_entry_bytes ||
      impl_->state.log.size() >= impl_->limits.maximum_log_entries ||
      impl_->last_index() >= std::numeric_limits<LogIndex>::max() - 1U) {
    return common::make_unexpected(invalid("Raft proposal type, size, or log bound invalid"));
  }
  const LogIndex index = impl_->last_index() + 1U;
  impl_->state.log.push_back(LogEntry{index, impl_->state.current_term, type, std::move(payload)});
  impl_->match_index[impl_->id] = index;
  impl_->next_index[impl_->id] = index + 1U;
  Transition transition;
  if (impl_->advance_commit(transition)) {
    impl_->append_to_all(transition);
  } else {
    impl_->append_to_all(transition);
  }
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

common::Status RaftNode::mark_applied(const LogIndex index) {
  if (index < impl_->state.applied_index || index > impl_->state.commit_index) {
    return invalid("Raft applied index must advance within the committed prefix");
  }
  impl_->state.applied_index = index;
  return common::Status::ok();
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
const PersistentState& RaftNode::persistent_state() const noexcept {
  return impl_->state;
}

} // namespace chronos::raft
