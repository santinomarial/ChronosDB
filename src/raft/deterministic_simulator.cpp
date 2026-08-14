#include "chronos/raft/deterministic_simulator.hpp"

#include <algorithm>
#include <limits>
#include <map>
#include <new>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace chronos::raft {
namespace {

[[nodiscard]] common::Status make_status(const common::StatusCode code, const char* message) {
  return {code, message};
}

[[nodiscard]] bool strictly_sorted_nodes(const std::span<const NodeId> nodes) {
  if (nodes.empty())
    return false;
  NodeId previous{};
  for (const NodeId node : nodes) {
    if (node == 0U || node <= previous)
      return false;
    previous = node;
  }
  return true;
}

[[nodiscard]] const LogEntry* retained_entry(const PersistentState& state, const LogIndex index) {
  if (index <= state.snapshot.last_included_index)
    return nullptr;
  const LogIndex relative = index - state.snapshot.last_included_index - 1U;
  if (relative >= state.log.size())
    return nullptr;
  return &state.log[static_cast<std::size_t>(relative)];
}

class FixedPrng {
public:
  explicit FixedPrng(const std::uint64_t seed) noexcept
      : state_(seed == 0U ? 0x9e3779b97f4a7c15ULL : seed) {}
  [[nodiscard]] std::uint64_t next() noexcept {
    state_ ^= state_ >> 12U;
    state_ ^= state_ << 25U;
    state_ ^= state_ >> 27U;
    return state_ * 0x2545f4914f6cdd1dULL;
  }

private:
  std::uint64_t state_;
};

} // namespace

class DeterministicRaftSimulator::Impl {
public:
  struct NodeSlot {
    NodeId id{};
    PersistentState durable;
    std::optional<RaftNode> active;
    std::optional<PendingSnapshotInstall> pending_snapshot;
    bool fail_next_persistence{};
    Term maximum_observed_term{};
    LogIndex maximum_observed_commit{};
  };
  struct NetworkSlot {
    std::uint64_t id{};
    NodeId source{};
    OutboundMessage outbound;
  };

  Impl(RaftSimulationConfig config, std::vector<NodeSlot> nodes,
       std::vector<std::optional<NetworkSlot>> network, std::vector<bool> links,
       std::vector<RaftSimulationAction> trace) noexcept
      : config_(std::move(config)), nodes_(std::move(nodes)), network_(std::move(network)),
        links_(std::move(links)), trace_(std::move(trace)) {}

  [[nodiscard]] NodeSlot* find_node(const NodeId id) noexcept {
    for (NodeSlot& node : nodes_)
      if (node.id == id)
        return &node;
    return nullptr;
  }
  [[nodiscard]] const NodeSlot* find_node(const NodeId id) const noexcept {
    for (const NodeSlot& node : nodes_)
      if (node.id == id)
        return &node;
    return nullptr;
  }
  [[nodiscard]] std::size_t node_index(const NodeId id) const noexcept {
    for (std::size_t index = 0U; index < nodes_.size(); ++index)
      if (nodes_[index].id == id)
        return index;
    return nodes_.size();
  }
  [[nodiscard]] NetworkSlot* find_message(const std::uint64_t id) noexcept {
    for (std::optional<NetworkSlot>& message : network_)
      if (message.has_value() && message->id == id)
        return &*message;
    return nullptr;
  }
  [[nodiscard]] std::optional<NetworkSlot>* find_message_slot(const std::uint64_t id) noexcept {
    for (std::optional<NetworkSlot>& message : network_)
      if (message.has_value() && message->id == id)
        return &message;
    return nullptr;
  }
  [[nodiscard]] std::optional<NetworkSlot>* empty_message_slot() noexcept {
    for (std::optional<NetworkSlot>& message : network_)
      if (!message.has_value())
        return &message;
    return nullptr;
  }
  [[nodiscard]] bool link_enabled(const NodeId source, const NodeId destination) const noexcept {
    const std::size_t from = node_index(source);
    const std::size_t to = node_index(destination);
    return from != nodes_.size() && to != nodes_.size() && links_[from * nodes_.size() + to];
  }
  [[nodiscard]] common::Status restart(NodeSlot& node) const {
    auto restarted =
        RaftNode::create(node.id, config_.initial_voters, node.durable, config_.limits.raft);
    if (!restarted.has_value())
      return restarted.error();
    node.active.emplace(std::move(*restarted));
    node.pending_snapshot.reset();
    node.fail_next_persistence = false;
    return common::Status::ok();
  }
  static void crash(NodeSlot& node) noexcept {
    node.active.reset();
    node.pending_snapshot.reset();
    node.fail_next_persistence = false;
  }
  [[nodiscard]] std::size_t free_messages() const noexcept {
    return network_.size() - pending_count_;
  }
  [[nodiscard]] common::Status apply_transition(NodeSlot& node, Transition transition) {
    if (transition.outbound.size() > free_messages()) {
      crash(node);
      return make_status(common::StatusCode::kResourceExhausted,
                         "Raft simulation network queue is full");
    }
    for (const OutboundMessage& outbound : transition.outbound) {
      if (find_node(outbound.destination) == nullptr) {
        crash(node);
        return make_status(common::StatusCode::kInvalidArgument,
                           "Raft simulation outbound destination is unknown");
      }
    }
    if (transition.persistent_state.has_value()) {
      if (node.fail_next_persistence) {
        crash(node);
        ++stats_.persistence_failures;
        return common::Status::ok();
      }
      try {
        PersistentState durable = *transition.persistent_state;
        node.durable = std::move(durable);
      } catch (const std::bad_alloc&) {
        crash(node);
        return make_status(common::StatusCode::kResourceExhausted,
                           "Raft simulation persistence allocation failed");
      } catch (const std::length_error&) {
        crash(node);
        return make_status(common::StatusCode::kResourceExhausted,
                           "Raft simulation persistent state exceeds container limits");
      }
    }
    if (next_message_id_ == std::numeric_limits<std::uint64_t>::max() &&
        !transition.outbound.empty()) {
      crash(node);
      return make_status(common::StatusCode::kResourceExhausted,
                         "Raft simulation message identity exhausted");
    }
    for (OutboundMessage& outbound : transition.outbound) {
      std::optional<NetworkSlot>* slot = empty_message_slot();
      if (slot == nullptr) {
        crash(node);
        return make_status(common::StatusCode::kCorruption,
                           "Raft simulation queue accounting is inconsistent");
      }
      slot->emplace(NetworkSlot{next_message_id_++, node.id, std::move(outbound)});
      ++pending_count_;
    }
    if (transition.snapshot_install.has_value())
      node.pending_snapshot = std::move(transition.snapshot_install);
    return check_safety();
  }

  [[nodiscard]] common::Status check_safety() {
    ++stats_.safety_checks;
    for (NodeSlot& node : nodes_) {
      const PersistentState& state = node.durable;
      const LogIndex last =
          state.log.empty() ? state.snapshot.last_included_index : state.log.back().index;
      if (state.applied_index > state.commit_index || state.commit_index > last ||
          state.current_term < node.maximum_observed_term ||
          state.commit_index < node.maximum_observed_commit) {
        return make_status(common::StatusCode::kCorruption,
                           "Raft simulation observed regressing durable state");
      }
      node.maximum_observed_term = state.current_term;
      node.maximum_observed_commit = state.commit_index;
      RaftNode* const active_node = node.active.has_value() ? &node.active.value() : nullptr;
      if (active_node != nullptr && active_node->role() == Role::kLeader) {
        const Term term = active_node->current_term();
        const auto [leader, inserted] = leaders_by_term_.emplace(term, node.id);
        if (!inserted && leader->second != node.id) {
          return make_status(common::StatusCode::kCorruption,
                             "Raft election safety violated in simulation");
        }
      }
      for (const LogEntry& entry : state.log) {
        if (entry.index > state.commit_index)
          break;
        const auto [committed, inserted] = committed_entries_.emplace(entry.index, entry);
        if (!inserted && committed->second != entry) {
          return make_status(common::StatusCode::kCorruption,
                             "Raft committed-prefix truth diverged in simulation");
        }
      }
    }

    for (const NodeSlot& node : nodes_) {
      for (const auto& [index, expected] : committed_entries_) {
        if (index > node.durable.commit_index)
          continue;
        if (index <= node.durable.snapshot.last_included_index)
          continue;
        const LogEntry* actual = retained_entry(node.durable, index);
        if (actual == nullptr || *actual != expected) {
          return make_status(common::StatusCode::kCorruption,
                             "Raft committed entry differs across replicas");
        }
      }
      const std::optional<RaftNode>& active = node.active;
      const RaftNode* const active_node = active.has_value() ? &active.value() : nullptr;
      if (active_node != nullptr && active_node->role() == Role::kLeader) {
        for (const auto& [index, expected] : committed_entries_) {
          if (expected.term >= active_node->current_term())
            continue;
          if (index <= node.durable.snapshot.last_included_index)
            continue;
          const LogEntry* actual = retained_entry(node.durable, index);
          if (actual == nullptr || *actual != expected) {
            return make_status(common::StatusCode::kCorruption,
                               "Raft leader completeness violated in simulation");
          }
        }
      }
    }

    for (std::size_t left = 0U; left < nodes_.size(); ++left) {
      for (std::size_t right = left + 1U; right < nodes_.size(); ++right) {
        const PersistentState& first = nodes_[left].durable;
        const PersistentState& second = nodes_[right].durable;
        if (first.snapshot.last_included_index == second.snapshot.last_included_index &&
            first.snapshot.last_included_index != 0U &&
            first.snapshot.last_included_term == second.snapshot.last_included_term &&
            (first.snapshot.configuration_index != second.snapshot.configuration_index ||
             first.snapshot.voters != second.snapshot.voters)) {
          return make_status(common::StatusCode::kCorruption,
                             "Raft equal snapshot positions have different membership");
        }
        for (const LogEntry& entry : first.log) {
          const LogEntry* matching = retained_entry(second, entry.index);
          if (matching == nullptr || matching->term != entry.term)
            continue;
          const LogIndex begin =
              std::max(first.snapshot.last_included_index, second.snapshot.last_included_index) +
              1U;
          for (LogIndex index = begin;; ++index) {
            const LogEntry* first_entry = retained_entry(first, index);
            const LogEntry* second_entry = retained_entry(second, index);
            if (first_entry == nullptr || second_entry == nullptr ||
                *first_entry != *second_entry) {
              return make_status(common::StatusCode::kCorruption,
                                 "Raft log matching violated in simulation");
            }
            if (index == entry.index)
              break;
          }
        }
      }
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status execute(const RaftSimulationAction& action) {
    return std::visit(
        [this](const auto& value) -> common::Status {
          using Action = std::decay_t<decltype(value)>;
          if constexpr (std::is_same_v<Action, RaftSimulationDeliver>) {
            std::optional<NetworkSlot>* slot = find_message_slot(value.message_id);
            if (slot == nullptr)
              return make_status(common::StatusCode::kNotFound,
                                 "Raft simulation message does not exist");
            NetworkSlot message = std::move(**slot);
            slot->reset();
            --pending_count_;
            NodeSlot* destination = find_node(message.outbound.destination);
            if (!link_enabled(message.source, message.outbound.destination) ||
                !destination->active.has_value()) {
              ++stats_.dropped;
              return check_safety();
            }
            auto transition =
                destination->active->receive(message.source, std::move(message.outbound.message));
            if (!transition.has_value())
              return transition.error();
            ++stats_.delivered;
            return apply_transition(*destination, std::move(*transition));
          } else if constexpr (std::is_same_v<Action, RaftSimulationDrop>) {
            std::optional<NetworkSlot>* slot = find_message_slot(value.message_id);
            if (slot == nullptr)
              return make_status(common::StatusCode::kNotFound,
                                 "Raft simulation message does not exist");
            slot->reset();
            --pending_count_;
            ++stats_.dropped;
            return check_safety();
          } else if constexpr (std::is_same_v<Action, RaftSimulationDuplicate>) {
            NetworkSlot* message = find_message(value.message_id);
            std::optional<NetworkSlot>* empty = empty_message_slot();
            if (message == nullptr)
              return make_status(common::StatusCode::kNotFound,
                                 "Raft simulation message does not exist");
            if (empty == nullptr)
              return make_status(common::StatusCode::kResourceExhausted,
                                 "Raft simulation network queue is full");
            if (next_message_id_ == std::numeric_limits<std::uint64_t>::max())
              return make_status(common::StatusCode::kResourceExhausted,
                                 "Raft simulation message identity exhausted");
            empty->emplace(NetworkSlot{next_message_id_++, message->source, message->outbound});
            ++pending_count_;
            ++stats_.duplicated;
            return check_safety();
          } else if constexpr (std::is_same_v<Action, RaftSimulationSetLink>) {
            const std::size_t source = node_index(value.source);
            const std::size_t destination = node_index(value.destination);
            if (source == nodes_.size() || destination == nodes_.size())
              return make_status(common::StatusCode::kNotFound,
                                 "Raft simulation link endpoint does not exist");
            links_[source * nodes_.size() + destination] = value.enabled;
            return check_safety();
          } else {
            NodeSlot* node = find_node(value.node_id);
            if (node == nullptr)
              return make_status(common::StatusCode::kNotFound,
                                 "Raft simulation node does not exist");
            if constexpr (std::is_same_v<Action, RaftSimulationCrash>) {
              if (!node->active.has_value())
                return make_status(common::StatusCode::kUnavailable,
                                   "Raft simulation node is already crashed");
              crash(*node);
              ++stats_.crashes;
              return check_safety();
            } else if constexpr (std::is_same_v<Action, RaftSimulationRestart>) {
              if (node->active.has_value())
                return make_status(common::StatusCode::kAlreadyExists,
                                   "Raft simulation node is already active");
              common::Status restarted = restart(*node);
              if (restarted.is_ok())
                ++stats_.restarts;
              return restarted.is_ok() ? check_safety() : restarted;
            } else if constexpr (std::is_same_v<Action, RaftSimulationFailNextPersistence>) {
              if (!node->active.has_value())
                return make_status(common::StatusCode::kUnavailable,
                                   "Raft simulation node is crashed");
              if (node->fail_next_persistence)
                return make_status(common::StatusCode::kAlreadyExists,
                                   "Raft simulation persistence failure is already armed");
              node->fail_next_persistence = true;
              return check_safety();
            } else {
              if (!node->active.has_value())
                return make_status(common::StatusCode::kUnavailable,
                                   "Raft simulation node is crashed");
              common::Result<Transition> transition = [&]() -> common::Result<Transition> {
                if constexpr (std::is_same_v<Action, RaftSimulationStartElection>)
                  return node->active->start_election();
                else if constexpr (std::is_same_v<Action, RaftSimulationPropose>)
                  return node->active->propose(value.type, value.payload);
                else if constexpr (std::is_same_v<Action, RaftSimulationHeartbeat>)
                  return node->active->heartbeat();
                else if constexpr (std::is_same_v<Action, RaftSimulationMarkApplied>)
                  return node->active->mark_applied(value.index);
                else if constexpr (std::is_same_v<Action, RaftSimulationBeginMembershipChange>)
                  return node->active->begin_membership_change(value.voters);
                else if constexpr (std::is_same_v<Action, RaftSimulationFinalizeMembershipChange>)
                  return node->active->finalize_membership_change();
                else if constexpr (std::is_same_v<Action, RaftSimulationCompleteSnapshotInstall>) {
                  if (!node->pending_snapshot.has_value())
                    return common::make_unexpected(
                        make_status(common::StatusCode::kInvalidArgument,
                                    "Raft simulation node has no pending snapshot installation"));
                  return node->active->complete_snapshot_install(node->pending_snapshot->source,
                                                                 node->pending_snapshot->snapshot,
                                                                 value.installed);
                } else {
                  return node->active->compact_snapshot(value.snapshot);
                }
              }();
              if (!transition.has_value())
                return transition.error();
              if constexpr (std::is_same_v<Action, RaftSimulationCompleteSnapshotInstall>)
                node->pending_snapshot.reset();
              return apply_transition(*node, std::move(*transition));
            }
          }
        },
        action);
  }

  RaftSimulationConfig config_;
  std::vector<NodeSlot> nodes_;
  std::vector<std::optional<NetworkSlot>> network_;
  std::vector<bool> links_;
  std::vector<RaftSimulationAction> trace_;
  std::map<Term, NodeId> leaders_by_term_;
  std::map<LogIndex, LogEntry> committed_entries_;
  std::size_t pending_count_{};
  std::uint64_t next_message_id_{1U};
  common::Status status_;
  RaftSimulationStats stats_;
};

DeterministicRaftSimulator::DeterministicRaftSimulator(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DeterministicRaftSimulator::~DeterministicRaftSimulator() = default;
DeterministicRaftSimulator::DeterministicRaftSimulator(DeterministicRaftSimulator&&) noexcept =
    default;
DeterministicRaftSimulator&
DeterministicRaftSimulator::operator=(DeterministicRaftSimulator&&) noexcept = default;

common::Result<DeterministicRaftSimulator>
DeterministicRaftSimulator::create(RaftSimulationConfig config) {
  if (!strictly_sorted_nodes(config.node_ids) || !strictly_sorted_nodes(config.initial_voters) ||
      config.limits.maximum_pending_messages == 0U || config.limits.maximum_trace_actions == 0U ||
      config.limits.maximum_shrink_replays == 0U)
    return common::make_unexpected(make_status(common::StatusCode::kInvalidArgument,
                                               "Raft simulation configuration is invalid"));
  for (const NodeId voter : config.initial_voters) {
    if (!std::binary_search(config.node_ids.begin(), config.node_ids.end(), voter))
      return common::make_unexpected(make_status(common::StatusCode::kInvalidArgument,
                                                 "Raft simulation voter is not a configured node"));
  }
  if (config.node_ids.size() > std::numeric_limits<std::size_t>::max() / config.node_ids.size())
    return common::make_unexpected(make_status(common::StatusCode::kResourceExhausted,
                                               "Raft simulation link matrix overflows"));
  try {
    std::vector<Impl::NodeSlot> nodes;
    nodes.reserve(config.node_ids.size());
    for (const NodeId id : config.node_ids) {
      auto node = RaftNode::create(id, config.initial_voters, {}, config.limits.raft);
      if (!node.has_value())
        return common::make_unexpected(node.error());
      nodes.push_back({.id = id, .active = std::move(*node)});
    }
    std::vector<std::optional<Impl::NetworkSlot>> network(config.limits.maximum_pending_messages);
    std::vector<bool> links(config.node_ids.size() * config.node_ids.size(), true);
    std::vector<RaftSimulationAction> trace;
    trace.reserve(config.limits.maximum_trace_actions);
    auto implementation =
        std::make_unique<Impl>(std::move(config), std::move(nodes), std::move(network),
                               std::move(links), std::move(trace));
    common::Status checked = implementation->check_safety();
    if (!checked.is_ok())
      return common::make_unexpected(checked);
    return DeterministicRaftSimulator{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        make_status(common::StatusCode::kResourceExhausted, "Raft simulation allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(make_status(common::StatusCode::kResourceExhausted,
                                               "Raft simulation bounds exceed container limits"));
  }
}

common::Status DeterministicRaftSimulator::step(RaftSimulationAction action) {
  if (!implementation_)
    return make_status(common::StatusCode::kInvalidArgument, "Raft simulation is empty");
  Impl& impl = *implementation_;
  if (!impl.status_.is_ok())
    return impl.status_;
  if (impl.trace_.size() == impl.config_.limits.maximum_trace_actions) {
    impl.status_ =
        make_status(common::StatusCode::kResourceExhausted, "Raft simulation trace is full");
    return impl.status_;
  }
  try {
    impl.trace_.push_back(std::move(action));
    ++impl.stats_.actions;
    common::Status result = impl.execute(impl.trace_.back());
    if (!result.is_ok())
      impl.status_ = result;
    return result;
  } catch (const std::bad_alloc&) {
    impl.status_ = make_status(common::StatusCode::kResourceExhausted,
                               "Raft simulation step allocation failed");
  } catch (const std::length_error&) {
    impl.status_ = make_status(common::StatusCode::kResourceExhausted,
                               "Raft simulation step exceeds container limits");
  }
  return impl.status_;
}

common::Status
DeterministicRaftSimulator::replay(const std::span<const RaftSimulationAction> actions) {
  for (const RaftSimulationAction& action : actions) {
    common::Status result = step(action);
    if (!result.is_ok())
      return result;
  }
  return common::Status::ok();
}

common::Status DeterministicRaftSimulator::run_seeded(const RaftSeededSimulationSchedule schedule) {
  if (!implementation_)
    return make_status(common::StatusCode::kInvalidArgument, "Raft simulation is empty");
  FixedPrng random(schedule.seed);
  try {
    for (std::size_t action_index = 0U; action_index < schedule.actions; ++action_index) {
      Impl& impl = *implementation_;
      std::vector<NodeId> active;
      std::vector<NodeId> inactive;
      std::vector<NodeId> leaders;
      std::vector<NodeId> applicable;
      active.reserve(impl.nodes_.size());
      inactive.reserve(impl.nodes_.size());
      leaders.reserve(impl.nodes_.size());
      applicable.reserve(impl.nodes_.size());
      for (const Impl::NodeSlot& node : impl.nodes_) {
        if (node.active.has_value()) {
          active.push_back(node.id);
          if (node.active->role() == Role::kLeader)
            leaders.push_back(node.id);
          if (node.active->applied_index() < node.active->commit_index())
            applicable.push_back(node.id);
        } else {
          inactive.push_back(node.id);
        }
      }
      std::vector<std::uint64_t> messages;
      messages.reserve(impl.pending_count_);
      for (const std::optional<Impl::NetworkSlot>& message : impl.network_)
        if (message.has_value())
          messages.push_back(message->id);
      const std::uint64_t choice = random.next() % 11U;
      RaftSimulationAction next = RaftSimulationSetLink{
          impl.nodes_[random.next() % impl.nodes_.size()].id,
          impl.nodes_[random.next() % impl.nodes_.size()].id, (random.next() & 1U) != 0U};
      if (choice == 0U && !messages.empty())
        next = RaftSimulationDeliver{messages[random.next() % messages.size()]};
      else if (choice == 1U && !messages.empty())
        next = RaftSimulationDrop{messages[random.next() % messages.size()]};
      else if (choice == 2U && !messages.empty() && impl.free_messages() != 0U)
        next = RaftSimulationDuplicate{messages[random.next() % messages.size()]};
      else if (choice == 4U && !leaders.empty())
        next = RaftSimulationHeartbeat{leaders[random.next() % leaders.size()]};
      else if (choice == 5U && !leaders.empty()) {
        std::vector<std::byte> payload(8U);
        const std::uint64_t value = random.next();
        for (std::size_t index = 0U; index < payload.size(); ++index)
          payload[index] = std::byte((value >> (index * 8U)) & 0xffU);
        next =
            RaftSimulationPropose{leaders[random.next() % leaders.size()], 1U, std::move(payload)};
      } else if (choice == 6U && active.size() > 1U)
        next = RaftSimulationCrash{active[random.next() % active.size()]};
      else if (choice == 7U && !inactive.empty())
        next = RaftSimulationRestart{inactive[random.next() % inactive.size()]};
      else if (choice == 8U && !applicable.empty()) {
        const NodeId node = applicable[random.next() % applicable.size()];
        next = RaftSimulationMarkApplied{node, impl.find_node(node)->active->commit_index()};
      } else if (choice == 9U && !active.empty()) {
        const NodeId node = active[random.next() % active.size()];
        if (!impl.find_node(node)->fail_next_persistence)
          next = RaftSimulationFailNextPersistence{node};
        else
          next = RaftSimulationStartElection{node};
      } else if (!active.empty()) {
        next = RaftSimulationStartElection{active[random.next() % active.size()]};
      }
      common::Status result = step(std::move(next));
      if (!result.is_ok())
        return result;
    }
    return common::Status::ok();
  } catch (const std::bad_alloc&) {
    implementation_->status_ = make_status(common::StatusCode::kResourceExhausted,
                                           "Raft seeded simulation allocation failed");
  } catch (const std::length_error&) {
    implementation_->status_ = make_status(common::StatusCode::kResourceExhausted,
                                           "Raft seeded simulation exceeds container limits");
  }
  return implementation_->status_;
}

common::Result<std::vector<RaftSimulationAction>> DeterministicRaftSimulator::shrink_failing_trace(
    const RaftSimulationConfig& config, const std::span<const RaftSimulationAction> failing_trace) {
  try {
    auto original = create(config);
    if (!original.has_value())
      return common::make_unexpected(original.error());
    const common::Status original_failure = original->replay(failing_trace);
    if (original_failure.is_ok())
      return common::make_unexpected(
          make_status(common::StatusCode::kInvalidArgument, "Raft simulation trace does not fail"));
    std::vector<RaftSimulationAction> best(failing_trace.begin(), failing_trace.end());
    std::size_t replay_count{};
    for (std::size_t index = 0U;
         index < best.size() && replay_count < config.limits.maximum_shrink_replays;) {
      std::vector<RaftSimulationAction> candidate;
      candidate.reserve(best.size() - 1U);
      candidate.insert(candidate.end(), best.begin(),
                       best.begin() + static_cast<std::ptrdiff_t>(index));
      candidate.insert(candidate.end(), best.begin() + static_cast<std::ptrdiff_t>(index + 1U),
                       best.end());
      auto simulation = create(config);
      if (!simulation.has_value())
        return common::make_unexpected(simulation.error());
      ++replay_count;
      const common::Status failure = simulation->replay(candidate);
      if (!failure.is_ok() && failure.code() == original_failure.code()) {
        best = std::move(candidate);
        index = 0U;
      } else {
        ++index;
      }
    }
    return best;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(make_status(common::StatusCode::kResourceExhausted,
                                               "Raft simulation shrink allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(make_status(common::StatusCode::kResourceExhausted,
                                               "Raft simulation shrink exceeds container limits"));
  }
}

const RaftNode* DeterministicRaftSimulator::active_node(const NodeId node_id) const noexcept {
  if (!implementation_)
    return nullptr;
  const Impl::NodeSlot* node = implementation_->find_node(node_id);
  return node == nullptr || !node->active.has_value() ? nullptr : &*node->active;
}
const PersistentState*
DeterministicRaftSimulator::durable_state(const NodeId node_id) const noexcept {
  if (!implementation_)
    return nullptr;
  const Impl::NodeSlot* node = implementation_->find_node(node_id);
  return node == nullptr ? nullptr : &node->durable;
}
common::Result<std::vector<RaftSimulationMessageRoute>>
DeterministicRaftSimulator::pending_messages() const {
  if (!implementation_)
    return common::make_unexpected(
        make_status(common::StatusCode::kInvalidArgument, "Raft simulation is empty"));
  try {
    std::vector<RaftSimulationMessageRoute> messages;
    messages.reserve(implementation_->pending_count_);
    for (const std::optional<Impl::NetworkSlot>& message : implementation_->network_)
      if (message.has_value())
        messages.push_back({message->id, message->source, message->outbound.destination});
    return messages;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(make_status(common::StatusCode::kResourceExhausted,
                                               "Raft simulation route allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(make_status(common::StatusCode::kResourceExhausted,
                                               "Raft simulation routes exceed container limits"));
  }
}
std::span<const RaftSimulationAction> DeterministicRaftSimulator::trace() const noexcept {
  return implementation_ ? std::span<const RaftSimulationAction>{implementation_->trace_}
                         : std::span<const RaftSimulationAction>{};
}
const common::Status& DeterministicRaftSimulator::status() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "Raft simulation is empty"};
  return implementation_ ? implementation_->status_ : empty;
}
const RaftSimulationStats& DeterministicRaftSimulator::stats() const noexcept {
  static const RaftSimulationStats empty{};
  return implementation_ ? implementation_->stats_ : empty;
}

} // namespace chronos::raft
