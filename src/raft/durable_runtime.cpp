#include "chronos/raft/durable_runtime.hpp"

#include <cstddef>
#include <map>
#include <memory>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::raft {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status unavailable(const char* message) {
  return common::Status{common::StatusCode::kUnavailable, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return common::Status{common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Result<MultiRaftRuntime>
restore_runtime(const NodeId local_node_id, const DurableMultiRaftLimits& limits,
                std::vector<RaftGroupConfiguration> groups,
                const std::vector<GroupPersistentState>& recovered) {
  if (groups.size() > limits.runtime.maximum_groups) {
    return common::make_unexpected(invalid("durable Multi-Raft group count exceeds limits"));
  }
  std::map<GroupId, GroupPersistentState> recovered_by_group;
  for (const GroupPersistentState& persistent : recovered) {
    if (!recovered_by_group.emplace(persistent.group_id, persistent).second) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kCorruption, "duplicate recovered Raft group state"});
    }
  }
  auto runtime = MultiRaftRuntime::create(local_node_id, limits.runtime);
  if (!runtime.has_value())
    return common::make_unexpected(runtime.error());
  std::map<GroupId, bool> configured;
  for (RaftGroupConfiguration& group : groups) {
    if (group.group_id.is_nil() || !configured.emplace(group.group_id, true).second) {
      return common::make_unexpected(invalid("durable Multi-Raft group configuration is invalid"));
    }
    auto state = recovered_by_group.find(group.group_id);
    common::Status status =
        state == recovered_by_group.end()
            ? runtime->add_group(group.group_id, std::move(group.voters))
            : runtime->add_group(group.group_id, std::move(group.voters), state->second.state,
                                 state->second.physical_sequence);
    if (!status.is_ok())
      return common::make_unexpected(status);
    if (state != recovered_by_group.end())
      recovered_by_group.erase(state);
  }
  if (!recovered_by_group.empty()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kCorruption, "recovered Raft group has no membership configuration"});
  }
  return runtime;
}

} // namespace

class DurableMultiRaftRuntime::Impl {
public:
  Impl(MultiRaftRuntime runtime_value, RaftPersistentLog log_value,
       const DurableMultiRaftLimits configured)
      : runtime(std::move(runtime_value)), log(std::move(log_value)), limits(configured) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (failure.is_ok())
      failure = std::move(status);
    return failure;
  }

  MultiRaftRuntime runtime;
  RaftPersistentLog log;
  DurableMultiRaftLimits limits;
  common::Status failure;
};

DurableMultiRaftRuntime::DurableMultiRaftRuntime(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
DurableMultiRaftRuntime::~DurableMultiRaftRuntime() = default;
DurableMultiRaftRuntime::DurableMultiRaftRuntime(DurableMultiRaftRuntime&&) noexcept = default;
DurableMultiRaftRuntime&
DurableMultiRaftRuntime::operator=(DurableMultiRaftRuntime&&) noexcept = default;

common::Result<DurableMultiRaftRuntime> DurableMultiRaftRuntime::create_new(
    const NodeId local_node_id, const RaftPersistentLogConfig& log_config,
    std::vector<RaftGroupConfiguration> groups, const DurableMultiRaftLimits limits) {
  if (limits.maximum_batch_operations == 0U || limits.maximum_batch_outbound == 0U) {
    return common::make_unexpected(invalid("durable Multi-Raft batch limits are invalid"));
  }
  auto runtime = restore_runtime(local_node_id, limits, std::move(groups), {});
  if (!runtime.has_value())
    return common::make_unexpected(runtime.error());
  auto log = RaftPersistentLog::create_new(log_config);
  if (!log.has_value())
    return common::make_unexpected(log.error());
  return DurableMultiRaftRuntime{
      std::make_unique<Impl>(std::move(*runtime), std::move(*log), limits)};
}

common::Result<DurableMultiRaftRuntime> DurableMultiRaftRuntime::open_existing(
    const NodeId local_node_id, const RaftPersistentLogConfig& log_config,
    const RaftPersistentLogOpenOptions& open_options, std::vector<RaftGroupConfiguration> groups,
    const DurableMultiRaftLimits limits) {
  if (limits.maximum_batch_operations == 0U || limits.maximum_batch_outbound == 0U) {
    return common::make_unexpected(invalid("durable Multi-Raft batch limits are invalid"));
  }
  auto log = RaftPersistentLog::open_existing(log_config, open_options);
  if (!log.has_value())
    return common::make_unexpected(log.error());
  auto runtime = restore_runtime(local_node_id, limits, std::move(groups),
                                 log->recovery().latest_group_states);
  if (!runtime.has_value())
    return common::make_unexpected(runtime.error());
  return DurableMultiRaftRuntime{
      std::make_unique<Impl>(std::move(*runtime), std::move(*log), limits)};
}

common::Result<std::vector<DurableRaftResult>>
DurableMultiRaftRuntime::execute_batch(std::vector<DurableRaftRequest> requests) {
  if (!impl_->failure.is_ok())
    return common::make_unexpected(impl_->failure);
  if (impl_->runtime.failed()) {
    return common::make_unexpected(impl_->fail(common::Status{
        common::StatusCode::kUnavailable, "durable Multi-Raft runtime has failed closed"}));
  }
  if (requests.empty() || requests.size() > impl_->limits.maximum_batch_operations) {
    return common::make_unexpected(invalid("durable Multi-Raft batch size is invalid"));
  }
  std::vector<DurableRaftResult> results;
  results.reserve(requests.size());
  std::size_t outbound_count = 0U;
  bool needs_sync = false;
  for (DurableRaftRequest& request : requests) {
    if (request.required_leader_term.has_value()) {
      if (*request.required_leader_term == 0U) {
        results.push_back(DurableRaftResult{invalid("required Raft leader term must be nonzero"),
                                            std::nullopt, std::nullopt});
        continue;
      }
      const RaftNode* const node = impl_->runtime.find_group(request.group_id);
      if (node == nullptr) {
        results.push_back(DurableRaftResult{
            common::Status{common::StatusCode::kNotFound, "Raft group does not exist"},
            std::nullopt, std::nullopt});
        continue;
      }
      if (node->role() != Role::kLeader || node->current_term() != *request.required_leader_term) {
        results.push_back(DurableRaftResult{
            unavailable("Raft operation is not admitted by the required current leader term"),
            std::nullopt, std::nullopt});
        continue;
      }
    }
    if (std::holds_alternative<ObserveGroupOperation>(request.operation)) {
      auto observation = observe_group(request.group_id);
      if (!observation.has_value()) {
        results.push_back(DurableRaftResult{observation.error(), std::nullopt, std::nullopt});
      } else {
        results.push_back(
            DurableRaftResult{common::Status::ok(), std::nullopt, std::move(*observation)});
      }
      continue;
    }
    common::Result<MultiRaftTransition> transition = std::visit(
        [&](auto&& operation) -> common::Result<MultiRaftTransition> {
          using T = std::remove_cvref_t<decltype(operation)>;
          if constexpr (std::is_same_v<T, StartElectionOperation>) {
            return impl_->runtime.start_election(request.group_id);
          } else if constexpr (std::is_same_v<T, ReceiveOperation>) {
            return impl_->runtime.receive(request.group_id, operation.source,
                                          std::move(operation.message));
          } else if constexpr (std::is_same_v<T, ProposeOperation>) {
            return impl_->runtime.propose(request.group_id, operation.type,
                                          std::move(operation.payload));
          } else if constexpr (std::is_same_v<T, ProposeExactRetainedOperation>) {
            return impl_->runtime.propose_exact_retained(request.group_id, operation.type,
                                                         std::move(operation.payload));
          } else if constexpr (std::is_same_v<T, CommitCurrentTermOperation>) {
            return impl_->runtime.commit_current_term(request.group_id);
          } else if constexpr (std::is_same_v<T, ObserveGroupOperation>) {
            return common::make_unexpected(
                invalid("Raft group observation dispatch is inconsistent"));
          } else if constexpr (std::is_same_v<T, BeginMembershipChangeOperation>) {
            return impl_->runtime.begin_membership_change(request.group_id,
                                                          std::move(operation.new_voters));
          } else if constexpr (std::is_same_v<T, FinalizeMembershipChangeOperation>) {
            return impl_->runtime.finalize_membership_change(request.group_id);
          } else if constexpr (std::is_same_v<T, CompleteSnapshotInstallOperation>) {
            return impl_->runtime.complete_snapshot_install(request.group_id, operation.source,
                                                            std::move(operation.snapshot),
                                                            operation.installed);
          } else if constexpr (std::is_same_v<T, CompactSnapshotOperation>) {
            return impl_->runtime.compact_snapshot(request.group_id, std::move(operation.snapshot));
          } else if constexpr (std::is_same_v<T, HeartbeatOperation>) {
            return impl_->runtime.heartbeat(request.group_id);
          } else if constexpr (std::is_same_v<T, BeginReadBarrierOperation>) {
            return impl_->runtime.begin_read_barrier(request.group_id);
          } else {
            return impl_->runtime.mark_applied(request.group_id, operation.index);
          }
        },
        std::move(request.operation));
    if (!transition.has_value()) {
      if (impl_->runtime.failed())
        return common::make_unexpected(impl_->fail(transition.error()));
      results.push_back(DurableRaftResult{transition.error(), std::nullopt, std::nullopt});
      continue;
    }
    if (transition->outbound.size() > impl_->limits.maximum_batch_outbound - outbound_count) {
      return common::make_unexpected(impl_->fail(
          common::Status{common::StatusCode::kResourceExhausted,
                         "durable Multi-Raft batch exceeds its outbound-message bound"}));
    }
    outbound_count += transition->outbound.size();
    MultiRaftTransition owned_transition = std::move(*transition);
    const std::optional<GroupPersistentState>& persistence = owned_transition.persistence;
    if (persistence.has_value()) {
      auto appended = impl_->log.append(persistence.value());
      if (!appended.has_value())
        return common::make_unexpected(impl_->fail(appended.error()));
      needs_sync = true;
    }
    results.push_back(
        DurableRaftResult{common::Status::ok(), std::move(owned_transition), std::nullopt});
  }
  if (needs_sync) {
    auto synchronized = impl_->log.synchronize();
    if (!synchronized.has_value())
      return common::make_unexpected(impl_->fail(synchronized.error()));
  }
  return results;
}

common::Result<RaftGroupObservation>
DurableMultiRaftRuntime::observe_group(const GroupId& group_id) const {
  if (!impl_->failure.is_ok())
    return common::make_unexpected(impl_->failure);
  if (impl_->runtime.failed()) {
    return common::make_unexpected(common::Status{common::StatusCode::kUnavailable,
                                                  "durable Multi-Raft runtime has failed closed"});
  }
  const RaftNode* const node = impl_->runtime.find_group(group_id);
  if (node == nullptr) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "Raft group does not exist"});
  }
  try {
    return RaftGroupObservation{
        .group_id = group_id,
        .node_id = node->node_id(),
        .role = node->role(),
        .current_term = node->current_term(),
        .leader_id = node->leader_id(),
        .last_log_index = node->last_log_index(),
        .commit_index = node->commit_index(),
        .applied_index = node->applied_index(),
        .voters = std::vector<NodeId>{node->voters().begin(), node->voters().end()},
        .committed_voters =
            std::vector<NodeId>{node->committed_voters().begin(), node->committed_voters().end()},
        .joint_old_voters =
            std::vector<NodeId>{node->joint_old_voters().begin(), node->joint_old_voters().end()},
        .joint_new_voters =
            std::vector<NodeId>{node->joint_new_voters().begin(), node->joint_new_voters().end()},
        .joint_membership_active = node->joint_membership_active(),
        .joint_membership_can_finalize = node->joint_membership_can_finalize(),
        .final_membership_pending = node->final_membership_pending()};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft group observation allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Raft group observation exceeded container limits"));
  }
}

const RaftNode* DurableMultiRaftRuntime::find_group(const GroupId& group_id) const noexcept {
  return impl_->runtime.find_group(group_id);
}

common::Result<QuorumSyncReceipt>
DurableMultiRaftRuntime::prove_quorum_sync(const GroupId& group_id, const LogIndex index) const {
  if (failed()) {
    return common::make_unexpected(failure_status());
  }
  const RaftNode* const node = impl_->runtime.find_group(group_id);
  if (node == nullptr) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "Raft group does not exist"});
  }
  if (node->role() != Role::kLeader) {
    return common::make_unexpected(
        unavailable("QUORUM_SYNC proof is available only on the current leader"));
  }
  if (index == 0U || index > node->commit_index()) {
    return common::make_unexpected(
        unavailable("Raft entry has not reached a quorum-synchronized commit"));
  }
  const PersistentState& state = node->persistent_state();
  Term entry_term = 0U;
  if (index == state.snapshot.last_included_index) {
    entry_term = state.snapshot.last_included_term;
  } else if (index > state.snapshot.last_included_index) {
    const LogIndex relative = index - state.snapshot.last_included_index - 1U;
    if (relative >= state.log.size() ||
        state.log[static_cast<std::size_t>(relative)].index != index) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kCorruption, "committed Raft entry is absent from persistent state"});
    }
    entry_term = state.log[static_cast<std::size_t>(relative)].term;
  }
  if (entry_term == 0U || impl_->log.durable_physical_sequence() == 0U) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kCorruption, "committed Raft entry has no durable term or frontier"});
  }
  return QuorumSyncReceipt{.group_id = group_id,
                           .leader_node_id = node->node_id(),
                           .leader_term = node->current_term(),
                           .log_index = index,
                           .entry_term = entry_term,
                           .local_durable_physical_sequence =
                               impl_->log.durable_physical_sequence()};
}

common::Result<RaftPersistentLogReclamation> DurableMultiRaftRuntime::checkpoint_and_reclaim() {
  if (failed())
    return common::make_unexpected(failure_status());
  auto checkpoint = impl_->runtime.create_persistence_checkpoint();
  if (!checkpoint.has_value())
    return common::make_unexpected(checkpoint.error());
  auto reclaimed = impl_->log.checkpoint_and_reclaim(*checkpoint);
  if (!reclaimed.has_value())
    return common::make_unexpected(impl_->fail(reclaimed.error()));
  return reclaimed;
}

RaftPhysicalPosition DurableMultiRaftRuntime::written_position() const noexcept {
  return impl_->log.written_position();
}

std::uint64_t DurableMultiRaftRuntime::durable_physical_sequence() const noexcept {
  return impl_->log.durable_physical_sequence();
}

bool DurableMultiRaftRuntime::failed() const noexcept {
  return !impl_->failure.is_ok() || impl_->runtime.failed() || impl_->log.is_failed();
}

common::Status DurableMultiRaftRuntime::failure_status() const {
  if (!impl_->failure.is_ok())
    return impl_->failure;
  if (impl_->log.is_failed())
    return impl_->log.failure_status();
  if (impl_->runtime.failed()) {
    return common::Status{common::StatusCode::kUnavailable,
                          "durable Multi-Raft runtime has failed closed"};
  }
  return common::Status::ok();
}

common::Status DurableMultiRaftRuntime::close() {
  return impl_->log.close();
}

} // namespace chronos::raft
