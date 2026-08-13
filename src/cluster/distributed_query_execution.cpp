#include "chronos/cluster/distributed_query_execution.hpp"

#include <cstddef>
#include <new>
#include <stdexcept>
#include <utility>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] bool
dispatch_matches(const query::DistributedAggregateFragmentDispatch& dispatch,
                 const query::DistributedTablet& planned,
                 const query::DistributedReadAdmission& admission,
                 const query::DistributedReadPolicy& policy,
                 const query::CompatibleDistributedAggregateSnapshot& snapshot) noexcept {
  const auto& fragment = dispatch.fragment;
  return fragment.tablet_id == planned.tablet_id && admission.tablet_id == planned.tablet_id &&
         fragment.database_id == snapshot.snapshot().database_id() &&
         fragment.snapshot_generation == snapshot.snapshot().generation() &&
         fragment.serving_node == admission.serving_node &&
         fragment.applied_position == admission.applied_position &&
         fragment.observed_leader_commit_position == admission.observed_leader_commit_position &&
         fragment.linearizable_barrier == admission.linearizable_barrier &&
         fragment.read_policy == policy;
}

} // namespace

DistributedQueryExecution::DistributedQueryExecution(
    query::CompatibleDistributedAggregateSnapshot snapshot,
    query::DistributedAggregateCoordinator coordinator, std::vector<SenderSlot> senders,
    std::map<schema::TabletId, std::size_t> sender_indexes) noexcept
    : snapshot_(std::move(snapshot)), coordinator_(std::move(coordinator)),
      senders_(std::move(senders)), sender_indexes_(std::move(sender_indexes)) {}

common::Result<DistributedQueryExecution>
DistributedQueryExecution::create(const raft::NodeId source_node_id,
                                  query::DistributedAggregatePlan plan,
                                  std::vector<query::DistributedReadAdmission> admissions,
                                  query::CompatibleDistributedAggregateSnapshot snapshot,
                                  const DistributedQueryExecutionLimits limits) {
  const auto dispatches = snapshot.dispatches();
  if (source_node_id == 0U || plan.fragments.size() != admissions.size() ||
      plan.fragments.size() != dispatches.size()) {
    return common::make_unexpected(
        invalid("distributed query execution authority count is invalid"));
  }
  try {
    std::vector<SenderSlot> senders;
    std::map<schema::TabletId, std::size_t> indexes;
    senders.reserve(dispatches.size());
    for (std::size_t index = 0U; index < dispatches.size(); ++index) {
      if (dispatches[index].fragment.query_id != plan.query_id ||
          !dispatch_matches(dispatches[index], plan.fragments[index], admissions[index],
                            plan.read_policy, snapshot) ||
          !indexes.emplace(plan.fragments[index].tablet_id, index).second) {
        return common::make_unexpected(
            invalid("distributed query execution authority is mixed or out of order"));
      }
      auto sender = DistributedQuerySender::create(source_node_id, dispatches[index], limits.retry);
      if (!sender.has_value())
        return common::make_unexpected(sender.error());
      senders.push_back({plan.fragments[index].tablet_id, std::move(*sender), false, false});
    }
    auto coordinator = query::DistributedAggregateCoordinator::create(
        std::move(plan), std::move(admissions), limits.coordinator);
    if (!coordinator.has_value())
      return common::make_unexpected(coordinator.error());
    return DistributedQueryExecution{std::move(snapshot), std::move(*coordinator),
                                     std::move(senders), std::move(indexes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "distributed query execution allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "distributed query execution exceeds container limits"});
  }
}

common::Result<DistributedQueryExecution> DistributedQueryExecution::create_from_bound_snapshot(
    const raft::NodeId source_node_id, query::DistributedAggregatePlan plan,
    query::CompatibleDistributedAggregateSnapshot snapshot,
    const DistributedQueryExecutionLimits limits) {
  try {
    std::vector<query::DistributedReadAdmission> admissions;
    admissions.reserve(snapshot.dispatches().size());
    for (const query::DistributedAggregateFragmentDispatch& dispatch : snapshot.dispatches()) {
      const query::DistributedAggregateFragment& fragment = dispatch.fragment;
      admissions.push_back(
          {.tablet_id = fragment.tablet_id,
           .serving_node = fragment.serving_node,
           .applied_position = fragment.applied_position,
           .observed_leader_commit_position = fragment.observed_leader_commit_position,
           .linearizable_barrier = fragment.linearizable_barrier});
    }
    return create(source_node_id, std::move(plan), std::move(admissions), std::move(snapshot),
                  limits);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "distributed query admission reconstruction allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "distributed query admission reconstruction exceeds container limits"});
  }
}

common::Result<std::size_t>
DistributedQueryExecution::sender_index(const schema::TabletId& tablet_id) const {
  const auto found = sender_indexes_.find(tablet_id);
  if (found == sender_indexes_.end())
    return common::make_unexpected(
        invalid("distributed query execution tablet does not belong to the plan"));
  return found->second;
}

common::Result<DistributedQueryAttempt>
DistributedQueryExecution::begin_attempt(const schema::TabletId& tablet_id, const TimePoint now) {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.begin_attempt(now);
}

common::Status DistributedQueryExecution::accept_response(const schema::TabletId& tablet_id,
                                                          const common::ByteView response_bytes,
                                                          const TimePoint now) {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return index.error();
  SenderSlot& slot = senders_[*index];
  const common::Status status = slot.sender.accept_response(response_bytes, now);
  if (!status.is_ok())
    return status;
  return publish_terminal_state(slot);
}

common::Status DistributedQueryExecution::record_transport_failure(
    const schema::TabletId& tablet_id, const common::StatusCode code, const TimePoint now) {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return index.error();
  SenderSlot& slot = senders_[*index];
  const common::Status status = slot.sender.record_transport_failure(code, now);
  if (!status.is_ok())
    return status;
  return publish_terminal_state(slot);
}

common::Status DistributedQueryExecution::publish_terminal_state(SenderSlot& slot) {
  if (slot.sender.state() == DistributedQuerySenderState::kSucceeded &&
      !slot.coordinator_result_delivered) {
    if (!slot.sender.result().has_value())
      return invalid("distributed query sender success has no result");
    const common::Status accepted = coordinator_.accept(*slot.sender.result());
    if (!accepted.is_ok()) {
      static_cast<void>(coordinator_.worker_failed(slot.tablet_id, accepted));
      slot.coordinator_failure_delivered = true;
      return accepted;
    }
    slot.coordinator_result_delivered = true;
  } else if (slot.sender.state() == DistributedQuerySenderState::kFailed &&
             !slot.coordinator_failure_delivered) {
    const common::StatusCode code =
        slot.sender.last_status_code().value_or(common::StatusCode::kInternal);
    const common::Status reported = coordinator_.worker_failed(
        slot.tablet_id, {code, "distributed query sender reached terminal failure"});
    if (!reported.is_ok())
      return reported;
    slot.coordinator_failure_delivered = true;
  }
  return common::Status::ok();
}

common::Result<DistributedQuerySenderState>
DistributedQueryExecution::sender_state(const schema::TabletId& tablet_id) const {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.state();
}

common::Result<std::optional<DistributedQueryExecution::TimePoint>>
DistributedQueryExecution::next_attempt_not_before(const schema::TabletId& tablet_id) const {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.next_attempt_not_before();
}

common::Result<std::optional<DistributedQueryLeaderHint>>
DistributedQueryExecution::suggested_leader(const schema::TabletId& tablet_id) const {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.suggested_leader();
}

common::Result<query::MergeableAggregateState> DistributedQueryExecution::finish() const {
  return coordinator_.finish();
}

const query::CompatibleDistributedAggregateSnapshot&
DistributedQueryExecution::snapshot() const noexcept {
  return snapshot_;
}

} // namespace chronos::cluster
