#include "chronos/cluster/distributed_vector_aggregate_query_execution_v2.hpp"

#include <new>
#include <stdexcept>
#include <utility>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

} // namespace

DistributedVectorAggregateQueryExecutionV2::DistributedVectorAggregateQueryExecutionV2(
    query::CompatibleDistributedVectorSnapshotV2 snapshot, query::QueryResourceContext resources,
    query::DistributedVectorAggregateCoordinatorV2 coordinator, std::vector<SenderSlot> senders,
    std::map<schema::TabletId, std::size_t> sender_indexes) noexcept
    : snapshot_(std::move(snapshot)), resources_(std::move(resources)),
      coordinator_(std::move(coordinator)), senders_(std::move(senders)),
      sender_indexes_(std::move(sender_indexes)) {}

common::Result<DistributedVectorAggregateQueryExecutionV2>
DistributedVectorAggregateQueryExecutionV2::create(
    const raft::NodeId source_node_id, query::CompatibleDistributedVectorSnapshotV2 snapshot,
    const DistributedVectorAggregateQueryExecutionLimitsV2 limits) {
  const auto dispatches = snapshot.dispatches();
  const auto definitions = snapshot.aggregate_definitions();
  if (source_node_id == 0U || dispatches.empty() || definitions.empty() ||
      definitions.size() > query::kMaximumUngroupedAggregateWidth ||
      limits.maximum_query_memory_bytes == 0U ||
      limits.maximum_query_memory_bytes >
          kMaximumDistributedVectorAggregateQueryExecutionMemoryBytesV2 ||
      dispatches.front().plan.mode != query::DistributedVectorPlanMode::kUngroupedAggregate) {
    return common::make_unexpected(
        invalid("vector aggregate query v2 execution authority is invalid"));
  }
  auto resources = query::QueryResourceContext::create(limits.maximum_query_memory_bytes);
  if (!resources.has_value())
    return common::make_unexpected(resources.error());

  try {
    const common::Uuid query_id = dispatches.front().query_id;
    std::vector<schema::TabletId> tablets;
    std::vector<SenderSlot> senders;
    std::map<schema::TabletId, std::size_t> indexes;
    tablets.reserve(dispatches.size());
    senders.reserve(dispatches.size());
    for (std::size_t index = 0U; index < dispatches.size(); ++index) {
      const query::DistributedVectorFragmentDispatch& dispatch = dispatches[index];
      if (dispatch.query_id != query_id ||
          dispatch.database_id != snapshot.snapshot().database_id() ||
          dispatch.snapshot_generation != snapshot.snapshot().generation() ||
          dispatch.plan != dispatches.front().plan ||
          !indexes.emplace(dispatch.tablet_id, index).second) {
        return common::make_unexpected(
            invalid("vector aggregate query v2 execution authority is mixed or duplicated"));
      }
      std::vector<query::VectorAggregateDefinition> sender_definitions(definitions.begin(),
                                                                       definitions.end());
      auto sender = DistributedVectorAggregateQuerySenderV2::create(
          source_node_id,
          query::DistributedVectorFragmentDispatchV2{dispatch, snapshot.result_schema()},
          std::move(sender_definitions), *resources, limits.sender);
      if (!sender.has_value())
        return common::make_unexpected(sender.error());
      tablets.push_back(dispatch.tablet_id);
      senders.push_back({dispatch.tablet_id, std::move(*sender), false, false});
    }
    std::vector<query::VectorAggregateDefinition> coordinator_definitions(definitions.begin(),
                                                                          definitions.end());
    auto coordinator = query::DistributedVectorAggregateCoordinatorV2::create(
        query_id, std::move(tablets), std::move(coordinator_definitions), snapshot.result_schema(),
        limits.coordinator);
    if (!coordinator.has_value())
      return common::make_unexpected(coordinator.error());
    return DistributedVectorAggregateQueryExecutionV2{std::move(snapshot), std::move(*resources),
                                                      std::move(*coordinator), std::move(senders),
                                                      std::move(indexes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("vector aggregate query v2 execution allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("vector aggregate query v2 execution exceeds limits"));
  }
}

common::Result<std::size_t>
DistributedVectorAggregateQueryExecutionV2::sender_index(const schema::TabletId& tablet_id) const {
  const auto found = sender_indexes_.find(tablet_id);
  if (found == sender_indexes_.end()) {
    return common::make_unexpected(
        invalid("vector aggregate query v2 execution tablet does not belong to the snapshot"));
  }
  return found->second;
}

common::Result<DistributedVectorAggregateQueryAttemptV2>
DistributedVectorAggregateQueryExecutionV2::begin_attempt(const schema::TabletId& tablet_id,
                                                          const TimePoint now) {
  if (finished_)
    return common::make_unexpected(invalid("vector aggregate query v2 execution is finished"));
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.begin_attempt(now);
}

common::Status DistributedVectorAggregateQueryExecutionV2::accept_responses(
    const schema::TabletId& tablet_id,
    const std::span<const DistributedVectorAggregateQueryResponseV2> responses,
    const TimePoint now) {
  if (finished_)
    return invalid("vector aggregate query v2 execution is finished");
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return index.error();
  SenderSlot& slot = senders_[*index];
  common::Status accepted = slot.sender.accept_responses(responses, now);
  if (!accepted.is_ok())
    return accepted;
  return publish_terminal_state(slot);
}

common::Status DistributedVectorAggregateQueryExecutionV2::record_transport_failure(
    const schema::TabletId& tablet_id, const common::StatusCode code, const TimePoint now) {
  if (finished_)
    return invalid("vector aggregate query v2 execution is finished");
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return index.error();
  SenderSlot& slot = senders_[*index];
  common::Status recorded = slot.sender.record_transport_failure(code, now);
  if (!recorded.is_ok())
    return recorded;
  return publish_terminal_state(slot);
}

common::Status
DistributedVectorAggregateQueryExecutionV2::publish_terminal_state(SenderSlot& slot) {
  if (slot.sender.state() == DistributedQuerySenderState::kSucceeded &&
      !slot.coordinator_result_delivered) {
    if (!slot.sender.result().has_value()) {
      const common::Status failure{common::StatusCode::kInternal,
                                   "vector aggregate query v2 sender success has no state vector"};
      static_cast<void>(coordinator_.worker_failed(slot.tablet_id, failure));
      slot.coordinator_failure_delivered = true;
      return failure;
    }
    // Guarded by the result-presence check above.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    for (const query::DistributedVectorAggregateExchangeMessage& message : *slot.sender.result()) {
      const common::Status accepted = coordinator_.accept(message);
      if (!accepted.is_ok()) {
        static_cast<void>(coordinator_.worker_failed(slot.tablet_id, accepted));
        slot.coordinator_failure_delivered = true;
        return accepted;
      }
    }
    slot.coordinator_result_delivered = true;
  } else if (slot.sender.state() == DistributedQuerySenderState::kFailed &&
             !slot.coordinator_failure_delivered) {
    const common::StatusCode code =
        slot.sender.last_status_code().value_or(common::StatusCode::kInternal);
    const common::Status reported = coordinator_.worker_failed(
        slot.tablet_id, {code, "vector aggregate query v2 sender reached terminal failure"});
    if (!reported.is_ok())
      return reported;
    slot.coordinator_failure_delivered = true;
  }
  return common::Status::ok();
}

common::Result<DistributedQuerySenderState>
DistributedVectorAggregateQueryExecutionV2::sender_state(const schema::TabletId& tablet_id) const {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.state();
}

common::Result<std::optional<DistributedVectorAggregateQueryExecutionV2::TimePoint>>
DistributedVectorAggregateQueryExecutionV2::next_attempt_not_before(
    const schema::TabletId& tablet_id) const {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.next_attempt_not_before();
}

common::Result<std::optional<DistributedQueryLeaderHint>>
DistributedVectorAggregateQueryExecutionV2::suggested_leader(
    const schema::TabletId& tablet_id) const {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.suggested_leader();
}

common::Result<DistributedVectorAggregateQueryExecutionResultV2>
DistributedVectorAggregateQueryExecutionV2::finish() {
  if (finished_) {
    return common::make_unexpected(
        invalid("vector aggregate query v2 execution is already finished"));
  }
  bool all_succeeded = true;
  for (const SenderSlot& slot : senders_) {
    if (slot.sender.state() == DistributedQuerySenderState::kFailed ||
        slot.coordinator_failure_delivered) {
      auto failed = std::move(coordinator_).finish();
      finished_ = true;
      return common::make_unexpected(
          failed.has_value()
              ? common::Status{common::StatusCode::kInternal,
                               "failed vector aggregate query v2 execution produced a result"}
              : failed.error());
    }
    all_succeeded = all_succeeded &&
                    slot.sender.state() == DistributedQuerySenderState::kSucceeded &&
                    slot.coordinator_result_delivered;
  }
  if (!all_succeeded) {
    return common::make_unexpected(
        unavailable("vector aggregate query v2 execution is incomplete"));
  }

  try {
    query::DistributedVectorPlanIntent plan = snapshot_.dispatches().front().plan;
    auto result = std::move(coordinator_).finish();
    if (!result.has_value())
      return common::make_unexpected(result.error());
    finished_ = true;
    return DistributedVectorAggregateQueryExecutionResultV2{.plan = std::move(plan),
                                                            .result = std::move(*result)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("vector aggregate query v2 execution result allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("vector aggregate query v2 execution result exceeds limits"));
  }
}

const query::CompatibleDistributedVectorSnapshotV2&
DistributedVectorAggregateQueryExecutionV2::snapshot() const noexcept {
  return snapshot_;
}

std::span<const query::VectorAggregateDefinition>
DistributedVectorAggregateQueryExecutionV2::definitions() const noexcept {
  return snapshot_.aggregate_definitions();
}

const query::QueryResourceContext&
DistributedVectorAggregateQueryExecutionV2::resources() const noexcept {
  return resources_;
}

} // namespace chronos::cluster
