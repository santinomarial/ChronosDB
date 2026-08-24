#include "chronos/cluster/distributed_mutable_vector_query_execution.hpp"

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

DistributedMutableVectorQueryExecution::DistributedMutableVectorQueryExecution(
    query::DistributedVectorPlanIntent plan, DistributedVectorResultCoordinatorV2 coordinator,
    std::vector<SenderSlot> senders,
    std::map<schema::TabletId, std::size_t> sender_indexes) noexcept
    : plan_(std::move(plan)), coordinator_(std::move(coordinator)), senders_(std::move(senders)),
      sender_indexes_(std::move(sender_indexes)) {}

common::Result<DistributedMutableVectorQueryExecution>
DistributedMutableVectorQueryExecution::create(
    const raft::NodeId source_node_id,
    std::vector<query::DistributedMutableVectorFragment> fragments,
    const DistributedMutableVectorQueryExecutionLimits limits) {
  if (source_node_id == 0U || fragments.empty()) {
    return common::make_unexpected(invalid("mutable vector query execution authority is invalid"));
  }
  try {
    const common::Uuid query_id = fragments.front().query_id;
    const manifest::DatabaseId database_id = fragments.front().database_id;
    const schema::TableId table_id = fragments.front().table_id;
    const schema::SchemaId destination_schema_id = fragments.front().destination_schema_id;
    const query::DistributedReadPolicy read_policy = fragments.front().read_policy;
    query::DistributedVectorPlanIntent plan = fragments.front().plan;
    query::DistributedVectorResultSchema result_schema = fragments.front().result_schema;
    std::vector<schema::TabletId> tablets;
    std::vector<SenderSlot> senders;
    std::map<schema::TabletId, std::size_t> indexes;
    tablets.reserve(fragments.size());
    senders.reserve(fragments.size());
    for (std::size_t index = 0U; index < fragments.size(); ++index) {
      query::DistributedMutableVectorFragment& fragment = fragments[index];
      const common::Status structural =
          query::validate_distributed_mutable_vector_fragment(fragment);
      if (!structural.is_ok())
        return common::make_unexpected(structural);
      if (fragment.query_id != query_id || fragment.database_id != database_id ||
          fragment.table_id != table_id ||
          fragment.destination_schema_id != destination_schema_id ||
          fragment.read_policy != read_policy || fragment.plan != plan ||
          fragment.result_schema != result_schema ||
          !indexes.emplace(fragment.tablet_id, index).second) {
        return common::make_unexpected(
            invalid("mutable vector query execution authority is mixed or duplicated"));
      }
      const schema::TabletId tablet_id = fragment.tablet_id;
      auto sender = DistributedMutableVectorQuerySender::create(source_node_id, std::move(fragment),
                                                                limits.sender);
      if (!sender.has_value())
        return common::make_unexpected(sender.error());
      tablets.push_back(tablet_id);
      senders.push_back({tablet_id, std::move(*sender), false, false});
    }
    auto coordinator = DistributedVectorResultCoordinatorV2::create(
        query_id, std::move(tablets), std::move(result_schema), limits.coordinator);
    if (!coordinator.has_value())
      return common::make_unexpected(coordinator.error());
    return DistributedMutableVectorQueryExecution{std::move(plan), std::move(*coordinator),
                                                  std::move(senders), std::move(indexes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable vector query execution allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("mutable vector query execution exceeds limits"));
  }
}

common::Result<std::size_t>
DistributedMutableVectorQueryExecution::sender_index(const schema::TabletId& tablet_id) const {
  const auto found = sender_indexes_.find(tablet_id);
  if (found == sender_indexes_.end()) {
    return common::make_unexpected(
        invalid("mutable vector query execution tablet does not belong to the query"));
  }
  return found->second;
}

common::Result<DistributedMutableVectorQueryAttempt>
DistributedMutableVectorQueryExecution::begin_attempt(const schema::TabletId& tablet_id,
                                                      const TimePoint now) {
  if (finished_)
    return common::make_unexpected(invalid("mutable vector query execution is finished"));
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.begin_attempt(now);
}

common::Status DistributedMutableVectorQueryExecution::accept_responses(
    const schema::TabletId& tablet_id,
    const std::span<const DistributedVectorQueryResponseV2> responses, const TimePoint now) {
  if (finished_)
    return invalid("mutable vector query execution is finished");
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return index.error();
  SenderSlot& slot = senders_[*index];
  common::Status accepted = slot.sender.accept_responses(responses, now);
  if (!accepted.is_ok())
    return accepted;
  return publish_terminal_state(slot);
}

common::Status DistributedMutableVectorQueryExecution::record_transport_failure(
    const schema::TabletId& tablet_id, const common::StatusCode code, const TimePoint now) {
  if (finished_)
    return invalid("mutable vector query execution is finished");
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return index.error();
  SenderSlot& slot = senders_[*index];
  common::Status recorded = slot.sender.record_transport_failure(code, now);
  if (!recorded.is_ok())
    return recorded;
  return publish_terminal_state(slot);
}

common::Status DistributedMutableVectorQueryExecution::publish_terminal_state(SenderSlot& slot) {
  if (slot.sender.state() == DistributedQuerySenderState::kSucceeded &&
      !slot.coordinator_result_delivered) {
    if (!slot.sender.result().has_value()) {
      const common::Status failure{common::StatusCode::kInternal,
                                   "mutable vector query sender success has no result stream"};
      static_cast<void>(coordinator_.worker_failed(slot.tablet_id, failure));
      slot.coordinator_failure_delivered = true;
      return failure;
    }
    // Guarded by the result-presence check above.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    for (const DistributedVectorResultExchangeMessage& message : *slot.sender.result()) {
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
        slot.tablet_id, {code, "mutable vector query sender reached terminal failure"});
    if (!reported.is_ok())
      return reported;
    slot.coordinator_failure_delivered = true;
  }
  return common::Status::ok();
}

common::Result<DistributedQuerySenderState>
DistributedMutableVectorQueryExecution::sender_state(const schema::TabletId& tablet_id) const {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.state();
}

common::Result<std::optional<DistributedMutableVectorQueryExecution::TimePoint>>
DistributedMutableVectorQueryExecution::next_attempt_not_before(
    const schema::TabletId& tablet_id) const {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.next_attempt_not_before();
}

common::Result<std::optional<DistributedQueryLeaderHint>>
DistributedMutableVectorQueryExecution::suggested_leader(const schema::TabletId& tablet_id) const {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.suggested_leader();
}

common::Result<DistributedVectorQueryExecutionResultV2>
DistributedMutableVectorQueryExecution::finish() {
  if (finished_)
    return common::make_unexpected(invalid("mutable vector query execution is already finished"));
  bool all_succeeded = true;
  for (const SenderSlot& slot : senders_) {
    if (slot.sender.state() == DistributedQuerySenderState::kFailed ||
        slot.coordinator_failure_delivered) {
      auto failed = std::move(coordinator_).finish();
      return common::make_unexpected(
          failed.has_value()
              ? common::Status{common::StatusCode::kInternal,
                               "failed mutable vector query execution produced a result"}
              : failed.error());
    }
    all_succeeded = all_succeeded &&
                    slot.sender.state() == DistributedQuerySenderState::kSucceeded &&
                    slot.coordinator_result_delivered;
  }
  if (!all_succeeded)
    return common::make_unexpected(unavailable("mutable vector query execution is incomplete"));

  try {
    auto result = std::move(coordinator_).finish();
    if (!result.has_value())
      return common::make_unexpected(result.error());
    finished_ = true;
    return DistributedVectorQueryExecutionResultV2{.plan = std::move(plan_),
                                                   .result = std::move(*result)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("mutable vector query execution result allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("mutable vector query execution result exceeds limits"));
  }
}

} // namespace chronos::cluster
