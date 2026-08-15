#include "chronos/cluster/distributed_grouped_query_execution.hpp"

#include <new>
#include <stdexcept>
#include <utility>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

} // namespace

DistributedGroupedQueryExecution::DistributedGroupedQueryExecution(
    query::CompatibleDistributedGroupedFloat64Snapshot snapshot,
    query::DistributedGroupedFloat64Coordinator coordinator, std::vector<SenderSlot> senders,
    std::map<schema::TabletId, std::size_t> sender_indexes) noexcept
    : snapshot_(std::move(snapshot)), coordinator_(std::move(coordinator)),
      senders_(std::move(senders)), sender_indexes_(std::move(sender_indexes)) {}

common::Result<DistributedGroupedQueryExecution> DistributedGroupedQueryExecution::create(
    const raft::NodeId source_node_id, query::CompatibleDistributedGroupedFloat64Snapshot snapshot,
    const DistributedGroupedQueryExecutionLimits limits) {
  const auto dispatches = snapshot.dispatches();
  if (source_node_id == 0U || dispatches.empty())
    return common::make_unexpected(invalid("grouped query execution authority is invalid"));

  try {
    const common::Uuid query_id = dispatches.front().fragment.aggregate.query_id;
    std::vector<schema::TabletId> tablets;
    std::vector<SenderSlot> senders;
    std::map<schema::TabletId, std::size_t> indexes;
    tablets.reserve(dispatches.size());
    senders.reserve(dispatches.size());
    for (std::size_t index = 0U; index < dispatches.size(); ++index) {
      const auto& dispatch = dispatches[index];
      const auto& fragment = dispatch.fragment.aggregate;
      if (fragment.query_id != query_id ||
          fragment.database_id != snapshot.snapshot().database_id() ||
          fragment.snapshot_generation != snapshot.snapshot().generation() ||
          !indexes.emplace(fragment.tablet_id, index).second) {
        return common::make_unexpected(
            invalid("grouped query execution authority is mixed or duplicated"));
      }
      auto sender = DistributedGroupedQuerySender::create(source_node_id, dispatch, limits.retry);
      if (!sender.has_value())
        return common::make_unexpected(sender.error());
      tablets.push_back(fragment.tablet_id);
      senders.push_back({fragment.tablet_id, std::move(*sender), false, false});
    }
    auto coordinator = query::DistributedGroupedFloat64Coordinator::create(
        query_id, std::move(tablets), limits.coordinator, limits.result);
    if (!coordinator.has_value())
      return common::make_unexpected(coordinator.error());
    return DistributedGroupedQueryExecution{std::move(snapshot), std::move(*coordinator),
                                            std::move(senders), std::move(indexes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "grouped query execution allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "grouped query execution exceeds container limits"});
  }
}

common::Result<std::size_t>
DistributedGroupedQueryExecution::sender_index(const schema::TabletId& tablet_id) const {
  const auto found = sender_indexes_.find(tablet_id);
  if (found == sender_indexes_.end())
    return common::make_unexpected(
        invalid("grouped query execution tablet does not belong to the snapshot"));
  return found->second;
}

common::Result<DistributedGroupedQueryAttempt>
DistributedGroupedQueryExecution::begin_attempt(const schema::TabletId& tablet_id,
                                                const TimePoint now) {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.begin_attempt(now);
}

common::Status DistributedGroupedQueryExecution::accept_responses(
    const schema::TabletId& tablet_id,
    const std::span<const DistributedGroupedQueryResponse> responses, const TimePoint now) {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return index.error();
  SenderSlot& slot = senders_[*index];
  common::Status accepted = slot.sender.accept_responses(responses, now);
  if (!accepted.is_ok())
    return accepted;
  return publish_terminal_state(slot);
}

common::Status DistributedGroupedQueryExecution::record_transport_failure(
    const schema::TabletId& tablet_id, const common::StatusCode code, const TimePoint now) {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return index.error();
  SenderSlot& slot = senders_[*index];
  common::Status recorded = slot.sender.record_transport_failure(code, now);
  if (!recorded.is_ok())
    return recorded;
  return publish_terminal_state(slot);
}

common::Status DistributedGroupedQueryExecution::publish_terminal_state(SenderSlot& slot) {
  if (slot.sender.state() == DistributedQuerySenderState::kSucceeded &&
      !slot.coordinator_result_delivered) {
    const auto result = slot.sender.result().transform(
        [](const std::vector<DistributedGroupedQueryResponsePayload>& payloads) {
          return std::span<const DistributedGroupedQueryResponsePayload>{payloads};
        });
    if (!result.has_value())
      return invalid("grouped query sender success has no result stream");
    for (const DistributedGroupedQueryResponsePayload& payload :
         result.value_or(std::span<const DistributedGroupedQueryResponsePayload>{})) {
      const common::Status accepted =
          std::holds_alternative<query::GroupedFloat64ExchangeMessage>(payload)
              ? coordinator_.accept(std::get<query::GroupedFloat64ExchangeMessage>(payload))
              : coordinator_.accept_terminal(
                    std::get<query::GroupedExchangeTerminalMessage>(payload));
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
        slot.tablet_id, {code, "grouped query sender reached terminal failure"});
    if (!reported.is_ok())
      return reported;
    slot.coordinator_failure_delivered = true;
  }
  return common::Status::ok();
}

common::Result<DistributedQuerySenderState>
DistributedGroupedQueryExecution::sender_state(const schema::TabletId& tablet_id) const {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.state();
}

common::Result<std::optional<DistributedGroupedQueryExecution::TimePoint>>
DistributedGroupedQueryExecution::next_attempt_not_before(const schema::TabletId& tablet_id) const {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.next_attempt_not_before();
}

common::Result<std::optional<DistributedQueryLeaderHint>>
DistributedGroupedQueryExecution::suggested_leader(const schema::TabletId& tablet_id) const {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.suggested_leader();
}

common::Result<std::vector<query::GroupedFloat64AggregateResult>>
DistributedGroupedQueryExecution::finish() const {
  return coordinator_.finish();
}

const query::CompatibleDistributedGroupedFloat64Snapshot&
DistributedGroupedQueryExecution::snapshot() const noexcept {
  return snapshot_;
}

} // namespace chronos::cluster
