#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_execution.hpp"

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

[[nodiscard]] bool valid_decode_limits(
    const query::DistributedVectorGroupedAggregateExchangeDecodeLimits& limits) noexcept {
  return limits.maximum_frame_length >=
             query::distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength &&
         limits.maximum_frame_length <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumFrameLength &&
         limits.maximum_key_payload_bytes > 0U &&
         limits.maximum_key_payload_bytes <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumKeyPayloadBytes &&
         limits.maximum_groups > 0U &&
         limits.maximum_groups <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups &&
         limits.maximum_group_keys > 0U &&
         limits.maximum_group_keys <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroupKeys &&
         limits.maximum_aggregates <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumAggregates &&
         limits.state.maximum_frame_length >=
             query::distributed_vector_aggregate_state_format::kMinimumFrameLength &&
         limits.state.maximum_frame_length <=
             query::distributed_vector_aggregate_state_format::kMaximumFrameLength &&
         limits.state.maximum_variable_extremum_bytes > 0U &&
         limits.state.maximum_variable_extremum_bytes <=
             query::distributed_vector_aggregate_state_format::kMaximumExtremumBytes;
}

} // namespace

DistributedMutableVectorGroupedAggregateQueryExecution::
    DistributedMutableVectorGroupedAggregateQueryExecution(
        query::DistributedVectorPlanIntent plan, query::DistributedVectorResultSchema result_schema,
        std::vector<query::VectorGroupKeyDefinition> keys,
        std::vector<query::VectorAggregateDefinition> aggregates,
        query::QueryResourceContext decode_resources,
        query::DistributedVectorGroupedAggregateCoordinator coordinator,
        std::vector<SenderSlot> senders,
        std::vector<DistributedMutableVectorGroupedAggregateQueryTarget> targets,
        DistributedMutableVectorQueryLogicalIdentity logical_identity,
        std::map<schema::TabletId, std::size_t> sender_indexes,
        const query::DistributedVectorGroupedAggregateExchangeDecodeLimits decode_limits) noexcept
    : plan_(std::move(plan)), result_schema_(std::move(result_schema)), keys_(std::move(keys)),
      aggregates_(std::move(aggregates)), decode_resources_(std::move(decode_resources)),
      coordinator_(std::move(coordinator)), senders_(std::move(senders)),
      targets_(std::move(targets)), logical_identity_(std::move(logical_identity)),
      sender_indexes_(std::move(sender_indexes)), decode_limits_(decode_limits) {}

common::Result<DistributedMutableVectorGroupedAggregateQueryExecution>
DistributedMutableVectorGroupedAggregateQueryExecution::create(
    const raft::NodeId source_node_id,
    std::vector<query::DistributedMutableVectorFragment> fragments,
    std::vector<query::VectorGroupKeyDefinition> keys,
    std::vector<query::VectorAggregateDefinition> aggregates,
    const DistributedMutableVectorGroupedAggregateQueryExecutionLimits limits) {
  if (source_node_id == 0U || fragments.empty() || keys.empty() ||
      limits.maximum_decode_memory_bytes == 0U ||
      limits.maximum_decode_memory_bytes >
          kMaximumDistributedMutableVectorGroupedAggregateQueryExecutionDecodeMemoryBytes ||
      !valid_decode_limits(limits.decode)) {
    return common::make_unexpected(
        invalid("mutable grouped query execution authority or limits are invalid"));
  }
  auto logical_identity = distributed_mutable_vector_query_logical_identity(fragments);
  if (!logical_identity.has_value())
    return common::make_unexpected(logical_identity.error());
  if (logical_identity->plan.mode != query::DistributedVectorPlanMode::kGroupedAggregate ||
      logical_identity->plan.group_key_input_indices.size() != keys.size() ||
      logical_identity->plan.aggregates.size() != aggregates.size() ||
      logical_identity->result_schema.columns.size() != keys.size() + aggregates.size()) {
    return common::make_unexpected(
        invalid("mutable grouped query execution plan authority differs"));
  }
  for (const query::DistributedMutableVectorFragment& fragment : fragments) {
    const common::Status authority =
        validate_distributed_mutable_vector_grouped_aggregate_query_authority(fragment, keys,
                                                                              aggregates);
    if (!authority.is_ok())
      return common::make_unexpected(authority);
  }
  auto resources = query::QueryResourceContext::create(limits.maximum_decode_memory_bytes);
  if (!resources.has_value())
    return common::make_unexpected(resources.error());

  try {
    std::vector<schema::TabletId> tablets;
    std::vector<SenderSlot> senders;
    std::vector<DistributedMutableVectorGroupedAggregateQueryTarget> targets;
    std::map<schema::TabletId, std::size_t> indexes;
    tablets.reserve(fragments.size());
    senders.reserve(fragments.size());
    targets.reserve(fragments.size());
    for (std::size_t index = 0U; index < fragments.size(); ++index) {
      query::DistributedMutableVectorFragment& fragment = fragments[index];
      const schema::TabletId tablet_id = fragment.tablet_id;
      const raft::NodeId serving_node = fragment.serving_node;
      std::vector<query::VectorGroupKeyDefinition> sender_keys(keys.begin(), keys.end());
      std::vector<query::VectorAggregateDefinition> sender_aggregates(aggregates.begin(),
                                                                      aggregates.end());
      auto sender = serving_node == source_node_id
                        ? DistributedMutableVectorGroupedAggregateQuerySender::create_local(
                              source_node_id, std::move(fragment), std::move(sender_keys),
                              std::move(sender_aggregates), *resources, limits.sender)
                        : DistributedMutableVectorGroupedAggregateQuerySender::create(
                              source_node_id, std::move(fragment), std::move(sender_keys),
                              std::move(sender_aggregates), *resources, limits.sender);
      if (!sender.has_value())
        return common::make_unexpected(sender.error());
      if (!indexes.emplace(tablet_id, index).second)
        return common::make_unexpected(
            invalid("mutable grouped query execution tablet is duplicated"));
      tablets.push_back(tablet_id);
      senders.push_back({tablet_id, std::move(*sender), false, false});
      targets.push_back({tablet_id, serving_node});
    }
    std::vector<query::VectorGroupKeyDefinition> coordinator_keys(keys.begin(), keys.end());
    std::vector<query::VectorAggregateDefinition> coordinator_aggregates(aggregates.begin(),
                                                                         aggregates.end());
    auto coordinator = query::DistributedVectorGroupedAggregateCoordinator::create(
        logical_identity->query_id, std::move(tablets), std::move(coordinator_keys),
        std::move(coordinator_aggregates), limits.coordinator);
    if (!coordinator.has_value())
      return common::make_unexpected(coordinator.error());
    query::DistributedVectorPlanIntent plan = logical_identity->plan;
    query::DistributedVectorResultSchema result_schema = logical_identity->result_schema;
    return DistributedMutableVectorGroupedAggregateQueryExecution{std::move(plan),
                                                                  std::move(result_schema),
                                                                  std::move(keys),
                                                                  std::move(aggregates),
                                                                  std::move(*resources),
                                                                  std::move(*coordinator),
                                                                  std::move(senders),
                                                                  std::move(targets),
                                                                  std::move(*logical_identity),
                                                                  std::move(indexes),
                                                                  limits.decode};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable grouped query execution allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("mutable grouped query execution exceeds limits"));
  }
}

common::Result<std::size_t> DistributedMutableVectorGroupedAggregateQueryExecution::sender_index(
    const schema::TabletId& tablet_id) const {
  const auto found = sender_indexes_.find(tablet_id);
  if (found == sender_indexes_.end()) {
    return common::make_unexpected(invalid("mutable grouped query execution tablet is unplanned"));
  }
  return found->second;
}

common::Result<DistributedMutableVectorGroupedAggregateQueryAttempt>
DistributedMutableVectorGroupedAggregateQueryExecution::begin_attempt(
    const schema::TabletId& tablet_id, const TimePoint now) {
  if (ready_ || failure_.has_value())
    return common::make_unexpected(invalid("mutable grouped query execution is sealed"));
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.begin_attempt(now);
}

common::Status DistributedMutableVectorGroupedAggregateQueryExecution::execute_local(
    const schema::TabletId& tablet_id,
    DistributedMutableVectorGroupedAggregateQueryWorkerService& worker, const TimePoint now) {
  if (ready_ || failure_.has_value())
    return failure_.value_or(invalid("mutable grouped query execution is sealed"));
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return index.error();
  SenderSlot& slot = senders_[*index];
  common::Status executed = slot.sender.execute_local(worker, now);
  if (!executed.is_ok())
    return executed;
  return publish_terminal_state(slot);
}

common::Status DistributedMutableVectorGroupedAggregateQueryExecution::accept_responses(
    const schema::TabletId& tablet_id,
    const std::span<const DistributedVectorGroupedAggregateQueryResponseV2> responses,
    const TimePoint now) {
  if (ready_ || failure_.has_value())
    return failure_.value_or(invalid("mutable grouped query execution is sealed"));
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return index.error();
  SenderSlot& slot = senders_[*index];
  common::Status accepted = slot.sender.accept_responses(responses, now);
  if (!accepted.is_ok())
    return accepted;
  return publish_terminal_state(slot);
}

common::Status DistributedMutableVectorGroupedAggregateQueryExecution::record_transport_failure(
    const schema::TabletId& tablet_id, const common::StatusCode code, const TimePoint now) {
  if (ready_ || failure_.has_value())
    return failure_.value_or(invalid("mutable grouped query execution is sealed"));
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
DistributedMutableVectorGroupedAggregateQueryExecution::publish_terminal_state(SenderSlot& slot) {
  if (slot.sender.state() == DistributedQuerySenderState::kSucceeded &&
      !slot.coordinator_result_delivered) {
    if (!slot.sender.result().has_value()) {
      const common::Status failure{common::StatusCode::kInternal,
                                   "mutable grouped sender success has no result"};
      static_cast<void>(coordinator_.worker_failed(slot.tablet_id, failure));
      slot.coordinator_failure_delivered = true;
      failure_.emplace(failure);
      return failure;
    }
    // Guarded by the result-presence check above.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    for (const auto& frame : *slot.sender.result()) {
      auto decoded = query::decode_distributed_vector_grouped_aggregate_exchange_message_exact(
          frame.bytes(), keys_, aggregates_, decode_resources_, decode_limits_);
      if (!decoded.has_value()) {
        static_cast<void>(coordinator_.worker_failed(slot.tablet_id, decoded.error()));
        slot.coordinator_failure_delivered = true;
        failure_.emplace(decoded.error());
        return *failure_;
      }
      if (decoded->position().tablet_id != slot.tablet_id) {
        const common::Status failure{common::StatusCode::kInvalidArgument,
                                     "mutable grouped result changed tablet identity"};
        static_cast<void>(coordinator_.worker_failed(slot.tablet_id, failure));
        slot.coordinator_failure_delivered = true;
        failure_.emplace(failure);
        return failure;
      }
      const common::Status merged = coordinator_.accept(*decoded);
      if (!merged.is_ok()) {
        static_cast<void>(coordinator_.worker_failed(slot.tablet_id, merged));
        slot.coordinator_failure_delivered = true;
        failure_.emplace(merged);
        return merged;
      }
    }
    slot.coordinator_result_delivered = true;
  } else if (slot.sender.state() == DistributedQuerySenderState::kFailed &&
             !slot.coordinator_failure_delivered) {
    const common::Status failure{
        slot.sender.last_status_code().value_or(common::StatusCode::kInternal),
        "mutable grouped sender reached terminal failure"};
    const common::Status reported = coordinator_.worker_failed(slot.tablet_id, failure);
    slot.coordinator_failure_delivered = true;
    failure_.emplace(reported.is_ok() ? failure : reported);
    return *failure_;
  }
  return common::Status::ok();
}

common::Result<DistributedQuerySenderState>
DistributedMutableVectorGroupedAggregateQueryExecution::sender_state(
    const schema::TabletId& tablet_id) const {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.state();
}

common::Result<std::optional<DistributedMutableVectorGroupedAggregateQueryExecution::TimePoint>>
DistributedMutableVectorGroupedAggregateQueryExecution::next_attempt_not_before(
    const schema::TabletId& tablet_id) const {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.next_attempt_not_before();
}

common::Result<std::optional<DistributedQueryLeaderHint>>
DistributedMutableVectorGroupedAggregateQueryExecution::suggested_leader(
    const schema::TabletId& tablet_id) const {
  auto index = sender_index(tablet_id);
  if (!index.has_value())
    return common::make_unexpected(index.error());
  return senders_[*index].sender.suggested_leader();
}

common::Status DistributedMutableVectorGroupedAggregateQueryExecution::finish() {
  if (ready_)
    return invalid("mutable grouped query execution is already finished");
  if (sources_taken_)
    return invalid("mutable grouped query execution sources were transferred");
  if (failure_.has_value())
    return *failure_;
  for (const SenderSlot& slot : senders_) {
    if (slot.sender.state() != DistributedQuerySenderState::kSucceeded ||
        !slot.coordinator_result_delivered) {
      return unavailable("mutable grouped query execution is incomplete");
    }
  }
  common::Status finished = coordinator_.finish();
  if (!finished.is_ok())
    return finished;
  ready_ = true;
  return common::Status::ok();
}

common::Result<std::vector<DistributedMutableVectorGroupedAggregateCompletedSource>>
DistributedMutableVectorGroupedAggregateQueryExecution::take_completed_sources() {
  if (ready_ || sources_taken_)
    return common::make_unexpected(
        invalid("mutable grouped query execution cannot transfer sources"));
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  for (const SenderSlot& slot : senders_) {
    if (slot.sender.state() != DistributedQuerySenderState::kSucceeded ||
        !slot.sender.result().has_value()) {
      return common::make_unexpected(
          unavailable("mutable grouped query execution sources are incomplete"));
    }
  }
  try {
    std::vector<DistributedMutableVectorGroupedAggregateCompletedSource> sources;
    sources.reserve(senders_.size());
    for (SenderSlot& slot : senders_) {
      auto messages = slot.sender.take_result();
      if (!messages.has_value())
        return common::make_unexpected(messages.error());
      sources.push_back({slot.tablet_id, std::move(*messages)});
    }
    sources_taken_ = true;
    return sources;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("mutable grouped query source transfer allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("mutable grouped query source transfer exceeds limits"));
  }
}

common::Result<query::PhysicalOperatorStep>
DistributedMutableVectorGroupedAggregateQueryExecution::next() {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  if (!ready_)
    return common::make_unexpected(invalid("mutable grouped query execution is not ready"));
  return coordinator_.next();
}

std::span<const DistributedMutableVectorGroupedAggregateQueryTarget>
DistributedMutableVectorGroupedAggregateQueryExecution::targets() const noexcept {
  return targets_;
}

std::span<const query::VectorGroupKeyDefinition>
DistributedMutableVectorGroupedAggregateQueryExecution::key_definitions() const noexcept {
  return keys_;
}

std::span<const query::VectorAggregateDefinition>
DistributedMutableVectorGroupedAggregateQueryExecution::aggregate_definitions() const noexcept {
  return aggregates_;
}

const query::DistributedVectorPlanIntent&
DistributedMutableVectorGroupedAggregateQueryExecution::plan() const noexcept {
  return plan_;
}

const query::DistributedVectorResultSchema&
DistributedMutableVectorGroupedAggregateQueryExecution::result_schema() const noexcept {
  return result_schema_;
}

const DistributedMutableVectorQueryLogicalIdentity&
DistributedMutableVectorGroupedAggregateQueryExecution::logical_identity() const noexcept {
  return logical_identity_;
}

const query::QueryResourceContext&
DistributedMutableVectorGroupedAggregateQueryExecution::decode_resources() const noexcept {
  return decode_resources_;
}

std::optional<query::QueryResourceContext>
DistributedMutableVectorGroupedAggregateQueryExecution::output_resources() const noexcept {
  return coordinator_.output_resources();
}

} // namespace chronos::cluster
