#include "chronos/cluster/distributed_vector_grouped_aggregate_query_execution_v2.hpp"

#include <new>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
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

DistributedVectorGroupedAggregateQueryExecutionV2::
    DistributedVectorGroupedAggregateQueryExecutionV2(
        query::CompatibleDistributedVectorSnapshotV2 snapshot,
        query::QueryResourceContext decode_resources,
        query::DistributedVectorGroupedAggregateCoordinator coordinator,
        std::map<schema::TabletId, bool> terminal_tablets,
        const query::DistributedVectorGroupedAggregateExchangeDecodeLimits decode_limits) noexcept
    : snapshot_(std::move(snapshot)), decode_resources_(std::move(decode_resources)),
      coordinator_(std::move(coordinator)), terminal_tablets_(std::move(terminal_tablets)),
      decode_limits_(decode_limits) {}

common::Result<DistributedVectorGroupedAggregateQueryExecutionV2>
DistributedVectorGroupedAggregateQueryExecutionV2::create(
    query::CompatibleDistributedVectorSnapshotV2 snapshot,
    const DistributedVectorGroupedAggregateQueryExecutionLimitsV2 limits) {
  const auto dispatches = snapshot.dispatches();
  const auto keys = snapshot.grouped_key_definitions();
  const auto definitions = snapshot.grouped_aggregate_definitions();
  if (dispatches.empty() || keys.empty() || !snapshot.aggregate_definitions().empty() ||
      dispatches.front().plan.mode != query::DistributedVectorPlanMode::kGroupedAggregate ||
      limits.maximum_decode_memory_bytes == 0U ||
      limits.maximum_decode_memory_bytes >
          kMaximumDistributedVectorGroupedAggregateQueryExecutionDecodeMemoryBytesV2 ||
      !valid_decode_limits(limits.decode)) {
    return common::make_unexpected(
        invalid("grouped vector query v2 execution authority or limits are invalid"));
  }
  auto resources = query::QueryResourceContext::create(limits.maximum_decode_memory_bytes);
  if (!resources.has_value())
    return common::make_unexpected(resources.error());

  try {
    const common::Uuid query_id = dispatches.front().query_id;
    std::vector<schema::TabletId> tablets;
    std::map<schema::TabletId, bool> terminal_tablets;
    tablets.reserve(dispatches.size());
    for (const query::DistributedVectorFragmentDispatch& dispatch : dispatches) {
      if (dispatch.query_id != query_id ||
          dispatch.database_id != snapshot.snapshot().database_id() ||
          dispatch.snapshot_generation != snapshot.snapshot().generation() ||
          dispatch.plan != dispatches.front().plan ||
          !terminal_tablets.emplace(dispatch.tablet_id, false).second) {
        return common::make_unexpected(
            invalid("grouped vector query v2 execution authority is mixed or duplicated"));
      }
      tablets.push_back(dispatch.tablet_id);
    }
    std::vector<query::VectorGroupKeyDefinition> coordinator_keys(keys.begin(), keys.end());
    std::vector<query::VectorAggregateDefinition> coordinator_definitions(definitions.begin(),
                                                                          definitions.end());
    auto coordinator = query::DistributedVectorGroupedAggregateCoordinator::create(
        query_id, std::move(tablets), std::move(coordinator_keys),
        std::move(coordinator_definitions), limits.coordinator);
    if (!coordinator.has_value())
      return common::make_unexpected(coordinator.error());
    return DistributedVectorGroupedAggregateQueryExecutionV2{
        std::move(snapshot), std::move(*resources), std::move(*coordinator),
        std::move(terminal_tablets), limits.decode};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped vector query v2 execution allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped vector query v2 execution exceeds limits"));
  }
}

common::Status
DistributedVectorGroupedAggregateQueryExecutionV2::fail_batch(const schema::TabletId& tablet_id,
                                                              common::Status failure) {
  if (failure_.has_value())
    return *failure_;
  const common::Status reported = coordinator_.worker_failed(tablet_id, failure);
  if (reported.is_ok())
    failure_.emplace(std::move(failure));
  else
    failure_.emplace(reported);
  return *failure_;
}

common::Status DistributedVectorGroupedAggregateQueryExecutionV2::accept_worker_frames(
    const schema::TabletId& tablet_id,
    const std::span<const query::EncodedDistributedVectorGroupedAggregateExchangeMessage> frames) {
  if (ready_)
    return invalid("grouped vector query v2 execution input is sealed");
  if (failure_.has_value())
    return *failure_;
  const auto planned = terminal_tablets_.find(tablet_id);
  if (planned == terminal_tablets_.end())
    return invalid("grouped vector query v2 execution tablet is unplanned");
  if (frames.empty())
    return fail_batch(tablet_id, invalid("grouped vector query v2 worker batch is empty"));

  bool terminal = false;
  for (const auto& frame : frames) {
    auto decoded = query::decode_distributed_vector_grouped_aggregate_exchange_message_exact(
        frame.bytes(), snapshot_.grouped_key_definitions(),
        snapshot_.grouped_aggregate_definitions(), decode_resources_, decode_limits_);
    if (!decoded.has_value())
      return fail_batch(tablet_id, decoded.error());
    if (decoded->position().tablet_id != tablet_id)
      return fail_batch(tablet_id,
                        invalid("grouped vector query v2 worker batch changed tablet identity"));
    const common::Status accepted = coordinator_.accept(*decoded);
    if (!accepted.is_ok())
      return fail_batch(tablet_id, accepted);
    terminal = decoded->position().terminal;
  }
  if (!terminal)
    return fail_batch(tablet_id,
                      invalid("grouped vector query v2 worker batch has no terminal frame"));
  planned->second = true;
  return common::Status::ok();
}

common::Status
DistributedVectorGroupedAggregateQueryExecutionV2::worker_failed(const schema::TabletId& tablet_id,
                                                                 common::Status failure) {
  if (ready_)
    return invalid("grouped vector query v2 execution input is sealed");
  if (!terminal_tablets_.contains(tablet_id))
    return invalid("grouped vector query v2 execution tablet is unplanned");
  if (failure.is_ok())
    return invalid("grouped vector query v2 execution worker failure is invalid");
  return fail_batch(tablet_id, std::move(failure));
}

common::Status DistributedVectorGroupedAggregateQueryExecutionV2::finish() {
  if (ready_)
    return invalid("grouped vector query v2 execution is already finished");
  if (failure_.has_value())
    return *failure_;
  const common::Status finished = coordinator_.finish();
  if (finished.is_ok())
    ready_ = true;
  return finished;
}

common::Result<query::PhysicalOperatorStep>
DistributedVectorGroupedAggregateQueryExecutionV2::next() {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  if (!ready_)
    return common::make_unexpected(
        invalid("grouped vector query v2 execution is not ready for output"));
  return coordinator_.next();
}

const query::CompatibleDistributedVectorSnapshotV2&
DistributedVectorGroupedAggregateQueryExecutionV2::snapshot() const noexcept {
  return snapshot_;
}

std::span<const query::VectorGroupKeyDefinition>
DistributedVectorGroupedAggregateQueryExecutionV2::key_definitions() const noexcept {
  return snapshot_.grouped_key_definitions();
}

std::span<const query::VectorAggregateDefinition>
DistributedVectorGroupedAggregateQueryExecutionV2::aggregate_definitions() const noexcept {
  return snapshot_.grouped_aggregate_definitions();
}

const query::QueryResourceContext&
DistributedVectorGroupedAggregateQueryExecutionV2::decode_resources() const noexcept {
  return decode_resources_;
}

} // namespace chronos::cluster
