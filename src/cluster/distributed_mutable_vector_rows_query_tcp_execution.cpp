#include "chronos/cluster/distributed_mutable_vector_rows_query_tcp_execution.hpp"

#include <new>
#include <stdexcept>
#include <utility>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

} // namespace

DistributedMutableVectorRowsQueryTcpExecution::DistributedMutableVectorRowsQueryTcpExecution(
    const raft::NodeId source_node_id,
    DistributedMutableVectorQueryExecutionLimits execution_limits,
    DistributedMutableVectorQueryTcpExecution scheduler,
    DistributedVectorRowFinalizationLimitsV2 finalization_limits)
    : source_node_id_(source_node_id), execution_limits_(execution_limits),
      scheduler_(std::move(scheduler)), finalization_limits_(finalization_limits) {}

common::Result<DistributedMutableVectorRowsQueryTcpExecution>
DistributedMutableVectorRowsQueryTcpExecution::create(
    std::vector<query::DistributedMutableVectorFragment> fragments,
    DistributedMutableVectorRowsQueryTcpExecutionConfig config) {
  try {
    auto execution = DistributedMutableVectorQueryExecution::create(
        config.source_node_id, std::move(fragments), config.execution);
    if (!execution.has_value())
      return common::make_unexpected(execution.error());
    auto scheduler = DistributedMutableVectorQueryTcpExecution::create(std::move(*execution),
                                                                       std::move(config.tcp));
    if (!scheduler.has_value())
      return common::make_unexpected(scheduler.error());
    return DistributedMutableVectorRowsQueryTcpExecution{
        config.source_node_id, config.execution, std::move(*scheduler), config.finalization};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable vector rows query allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("mutable vector rows query exceeds limits"));
  }
}

common::Status DistributedMutableVectorRowsQueryTcpExecution::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  if (state_ == DistributedMutableVectorRowsQueryTcpExecutionState::kComplete)
    return common::Status::ok();
  if (state_ == DistributedMutableVectorRowsQueryTcpExecutionState::kFailed ||
      state_ == DistributedMutableVectorRowsQueryTcpExecutionState::kCancelled) {
    return failure_;
  }
  common::Status polled = scheduler_.poll_once(maximum_wait);
  if (!polled.is_ok()) {
    failure_ = std::move(polled);
    scheduler_failed_ =
        scheduler_.state() == DistributedMutableVectorQueryTcpExecutionState::kFailed;
    state_ = scheduler_.state() == DistributedMutableVectorQueryTcpExecutionState::kCancelled
                 ? DistributedMutableVectorRowsQueryTcpExecutionState::kCancelled
                 : DistributedMutableVectorRowsQueryTcpExecutionState::kFailed;
    return failure_;
  }
  if (scheduler_.state() != DistributedMutableVectorQueryTcpExecutionState::kComplete)
    return common::Status::ok();
  auto input = scheduler_.take_result();
  if (!input.has_value()) {
    failure_ = std::move(input.error());
    state_ = DistributedMutableVectorRowsQueryTcpExecutionState::kFailed;
    return failure_;
  }
  auto finalized = finalize_distributed_vector_rows_v2(std::move(*input), finalization_limits_);
  if (!finalized.has_value()) {
    failure_ = std::move(finalized.error());
    state_ = DistributedMutableVectorRowsQueryTcpExecutionState::kFailed;
    return failure_;
  }
  result_.emplace(std::move(*finalized));
  state_ = DistributedMutableVectorRowsQueryTcpExecutionState::kComplete;
  return common::Status::ok();
}

common::Status DistributedMutableVectorRowsQueryTcpExecution::cancel() {
  if (state_ == DistributedMutableVectorRowsQueryTcpExecutionState::kComplete)
    return common::Status::ok();
  if (state_ == DistributedMutableVectorRowsQueryTcpExecutionState::kFailed ||
      state_ == DistributedMutableVectorRowsQueryTcpExecutionState::kCancelled) {
    return failure_;
  }
  failure_ = scheduler_.cancel();
  state_ = DistributedMutableVectorRowsQueryTcpExecutionState::kCancelled;
  return failure_;
}

common::Status DistributedMutableVectorRowsQueryTcpExecution::rebind(
    std::vector<query::DistributedMutableVectorFragment> fragments,
    DistributedMutableVectorQueryTcpExecutionConfig tcp) {
  if (state_ != DistributedMutableVectorRowsQueryTcpExecutionState::kFailed || !scheduler_failed_) {
    return invalid("mutable vector rows query TCP execution is not eligible for rebinding");
  }
  auto execution = DistributedMutableVectorQueryExecution::create(
      source_node_id_, std::move(fragments), execution_limits_);
  if (!execution.has_value())
    return std::move(execution.error());
  common::Status rebound = scheduler_.rebind(std::move(*execution), std::move(tcp));
  if (!rebound.is_ok())
    return rebound;
  scheduler_failed_ = false;
  state_ = DistributedMutableVectorRowsQueryTcpExecutionState::kRunning;
  return common::Status::ok();
}

DistributedMutableVectorRowsQueryTcpExecutionState
DistributedMutableVectorRowsQueryTcpExecution::state() const noexcept {
  return state_;
}

DistributedMutableVectorQueryTcpExecutionMetrics
DistributedMutableVectorRowsQueryTcpExecution::metrics() const noexcept {
  return scheduler_.metrics();
}

const std::optional<DistributedVectorRowsFinalizedResultV2>&
DistributedMutableVectorRowsQueryTcpExecution::result() const noexcept {
  return result_;
}

common::Result<DistributedVectorRowsFinalizedResultV2>
DistributedMutableVectorRowsQueryTcpExecution::take_result() {
  try {
    if (state_ != DistributedMutableVectorRowsQueryTcpExecutionState::kComplete ||
        !result_.has_value()) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kUnavailable, "mutable vector rows query result is unavailable"});
    }
    DistributedVectorRowsFinalizedResultV2 result = std::move(*result_);
    result_.reset();
    return result;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable vector rows result transfer failed"));
  }
}

const common::Status& DistributedMutableVectorRowsQueryTcpExecution::failure() const noexcept {
  return failure_;
}

common::Result<std::optional<DistributedQueryLeaderHint>>
DistributedMutableVectorRowsQueryTcpExecution::suggested_leader(
    const schema::TabletId& tablet_id) const {
  return scheduler_.suggested_leader(tablet_id);
}

} // namespace chronos::cluster
