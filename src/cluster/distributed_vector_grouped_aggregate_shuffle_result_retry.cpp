#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_retry.hpp"

#include <algorithm>
#include <chrono>
#include <memory>
#include <utility>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

[[nodiscard]] bool retryable_status(const common::StatusCode code) noexcept {
  return code == common::StatusCode::kUnavailable || code == common::StatusCode::kIoError ||
         code == common::StatusCode::kResourceExhausted;
}

[[nodiscard]] bool valid_retry_limits(const DistributedQueryRetryLimits& limits) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      DistributedVectorGroupedAggregateShuffleResultRetry::TimePoint::duration::max());
  return limits.maximum_attempts > 0U && limits.maximum_attempts <= 1024U &&
         limits.initial_backoff.count() > 0 && limits.maximum_backoff >= limits.initial_backoff &&
         limits.maximum_backoff <= maximum;
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleResultRetry::TimePoint
saturating_add(const DistributedVectorGroupedAggregateShuffleResultRetry::TimePoint now,
               const std::chrono::milliseconds delay) noexcept {
  const auto converted = std::chrono::duration_cast<
      DistributedVectorGroupedAggregateShuffleResultRetry::TimePoint::duration>(delay);
  if (now > DistributedVectorGroupedAggregateShuffleResultRetry::TimePoint::max() - converted)
    return DistributedVectorGroupedAggregateShuffleResultRetry::TimePoint::max();
  return now + converted;
}

} // namespace

DistributedVectorGroupedAggregateShuffleResultRetry::
    DistributedVectorGroupedAggregateShuffleResultRetry(
        const DistributedVectorGroupedAggregateShuffleAuthority& authority,
        const query::DistributedVectorResultSchema& result_schema,
        const DistributedVectorGroupedAggregateShuffleResultRoute route,
        std::vector<std::vector<std::byte>> encoded_result_batches,
        const DistributedVectorGroupedAggregateShuffleResultRetryLimits limits) noexcept
    : authority_(authority), result_schema_(result_schema), partition_id_(route.partition_id),
      source_node_id_(route.source_node_id), coordinator_node_id_(route.coordinator_node_id),
      encoded_result_batches_(std::move(encoded_result_batches)), limits_(limits),
      next_backoff_(limits.retry.initial_backoff) {}

common::Result<DistributedVectorGroupedAggregateShuffleResultRetry>
DistributedVectorGroupedAggregateShuffleResultRetry::create(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema,
    const DistributedVectorGroupedAggregateShuffleResultRoute route,
    std::vector<std::vector<std::byte>> encoded_result_batches,
    const DistributedVectorGroupedAggregateShuffleResultRetryLimits limits) {
  if (!valid_retry_limits(limits.retry) ||
      !validate_distributed_vector_grouped_aggregate_shuffle_result_stream_limits(limits.stream)) {
    return common::make_unexpected(invalid("grouped shuffle result retry limits are invalid"));
  }
  auto validated = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
      authority, result_schema, route.partition_id, route.source_node_id, route.coordinator_node_id,
      encoded_result_batches, limits.stream);
  if (!validated.has_value())
    return common::make_unexpected(validated.error());
  return DistributedVectorGroupedAggregateShuffleResultRetry{
      authority, result_schema, route, std::move(encoded_result_batches), limits};
}

common::Result<DistributedVectorGroupedAggregateShuffleResultAttempt>
DistributedVectorGroupedAggregateShuffleResultRetry::begin_attempt(const TimePoint now) {
  if (state_ == DistributedVectorGroupedAggregateShuffleResultRetryState::kSucceeded ||
      state_ == DistributedVectorGroupedAggregateShuffleResultRetryState::kFailed) {
    return common::make_unexpected(invalid("grouped shuffle result retry owner is terminal"));
  }
  if (state_ == DistributedVectorGroupedAggregateShuffleResultRetryState::kAttemptActive)
    return common::make_unexpected(unavailable("grouped shuffle result attempt is active"));
  if (state_ == DistributedVectorGroupedAggregateShuffleResultRetryState::kBackoff &&
      now < *next_attempt_not_before_) {
    return common::make_unexpected(unavailable("grouped shuffle result retry backoff is active"));
  }
  if (attempts_started_ >= limits_.retry.maximum_attempts)
    return common::make_unexpected(invalid("grouped shuffle result retry budget is exhausted"));
  auto stream = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
      authority_.get(), result_schema_.get(), partition_id_, source_node_id_, coordinator_node_id_,
      encoded_result_batches_, limits_.stream);
  if (!stream.has_value())
    return common::make_unexpected(stream.error());
  ++attempts_started_;
  state_ = DistributedVectorGroupedAggregateShuffleResultRetryState::kAttemptActive;
  next_attempt_not_before_.reset();
  return DistributedVectorGroupedAggregateShuffleResultAttempt{.attempt_number = attempts_started_,
                                                               .target_node_id =
                                                                   coordinator_node_id_,
                                                               .stream = std::move(*stream)};
}

common::Status DistributedVectorGroupedAggregateShuffleResultRetry::record_acknowledged() {
  if (state_ != DistributedVectorGroupedAggregateShuffleResultRetryState::kAttemptActive)
    return invalid("grouped shuffle result retry owner has no active attempt");
  state_ = DistributedVectorGroupedAggregateShuffleResultRetryState::kSucceeded;
  last_status_code_ = common::StatusCode::kOk;
  next_attempt_not_before_.reset();
  return common::Status::ok();
}

common::Status DistributedVectorGroupedAggregateShuffleResultRetry::record_attempt_failure(
    const common::StatusCode code, const TimePoint now) {
  if (state_ != DistributedVectorGroupedAggregateShuffleResultRetryState::kAttemptActive)
    return invalid("grouped shuffle result retry owner has no active attempt");
  if (code == common::StatusCode::kOk)
    return invalid("grouped shuffle result attempt failure cannot be OK");
  return schedule(code, now);
}

common::Status
DistributedVectorGroupedAggregateShuffleResultRetry::schedule(const common::StatusCode code,
                                                              const TimePoint now) {
  last_status_code_ = code;
  if (!retryable_status(code) || attempts_started_ >= limits_.retry.maximum_attempts) {
    state_ = DistributedVectorGroupedAggregateShuffleResultRetryState::kFailed;
    next_attempt_not_before_.reset();
    return common::Status::ok();
  }
  state_ = DistributedVectorGroupedAggregateShuffleResultRetryState::kBackoff;
  next_attempt_not_before_ = saturating_add(now, next_backoff_);
  if (next_backoff_ < limits_.retry.maximum_backoff) {
    const auto current = next_backoff_.count();
    const auto maximum = limits_.retry.maximum_backoff.count();
    next_backoff_ = current > maximum / 2
                        ? limits_.retry.maximum_backoff
                        : std::min(next_backoff_ * 2, limits_.retry.maximum_backoff);
  }
  return common::Status::ok();
}

DistributedVectorGroupedAggregateShuffleResultRetryState
DistributedVectorGroupedAggregateShuffleResultRetry::state() const noexcept {
  return state_;
}

std::size_t DistributedVectorGroupedAggregateShuffleResultRetry::attempts_started() const noexcept {
  return attempts_started_;
}

std::optional<DistributedVectorGroupedAggregateShuffleResultRetry::TimePoint>
DistributedVectorGroupedAggregateShuffleResultRetry::next_attempt_not_before() const noexcept {
  return next_attempt_not_before_;
}

std::optional<common::StatusCode>
DistributedVectorGroupedAggregateShuffleResultRetry::last_status_code() const noexcept {
  return last_status_code_;
}

std::uint32_t DistributedVectorGroupedAggregateShuffleResultRetry::partition_id() const noexcept {
  return partition_id_;
}

raft::NodeId DistributedVectorGroupedAggregateShuffleResultRetry::source_node_id() const noexcept {
  return source_node_id_;
}

raft::NodeId DistributedVectorGroupedAggregateShuffleResultRetry::target_node_id() const noexcept {
  return coordinator_node_id_;
}

const DistributedVectorGroupedAggregateShuffleAuthority*
DistributedVectorGroupedAggregateShuffleResultRetry::authority() const noexcept {
  return std::addressof(authority_.get());
}

const query::DistributedVectorResultSchema*
DistributedVectorGroupedAggregateShuffleResultRetry::result_schema() const noexcept {
  return std::addressof(result_schema_.get());
}

} // namespace chronos::cluster
