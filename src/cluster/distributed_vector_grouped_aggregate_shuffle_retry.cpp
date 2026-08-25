#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_retry.hpp"

#include <algorithm>
#include <chrono>
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
      DistributedVectorGroupedAggregateShuffleRetry::TimePoint::duration::max());
  return limits.maximum_attempts > 0U && limits.maximum_attempts <= 1024U &&
         limits.initial_backoff.count() > 0 && limits.maximum_backoff >= limits.initial_backoff &&
         limits.maximum_backoff <= maximum;
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleRetry::TimePoint
saturating_add(const DistributedVectorGroupedAggregateShuffleRetry::TimePoint now,
               const std::chrono::milliseconds delay) noexcept {
  const auto converted = std::chrono::duration_cast<
      DistributedVectorGroupedAggregateShuffleRetry::TimePoint::duration>(delay);
  if (now > DistributedVectorGroupedAggregateShuffleRetry::TimePoint::max() - converted)
    return DistributedVectorGroupedAggregateShuffleRetry::TimePoint::max();
  return now + converted;
}

} // namespace

DistributedVectorGroupedAggregateShuffleRetry::DistributedVectorGroupedAggregateShuffleRetry(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    DistributedVectorGroupedAggregateShuffleEdge edge,
    std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> messages,
    query::QueryResourceContext resources,
    const DistributedVectorGroupedAggregateShuffleRetryLimits limits) noexcept
    : authority_(authority), edge_(edge), messages_(std::move(messages)),
      resources_(std::move(resources)), limits_(limits),
      next_backoff_(limits.retry.initial_backoff) {}

common::Result<DistributedVectorGroupedAggregateShuffleRetry>
DistributedVectorGroupedAggregateShuffleRetry::create(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    DistributedVectorGroupedAggregateShuffleEdge edge,
    std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> messages,
    query::QueryResourceContext resources,
    const DistributedVectorGroupedAggregateShuffleRetryLimits limits) {
  if (!valid_retry_limits(limits.retry) ||
      !validate_distributed_vector_grouped_aggregate_shuffle_stream_limits(limits.stream)) {
    return common::make_unexpected(invalid("grouped shuffle retry limits are invalid"));
  }
  auto validated = DistributedVectorGroupedAggregateShuffleStreamSender::create(
      authority, edge, messages, resources, limits.stream);
  if (!validated.has_value())
    return common::make_unexpected(validated.error());
  return DistributedVectorGroupedAggregateShuffleRetry{authority, edge, std::move(messages),
                                                       std::move(resources), limits};
}

common::Result<DistributedVectorGroupedAggregateShuffleAttempt>
DistributedVectorGroupedAggregateShuffleRetry::begin_attempt(const TimePoint now) {
  if (state_ == DistributedVectorGroupedAggregateShuffleRetryState::kSucceeded ||
      state_ == DistributedVectorGroupedAggregateShuffleRetryState::kFailed) {
    return common::make_unexpected(invalid("grouped shuffle retry owner is terminal"));
  }
  if (state_ == DistributedVectorGroupedAggregateShuffleRetryState::kAttemptActive) {
    return common::make_unexpected(unavailable("grouped shuffle attempt is active"));
  }
  if (state_ == DistributedVectorGroupedAggregateShuffleRetryState::kBackoff &&
      now < *next_attempt_not_before_) {
    return common::make_unexpected(unavailable("grouped shuffle retry backoff is active"));
  }
  if (attempts_started_ >= limits_.retry.maximum_attempts) {
    return common::make_unexpected(invalid("grouped shuffle retry budget is exhausted"));
  }
  auto stream = DistributedVectorGroupedAggregateShuffleStreamSender::create(
      authority_.get(), edge_, messages_, resources_, limits_.stream);
  if (!stream.has_value())
    return common::make_unexpected(stream.error());
  ++attempts_started_;
  state_ = DistributedVectorGroupedAggregateShuffleRetryState::kAttemptActive;
  next_attempt_not_before_.reset();
  return DistributedVectorGroupedAggregateShuffleAttempt{.attempt_number = attempts_started_,
                                                         .target_node_id = edge_.target_node_id,
                                                         .stream = std::move(*stream)};
}

common::Status DistributedVectorGroupedAggregateShuffleRetry::record_acknowledged() {
  if (state_ != DistributedVectorGroupedAggregateShuffleRetryState::kAttemptActive)
    return invalid("grouped shuffle retry owner has no active attempt");
  state_ = DistributedVectorGroupedAggregateShuffleRetryState::kSucceeded;
  last_status_code_ = common::StatusCode::kOk;
  next_attempt_not_before_.reset();
  return common::Status::ok();
}

common::Status
DistributedVectorGroupedAggregateShuffleRetry::record_attempt_failure(const common::StatusCode code,
                                                                      const TimePoint now) {
  if (state_ != DistributedVectorGroupedAggregateShuffleRetryState::kAttemptActive)
    return invalid("grouped shuffle retry owner has no active attempt");
  if (code == common::StatusCode::kOk)
    return invalid("grouped shuffle attempt failure cannot be OK");
  return schedule(code, now);
}

common::Status
DistributedVectorGroupedAggregateShuffleRetry::schedule(const common::StatusCode code,
                                                        const TimePoint now) {
  last_status_code_ = code;
  if (!retryable_status(code) || attempts_started_ >= limits_.retry.maximum_attempts) {
    state_ = DistributedVectorGroupedAggregateShuffleRetryState::kFailed;
    next_attempt_not_before_.reset();
    return common::Status::ok();
  }
  state_ = DistributedVectorGroupedAggregateShuffleRetryState::kBackoff;
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

DistributedVectorGroupedAggregateShuffleRetryState
DistributedVectorGroupedAggregateShuffleRetry::state() const noexcept {
  return state_;
}

std::size_t DistributedVectorGroupedAggregateShuffleRetry::attempts_started() const noexcept {
  return attempts_started_;
}

std::optional<DistributedVectorGroupedAggregateShuffleRetry::TimePoint>
DistributedVectorGroupedAggregateShuffleRetry::next_attempt_not_before() const noexcept {
  return next_attempt_not_before_;
}

std::optional<common::StatusCode>
DistributedVectorGroupedAggregateShuffleRetry::last_status_code() const noexcept {
  return last_status_code_;
}

const DistributedVectorGroupedAggregateShuffleEdge&
DistributedVectorGroupedAggregateShuffleRetry::edge() const noexcept {
  return edge_;
}

const DistributedVectorGroupedAggregateShuffleAuthority&
DistributedVectorGroupedAggregateShuffleRetry::authority() const noexcept {
  return authority_.get();
}

} // namespace chronos::cluster
