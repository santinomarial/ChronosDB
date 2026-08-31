#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_coordinator_execution.hpp"

#include <algorithm>
#include <chrono>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

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

template <typename Value>
[[nodiscard]] Value* optional_pointer(std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
}

} // namespace

class DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::Impl {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  Impl(
      const DistributedVectorGroupedAggregateShuffleAuthority& authority,
      const DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2& finalization_authority,
      const query::DistributedVectorGroupedAggregateCoordinatorProjection* projection,
      DistributedVectorGroupedAggregateShuffleResultCollector collector,
      DistributedVectorGroupedAggregateShuffleResultTcpServer server,
      DistributedVectorGroupedAggregateShuffleCollectedResultExecutionLimits materialization,
      DistributedVectorGroupedAggregateFinalizationLimitsV2 finalization,
      const std::size_t maximum_collected_encoded_bytes,
      const std::optional<TimePoint> execution_deadline)
      : authority_(authority), finalization_authority_(finalization_authority),
        projection_(projection), collector_(std::move(collector)), server_(std::move(server)),
        materialization_limits_(materialization), finalization_limits_(finalization),
        maximum_collected_encoded_bytes_(maximum_collected_encoded_bytes),
        execution_deadline_(execution_deadline) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ ==
        DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kRunning) {
      static_cast<void>(server_.shutdown());
      pending_stream_.reset();
      local_streams_.clear();
      ready_streams_.reset();
      result_.reset();
      failure_ = std::move(status);
      state_ = DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kFailed;
    }
    return failure_;
  }

  [[nodiscard]] common::Status cancel(common::Status status) {
    if (state_ ==
            DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kComplete ||
        state_ ==
            DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kResultTaken) {
      return common::Status::ok();
    }
    if (state_ ==
        DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kRunning) {
      static_cast<void>(server_.shutdown());
      pending_stream_.reset();
      local_streams_.clear();
      ready_streams_.reset();
      result_.reset();
      failure_ = std::move(status);
      state_ = DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kCancelled;
    }
    return failure_;
  }

  [[nodiscard]] bool deadline_expired(const TimePoint now) const noexcept {
    return execution_deadline_.has_value() && now >= *execution_deadline_;
  }

  [[nodiscard]] std::chrono::milliseconds bounded_wait(const std::chrono::milliseconds maximum_wait,
                                                       const TimePoint now) const noexcept {
    if (!execution_deadline_.has_value())
      return maximum_wait;
    if (*execution_deadline_ <= now)
      return std::chrono::milliseconds{0};
    return std::min(maximum_wait, std::chrono::duration_cast<std::chrono::milliseconds>(
                                      *execution_deadline_ - now));
  }

  [[nodiscard]] common::Status admit_pending() {
    if (!pending_stream_.has_value())
      return common::Status::ok();
    const auto& stream = *pending_stream_;
    const auto collector_metrics = collector_.metrics();
    if (!collector_.contains_partition(stream.partition_id) &&
        stream.encoded_bytes >
            maximum_collected_encoded_bytes_ - collector_metrics.retained_encoded_bytes) {
      return fail(exhausted("grouped shuffle result collection byte limit is exhausted"));
    }
    const common::Status accepted = collector_.accept_stream_preserving(*pending_stream_);
    if (!accepted.is_ok()) {
      return accepted.code() == common::StatusCode::kResourceExhausted ? accepted : fail(accepted);
    }
    pending_stream_.reset();
    return common::Status::ok();
  }

  [[nodiscard]] common::Status drain_server() {
    common::Status admitted = admit_pending();
    if (!admitted.is_ok())
      return admitted;
    while (!collector_.ready() && server_.metrics().retained_streams > 0U) {
      auto stream = server_.take_next_complete_stream();
      if (!stream.has_value())
        return fail(stream.error());
      pending_stream_.emplace(std::move(*stream));
      admitted = admit_pending();
      if (!admitted.is_ok())
        return admitted;
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status drain_local() {
    common::Status admitted = admit_pending();
    if (!admitted.is_ok())
      return admitted;
    while (!collector_.ready() && local_stream_index_ < local_streams_.size()) {
      pending_stream_.emplace(std::move(local_streams_[local_stream_index_++]));
      admitted = admit_pending();
      if (!admitted.is_ok())
        return admitted;
    }
    if (local_stream_index_ == local_streams_.size()) {
      local_streams_.clear();
      local_stream_index_ = 0U;
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status finalize_if_ready() {
    if (!collector_.ready())
      return common::Status::ok();
    if (!ready_streams_.has_value()) {
      auto streams = collector_.take_complete_streams();
      if (!streams.has_value())
        return streams.error().code() == common::StatusCode::kResourceExhausted
                   ? streams.error()
                   : fail(streams.error());
      ready_streams_.emplace(std::move(*streams));
    }
    auto materialized =
        DistributedVectorGroupedAggregateShuffleCollectedResultExecution::create_preserving(
            authority_.get(), finalization_authority_.get().result_schema(), *ready_streams_,
            materialization_limits_);
    if (!materialized.has_value()) {
      return materialized.error().code() == common::StatusCode::kResourceExhausted
                 ? materialized.error()
                 : fail(materialized.error());
    }
    ready_streams_.reset();
    const common::Status shutdown = server_.shutdown();
    if (!shutdown.is_ok())
      return fail(shutdown);
    if (deadline_expired(TimePoint::clock::now())) {
      return cancel(
          {common::StatusCode::kCancelled, "grouped shuffle result coordinator deadline expired"});
    }
    if (metrics_.finalization_attempts != std::numeric_limits<std::uint64_t>::max())
      ++metrics_.finalization_attempts;
    common::Result<DistributedVectorRowsFinalizedResultV2> finalized =
        projection_ == nullptr
            ? finalize_distributed_vector_grouped_aggregate_shuffle_v2(
                  *materialized, finalization_authority_.get(), finalization_limits_)
            : finalize_distributed_vector_grouped_aggregate_shuffle_with_projection_v2(
                  *materialized, finalization_authority_.get(), *projection_, finalization_limits_);
    if (!finalized.has_value())
      return fail(finalized.error());
    if (deadline_expired(TimePoint::clock::now())) {
      return cancel(
          {common::StatusCode::kCancelled, "grouped shuffle result coordinator deadline expired"});
    }
    metrics_.finalized_rows = finalized->row_count;
    metrics_.finalized_encoded_bytes = finalized->encoded_bytes;
    result_.emplace(std::move(*finalized));
    state_ = DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kComplete;
    return common::Status::ok();
  }

  std::reference_wrapper<const DistributedVectorGroupedAggregateShuffleAuthority> authority_;
  std::reference_wrapper<const DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2>
      finalization_authority_;
  const query::DistributedVectorGroupedAggregateCoordinatorProjection* projection_{};
  DistributedVectorGroupedAggregateShuffleResultCollector collector_;
  DistributedVectorGroupedAggregateShuffleResultTcpServer server_;
  DistributedVectorGroupedAggregateShuffleCollectedResultExecutionLimits materialization_limits_;
  DistributedVectorGroupedAggregateFinalizationLimitsV2 finalization_limits_;
  std::size_t maximum_collected_encoded_bytes_{};
  std::optional<TimePoint> execution_deadline_;
  std::optional<DistributedVectorGroupedAggregateShuffleCompleteResultStream> pending_stream_;
  std::vector<DistributedVectorGroupedAggregateShuffleCompleteResultStream> local_streams_;
  std::size_t local_stream_index_{};
  std::optional<std::vector<DistributedVectorGroupedAggregateShuffleCompleteResultStream>>
      ready_streams_;
  std::optional<DistributedVectorRowsFinalizedResultV2> result_;
  DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionMetrics metrics_;
  DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState state_{
      DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kRunning};
  common::Status failure_{common::StatusCode::kInternal,
                          "grouped shuffle result coordinator has not failed"};
};

DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::
    DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution() noexcept = default;
DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::
    DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::
    ~DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution() = default;
DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::
    DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution(
        DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution&&) noexcept = default;
DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution&
DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::operator=(
    DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution>
DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::create(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2& finalization_authority,
    DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionConfig config) {
  try {
    if (std::addressof(finalization_authority.shuffle_authority()) != std::addressof(authority)) {
      return common::make_unexpected(
          invalid("grouped shuffle result coordinator finalization authority differs"));
    }
    const DistributedVectorGroupedAggregateShuffleCollectedResultExecutionLimits materialization{
        .stream = config.carrier_limits.stream,
        .maximum_batch_working_bytes = config.maximum_batch_working_bytes,
        .maximum_working_memory_bytes = config.maximum_working_memory_bytes};
    const common::Status materialization_status =
        validate_distributed_vector_grouped_aggregate_shuffle_collected_result_execution_limits(
            materialization);
    const common::Status finalization_status =
        validate_distributed_vector_grouped_aggregate_finalization_limits_v2(
            config.finalization_limits);
    if (!materialization_status.is_ok())
      return common::make_unexpected(materialization_status);
    if (!finalization_status.is_ok())
      return common::make_unexpected(finalization_status);

    auto collector = DistributedVectorGroupedAggregateShuffleResultCollector::create(
        authority, finalization_authority.result_schema(), config.coordinator_node_id,
        {.stream = config.carrier_limits.stream,
         .maximum_total_encoded_bytes = config.maximum_collected_encoded_bytes});
    if (!collector.has_value())
      return common::make_unexpected(collector.error());
    auto server = DistributedVectorGroupedAggregateShuffleResultTcpServer::start(
        {.listener = config.listener,
         .tls = std::move(config.tls),
         .authenticator = config.authenticator,
         .node_authorizer = config.node_authorizer,
         .authority = &authority,
         .result_schema = &finalization_authority.result_schema(),
         .coordinator_node_id = config.coordinator_node_id,
         .carrier_limits = config.carrier_limits,
         .maximum_retained_streams = config.maximum_retained_server_streams,
         .maximum_accepts_per_poll = config.maximum_accepts_per_poll});
    if (!server.has_value())
      return common::make_unexpected(server.error());
    return DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution{
        std::make_unique<Impl>(authority, finalization_authority, config.projection,
                               std::move(*collector), std::move(*server), materialization,
                               config.finalization_limits, config.maximum_collected_encoded_bytes,
                               config.execution_deadline)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped shuffle result coordinator allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle result coordinator exceeds limits"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return invalid("grouped shuffle result coordinator is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return invalid("grouped shuffle result coordinator poll timeout is invalid");
  Impl& impl = *implementation_;
  if (impl.state_ ==
          DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kFailed ||
      impl.state_ ==
          DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kCancelled) {
    return impl.failure_;
  }
  if (impl.state_ !=
      DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kRunning) {
    return common::Status::ok();
  }
  auto now = std::chrono::steady_clock::now();
  if (impl.deadline_expired(now)) {
    return impl.cancel(
        {common::StatusCode::kCancelled, "grouped shuffle result coordinator deadline expired"});
  }
  if (impl.metrics_.polls != std::numeric_limits<std::uint64_t>::max())
    ++impl.metrics_.polls;
  const common::Status driven = impl.server_.poll_once(impl.bounded_wait(maximum_wait, now));
  if (!driven.is_ok())
    return impl.fail(driven);
  now = std::chrono::steady_clock::now();
  if (impl.deadline_expired(now)) {
    return impl.cancel(
        {common::StatusCode::kCancelled, "grouped shuffle result coordinator deadline expired"});
  }
  common::Status drained = impl.drain_local();
  if (!drained.is_ok())
    return drained;
  drained = impl.drain_server();
  if (!drained.is_ok())
    return drained;
  return impl.finalize_if_ready();
}

common::Status
DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::accept_local_streams(
    std::vector<DistributedVectorGroupedAggregateShuffleCompleteResultStream> streams) {
  if (!implementation_)
    return invalid("grouped shuffle result coordinator is empty");
  Impl& impl = *implementation_;
  if (impl.state_ !=
          DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kRunning ||
      !impl.local_streams_.empty() || impl.local_stream_index_ != 0U || streams.empty()) {
    return invalid("grouped shuffle local result handoff is invalid");
  }
  impl.local_streams_ = std::move(streams);
  const common::Status drained = impl.drain_local();
  return drained.code() == common::StatusCode::kResourceExhausted ? common::Status::ok() : drained;
}

common::Status DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::cancel() {
  if (!implementation_)
    return invalid("grouped shuffle result coordinator is empty");
  return implementation_->cancel(
      {common::StatusCode::kCancelled, "grouped shuffle result coordinator was cancelled"});
}

common::Result<DistributedVectorRowsFinalizedResultV2>
DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::take_result() {
  if (!implementation_ ||
      implementation_->state_ !=
          DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kComplete) {
    return common::make_unexpected(
        unavailable("grouped shuffle coordinator finalized result is unavailable"));
  }
  auto* finalized = optional_pointer(implementation_->result_);
  if (finalized == nullptr) {
    return common::make_unexpected(
        unavailable("grouped shuffle coordinator finalized result is unavailable"));
  }
  auto result = std::move(*finalized);
  implementation_->result_.reset();
  implementation_->state_ =
      DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kResultTaken;
  return result;
}

network::Ipv4Endpoint
DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::bound_endpoint()
    const noexcept {
  return implementation_ ? implementation_->server_.bound_endpoint() : network::Ipv4Endpoint{};
}

DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState
DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::state() const noexcept {
  return implementation_
             ? implementation_->state_
             : DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kFailed;
}

DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionMetrics
DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::metrics() const noexcept {
  if (!implementation_)
    return {};
  auto metrics = implementation_->metrics_;
  metrics.server = implementation_->server_.metrics();
  metrics.collector = implementation_->collector_.metrics();
  return metrics;
}

const common::Status&
DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "grouped shuffle result coordinator is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::cluster
