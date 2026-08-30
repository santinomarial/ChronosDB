#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_shuffle_job_execution.hpp"

#include <climits>
#include <new>
#include <optional>
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

class DistributedMutableVectorGroupedAggregateShuffleJobExecution::Impl {
public:
  explicit Impl(DistributedVectorGroupedAggregateShuffleAuthority owned_authority)
      : authority(std::move(owned_authority)) {}

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (execution_state !=
            DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kFailed &&
        execution_state !=
            DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kCancelled) {
      if (workers.has_value())
        static_cast<void>(workers->cancel());
      if (reducers.has_value())
        static_cast<void>(reducers->cancel());
      execution_failure = std::move(failure);
      execution_state = DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kFailed;
    }
    return execution_failure;
  }

  DistributedVectorGroupedAggregateShuffleAuthority authority;
  std::optional<DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2> finalization;
  std::optional<DistributedMutableVectorGroupedAggregateQueryTcpExecution> workers;
  std::optional<DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution> reducers;
  DistributedMutableVectorGroupedAggregateShuffleJobExecutionState execution_state{
      DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kPreparingReducers};
  common::Status execution_failure{common::StatusCode::kInternal,
                                   "mutable grouped shuffle job execution has not failed"};
};

DistributedMutableVectorGroupedAggregateShuffleJobExecution::
    DistributedMutableVectorGroupedAggregateShuffleJobExecution() noexcept = default;
DistributedMutableVectorGroupedAggregateShuffleJobExecution::
    ~DistributedMutableVectorGroupedAggregateShuffleJobExecution() = default;
DistributedMutableVectorGroupedAggregateShuffleJobExecution::
    DistributedMutableVectorGroupedAggregateShuffleJobExecution(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedMutableVectorGroupedAggregateShuffleJobExecution::
    DistributedMutableVectorGroupedAggregateShuffleJobExecution(
        DistributedMutableVectorGroupedAggregateShuffleJobExecution&&) noexcept = default;
DistributedMutableVectorGroupedAggregateShuffleJobExecution&
DistributedMutableVectorGroupedAggregateShuffleJobExecution::operator=(
    DistributedMutableVectorGroupedAggregateShuffleJobExecution&&) noexcept = default;

common::Result<DistributedMutableVectorGroupedAggregateShuffleJobExecution>
DistributedMutableVectorGroupedAggregateShuffleJobExecution::create(
    const raft::NodeId source_node_id,
    std::vector<query::DistributedMutableVectorFragment> fragments,
    std::vector<query::VectorGroupKeyDefinition> keys,
    std::vector<query::VectorAggregateDefinition> aggregates,
    DistributedMutableVectorGroupedAggregateShuffleJobExecutionConfig config) {
  if (config.worker_transport.coordinator_projection.has_value() ||
      config.reducers.coordinator_node_id != source_node_id) {
    return common::make_unexpected(invalid("mutable grouped shuffle job configuration is invalid"));
  }
  try {
    auto authority =
        DistributedVectorGroupedAggregateShuffleAuthority::create_from_mutable_fragments(
            fragments, keys, aggregates, config.authority);
    if (!authority.has_value())
      return common::make_unexpected(authority.error());
    auto implementation = std::make_unique<Impl>(std::move(*authority));
    auto finalization = DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2::create(
        implementation->authority, fragments);
    if (!finalization.has_value())
      return common::make_unexpected(finalization.error());
    implementation->finalization.emplace(std::move(*finalization));
    auto worker_execution = DistributedMutableVectorGroupedAggregateQueryExecution::create(
        source_node_id, std::move(fragments), std::move(keys), std::move(aggregates),
        config.worker_execution);
    if (!worker_execution.has_value())
      return common::make_unexpected(worker_execution.error());
    config.worker_transport.publication =
        DistributedMutableVectorGroupedAggregateQueryPublication::kCompletedSources;
    auto workers = DistributedMutableVectorGroupedAggregateQueryTcpExecution::create(
        std::move(*worker_execution), std::move(config.worker_transport));
    if (!workers.has_value())
      return common::make_unexpected(workers.error());
    implementation->workers.emplace(std::move(*workers));
    auto reducers = DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::create(
        implementation->authority, *implementation->finalization, std::move(config.reducers));
    if (!reducers.has_value())
      return common::make_unexpected(reducers.error());
    implementation->reducers.emplace(std::move(*reducers));
    return DistributedMutableVectorGroupedAggregateShuffleJobExecution{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable grouped shuffle job allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("mutable grouped shuffle job exceeds limits"));
  }
}

common::Status DistributedMutableVectorGroupedAggregateShuffleJobExecution::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return invalid("mutable grouped shuffle job execution is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return invalid("mutable grouped shuffle job poll timeout is invalid");
  Impl& impl = *implementation_;
  if (impl.execution_state ==
          DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kFailed ||
      impl.execution_state ==
          DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kCancelled)
    return impl.execution_failure;
  if (impl.execution_state ==
          DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kComplete ||
      impl.execution_state ==
          DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kResultTaken)
    return common::Status::ok();

  if (impl.execution_state ==
      DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kPreparingReducers) {
    common::Status progress = impl.reducers->poll_once(maximum_wait);
    if (!progress.is_ok())
      return impl.fail(std::move(progress));
    if (impl.reducers->state() ==
        DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kPrepared) {
      impl.execution_state =
          DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kCollectingSources;
    }
    return common::Status::ok();
  }

  if (impl.execution_state ==
      DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kCollectingSources) {
    common::Status progress = impl.workers->poll_once(maximum_wait);
    if (!progress.is_ok()) {
      const auto worker_state = impl.workers->state();
      if (worker_state ==
              DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kRunning &&
          progress.code() == common::StatusCode::kResourceExhausted) {
        return progress;
      }
      return impl.fail(std::move(progress));
    }
    if (impl.workers->state() !=
        DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kComplete) {
      return common::Status::ok();
    }
    auto completed = impl.workers->take_completed_sources();
    if (!completed.has_value())
      return impl.fail(completed.error());
    completed->clear();
    const common::Status sealed = impl.reducers->seal();
    if (!sealed.is_ok())
      return impl.fail(sealed);
    impl.execution_state =
        DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kSealingReducers;
  }

  const common::Status progress = impl.reducers->poll_once(maximum_wait);
  if (!progress.is_ok())
    return impl.fail(progress);
  const auto reducer_state = impl.reducers->state();
  if (reducer_state ==
      DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCollectingResults) {
    impl.execution_state =
        DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kCollectingResults;
  } else if (reducer_state ==
             DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kComplete) {
    impl.execution_state =
        DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kComplete;
  }
  return common::Status::ok();
}

common::Status DistributedMutableVectorGroupedAggregateShuffleJobExecution::cancel() {
  if (!implementation_)
    return invalid("mutable grouped shuffle job execution is empty");
  Impl& impl = *implementation_;
  if (impl.execution_state ==
          DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kComplete ||
      impl.execution_state ==
          DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kResultTaken)
    return common::Status::ok();
  if (impl.execution_state !=
          DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kFailed &&
      impl.execution_state !=
          DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kCancelled) {
    static_cast<void>(impl.workers->cancel());
    static_cast<void>(impl.reducers->cancel());
    impl.execution_failure = {common::StatusCode::kCancelled,
                              "mutable grouped shuffle job execution was cancelled"};
    impl.execution_state =
        DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kCancelled;
  }
  return impl.execution_failure;
}

common::Result<DistributedVectorRowsFinalizedResultV2>
DistributedMutableVectorGroupedAggregateShuffleJobExecution::take_result() {
  if (!implementation_ ||
      implementation_->execution_state !=
          DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kComplete) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kUnavailable, "mutable grouped shuffle job result is unavailable"});
  }
  auto result = implementation_->reducers->take_result();
  if (result.has_value())
    implementation_->execution_state =
        DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kResultTaken;
  return result;
}

DistributedMutableVectorGroupedAggregateShuffleJobExecutionState
DistributedMutableVectorGroupedAggregateShuffleJobExecution::state() const noexcept {
  return implementation_
             ? implementation_->execution_state
             : DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kFailed;
}

DistributedMutableVectorGroupedAggregateShuffleJobExecutionMetrics
DistributedMutableVectorGroupedAggregateShuffleJobExecution::metrics() const noexcept {
  return implementation_
             ? DistributedMutableVectorGroupedAggregateShuffleJobExecutionMetrics{.workers =
                                                                                      implementation_
                                                                                          ->workers
                                                                                          ->metrics(),
                                                                                  .reducers =
                                                                                      implementation_
                                                                                          ->reducers
                                                                                          ->metrics()}
             : DistributedMutableVectorGroupedAggregateShuffleJobExecutionMetrics{};
}

const common::Status&
DistributedMutableVectorGroupedAggregateShuffleJobExecution::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "mutable grouped shuffle job execution is empty"};
  return implementation_ ? implementation_->execution_failure : empty;
}

} // namespace chronos::cluster
