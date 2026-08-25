#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_shuffle_execution.hpp"

#include <climits>
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

class DistributedMutableVectorGroupedAggregateShuffleExecution::Impl {
public:
  Impl(DistributedVectorGroupedAggregateShuffleAuthority owned_authority,
       std::vector<query::DistributedMutableVectorFragment> owned_fragments,
       DistributedMutableVectorGroupedAggregateQueryTcpExecution owned_workers,
       DistributedVectorGroupedAggregateShuffleQueryExecutionConfig owned_shuffle_config)
      : authority(std::move(owned_authority)), fragments(std::move(owned_fragments)),
        workers(std::move(owned_workers)), shuffle_config(std::move(owned_shuffle_config)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (execution_state ==
            DistributedMutableVectorGroupedAggregateShuffleExecutionState::kCollectingSources ||
        execution_state ==
            DistributedMutableVectorGroupedAggregateShuffleExecutionState::kShuffling) {
      static_cast<void>(workers.cancel());
      if (shuffle.has_value())
        static_cast<void>(shuffle->cancel());
      execution_failure = std::move(status);
      execution_state = DistributedMutableVectorGroupedAggregateShuffleExecutionState::kFailed;
    }
    return execution_failure;
  }

  DistributedVectorGroupedAggregateShuffleAuthority authority;
  std::vector<query::DistributedMutableVectorFragment> fragments;
  DistributedMutableVectorGroupedAggregateQueryTcpExecution workers;
  DistributedVectorGroupedAggregateShuffleQueryExecutionConfig shuffle_config;
  std::optional<DistributedVectorGroupedAggregateShuffleQueryExecution> shuffle;
  DistributedMutableVectorGroupedAggregateShuffleExecutionState execution_state{
      DistributedMutableVectorGroupedAggregateShuffleExecutionState::kCollectingSources};
  common::Status execution_failure{common::StatusCode::kInternal,
                                   "mutable grouped shuffle execution has not failed"};
};

DistributedMutableVectorGroupedAggregateShuffleExecution::
    DistributedMutableVectorGroupedAggregateShuffleExecution() noexcept = default;
DistributedMutableVectorGroupedAggregateShuffleExecution::
    ~DistributedMutableVectorGroupedAggregateShuffleExecution() = default;
DistributedMutableVectorGroupedAggregateShuffleExecution::
    DistributedMutableVectorGroupedAggregateShuffleExecution(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedMutableVectorGroupedAggregateShuffleExecution::
    DistributedMutableVectorGroupedAggregateShuffleExecution(
        DistributedMutableVectorGroupedAggregateShuffleExecution&&) noexcept = default;
DistributedMutableVectorGroupedAggregateShuffleExecution&
DistributedMutableVectorGroupedAggregateShuffleExecution::operator=(
    DistributedMutableVectorGroupedAggregateShuffleExecution&&) noexcept = default;

common::Result<DistributedMutableVectorGroupedAggregateShuffleExecution>
DistributedMutableVectorGroupedAggregateShuffleExecution::create(
    const raft::NodeId source_node_id,
    std::vector<query::DistributedMutableVectorFragment> fragments,
    std::vector<query::VectorGroupKeyDefinition> keys,
    std::vector<query::VectorAggregateDefinition> aggregates,
    DistributedMutableVectorGroupedAggregateShuffleExecutionConfig config) {
  if (config.worker_transport.coordinator_projection.has_value()) {
    return common::make_unexpected(
        invalid("mutable grouped shuffle worker projection belongs to final shuffle"));
  }
  try {
    auto authority =
        DistributedVectorGroupedAggregateShuffleAuthority::create_from_mutable_fragments(
            fragments, keys, aggregates, config.authority);
    if (!authority.has_value())
      return common::make_unexpected(authority.error());
    std::vector<query::DistributedMutableVectorFragment> worker_fragments{fragments};
    auto execution = DistributedMutableVectorGroupedAggregateQueryExecution::create(
        source_node_id, std::move(worker_fragments), std::move(keys), std::move(aggregates),
        config.worker_execution);
    if (!execution.has_value())
      return common::make_unexpected(execution.error());
    config.worker_transport.publication =
        DistributedMutableVectorGroupedAggregateQueryPublication::kCompletedSources;
    auto workers = DistributedMutableVectorGroupedAggregateQueryTcpExecution::create(
        std::move(*execution), std::move(config.worker_transport));
    if (!workers.has_value())
      return common::make_unexpected(workers.error());
    return DistributedMutableVectorGroupedAggregateShuffleExecution{
        std::make_unique<Impl>(std::move(*authority), std::move(fragments), std::move(*workers),
                               std::move(config.shuffle))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable grouped shuffle allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("mutable grouped shuffle exceeds limits"));
  }
}

common::Status DistributedMutableVectorGroupedAggregateShuffleExecution::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return invalid("mutable grouped shuffle execution is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return invalid("mutable grouped shuffle poll timeout is invalid");
  Impl& impl = *implementation_;
  if (impl.execution_state ==
          DistributedMutableVectorGroupedAggregateShuffleExecutionState::kFailed ||
      impl.execution_state ==
          DistributedMutableVectorGroupedAggregateShuffleExecutionState::kCancelled) {
    return impl.execution_failure;
  }
  if (impl.execution_state ==
      DistributedMutableVectorGroupedAggregateShuffleExecutionState::kComplete) {
    return common::Status::ok();
  }
  if (impl.execution_state ==
      DistributedMutableVectorGroupedAggregateShuffleExecutionState::kCollectingSources) {
    const common::Status polled = impl.workers.poll_once(maximum_wait);
    if (!polled.is_ok()) {
      const auto worker_state = impl.workers.state();
      return worker_state ==
                         DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kRunning &&
                     polled.code() == common::StatusCode::kResourceExhausted
                 ? polled
                 : impl.fail(polled);
    }
    if (impl.workers.state() !=
        DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kComplete) {
      return common::Status::ok();
    }
    auto completed = impl.workers.take_completed_sources();
    if (!completed.has_value())
      return impl.fail(completed.error());
    try {
      std::vector<DistributedVectorGroupedAggregateShuffleSourceInput> sources;
      sources.reserve(completed->size());
      for (auto& source : *completed)
        sources.push_back({source.tablet_id, std::move(source.messages)});
      auto shuffle = DistributedVectorGroupedAggregateShuffleQueryExecution::create(
          std::move(impl.authority), std::move(impl.fragments), std::move(sources),
          std::move(impl.shuffle_config));
      if (!shuffle.has_value())
        return impl.fail(shuffle.error());
      impl.shuffle.emplace(std::move(*shuffle));
      impl.execution_state =
          DistributedMutableVectorGroupedAggregateShuffleExecutionState::kShuffling;
    } catch (const std::bad_alloc&) {
      return impl.fail(exhausted("mutable grouped shuffle source handoff allocation failed"));
    } catch (const std::length_error&) {
      return impl.fail(exhausted("mutable grouped shuffle source handoff exceeds limits"));
    }
  }
  if (!impl.shuffle.has_value())
    return impl.fail({common::StatusCode::kInternal, "mutable grouped shuffle phase is absent"});
  const common::Status shuffled = impl.shuffle->poll_once(maximum_wait);
  if (!shuffled.is_ok()) {
    const auto shuffle_state = impl.shuffle->state();
    return shuffle_state == DistributedVectorGroupedAggregateShuffleQueryExecutionState::kRunning &&
                   shuffled.code() == common::StatusCode::kResourceExhausted
               ? shuffled
               : impl.fail(shuffled);
  }
  if (impl.shuffle->state() ==
      DistributedVectorGroupedAggregateShuffleQueryExecutionState::kComplete) {
    impl.execution_state = DistributedMutableVectorGroupedAggregateShuffleExecutionState::kComplete;
  }
  return common::Status::ok();
}

common::Status DistributedMutableVectorGroupedAggregateShuffleExecution::cancel() {
  if (!implementation_)
    return invalid("mutable grouped shuffle execution is empty");
  Impl& impl = *implementation_;
  if (impl.execution_state ==
      DistributedMutableVectorGroupedAggregateShuffleExecutionState::kComplete) {
    return common::Status::ok();
  }
  if (impl.execution_state ==
          DistributedMutableVectorGroupedAggregateShuffleExecutionState::kCollectingSources ||
      impl.execution_state ==
          DistributedMutableVectorGroupedAggregateShuffleExecutionState::kShuffling) {
    static_cast<void>(impl.workers.cancel());
    if (impl.shuffle.has_value())
      static_cast<void>(impl.shuffle->cancel());
    impl.execution_failure = {common::StatusCode::kCancelled,
                              "mutable grouped shuffle execution was cancelled"};
    impl.execution_state =
        DistributedMutableVectorGroupedAggregateShuffleExecutionState::kCancelled;
  }
  return impl.execution_failure;
}

DistributedMutableVectorGroupedAggregateShuffleExecutionState
DistributedMutableVectorGroupedAggregateShuffleExecution::state() const noexcept {
  return implementation_ ? implementation_->execution_state
                         : DistributedMutableVectorGroupedAggregateShuffleExecutionState::kFailed;
}

DistributedMutableVectorGroupedAggregateShuffleExecutionMetrics
DistributedMutableVectorGroupedAggregateShuffleExecution::metrics() const noexcept {
  if (!implementation_)
    return {};
  return {.workers = implementation_->workers.metrics(),
          .shuffle = implementation_->shuffle.has_value()
                         ? implementation_->shuffle->metrics()
                         : DistributedVectorGroupedAggregateShuffleQueryExecutionMetrics{}};
}

const std::optional<DistributedVectorRowsFinalizedResultV2>&
DistributedMutableVectorGroupedAggregateShuffleExecution::result() const noexcept {
  static const std::optional<DistributedVectorRowsFinalizedResultV2> empty;
  return implementation_ && implementation_->shuffle.has_value()
             ? implementation_->shuffle->result()
             : empty;
}

const common::Status&
DistributedMutableVectorGroupedAggregateShuffleExecution::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "mutable grouped shuffle execution is empty"};
  return implementation_ ? implementation_->execution_failure : empty;
}

} // namespace chronos::cluster
