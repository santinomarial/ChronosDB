#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_query_execution.hpp"

#include <algorithm>
#include <climits>
#include <iterator>
#include <map>
#include <new>
#include <set>
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

class DistributedVectorGroupedAggregateShuffleQueryExecution::Impl {
public:
  explicit Impl(DistributedVectorGroupedAggregateShuffleAuthority value,
                DistributedVectorGroupedAggregateShuffleQueryExecutionConfig value_config)
      : authority(std::move(value)), config(std::move(value_config)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (execution_state == DistributedVectorGroupedAggregateShuffleQueryExecutionState::kRunning) {
      if (transport.has_value())
        static_cast<void>(transport->cancel());
      for (auto& destination : destinations)
        static_cast<void>(destination.cancel());
      execution_failure = std::move(status);
      execution_state = DistributedVectorGroupedAggregateShuffleQueryExecutionState::kFailed;
    }
    return execution_failure;
  }

  [[nodiscard]] common::Status publish_if_ready() {
    const bool transport_complete =
        !transport.has_value() ||
        transport->state() == DistributedVectorGroupedAggregateShuffleTcpExecutionState::kComplete;
    std::size_t ready{};
    for (const auto& destination : destinations) {
      const auto destination_state = destination.state();
      if (destination_state ==
              DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kFailed ||
          destination_state ==
              DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kCancelled) {
        return fail(destination.failure());
      }
      if (destination_state ==
              DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kReady ||
          destination_state ==
              DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kComplete) {
        ++ready;
      }
    }
    execution_metrics.ready_destinations = ready;
    if (!transport_complete || ready != destinations.size())
      return common::Status::ok();

    for (auto& destination : destinations) {
      const common::Status sealed = destination.seal_transport();
      if (!sealed.is_ok())
        return fail(sealed);
    }
    auto gathered = DistributedVectorGroupedAggregateShuffleResultExecution::create(
        authority, std::move(destinations), config.maximum_result_working_memory_bytes);
    if (!gathered.has_value())
      return fail(gathered.error());
    result_execution.emplace(std::move(*gathered));
    auto finalized = config.coordinator_projection.has_value()
                         ? finalize_distributed_vector_grouped_aggregate_shuffle_with_projection_v2(
                               *result_execution, *finalization_authority,
                               *config.coordinator_projection, config.finalization)
                         : finalize_distributed_vector_grouped_aggregate_shuffle_v2(
                               *result_execution, *finalization_authority, config.finalization);
    if (!finalized.has_value())
      return fail(finalized.error());
    execution_metrics.result = result_execution->metrics();
    finalized_result.emplace(std::move(*finalized));
    execution_state = DistributedVectorGroupedAggregateShuffleQueryExecutionState::kComplete;
    return common::Status::ok();
  }

  DistributedVectorGroupedAggregateShuffleAuthority authority;
  DistributedVectorGroupedAggregateShuffleQueryExecutionConfig config;
  std::optional<DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2>
      finalization_authority;
  std::vector<DistributedVectorGroupedAggregateShuffleDestinationExecution> destinations;
  std::optional<DistributedVectorGroupedAggregateShuffleTcpExecution> transport;
  std::optional<DistributedVectorGroupedAggregateShuffleResultExecution> result_execution;
  std::optional<DistributedVectorRowsFinalizedResultV2> finalized_result;
  DistributedVectorGroupedAggregateShuffleQueryExecutionMetrics execution_metrics;
  DistributedVectorGroupedAggregateShuffleQueryExecutionState execution_state{
      DistributedVectorGroupedAggregateShuffleQueryExecutionState::kRunning};
  common::Status execution_failure{common::StatusCode::kInternal,
                                   "grouped shuffle query execution has not failed"};
};

DistributedVectorGroupedAggregateShuffleQueryExecution::
    DistributedVectorGroupedAggregateShuffleQueryExecution() noexcept = default;
DistributedVectorGroupedAggregateShuffleQueryExecution::
    ~DistributedVectorGroupedAggregateShuffleQueryExecution() = default;
DistributedVectorGroupedAggregateShuffleQueryExecution::
    DistributedVectorGroupedAggregateShuffleQueryExecution(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateShuffleQueryExecution::
    DistributedVectorGroupedAggregateShuffleQueryExecution(
        DistributedVectorGroupedAggregateShuffleQueryExecution&&) noexcept = default;
DistributedVectorGroupedAggregateShuffleQueryExecution&
DistributedVectorGroupedAggregateShuffleQueryExecution::operator=(
    DistributedVectorGroupedAggregateShuffleQueryExecution&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateShuffleQueryExecution>
DistributedVectorGroupedAggregateShuffleQueryExecution::create(
    DistributedVectorGroupedAggregateShuffleAuthority authority,
    std::vector<query::DistributedMutableVectorFragment> fragments,
    std::vector<DistributedVectorGroupedAggregateShuffleSourceInput> sources,
    DistributedVectorGroupedAggregateShuffleQueryExecutionConfig config) {
  if (config.destinations.empty() || config.maximum_planning_memory_bytes == 0U ||
      config.maximum_planning_memory_bytes >
          query::kMaximumDistributedVectorGroupedPartitionBytes ||
      config.maximum_result_working_memory_bytes == 0U ||
      config.maximum_result_working_memory_bytes >
          kMaximumDistributedVectorGroupedAggregateShuffleResultWorkingMemoryBytes) {
    return common::make_unexpected(invalid("grouped shuffle query configuration is invalid"));
  }
  try {
    auto implementation = std::make_unique<Impl>(std::move(authority), std::move(config));
    auto finalization = DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2::create(
        implementation->authority, fragments);
    if (!finalization.has_value())
      return common::make_unexpected(finalization.error());
    implementation->finalization_authority.emplace(std::move(*finalization));

    std::set<raft::NodeId> expected_destination_nodes;
    for (const auto& destination : implementation->authority.destinations())
      expected_destination_nodes.insert(destination.node_id);
    std::map<raft::NodeId, std::size_t> destination_indexes;
    implementation->destinations.reserve(implementation->config.destinations.size());
    for (auto& destination_config : implementation->config.destinations) {
      if (!expected_destination_nodes.contains(destination_config.local_node_id) ||
          destination_indexes.contains(destination_config.local_node_id)) {
        return common::make_unexpected(
            invalid("grouped shuffle query destination coverage is invalid"));
      }
      auto destination = DistributedVectorGroupedAggregateShuffleDestinationExecution::start(
          implementation->authority, std::move(destination_config));
      if (!destination.has_value())
        return common::make_unexpected(destination.error());
      destination_indexes.emplace(destination->local_node_id(),
                                  implementation->destinations.size());
      implementation->destinations.push_back(std::move(*destination));
    }
    if (destination_indexes.size() != expected_destination_nodes.size()) {
      return common::make_unexpected(
          invalid("grouped shuffle query destination coverage is incomplete"));
    }

    if (sources.size() != implementation->authority.sources().size()) {
      return common::make_unexpected(invalid("grouped shuffle query source coverage is invalid"));
    }
    auto resources =
        query::QueryResourceContext::create(implementation->config.maximum_planning_memory_bytes);
    if (!resources.has_value())
      return common::make_unexpected(resources.error());
    std::set<schema::TabletId> seen_sources;
    std::vector<DistributedVectorGroupedAggregateShuffleRetry> retries;
    for (auto& source : sources) {
      if (!seen_sources.emplace(source.tablet_id).second) {
        return common::make_unexpected(
            invalid("grouped shuffle query source tablet is duplicated"));
      }
      auto plan = DistributedVectorGroupedAggregateShuffleSourcePlan::create(
          implementation->authority, source.tablet_id, source.messages, *resources,
          implementation->config.source_plan);
      if (!plan.has_value())
        return common::make_unexpected(plan.error());
      const auto plan_metrics = plan->metrics();
      implementation->execution_metrics.local_edges += plan_metrics.local_edges;
      implementation->execution_metrics.remote_edges += plan_metrics.remote_edges;
      for (const auto& stream : plan->local_streams()) {
        const auto destination = destination_indexes.find(stream.edge.target_node_id);
        if (destination == destination_indexes.end()) {
          return common::make_unexpected(
              invalid("grouped shuffle local edge has no destination owner"));
        }
        const common::Status accepted =
            implementation->destinations[destination->second].accept_local_stream(stream);
        if (!accepted.is_ok())
          return common::make_unexpected(accepted);
      }
      auto remote = plan->take_remote_retries();
      retries.insert(retries.end(), std::make_move_iterator(remote.begin()),
                     std::make_move_iterator(remote.end()));
    }
    if (seen_sources.size() != implementation->authority.sources().size()) {
      return common::make_unexpected(
          invalid("grouped shuffle query source coverage is incomplete"));
    }
    implementation->execution_metrics.source_tablets = seen_sources.size();
    implementation->execution_metrics.destination_nodes = destination_indexes.size();

    if (retries.empty()) {
      if (implementation->config.transport.has_value()) {
        return common::make_unexpected(
            invalid("grouped shuffle local-only query supplied remote transport"));
      }
    } else {
      if (!implementation->config.transport.has_value()) {
        return common::make_unexpected(
            invalid("grouped shuffle query remote edges have no transport"));
      }
      auto transport = DistributedVectorGroupedAggregateShuffleTcpExecution::create(
          implementation->authority, std::move(retries),
          std::move(*implementation->config.transport));
      if (!transport.has_value())
        return common::make_unexpected(transport.error());
      implementation->transport.emplace(std::move(*transport));
    }
    return DistributedVectorGroupedAggregateShuffleQueryExecution{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("shuffle query alloc"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("shuffle query length"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleQueryExecution::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return invalid("grouped shuffle query execution is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return invalid("grouped shuffle query poll timeout is invalid");
  Impl& impl = *implementation_;
  if (impl.execution_state ==
          DistributedVectorGroupedAggregateShuffleQueryExecutionState::kFailed ||
      impl.execution_state ==
          DistributedVectorGroupedAggregateShuffleQueryExecutionState::kCancelled) {
    return impl.execution_failure;
  }
  if (impl.execution_state ==
      DistributedVectorGroupedAggregateShuffleQueryExecutionState::kComplete) {
    return common::Status::ok();
  }
  for (auto& destination : impl.destinations) {
    const common::Status polled = destination.poll_once(std::chrono::milliseconds{0});
    if (!polled.is_ok())
      return polled.code() == common::StatusCode::kResourceExhausted ? polled : impl.fail(polled);
  }
  if (impl.transport.has_value()) {
    const common::Status polled = impl.transport->poll_once(maximum_wait);
    impl.execution_metrics.transport = impl.transport->metrics();
    if (!polled.is_ok())
      return polled.code() == common::StatusCode::kResourceExhausted ? polled : impl.fail(polled);
  }
  for (auto& destination : impl.destinations) {
    const common::Status polled = destination.poll_once(std::chrono::milliseconds{0});
    if (!polled.is_ok())
      return polled.code() == common::StatusCode::kResourceExhausted ? polled : impl.fail(polled);
  }
  return impl.publish_if_ready();
}

common::Status DistributedVectorGroupedAggregateShuffleQueryExecution::cancel() {
  if (!implementation_)
    return invalid("grouped shuffle query execution is empty");
  Impl& impl = *implementation_;
  if (impl.execution_state ==
      DistributedVectorGroupedAggregateShuffleQueryExecutionState::kComplete) {
    return common::Status::ok();
  }
  if (impl.execution_state ==
      DistributedVectorGroupedAggregateShuffleQueryExecutionState::kRunning) {
    if (impl.transport.has_value())
      static_cast<void>(impl.transport->cancel());
    for (auto& destination : impl.destinations)
      static_cast<void>(destination.cancel());
    impl.execution_failure = {common::StatusCode::kCancelled,
                              "grouped shuffle query execution was cancelled"};
    impl.execution_state = DistributedVectorGroupedAggregateShuffleQueryExecutionState::kCancelled;
  }
  return impl.execution_failure;
}

DistributedVectorGroupedAggregateShuffleQueryExecutionState
DistributedVectorGroupedAggregateShuffleQueryExecution::state() const noexcept {
  return implementation_ ? implementation_->execution_state
                         : DistributedVectorGroupedAggregateShuffleQueryExecutionState::kFailed;
}

DistributedVectorGroupedAggregateShuffleQueryExecutionMetrics
DistributedVectorGroupedAggregateShuffleQueryExecution::metrics() const noexcept {
  return implementation_ ? implementation_->execution_metrics
                         : DistributedVectorGroupedAggregateShuffleQueryExecutionMetrics{};
}

const std::optional<DistributedVectorRowsFinalizedResultV2>&
DistributedVectorGroupedAggregateShuffleQueryExecution::result() const noexcept {
  static const std::optional<DistributedVectorRowsFinalizedResultV2> empty;
  return implementation_ ? implementation_->finalized_result : empty;
}

common::Result<DistributedVectorRowsFinalizedResultV2>
DistributedVectorGroupedAggregateShuffleQueryExecution::take_result() {
  if (!implementation_ ||
      implementation_->execution_state !=
          DistributedVectorGroupedAggregateShuffleQueryExecutionState::kComplete ||
      !implementation_->finalized_result.has_value()) {
    return common::make_unexpected(common::Status{common::StatusCode::kUnavailable,
                                                  "grouped shuffle query result is unavailable"});
  }
  auto result = std::move(*implementation_->finalized_result);
  implementation_->finalized_result.reset();
  return result;
}

const common::Status&
DistributedVectorGroupedAggregateShuffleQueryExecution::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "grouped shuffle query execution is empty"};
  return implementation_ ? implementation_->execution_failure : empty;
}

} // namespace chronos::cluster
