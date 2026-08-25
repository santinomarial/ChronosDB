#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_destination_execution.hpp"

#include <climits>
#include <cstddef>
#include <functional>
#include <limits>
#include <map>
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

void increment_saturated(std::uint64_t& value) noexcept {
  if (value != std::numeric_limits<std::uint64_t>::max())
    ++value;
}

} // namespace

class DistributedVectorGroupedAggregateShuffleDestinationExecution::Impl {
public:
  Impl(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
       DistributedVectorGroupedAggregateShuffleDestinationExecutionConfig config,
       std::vector<DistributedVectorGroupedAggregateShuffleReducer> reducers,
       std::map<std::uint32_t, std::size_t> reducer_indexes, std::vector<bool> output_complete,
       std::optional<DistributedVectorGroupedAggregateShuffleTcpServer> server)
      : authority_(authority), config_(std::move(config)), reducers_(std::move(reducers)),
        reducer_indexes_(std::move(reducer_indexes)), output_complete_(std::move(output_complete)),
        server_(std::move(server)) {
    metrics_.local_partitions = reducers_.size();
  }

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ == DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kReceiving ||
        state_ == DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kReady) {
      if (server_.has_value())
        static_cast<void>(server_->shutdown());
      pending_remote_stream_.reset();
      metrics_.pending_remote_streams = 0U;
      failure_ = std::move(status);
      state_ = DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kFailed;
    }
    return failure_;
  }

  [[nodiscard]] common::Status
  admit(const DistributedVectorGroupedAggregateShuffleCompleteStream& stream, const bool remote) {
    const auto reducer = reducer_indexes_.find(stream.edge.partition_id);
    if (reducer == reducer_indexes_.end())
      return fail(invalid("grouped shuffle stream targets no reducer on this node"));
    common::Status accepted = reducers_[reducer->second].accept_stream(stream);
    if (!accepted.is_ok()) {
      return accepted.code() == common::StatusCode::kResourceExhausted ? accepted : fail(accepted);
    }
    if (remote)
      increment_saturated(metrics_.remote_stream_deliveries);
    else
      increment_saturated(metrics_.local_stream_deliveries);
    return common::Status::ok();
  }

  [[nodiscard]] common::Status finish_ready_reducers() {
    std::size_t ready{};
    for (auto& reducer : reducers_) {
      if (!reducer.ready() &&
          reducer.metrics().accepted_sources == authority_.get().sources().size()) {
        common::Status finished = reducer.finish();
        if (!finished.is_ok()) {
          return finished.code() == common::StatusCode::kResourceExhausted ? finished
                                                                           : fail(finished);
        }
      }
      if (reducer.ready())
        ++ready;
    }
    metrics_.ready_partitions = ready;
    if (ready == reducers_.size()) {
      state_ = server_.has_value()
                   ? DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kReady
                   : DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kComplete;
    }
    return common::Status::ok();
  }

  std::reference_wrapper<const DistributedVectorGroupedAggregateShuffleAuthority> authority_;
  DistributedVectorGroupedAggregateShuffleDestinationExecutionConfig config_;
  std::vector<DistributedVectorGroupedAggregateShuffleReducer> reducers_;
  std::map<std::uint32_t, std::size_t> reducer_indexes_;
  std::vector<bool> output_complete_;
  std::optional<DistributedVectorGroupedAggregateShuffleTcpServer> server_;
  std::optional<DistributedVectorGroupedAggregateShuffleCompleteStream> pending_remote_stream_;
  DistributedVectorGroupedAggregateShuffleDestinationExecutionMetrics metrics_;
  DistributedVectorGroupedAggregateShuffleDestinationExecutionState state_{
      DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kReceiving};
  common::Status failure_{common::StatusCode::kInternal,
                          "grouped shuffle destination execution has not failed"};
};

DistributedVectorGroupedAggregateShuffleDestinationExecution::
    DistributedVectorGroupedAggregateShuffleDestinationExecution() noexcept = default;
DistributedVectorGroupedAggregateShuffleDestinationExecution::
    ~DistributedVectorGroupedAggregateShuffleDestinationExecution() = default;
DistributedVectorGroupedAggregateShuffleDestinationExecution::
    DistributedVectorGroupedAggregateShuffleDestinationExecution(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateShuffleDestinationExecution::
    DistributedVectorGroupedAggregateShuffleDestinationExecution(
        DistributedVectorGroupedAggregateShuffleDestinationExecution&&) noexcept = default;
DistributedVectorGroupedAggregateShuffleDestinationExecution&
DistributedVectorGroupedAggregateShuffleDestinationExecution::operator=(
    DistributedVectorGroupedAggregateShuffleDestinationExecution&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateShuffleDestinationExecution>
DistributedVectorGroupedAggregateShuffleDestinationExecution::start(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    DistributedVectorGroupedAggregateShuffleDestinationExecutionConfig config) {
  if (config.local_node_id == 0U || config.maximum_reducer_admissions_per_poll == 0U ||
      config.maximum_reducer_admissions_per_poll > 65536U) {
    return common::make_unexpected(
        invalid("grouped shuffle destination execution configuration is invalid"));
  }
  try {
    std::vector<DistributedVectorGroupedAggregateShuffleReducer> reducers;
    std::map<std::uint32_t, std::size_t> reducer_indexes;
    reducers.reserve(authority.destinations().size());
    for (const auto& destination : authority.destinations()) {
      if (destination.node_id != config.local_node_id)
        continue;
      auto reducer = DistributedVectorGroupedAggregateShuffleReducer::create(
          authority, destination.partition_id, config.local_node_id, config.reducer_limits);
      if (!reducer.has_value())
        return common::make_unexpected(reducer.error());
      reducer_indexes.emplace(destination.partition_id, reducers.size());
      reducers.push_back(std::move(*reducer));
    }
    if (reducers.empty()) {
      return common::make_unexpected(
          invalid("grouped shuffle destination node owns no authority partition"));
    }

    bool needs_server{};
    for (const auto& source : authority.sources())
      needs_server = needs_server || source.node_id != config.local_node_id;
    std::optional<DistributedVectorGroupedAggregateShuffleTcpServer> server;
    if (needs_server) {
      if (!config.resources.has_value()) {
        return common::make_unexpected(
            invalid("grouped shuffle remote destination has no query resources"));
      }
      auto started = DistributedVectorGroupedAggregateShuffleTcpServer::start(
          {.listener = config.listener,
           .tls = config.tls,
           .authenticator = config.authenticator,
           .node_authorizer = config.node_authorizer,
           .authority = &authority,
           .local_node_id = config.local_node_id,
           .resources = *config.resources,
           .carrier_limits = config.carrier_limits,
           .maximum_retained_streams = config.maximum_retained_streams,
           .maximum_accepts_per_poll = config.maximum_accepts_per_poll});
      if (!started.has_value())
        return common::make_unexpected(started.error());
      server.emplace(std::move(*started));
    }
    std::vector<bool> output_complete(reducers.size(), false);
    return DistributedVectorGroupedAggregateShuffleDestinationExecution{std::make_unique<Impl>(
        authority, std::move(config), std::move(reducers), std::move(reducer_indexes),
        std::move(output_complete), std::move(server))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped shuffle destination execution allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("grouped shuffle destination execution exceeds limits"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleDestinationExecution::accept_local_stream(
    const DistributedVectorGroupedAggregateShuffleCompleteStream& stream) {
  if (!implementation_)
    return invalid("grouped shuffle destination execution is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kFailed ||
      impl.state_ ==
          DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kCancelled) {
    return impl.failure_;
  }
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kComplete) {
    return invalid("grouped shuffle destination execution is sealed");
  }
  if (stream.edge.source_node_id != impl.config_.local_node_id ||
      stream.edge.target_node_id != impl.config_.local_node_id) {
    return impl.fail(invalid("grouped shuffle local delivery is not a self-route"));
  }
  common::Status accepted = impl.admit(stream, false);
  if (!accepted.is_ok())
    return accepted;
  return impl.finish_ready_reducers();
}

common::Status DistributedVectorGroupedAggregateShuffleDestinationExecution::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return invalid("grouped shuffle destination execution is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return invalid("grouped shuffle destination execution poll timeout is invalid");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kFailed ||
      impl.state_ ==
          DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kCancelled) {
    return impl.failure_;
  }
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kComplete) {
    return common::Status::ok();
  }

  if (impl.pending_remote_stream_.has_value()) {
    common::Status accepted = impl.admit(*impl.pending_remote_stream_, true);
    if (!accepted.is_ok())
      return accepted;
    impl.pending_remote_stream_.reset();
    impl.metrics_.pending_remote_streams = 0U;
  }

  if (impl.server_.has_value()) {
    for (std::size_t admitted = 0U; admitted < impl.config_.maximum_reducer_admissions_per_poll &&
                                    impl.server_->metrics().retained_streams != 0U;
         ++admitted) {
      auto stream = impl.server_->take_next_complete_stream();
      if (!stream.has_value())
        return impl.fail(stream.error());
      impl.pending_remote_stream_.emplace(std::move(*stream));
      impl.metrics_.pending_remote_streams = 1U;
      common::Status accepted = impl.admit(*impl.pending_remote_stream_, true);
      if (!accepted.is_ok())
        return accepted;
      impl.pending_remote_stream_.reset();
      impl.metrics_.pending_remote_streams = 0U;
    }
    common::Status finished = impl.finish_ready_reducers();
    if (!finished.is_ok())
      return finished;
    const auto wait = impl.server_->metrics().retained_streams == 0U ? maximum_wait
                                                                     : std::chrono::milliseconds{0};
    common::Status driven = impl.server_->poll_once(wait);
    if (!driven.is_ok())
      return impl.fail(driven);
  }
  return impl.finish_ready_reducers();
}

common::Status DistributedVectorGroupedAggregateShuffleDestinationExecution::seal_transport() {
  if (!implementation_)
    return invalid("grouped shuffle destination execution is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kFailed ||
      impl.state_ ==
          DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kCancelled) {
    return impl.failure_;
  }
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kComplete) {
    return common::Status::ok();
  }
  if (impl.state_ != DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kReady ||
      impl.pending_remote_stream_.has_value()) {
    return unavailable("grouped shuffle destination execution is not ready to seal transport");
  }
  if (impl.server_.has_value()) {
    common::Status stopped = impl.server_->shutdown();
    if (!stopped.is_ok())
      return impl.fail(stopped);
  }
  impl.state_ = DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kComplete;
  return common::Status::ok();
}

common::Status DistributedVectorGroupedAggregateShuffleDestinationExecution::cancel() {
  if (!implementation_)
    return invalid("grouped shuffle destination execution is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kComplete) {
    return common::Status::ok();
  }
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kFailed ||
      impl.state_ ==
          DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kCancelled) {
    return impl.failure_;
  }
  if (impl.server_.has_value())
    static_cast<void>(impl.server_->shutdown());
  impl.pending_remote_stream_.reset();
  impl.metrics_.pending_remote_streams = 0U;
  impl.failure_ = {common::StatusCode::kCancelled,
                   "grouped shuffle destination execution was cancelled"};
  impl.state_ = DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kCancelled;
  return impl.failure_;
}

common::Result<query::PhysicalOperatorStep>
DistributedVectorGroupedAggregateShuffleDestinationExecution::next(
    const std::uint32_t partition_id) {
  if (!implementation_)
    return common::make_unexpected(invalid("grouped shuffle destination execution is empty"));
  Impl& impl = *implementation_;
  if (impl.state_ != DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kReady &&
      impl.state_ != DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kComplete) {
    return common::make_unexpected(
        invalid("grouped shuffle destination execution output is not ready"));
  }
  const auto reducer = impl.reducer_indexes_.find(partition_id);
  if (reducer == impl.reducer_indexes_.end())
    return common::make_unexpected(
        invalid("grouped shuffle partition is not owned by this destination"));
  if (impl.output_complete_[reducer->second])
    return query::PhysicalOperatorStep::end();
  auto step = impl.reducers_[reducer->second].next();
  if (!step.has_value())
    return step;
  if (step->kind() == query::PhysicalOperatorStepKind::kEnd) {
    impl.output_complete_[reducer->second] = true;
    ++impl.metrics_.completed_output_partitions;
  } else {
    increment_saturated(impl.metrics_.output_chunks);
  }
  return step;
}

DistributedVectorGroupedAggregateShuffleDestinationExecutionState
DistributedVectorGroupedAggregateShuffleDestinationExecution::state() const noexcept {
  return implementation_
             ? implementation_->state_
             : DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kFailed;
}

DistributedVectorGroupedAggregateShuffleDestinationExecutionMetrics
DistributedVectorGroupedAggregateShuffleDestinationExecution::metrics() const noexcept {
  return implementation_ ? implementation_->metrics_
                         : DistributedVectorGroupedAggregateShuffleDestinationExecutionMetrics{};
}

DistributedVectorGroupedAggregateShuffleTcpServerMetrics
DistributedVectorGroupedAggregateShuffleDestinationExecution::transport_metrics() const noexcept {
  return implementation_ && implementation_->server_.has_value()
             ? implementation_->server_->metrics()
             : DistributedVectorGroupedAggregateShuffleTcpServerMetrics{};
}

common::Result<DistributedVectorGroupedAggregateShuffleReducerMetrics>
DistributedVectorGroupedAggregateShuffleDestinationExecution::reducer_metrics(
    const std::uint32_t partition_id) const {
  if (!implementation_)
    return common::make_unexpected(invalid("grouped shuffle destination execution is empty"));
  const auto reducer = implementation_->reducer_indexes_.find(partition_id);
  if (reducer == implementation_->reducer_indexes_.end()) {
    return common::make_unexpected(
        invalid("grouped shuffle partition is not owned by this destination"));
  }
  return implementation_->reducers_[reducer->second].metrics();
}

network::Ipv4Endpoint
DistributedVectorGroupedAggregateShuffleDestinationExecution::bound_endpoint() const noexcept {
  return implementation_ && implementation_->server_.has_value()
             ? implementation_->server_->bound_endpoint()
             : network::Ipv4Endpoint{};
}

const DistributedVectorGroupedAggregateShuffleAuthority*
DistributedVectorGroupedAggregateShuffleDestinationExecution::authority() const noexcept {
  return implementation_ ? std::addressof(implementation_->authority_.get()) : nullptr;
}

raft::NodeId
DistributedVectorGroupedAggregateShuffleDestinationExecution::local_node_id() const noexcept {
  return implementation_ ? implementation_->config_.local_node_id : 0U;
}

const common::Status&
DistributedVectorGroupedAggregateShuffleDestinationExecution::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "grouped shuffle destination execution is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::cluster
