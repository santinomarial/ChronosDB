#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_coordinator_execution.hpp"

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_authority_codec.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <functional>
#include <limits>
#include <new>
#include <poll.h>
#include <ranges>
#include <set>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}

using Acquisition = DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition;

[[nodiscard]] bool acquisition_running(const Acquisition& acquisition) noexcept {
  return acquisition.state() ==
         DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kRunning;
}

[[nodiscard]] std::chrono::milliseconds
bounded_wait(const std::chrono::milliseconds maximum_wait,
             const std::chrono::steady_clock::time_point now,
             const std::chrono::steady_clock::time_point deadline) noexcept {
  if (deadline <= now)
    return std::chrono::milliseconds{0};
  return std::min(maximum_wait,
                  std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now));
}

[[nodiscard]] std::uint64_t saturated_add(const std::uint64_t left,
                                          const std::uint64_t right) noexcept {
  return right > std::numeric_limits<std::uint64_t>::max() - left
             ? std::numeric_limits<std::uint64_t>::max()
             : left + right;
}

} // namespace

class DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::Impl {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  Impl(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
       DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionConfig configured,
       std::vector<DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition>
           prepare_acquisitions,
       std::vector<pollfd> descriptors,
       DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution result_execution)
      : authority_(authority), query_id_(authority.query_id()), config_(std::move(configured)),
        acquisitions_(std::move(prepare_acquisitions)), poll_descriptors_(std::move(descriptors)),
        result_(std::move(result_execution)) {
    metrics_.reducer_nodes = acquisitions_.size();
  }

  void refresh_control_metrics() noexcept {
    metrics_.control_attempts_started = committed_control_attempts_started_;
    metrics_.control_retries_started = committed_control_retries_started_;
    metrics_.control_failed_attempts = committed_control_failed_attempts_;
    for (const auto& acquisition : acquisitions_) {
      const auto current = acquisition.metrics();
      metrics_.control_attempts_started =
          saturated_add(metrics_.control_attempts_started, current.attempts_started);
      metrics_.control_retries_started =
          saturated_add(metrics_.control_retries_started, current.retries_started);
      metrics_.control_failed_attempts =
          saturated_add(metrics_.control_failed_attempts, current.failed_attempts);
    }
  }

  void commit_control_metrics() noexcept {
    refresh_control_metrics();
    committed_control_attempts_started_ = metrics_.control_attempts_started;
    committed_control_retries_started_ = metrics_.control_retries_started;
    committed_control_failed_attempts_ = metrics_.control_failed_attempts;
  }

  void cancel_controls() noexcept {
    for (auto& acquisition : acquisitions_)
      if (acquisition_running(acquisition))
        static_cast<void>(acquisition.cancel());
    refresh_control_metrics();
  }

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (state_ != DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kFailed &&
        state_ !=
            DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCancelled) {
      cancel_controls();
      static_cast<void>(result_.cancel());
      prepared_routes_.clear();
      failure_ = std::move(failure);
      state_ = DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kFailed;
    }
    return failure_;
  }

  [[nodiscard]] common::Status expire() {
    return fail(status(common::StatusCode::kCancelled,
                       "grouped shuffle reducer-job coordinator deadline expired"));
  }

  [[nodiscard]] common::Status drive_controls() {
    std::size_t completed{};
    for (auto& acquisition : acquisitions_) {
      if (acquisition_running(acquisition)) {
        const common::Status progress = acquisition.poll_once(std::chrono::milliseconds{0});
        if (!progress.is_ok() &&
            acquisition.state() !=
                DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kFailed) {
          return fail(progress);
        }
      }
      if (acquisition.state() ==
          DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kFailed) {
        return fail(acquisition.failure());
      }
      completed +=
          acquisition.state() ==
                  DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kComplete
              ? 1U
              : 0U;
    }
    refresh_control_metrics();
    if (completed != acquisitions_.size())
      return common::Status::ok();
    return state_ ==
                   DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kPreparing
               ? publish_prepared()
               : publish_sealed();
  }

  [[nodiscard]] common::Status publish_prepared() {
    try {
      std::vector<DistributedQueryNodeRoute> prepared;
      prepared.reserve(acquisitions_.size());
      for (std::size_t index = 0U; index < acquisitions_.size(); ++index) {
        auto response = acquisitions_[index].result();
        if (!response.has_value())
          return fail(response.error());
        if (response->status_code != common::StatusCode::kOk) {
          return fail(
              status(response->status_code, "grouped shuffle reducer PREPARE was not accepted"));
        }
        const raft::NodeId target = config_.reducer_control_routes[index].node_id;
        const bool needs_remote_endpoint =
            std::ranges::any_of(authority_.get().sources(),
                                [target](const auto& source) { return source.node_id != target; });
        if (!response->reducer_shuffle_endpoint.has_value()) {
          if (needs_remote_endpoint) {
            return fail(status(common::StatusCode::kCorruption,
                               "remote grouped shuffle reducer returned no endpoint"));
          }
          continue;
        }
        prepared.push_back({.node_id = config_.reducer_control_routes[index].node_id,
                            .endpoints = {*response->reducer_shuffle_endpoint},
                            .tls_context = config_.reducer_control_routes[index].tls_context});
      }
      prepared_routes_ = std::move(prepared);
      metrics_.prepared_reducers = acquisitions_.size();
      commit_control_metrics();
      acquisitions_.clear();
      state_ = DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kPrepared;
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return fail(status(common::StatusCode::kResourceExhausted,
                         "grouped shuffle prepared-route allocation failed"));
    } catch (const std::length_error&) {
      return fail(status(common::StatusCode::kResourceExhausted,
                         "grouped shuffle prepared routes exceed container limits"));
    }
  }

  [[nodiscard]] common::Status publish_sealed() {
    for (auto& acquisition : acquisitions_) {
      auto response = acquisition.result();
      if (!response.has_value())
        return fail(response.error());
      if (response->status_code != common::StatusCode::kOk ||
          response->reducer_shuffle_endpoint.has_value()) {
        return fail(status(response->status_code == common::StatusCode::kOk
                               ? common::StatusCode::kCorruption
                               : response->status_code,
                           "grouped shuffle reducer SEAL was not accepted"));
      }
    }
    metrics_.sealed_reducers = acquisitions_.size();
    commit_control_metrics();
    acquisitions_.clear();
    state_ =
        DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCollectingResults;
    return common::Status::ok();
  }

  std::reference_wrapper<const DistributedVectorGroupedAggregateShuffleAuthority> authority_;
  common::Uuid query_id_;
  DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionConfig config_;
  std::vector<DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition> acquisitions_;
  std::vector<pollfd> poll_descriptors_;
  DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution result_;
  std::vector<DistributedQueryNodeRoute> prepared_routes_;
  DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionMetrics metrics_;
  std::uint64_t committed_control_attempts_started_{};
  std::uint64_t committed_control_retries_started_{};
  std::uint64_t committed_control_failed_attempts_{};
  DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState state_{
      DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kPreparing};
  common::Status failure_{common::StatusCode::kInternal,
                          "grouped shuffle reducer-job coordinator has not failed"};
};

DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::
    DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution() noexcept = default;
DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::
    DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::
    ~DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution() = default;
DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::
    DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution(
        DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution&&) noexcept = default;
DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution&
DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::operator=(
    DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution>
DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::create(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2& finalization_authority,
    DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionConfig config) {
  const auto now = std::chrono::steady_clock::now();
  if (std::addressof(finalization_authority.shuffle_authority()) != std::addressof(authority) ||
      config.coordinator_node_id == 0U || config.authenticator == nullptr ||
      config.node_authorizer == nullptr || config.reducer_control_routes.empty() ||
      config.maximum_reducer_nodes == 0U || config.maximum_reducer_nodes > 4096U ||
      config.reducer_control_routes.size() > config.maximum_reducer_nodes ||
      config.reducer_execution_timeout.count() <= 0 ||
      config.reducer_execution_timeout >
          distributed_vector_grouped_aggregate_shuffle_job_control_format::
              kMaximumExecutionTimeout ||
      config.execution_deadline <= now ||
      config.result.coordinator_node_id != config.coordinator_node_id ||
      config.result.authenticator != config.authenticator ||
      config.result.node_authorizer != config.node_authorizer ||
      (config.result.execution_deadline.has_value() &&
       *config.result.execution_deadline != config.execution_deadline)) {
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument,
               "grouped shuffle reducer-job coordinator configuration is invalid"));
  }
  if (!std::ranges::is_sorted(config.reducer_control_routes, {},
                              &DistributedQueryNodeRoute::node_id) ||
      std::ranges::adjacent_find(config.reducer_control_routes, {},
                                 &DistributedQueryNodeRoute::node_id) !=
          config.reducer_control_routes.end()) {
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument,
               "grouped shuffle reducer-job coordinator routes are not canonical"));
  }

  try {
    std::set<raft::NodeId> reducer_nodes;
    for (const auto& destination : authority.destinations())
      reducer_nodes.insert(destination.node_id);
    if (reducer_nodes.size() != config.reducer_control_routes.size() ||
        !std::ranges::equal(reducer_nodes, config.reducer_control_routes, {}, std::identity{},
                            &DistributedQueryNodeRoute::node_id)) {
      return common::make_unexpected(
          status(common::StatusCode::kInvalidArgument,
                 "grouped shuffle reducer-job coordinator route coverage differs from authority"));
    }

    config.result.execution_deadline = config.execution_deadline;
    auto result = DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::create(
        authority, finalization_authority, config.result);
    if (!result.has_value())
      return common::make_unexpected(result.error());
    const network::Ipv4Endpoint result_endpoint = result->bound_endpoint();
    auto encoded_authority =
        encode_distributed_vector_grouped_aggregate_shuffle_authority(authority);
    if (!encoded_authority.has_value())
      return common::make_unexpected(encoded_authority.error());

    std::vector<DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition> acquisitions;
    acquisitions.reserve(config.reducer_control_routes.size());
    for (const auto& route : config.reducer_control_routes) {
      auto owned_authority = decode_distributed_vector_grouped_aggregate_shuffle_authority_exact(
          encoded_authority->bytes(), config.carrier_limits.request.authority);
      if (!owned_authority.has_value())
        return common::make_unexpected(owned_authority.error());
      auto acquisition = DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::create(
          {.route = route,
           .authenticator = config.authenticator,
           .node_authorizer = config.node_authorizer,
           .request =
               DistributedVectorGroupedAggregateShuffleJobControlRequest{
                   DistributedVectorGroupedAggregateShuffleJobPrepare{
                       .coordinator_node_id = config.coordinator_node_id,
                       .target_node_id = route.node_id,
                       .coordinator_result_endpoint = result_endpoint,
                       .execution_timeout = config.reducer_execution_timeout,
                       .authority = std::move(*owned_authority),
                       .result_schema = finalization_authority.result_schema()}},
           .carrier_limits = config.carrier_limits,
           .connect_timeout = config.connect_timeout,
           .retry = config.prepare_retry,
           .execution_deadline = config.execution_deadline});
      if (!acquisition.has_value())
        return common::make_unexpected(acquisition.error());
      acquisitions.push_back(std::move(*acquisition));
    }
    std::vector<pollfd> descriptors(acquisitions.size());
    return DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution{
        std::make_unique<Impl>(authority, std::move(config), std::move(acquisitions),
                               std::move(descriptors), std::move(*result))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted,
               "grouped shuffle reducer-job coordinator allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted,
               "grouped shuffle reducer-job coordinator exceeds container limits"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument,
                  "grouped shuffle reducer-job coordinator is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return status(common::StatusCode::kInvalidArgument,
                  "grouped shuffle reducer-job coordinator poll timeout is invalid");
  Impl& impl = *implementation_;
  if (impl.state_ ==
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kFailed ||
      impl.state_ ==
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCancelled)
    return impl.failure_;
  if (impl.state_ ==
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kComplete ||
      impl.state_ ==
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kResultTaken ||
      impl.state_ ==
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kPrepared)
    return common::Status::ok();
  auto now = Impl::TimePoint::clock::now();
  if (now >= impl.config_.execution_deadline)
    return impl.expire();

  if (impl.state_ ==
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kPreparing ||
      impl.state_ ==
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kSealing) {
    common::Status progress = impl.drive_controls();
    if (!progress.is_ok() ||
        (impl.state_ !=
             DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kPreparing &&
         impl.state_ !=
             DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kSealing))
      return progress;

    std::size_t count{};
    auto wait = bounded_wait(maximum_wait, now, impl.config_.execution_deadline);
    for (const auto& acquisition : impl.acquisitions_) {
      if (!acquisition_running(acquisition))
        continue;
      if (const auto deadline = acquisition.wake_deadline(); deadline.has_value())
        wait = bounded_wait(wait, now, *deadline);
      if (acquisition.descriptor() < 0)
        continue;
      const auto interest = acquisition.interest();
      impl.poll_descriptors_[count++] = {
          .fd = acquisition.descriptor(),
          .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                       (interest.want_write ? POLLOUT : 0))};
    }
    const int ready = ::poll(impl.poll_descriptors_.data(), static_cast<nfds_t>(count),
                             static_cast<int>(wait.count()));
    if (ready < 0 && errno != EINTR)
      return impl.fail(status(common::StatusCode::kIoError,
                              "polling grouped shuffle reducer-job coordinator failed"));
    if (Impl::TimePoint::clock::now() >= impl.config_.execution_deadline)
      return impl.expire();
    return impl.drive_controls();
  }

  const common::Status progress =
      impl.result_.poll_once(bounded_wait(maximum_wait, now, impl.config_.execution_deadline));
  impl.metrics_.result = impl.result_.metrics();
  if (!progress.is_ok())
    return impl.fail(progress);
  if (impl.result_.state() ==
      DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kComplete)
    impl.state_ = DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kComplete;
  return common::Status::ok();
}

common::Status DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::seal() {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument,
                  "grouped shuffle reducer-job coordinator is empty");
  Impl& impl = *implementation_;
  if (impl.state_ !=
      DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kPrepared)
    return status(common::StatusCode::kInvalidArgument,
                  "grouped shuffle reducer-job coordinator is not prepared");
  if (Impl::TimePoint::clock::now() >= impl.config_.execution_deadline)
    return impl.expire();
  try {
    std::vector<DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition> seals;
    seals.reserve(impl.config_.reducer_control_routes.size());
    for (const auto& route : impl.config_.reducer_control_routes) {
      auto acquisition = DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::create(
          {.route = route,
           .authenticator = impl.config_.authenticator,
           .node_authorizer = impl.config_.node_authorizer,
           .request =
               DistributedVectorGroupedAggregateShuffleJobControlRequest{
                   DistributedVectorGroupedAggregateShuffleJobSeal{
                       impl.query_id_, impl.config_.coordinator_node_id, route.node_id}},
           .carrier_limits = impl.config_.carrier_limits,
           .connect_timeout = impl.config_.connect_timeout,
           .retry = impl.config_.seal_retry,
           .execution_deadline = impl.config_.execution_deadline,
           .retry_unavailable_response = true});
      if (!acquisition.has_value())
        return impl.fail(acquisition.error());
      seals.push_back(std::move(*acquisition));
    }
    impl.acquisitions_ = std::move(seals);
    impl.state_ = DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kSealing;
    return common::Status::ok();
  } catch (const std::bad_alloc&) {
    return impl.fail(status(common::StatusCode::kResourceExhausted,
                            "grouped shuffle reducer SEAL allocation failed"));
  } catch (const std::length_error&) {
    return impl.fail(status(common::StatusCode::kResourceExhausted,
                            "grouped shuffle reducer SEAL set exceeds container limits"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::cancel() {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument,
                  "grouped shuffle reducer-job coordinator is empty");
  Impl& impl = *implementation_;
  if (impl.state_ ==
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kFailed ||
      impl.state_ ==
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCancelled)
    return impl.failure_;
  if (impl.state_ ==
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kComplete ||
      impl.state_ ==
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kResultTaken)
    return common::Status::ok();
  impl.cancel_controls();
  static_cast<void>(impl.result_.cancel());
  impl.prepared_routes_.clear();
  impl.failure_ = status(common::StatusCode::kCancelled,
                         "grouped shuffle reducer-job coordinator was cancelled");
  impl.state_ = DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCancelled;
  return impl.failure_;
}

std::span<const DistributedQueryNodeRoute>
DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::prepared_routes() const noexcept {
  if (!implementation_ ||
      (implementation_->state_ !=
           DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kPrepared &&
       implementation_->state_ !=
           DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kSealing &&
       implementation_->state_ !=
           DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::
               kCollectingResults))
    return {};
  return implementation_->prepared_routes_;
}

common::Result<DistributedVectorRowsFinalizedResultV2>
DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::take_result() {
  if (!implementation_ ||
      implementation_->state_ !=
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kComplete)
    return common::make_unexpected(status(common::StatusCode::kUnavailable,
                                          "grouped shuffle reducer-job result is unavailable"));
  auto result = implementation_->result_.take_result();
  if (result.has_value())
    implementation_->state_ =
        DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kResultTaken;
  return result;
}

DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState
DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::state() const noexcept {
  return implementation_
             ? implementation_->state_
             : DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kFailed;
}

DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionMetrics
DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::metrics() const noexcept {
  return implementation_ ? implementation_->metrics_
                         : DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionMetrics{};
}

const common::Status&
DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "grouped shuffle reducer-job coordinator is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::cluster
