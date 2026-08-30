#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_coordinator_execution.hpp"

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_authority_codec.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_service.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <functional>
#include <limits>
#include <new>
#include <optional>
#include <poll.h>
#include <ranges>
#include <set>
#include <stdexcept>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}

using RemoteAcquisition = DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition;
using AcquisitionState = DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState;
using AcquisitionMetrics = DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionMetrics;

[[nodiscard]] bool retryable(const common::StatusCode code) noexcept {
  return code == common::StatusCode::kUnavailable || code == common::StatusCode::kIoError ||
         code == common::StatusCode::kResourceExhausted;
}

[[nodiscard]] std::chrono::steady_clock::time_point
saturating_deadline(std::chrono::steady_clock::time_point now,
                    std::chrono::milliseconds duration) noexcept;

class LocalAcquisition {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  LocalAcquisition(DistributedVectorGroupedAggregateShuffleJobService& service,
                   DistributedVectorGroupedAggregateShuffleJobControlRequest request,
                   const DistributedVectorGroupedAggregateShuffleJobControlTcpRetryLimits retry,
                   const TimePoint execution_deadline, const bool retry_unavailable) noexcept
      : service_(&service), request_(std::move(request)), retry_(retry),
        execution_deadline_(execution_deadline), next_backoff_(retry.initial_backoff),
        retry_unavailable_(retry_unavailable) {
    if (const auto* seal =
            std::get_if<DistributedVectorGroupedAggregateShuffleJobSeal>(&request_)) {
      seal_.emplace(*seal);
    }
  }

  [[nodiscard]] common::Status poll_once(const std::chrono::milliseconds maximum_wait) {
    if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
      return fail(status(common::StatusCode::kInvalidArgument,
                         "local grouped shuffle control poll timeout is invalid"));
    if (state_ == AcquisitionState::kFailed || state_ == AcquisitionState::kCancelled)
      return failure_;
    if (state_ == AcquisitionState::kComplete)
      return common::Status::ok();
    const TimePoint now = TimePoint::clock::now();
    if (now >= execution_deadline_)
      return fail(
          status(common::StatusCode::kCancelled, "local grouped shuffle control deadline expired"));
    if (next_attempt_not_before_.has_value() && now < *next_attempt_not_before_)
      return common::Status::ok();
    ++metrics_.attempts_started;
    if (metrics_.attempts_started > 1U)
      ++metrics_.retries_started;
    metrics_.active_attempts = 1U;
    common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponse> response =
        seal_.has_value()
            ? service_->receive_local(
                  DistributedVectorGroupedAggregateShuffleJobControlRequest{*seal_}, now)
            : service_->receive_local(std::move(request_), now);
    metrics_.active_attempts = 0U;
    if (!response.has_value())
      return schedule(response.error(), now);
    if (retry_unavailable_ && response->status_code == common::StatusCode::kUnavailable)
      return schedule(status(common::StatusCode::kUnavailable,
                             "local grouped shuffle control response is not ready"),
                      now);
    result_.emplace(*response);
    ++metrics_.completed_attempts;
    state_ = AcquisitionState::kComplete;
    next_attempt_not_before_.reset();
    return common::Status::ok();
  }

  [[nodiscard]] common::Status cancel() {
    if (state_ == AcquisitionState::kRunning) {
      state_ = AcquisitionState::kCancelled;
      metrics_.active_attempts = 0U;
      next_attempt_not_before_.reset();
      failure_ =
          status(common::StatusCode::kCancelled, "local grouped shuffle control was cancelled");
    }
    return state_ == AcquisitionState::kCancelled ? failure_ : common::Status::ok();
  }

  [[nodiscard]] AcquisitionState state() const noexcept {
    return state_;
  }
  [[nodiscard]] AcquisitionMetrics metrics() const noexcept {
    return metrics_;
  }
  [[nodiscard]] static int descriptor() noexcept {
    return -1;
  }
  [[nodiscard]] static DistributedVectorGroupedAggregateShuffleJobControlTlsInterest
  interest() noexcept {
    return {};
  }
  [[nodiscard]] std::optional<TimePoint> wake_deadline() const noexcept {
    return state_ == AcquisitionState::kRunning
               ? std::optional<TimePoint>{next_attempt_not_before_.value_or(
                     TimePoint::clock::now())}
               : std::nullopt;
  }
  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponse>
  result() const {
    if (state_ != AcquisitionState::kComplete || !result_.has_value())
      return common::make_unexpected(status(common::StatusCode::kUnavailable,
                                            "local grouped shuffle control result unavailable"));
    return *result_;
  }
  [[nodiscard]] const common::Status& failure() const noexcept {
    return failure_;
  }

private:
  [[nodiscard]] common::Status schedule(common::Status failure, const TimePoint now) {
    ++metrics_.failed_attempts;
    if (!retryable(failure.code()) || !seal_.has_value() ||
        metrics_.attempts_started >= retry_.maximum_attempts) {
      return fail(std::move(failure));
    }
    failure_ = std::move(failure);
    next_attempt_not_before_ = saturating_deadline(now, next_backoff_);
    if (*next_attempt_not_before_ > execution_deadline_)
      next_attempt_not_before_ = execution_deadline_;
    if (next_backoff_ < retry_.maximum_backoff) {
      const auto current = next_backoff_.count();
      const auto maximum = retry_.maximum_backoff.count();
      next_backoff_ = current > maximum / 2 ? retry_.maximum_backoff
                                            : std::min(next_backoff_ * 2, retry_.maximum_backoff);
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status fail(common::Status failure) {
    failure_ = std::move(failure);
    state_ = AcquisitionState::kFailed;
    metrics_.active_attempts = 0U;
    next_attempt_not_before_.reset();
    return failure_;
  }

  DistributedVectorGroupedAggregateShuffleJobService* service_{};
  DistributedVectorGroupedAggregateShuffleJobControlRequest request_;
  std::optional<DistributedVectorGroupedAggregateShuffleJobSeal> seal_;
  DistributedVectorGroupedAggregateShuffleJobControlTcpRetryLimits retry_;
  TimePoint execution_deadline_;
  std::chrono::milliseconds next_backoff_;
  std::optional<TimePoint> next_attempt_not_before_;
  std::optional<DistributedVectorGroupedAggregateShuffleJobControlResponse> result_;
  AcquisitionMetrics metrics_;
  AcquisitionState state_{AcquisitionState::kRunning};
  common::Status failure_{common::StatusCode::kInternal,
                          "local grouped shuffle control has not failed"};
  bool retry_unavailable_{};
};

class Acquisition {
public:
  explicit Acquisition(RemoteAcquisition remote) : owner_(std::move(remote)) {}
  explicit Acquisition(LocalAcquisition local) : owner_(std::move(local)) {}

  [[nodiscard]] common::Status poll_once(const std::chrono::milliseconds wait) {
    return std::visit([wait](auto& owner) { return owner.poll_once(wait); }, owner_);
  }
  [[nodiscard]] common::Status cancel() {
    return std::visit([](auto& owner) { return owner.cancel(); }, owner_);
  }
  [[nodiscard]] AcquisitionState state() const {
    return std::visit([](const auto& owner) { return owner.state(); }, owner_);
  }
  [[nodiscard]] AcquisitionMetrics metrics() const {
    return std::visit([](const auto& owner) { return owner.metrics(); }, owner_);
  }
  [[nodiscard]] int descriptor() const {
    return std::visit([](const auto& owner) { return owner.descriptor(); }, owner_);
  }
  [[nodiscard]] DistributedVectorGroupedAggregateShuffleJobControlTlsInterest interest() const {
    return std::visit([](const auto& owner) { return owner.interest(); }, owner_);
  }
  [[nodiscard]] std::optional<std::chrono::steady_clock::time_point> wake_deadline() const {
    return std::visit([](const auto& owner) { return owner.wake_deadline(); }, owner_);
  }
  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponse>
  result() const {
    return std::visit([](const auto& owner) { return owner.result(); }, owner_);
  }
  [[nodiscard]] const common::Status& failure() const {
    return std::visit([](const auto& owner) -> const common::Status& { return owner.failure(); },
                      owner_);
  }

private:
  std::variant<RemoteAcquisition, LocalAcquisition> owner_;
};

[[nodiscard]] common::Result<Acquisition> create_acquisition(
    const DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionConfig& config,
    const DistributedQueryNodeRoute& route,
    DistributedVectorGroupedAggregateShuffleJobControlRequest request,
    const DistributedVectorGroupedAggregateShuffleJobControlTcpRetryLimits retry,
    const std::chrono::steady_clock::time_point execution_deadline,
    const bool retry_unavailable_response = false) {
  if (route.node_id == config.coordinator_node_id) {
    if (config.local_reducer_job_service == nullptr) {
      return common::make_unexpected(
          status(common::StatusCode::kInvalidArgument,
                 "local grouped shuffle reducer service is unavailable"));
    }
    return Acquisition{LocalAcquisition{*config.local_reducer_job_service, std::move(request),
                                        retry, execution_deadline, retry_unavailable_response}};
  }
  auto remote =
      RemoteAcquisition::create({.route = route,
                                 .authenticator = config.authenticator,
                                 .node_authorizer = config.node_authorizer,
                                 .request = std::move(request),
                                 .carrier_limits = config.carrier_limits,
                                 .connect_timeout = config.connect_timeout,
                                 .retry = retry,
                                 .execution_deadline = execution_deadline,
                                 .retry_unavailable_response = retry_unavailable_response});
  if (!remote.has_value())
    return common::make_unexpected(remote.error());
  return Acquisition{std::move(*remote)};
}

inline constexpr std::chrono::milliseconds kLeaseActivePollInterval{10};

[[nodiscard]] bool acquisition_running(const Acquisition& acquisition) {
  return acquisition.state() == AcquisitionState::kRunning;
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

[[nodiscard]] std::chrono::steady_clock::time_point
saturating_deadline(const std::chrono::steady_clock::time_point now,
                    const std::chrono::milliseconds duration) noexcept {
  const auto remaining = std::chrono::steady_clock::time_point::max() - now;
  return duration >= remaining ? std::chrono::steady_clock::time_point::max() : now + duration;
}

[[nodiscard]] bool valid_retry(
    const DistributedVectorGroupedAggregateShuffleJobControlTcpRetryLimits& retry) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::duration::max());
  return retry.maximum_attempts > 0U && retry.maximum_attempts <= 1024U &&
         retry.initial_backoff.count() > 0 && retry.maximum_backoff >= retry.initial_backoff &&
         retry.maximum_backoff <= maximum;
}

} // namespace

class DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::Impl {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  enum class TerminalAfterCancel : std::uint8_t {
    kFailed = 1,
    kCancelled = 2,
  };

  Impl(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
       DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionConfig configured,
       std::vector<Acquisition> prepare_acquisitions, std::vector<pollfd> descriptors,
       DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution result_execution)
      : authority_(authority), query_id_(authority.query_id()), config_(std::move(configured)),
        acquisitions_(std::move(prepare_acquisitions)), poll_descriptors_(std::move(descriptors)),
        result_(std::move(result_execution)) {
    metrics_.reducer_nodes = acquisitions_.size();
  }

  void refresh_control_metrics() {
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

  void refresh_lease_metrics() {
    metrics_.lease_attempts_started = committed_lease_attempts_started_;
    metrics_.lease_retries_started = committed_lease_retries_started_;
    metrics_.lease_failed_attempts = committed_lease_failed_attempts_;
    for (const auto& acquisition : lease_acquisitions_) {
      const auto current = acquisition.metrics();
      metrics_.lease_attempts_started =
          saturated_add(metrics_.lease_attempts_started, current.attempts_started);
      metrics_.lease_retries_started =
          saturated_add(metrics_.lease_retries_started, current.retries_started);
      metrics_.lease_failed_attempts =
          saturated_add(metrics_.lease_failed_attempts, current.failed_attempts);
    }
  }

  void commit_control_metrics() {
    refresh_control_metrics();
    committed_control_attempts_started_ = metrics_.control_attempts_started;
    committed_control_retries_started_ = metrics_.control_retries_started;
    committed_control_failed_attempts_ = metrics_.control_failed_attempts;
  }

  void commit_lease_metrics() {
    refresh_lease_metrics();
    committed_lease_attempts_started_ = metrics_.lease_attempts_started;
    committed_lease_retries_started_ = metrics_.lease_retries_started;
    committed_lease_failed_attempts_ = metrics_.lease_failed_attempts;
  }

  void cancel_controls() {
    for (auto& acquisition : acquisitions_)
      if (acquisition_running(acquisition))
        static_cast<void>(acquisition.cancel());
    for (auto& acquisition : lease_acquisitions_)
      if (acquisition_running(acquisition))
        static_cast<void>(acquisition.cancel());
    refresh_control_metrics();
    refresh_lease_metrics();
  }

  void finish_cancellation() noexcept {
    state_ = terminal_after_cancel_ == TerminalAfterCancel::kCancelled
                 ? DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCancelled
                 : DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kFailed;
    terminal_after_cancel_.reset();
  }

  [[nodiscard]] common::Status cancellation_delivery_failed() {
    cancel_controls();
    if (metrics_.cancel_delivery_failures != std::numeric_limits<std::uint64_t>::max())
      ++metrics_.cancel_delivery_failures;
    acquisitions_.clear();
    finish_cancellation();
    return common::Status::ok();
  }

  [[nodiscard]] common::Status begin_cancellation(common::Status terminal_failure,
                                                  const TerminalAfterCancel terminal) {
    cancel_controls();
    static_cast<void>(result_.cancel());
    prepared_routes_.clear();
    failure_ = std::move(terminal_failure);
    terminal_after_cancel_ = terminal;
    commit_control_metrics();
    commit_lease_metrics();
    acquisitions_.clear();
    lease_acquisitions_.clear();
    try {
      std::vector<Acquisition> cancels;
      cancels.reserve(config_.reducer_control_routes.size());
      for (const auto& route : config_.reducer_control_routes) {
        auto acquisition =
            create_acquisition(config_, route,
                               DistributedVectorGroupedAggregateShuffleJobControlRequest{
                                   DistributedVectorGroupedAggregateShuffleJobCancel{
                                       query_id_, config_.coordinator_node_id, route.node_id}},
                               config_.cancel_retry, config_.execution_deadline);
        if (!acquisition.has_value())
          return cancellation_delivery_failed();
        cancels.push_back(std::move(*acquisition));
      }
      acquisitions_ = std::move(cancels);
      state_ = DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCancelling;
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return cancellation_delivery_failed();
    } catch (const std::length_error&) {
      return cancellation_delivery_failed();
    }
  }

  [[nodiscard]] common::Status start_lease_round(const TimePoint now, const bool activation) {
    try {
      std::vector<Acquisition> renewals;
      renewals.reserve(config_.reducer_control_routes.size());
      const TimePoint round_deadline =
          std::min(config_.execution_deadline, saturating_deadline(now, config_.lease_duration));
      for (const auto& route : config_.reducer_control_routes) {
        auto acquisition =
            create_acquisition(config_, route,
                               DistributedVectorGroupedAggregateShuffleJobControlRequest{
                                   DistributedVectorGroupedAggregateShuffleJobRenewLease{
                                       .query_id = query_id_,
                                       .coordinator_node_id = config_.coordinator_node_id,
                                       .target_node_id = route.node_id,
                                       .lease_duration = config_.lease_duration}},
                               config_.lease_retry, round_deadline);
        if (!acquisition.has_value())
          return fail(acquisition.error());
        renewals.push_back(std::move(*acquisition));
      }
      lease_acquisitions_ = std::move(renewals);
      next_lease_renewal_ = saturating_deadline(now, config_.lease_renew_interval);
      activating_lease_ = activation;
      if (activation) {
        state_ =
            DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kActivatingLease;
      }
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return fail(status(common::StatusCode::kResourceExhausted,
                         "grouped shuffle reducer lease allocation failed"));
    } catch (const std::length_error&) {
      return fail(status(common::StatusCode::kResourceExhausted,
                         "grouped shuffle reducer lease set exceeds container limits"));
    }
  }

  [[nodiscard]] common::Status drive_lease_controls() {
    if (lease_acquisitions_.empty())
      return common::Status::ok();
    std::size_t completed{};
    for (auto& acquisition : lease_acquisitions_) {
      if (acquisition_running(acquisition)) {
        const common::Status progress = acquisition.poll_once(std::chrono::milliseconds{0});
        if (!progress.is_ok() &&
            acquisition.state() !=
                DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kFailed) {
          if (metrics_.lease_failures != std::numeric_limits<std::uint64_t>::max())
            ++metrics_.lease_failures;
          return fail(progress);
        }
      }
      if (acquisition.state() ==
          DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kFailed) {
        if (metrics_.lease_failures != std::numeric_limits<std::uint64_t>::max())
          ++metrics_.lease_failures;
        return fail(acquisition.failure());
      }
      completed +=
          acquisition.state() ==
                  DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kComplete
              ? 1U
              : 0U;
    }
    refresh_lease_metrics();
    if (completed != lease_acquisitions_.size())
      return common::Status::ok();
    for (auto& acquisition : lease_acquisitions_) {
      auto response = acquisition.result();
      if (!response.has_value() || response->status_code != common::StatusCode::kOk ||
          response->action !=
              DistributedVectorGroupedAggregateShuffleJobControlAction::kRenewLease ||
          response->reducer_shuffle_endpoint.has_value()) {
        if (metrics_.lease_failures != std::numeric_limits<std::uint64_t>::max())
          ++metrics_.lease_failures;
        return fail(response.has_value()
                        ? status(response->status_code == common::StatusCode::kOk
                                     ? common::StatusCode::kCorruption
                                     : response->status_code,
                                 "grouped shuffle reducer lease renewal was not accepted")
                        : response.error());
      }
    }
    metrics_.lease_responses_accepted = saturated_add(
        metrics_.lease_responses_accepted, static_cast<std::uint64_t>(lease_acquisitions_.size()));
    if (metrics_.lease_rounds_completed != std::numeric_limits<std::uint64_t>::max())
      ++metrics_.lease_rounds_completed;
    commit_lease_metrics();
    lease_acquisitions_.clear();
    lease_activated_ = true;
    if (activating_lease_) {
      activating_lease_ = false;
      state_ = DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kPrepared;
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status maintain_lease(const TimePoint now) {
    if (!lease_acquisitions_.empty())
      return drive_lease_controls();
    if (!lease_activated_ || !next_lease_renewal_.has_value() || now < *next_lease_renewal_) {
      return common::Status::ok();
    }
    const common::Status started = start_lease_round(now, false);
    return started.is_ok() ? drive_lease_controls() : started;
  }

  [[nodiscard]] std::optional<TimePoint> lease_wake_deadline(const TimePoint now) const {
    if (lease_acquisitions_.empty())
      return next_lease_renewal_;
    TimePoint deadline = saturating_deadline(now, kLeaseActivePollInterval);
    for (const auto& acquisition : lease_acquisitions_)
      if (const auto wake = acquisition.wake_deadline(); wake.has_value())
        deadline = std::min(deadline, *wake);
    return deadline;
  }

  void stop_lease() {
    for (auto& acquisition : lease_acquisitions_)
      if (acquisition_running(acquisition))
        static_cast<void>(acquisition.cancel());
    commit_lease_metrics();
    lease_acquisitions_.clear();
    next_lease_renewal_.reset();
    lease_activated_ = false;
    activating_lease_ = false;
  }

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (state_ == DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kFailed ||
        state_ ==
            DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCancelled) {
      return failure_;
    }
    if (state_ ==
        DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCancelling) {
      return cancellation_delivery_failed();
    }
    return begin_cancellation(std::move(failure), TerminalAfterCancel::kFailed);
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
          if (state_ ==
              DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCancelling) {
            static_cast<void>(acquisition.cancel());
            ++completed;
            continue;
          }
          return fail(progress);
        }
      }
      if (acquisition.state() ==
          DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kFailed) {
        if (state_ ==
            DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCancelling) {
          ++completed;
          continue;
        }
        return fail(acquisition.failure());
      }
      completed +=
          acquisition.state() ==
                      DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::
                          kComplete ||
                  (state_ == DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::
                                 kCancelling &&
                   acquisition.state() ==
                       DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::
                           kCancelled)
              ? 1U
              : 0U;
    }
    refresh_control_metrics();
    if (completed != acquisitions_.size())
      return common::Status::ok();
    if (state_ == DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kPreparing)
      return publish_prepared();
    if (state_ ==
        DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kInstallingRoutes)
      return publish_routes_installed();
    if (state_ == DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCancelling)
      return publish_cancelled();
    return publish_sealed();
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
        const network::Ipv4Endpoint reducer_shuffle_endpoint =
            response->reducer_shuffle_endpoint.value_or(network::Ipv4Endpoint{});
        prepared.push_back({.node_id = config_.reducer_control_routes[index].node_id,
                            .endpoints = {reducer_shuffle_endpoint},
                            .tls_context = config_.reducer_control_routes[index].tls_context});
      }
      prepared_routes_ = std::move(prepared);
      metrics_.prepared_reducers = acquisitions_.size();
      commit_control_metrics();
      acquisitions_.clear();

      std::vector<DistributedVectorGroupedAggregateShuffleJobRoute> wire_routes;
      wire_routes.reserve(prepared_routes_.size());
      for (const auto& route : prepared_routes_) {
        if (route.endpoints.size() != 1U)
          return fail(status(common::StatusCode::kCorruption,
                             "prepared grouped shuffle route has invalid endpoint coverage"));
        wire_routes.push_back({.node_id = route.node_id, .endpoint = route.endpoints.front()});
      }
      std::vector<Acquisition> installs;
      installs.reserve(config_.reducer_control_routes.size());
      for (const auto& route : config_.reducer_control_routes) {
        auto acquisition =
            create_acquisition(config_, route,
                               DistributedVectorGroupedAggregateShuffleJobControlRequest{
                                   DistributedVectorGroupedAggregateShuffleJobInstallRoutes{
                                       .query_id = query_id_,
                                       .coordinator_node_id = config_.coordinator_node_id,
                                       .target_node_id = route.node_id,
                                       .routes = wire_routes}},
                               config_.route_install_retry, config_.execution_deadline);
        if (!acquisition.has_value())
          return fail(acquisition.error());
        installs.push_back(std::move(*acquisition));
      }
      acquisitions_ = std::move(installs);
      state_ =
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kInstallingRoutes;
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return fail(status(common::StatusCode::kResourceExhausted,
                         "grouped shuffle prepared-route allocation failed"));
    } catch (const std::length_error&) {
      return fail(status(common::StatusCode::kResourceExhausted,
                         "grouped shuffle prepared routes exceed container limits"));
    }
  }

  [[nodiscard]] common::Status publish_routes_installed() {
    for (auto& acquisition : acquisitions_) {
      auto response = acquisition.result();
      if (!response.has_value())
        return fail(response.error());
      if (response->status_code != common::StatusCode::kOk ||
          response->reducer_shuffle_endpoint.has_value()) {
        return fail(status(response->status_code == common::StatusCode::kOk
                               ? common::StatusCode::kCorruption
                               : response->status_code,
                           "grouped shuffle reducer route installation was not accepted"));
      }
    }
    metrics_.route_installed_reducers = acquisitions_.size();
    commit_control_metrics();
    acquisitions_.clear();
    return start_lease_round(TimePoint::clock::now(), true);
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
    if (config_.local_reducer_job_service != nullptr &&
        std::ranges::any_of(config_.reducer_control_routes, [&](const auto& route) {
          return route.node_id == config_.coordinator_node_id;
        })) {
      auto local_results = config_.local_reducer_job_service->take_local_result_streams(query_id_);
      if (!local_results.has_value())
        return fail(local_results.error());
      const common::Status accepted = result_.accept_local_streams(std::move(*local_results));
      if (!accepted.is_ok())
        return fail(accepted);
    }
    metrics_.sealed_reducers = acquisitions_.size();
    commit_control_metrics();
    acquisitions_.clear();
    state_ =
        DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCollectingResults;
    return common::Status::ok();
  }

  [[nodiscard]] common::Status publish_cancelled() {
    std::size_t acknowledged{};
    bool delivery_failed{};
    for (auto& acquisition : acquisitions_) {
      if (acquisition.state() !=
          DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kComplete) {
        delivery_failed = true;
        continue;
      }
      auto response = acquisition.result();
      if (!response.has_value() || response->status_code != common::StatusCode::kOk ||
          response->reducer_shuffle_endpoint.has_value()) {
        delivery_failed = true;
        continue;
      }
      ++acknowledged;
    }
    metrics_.cancelled_reducers = acknowledged;
    if (delivery_failed &&
        metrics_.cancel_delivery_failures != std::numeric_limits<std::uint64_t>::max()) {
      ++metrics_.cancel_delivery_failures;
    }
    commit_control_metrics();
    acquisitions_.clear();
    finish_cancellation();
    return common::Status::ok();
  }

  std::reference_wrapper<const DistributedVectorGroupedAggregateShuffleAuthority> authority_;
  common::Uuid query_id_;
  DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionConfig config_;
  std::vector<Acquisition> acquisitions_;
  std::vector<Acquisition> lease_acquisitions_;
  std::vector<pollfd> poll_descriptors_;
  DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution result_;
  std::vector<DistributedQueryNodeRoute> prepared_routes_;
  DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionMetrics metrics_;
  std::uint64_t committed_control_attempts_started_{};
  std::uint64_t committed_control_retries_started_{};
  std::uint64_t committed_control_failed_attempts_{};
  std::uint64_t committed_lease_attempts_started_{};
  std::uint64_t committed_lease_retries_started_{};
  std::uint64_t committed_lease_failed_attempts_{};
  DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState state_{
      DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kPreparing};
  std::optional<TerminalAfterCancel> terminal_after_cancel_;
  std::optional<TimePoint> next_lease_renewal_;
  bool lease_activated_{};
  bool activating_lease_{};
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
      !valid_retry(config.prepare_retry) || !valid_retry(config.route_install_retry) ||
      !valid_retry(config.seal_retry) || !valid_retry(config.cancel_retry) ||
      !valid_retry(config.lease_retry) || config.lease_duration.count() <= 0 ||
      config.lease_duration > distributed_vector_grouped_aggregate_shuffle_job_control_format::
                                  kMaximumExecutionTimeout ||
      config.lease_renew_interval.count() <= 0 ||
      config.lease_renew_interval >= config.lease_duration || config.execution_deadline <= now ||
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

  const bool has_local_reducer =
      std::ranges::any_of(config.reducer_control_routes, [&](const auto& route) {
        return route.node_id == config.coordinator_node_id;
      });
  if (has_local_reducer && config.local_reducer_job_service == nullptr) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "grouped shuffle local reducer service is absent"));
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

    std::vector<Acquisition> acquisitions;
    acquisitions.reserve(config.reducer_control_routes.size());
    for (const auto& route : config.reducer_control_routes) {
      auto owned_authority = decode_distributed_vector_grouped_aggregate_shuffle_authority_exact(
          encoded_authority->bytes(), config.carrier_limits.request.authority);
      if (!owned_authority.has_value())
        return common::make_unexpected(owned_authority.error());
      auto acquisition =
          create_acquisition(config, route,
                             DistributedVectorGroupedAggregateShuffleJobControlRequest{
                                 DistributedVectorGroupedAggregateShuffleJobPrepare{
                                     .coordinator_node_id = config.coordinator_node_id,
                                     .target_node_id = route.node_id,
                                     .coordinator_result_endpoint = result_endpoint,
                                     .execution_timeout = config.reducer_execution_timeout,
                                     .authority = std::move(*owned_authority),
                                     .result_schema = finalization_authority.result_schema()}},
                             config.prepare_retry, config.execution_deadline);
      if (!acquisition.has_value())
        return common::make_unexpected(acquisition.error());
      acquisitions.push_back(std::move(*acquisition));
    }
    std::vector<pollfd> descriptors(acquisitions.size() * 2U);
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
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kResultTaken)
    return common::Status::ok();
  auto now = Impl::TimePoint::clock::now();
  if (now >= impl.config_.execution_deadline)
    return impl.expire();

  const auto controls_active = [&impl]() noexcept {
    return impl.state_ ==
               DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kPreparing ||
           impl.state_ == DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::
                              kInstallingRoutes ||
           impl.state_ ==
               DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kSealing ||
           impl.state_ ==
               DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCancelling;
  };

  if (controls_active()) {
    common::Status progress = impl.drive_controls();
    if (!progress.is_ok())
      return progress;
  }

  if (impl.state_ !=
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kPreparing &&
      impl.state_ !=
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kInstallingRoutes &&
      impl.state_ !=
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCancelling) {
    const common::Status lease_progress = impl.maintain_lease(now);
    if (!lease_progress.is_ok())
      return lease_progress;
  }

  if (impl.state_ ==
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kPrepared &&
      impl.lease_acquisitions_.empty()) {
    return common::Status::ok();
  }

  if (controls_active() ||
      (impl.state_ != DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::
                          kCollectingResults &&
       !impl.lease_acquisitions_.empty())) {
    std::size_t count{};
    auto wait = bounded_wait(maximum_wait, now, impl.config_.execution_deadline);
    const auto append_descriptors = [&](const auto& acquisitions) {
      for (const auto& acquisition : acquisitions) {
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
    };
    append_descriptors(impl.acquisitions_);
    append_descriptors(impl.lease_acquisitions_);

    const int ready = ::poll(impl.poll_descriptors_.data(), static_cast<nfds_t>(count),
                             static_cast<int>(wait.count()));
    if (ready < 0 && errno != EINTR)
      return impl.fail(status(common::StatusCode::kIoError,
                              "polling grouped shuffle reducer-job coordinator failed"));
    if (Impl::TimePoint::clock::now() >= impl.config_.execution_deadline)
      return impl.expire();
    common::Status progress = common::Status::ok();
    if (controls_active())
      progress = impl.drive_controls();
    if (!progress.is_ok())
      return progress;
    if (impl.state_ !=
        DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCancelling) {
      progress = impl.maintain_lease(Impl::TimePoint::clock::now());
    }
    return progress;
  }

  auto result_wait = bounded_wait(maximum_wait, now, impl.config_.execution_deadline);
  if (const auto lease_wake = impl.lease_wake_deadline(now); lease_wake.has_value())
    result_wait = bounded_wait(result_wait, now, *lease_wake);
  const common::Status progress = impl.result_.poll_once(result_wait);
  impl.metrics_.result = impl.result_.metrics();
  if (!progress.is_ok())
    return impl.fail(progress);
  if (impl.result_.state() ==
      DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kComplete) {
    impl.stop_lease();
    impl.state_ = DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kComplete;
    return common::Status::ok();
  }
  common::Status lease_progress = impl.maintain_lease(Impl::TimePoint::clock::now());
  if (!lease_progress.is_ok())
    return lease_progress;
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
    std::vector<Acquisition> seals;
    seals.reserve(impl.config_.reducer_control_routes.size());
    for (const auto& route : impl.config_.reducer_control_routes) {
      auto acquisition = create_acquisition(
          impl.config_, route,
          DistributedVectorGroupedAggregateShuffleJobControlRequest{
              DistributedVectorGroupedAggregateShuffleJobSeal{
                  impl.query_id_, impl.config_.coordinator_node_id, route.node_id}},
          impl.config_.seal_retry, impl.config_.execution_deadline, true);
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
      DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCancelling)
    return common::Status::ok();
  if (impl.state_ ==
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kComplete ||
      impl.state_ ==
          DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kResultTaken)
    return common::Status::ok();
  return impl.begin_cancellation(status(common::StatusCode::kCancelled,
                                        "grouped shuffle reducer-job coordinator was cancelled"),
                                 Impl::TerminalAfterCancel::kCancelled);
}

std::optional<std::chrono::steady_clock::time_point>
DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::wake_deadline() const {
  if (!implementation_)
    return std::nullopt;
  const auto state = implementation_->state_;
  if (state == DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kFailed ||
      state == DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCancelled ||
      state == DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kComplete ||
      state == DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kResultTaken ||
      state == DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCancelling) {
    return std::nullopt;
  }
  return implementation_->lease_wake_deadline(Impl::TimePoint::clock::now());
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
