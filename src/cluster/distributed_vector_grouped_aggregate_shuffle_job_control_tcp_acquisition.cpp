#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_tcp_acquisition.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <memory>
#include <new>
#include <optional>
#include <poll.h>
#include <ranges>
#include <span>
#include <stdexcept>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}

template <typename Value>
[[nodiscard]] const Value* optional_pointer(const std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
}

using Acquisition = DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition;

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      Acquisition::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] bool zero_address(const std::array<std::uint8_t, 4U>& address) noexcept {
  return std::ranges::all_of(address, [](const std::uint8_t byte) { return byte == 0U; });
}

[[nodiscard]] bool retryable(const common::StatusCode code) noexcept {
  return code == common::StatusCode::kUnavailable || code == common::StatusCode::kIoError ||
         code == common::StatusCode::kResourceExhausted;
}

[[nodiscard]] Acquisition::TimePoint
saturating_add(const Acquisition::TimePoint now, const std::chrono::milliseconds delay) noexcept {
  const auto converted = std::chrono::duration_cast<Acquisition::TimePoint::duration>(delay);
  return now > Acquisition::TimePoint::max() - converted ? Acquisition::TimePoint::max()
                                                         : now + converted;
}

[[nodiscard]] std::chrono::milliseconds
bounded_wait(const std::chrono::milliseconds maximum_wait, const Acquisition::TimePoint now,
             const Acquisition::TimePoint deadline) noexcept {
  if (deadline <= now)
    return std::chrono::milliseconds{0};
  return std::min(maximum_wait,
                  std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now));
}

[[nodiscard]] raft::NodeId
target_node(const DistributedVectorGroupedAggregateShuffleJobControlRequest& request) noexcept {
  if (const auto* prepare =
          std::get_if<DistributedVectorGroupedAggregateShuffleJobPrepare>(&request)) {
    return prepare->target_node_id;
  }
  const auto* seal = std::get_if<DistributedVectorGroupedAggregateShuffleJobSeal>(&request);
  if (seal != nullptr)
    return seal->target_node_id;
  const auto* routes =
      std::get_if<DistributedVectorGroupedAggregateShuffleJobInstallRoutes>(&request);
  if (routes != nullptr)
    return routes->target_node_id;
  const auto* cancel = std::get_if<DistributedVectorGroupedAggregateShuffleJobCancel>(&request);
  if (cancel != nullptr)
    return cancel->target_node_id;
  const auto* renewal =
      std::get_if<DistributedVectorGroupedAggregateShuffleJobRenewLease>(&request);
  return renewal == nullptr ? 0U : renewal->target_node_id;
}

[[nodiscard]] bool
is_seal(const DistributedVectorGroupedAggregateShuffleJobControlRequest& request) noexcept {
  return std::holds_alternative<DistributedVectorGroupedAggregateShuffleJobSeal>(request);
}

[[nodiscard]] common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest>
encode_request(const DistributedVectorGroupedAggregateShuffleJobControlRequest& request) {
  if (const auto* prepare =
          std::get_if<DistributedVectorGroupedAggregateShuffleJobPrepare>(&request)) {
    return encode_distributed_vector_grouped_aggregate_shuffle_job_prepare_v1(*prepare);
  }
  if (const auto* seal = std::get_if<DistributedVectorGroupedAggregateShuffleJobSeal>(&request))
    return encode_distributed_vector_grouped_aggregate_shuffle_job_seal_v1(*seal);
  if (const auto* routes =
          std::get_if<DistributedVectorGroupedAggregateShuffleJobInstallRoutes>(&request)) {
    return encode_distributed_vector_grouped_aggregate_shuffle_job_install_routes_v2(*routes);
  }
  if (const auto* cancel = std::get_if<DistributedVectorGroupedAggregateShuffleJobCancel>(&request))
    return encode_distributed_vector_grouped_aggregate_shuffle_job_cancel_v3(*cancel);
  return encode_distributed_vector_grouped_aggregate_shuffle_job_renew_lease_v4(
      std::get<DistributedVectorGroupedAggregateShuffleJobRenewLease>(request));
}

} // namespace

class DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::Impl {
public:
  Impl(DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionConfig configured,
       std::vector<std::byte> encoded_request)
      : config(std::move(configured)), request_bytes(std::move(encoded_request)),
        next_backoff(config.retry.initial_backoff) {}

  [[nodiscard]] DistributedVectorGroupedAggregateShuffleJobControlTcpClient*
  active_client() noexcept {
    return client.has_value() ? &client.operator*() : nullptr;
  }

  [[nodiscard]] const DistributedVectorGroupedAggregateShuffleJobControlTcpClient*
  active_client() const noexcept {
    return client.has_value() ? &client.operator*() : nullptr;
  }

  [[nodiscard]] common::Status fail(common::Status failure) {
    client.reset();
    acquisition_metrics.active_attempts = 0U;
    acquisition_failure = std::move(failure);
    acquisition_state =
        DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kFailed;
    next_attempt_not_before.reset();
    return acquisition_failure;
  }

  [[nodiscard]] common::Status expire() {
    return fail(status(common::StatusCode::kCancelled,
                       "grouped shuffle reducer-job control execution deadline expired"));
  }

  [[nodiscard]] bool expired(const TimePoint now) const noexcept {
    return config.execution_deadline.has_value() && now >= *config.execution_deadline;
  }

  [[nodiscard]] common::Status schedule_failure(common::Status failure, const TimePoint now) {
    client.reset();
    acquisition_metrics.active_attempts = 0U;
    ++acquisition_metrics.failed_attempts;
    if (expired(now))
      return expire();
    if (!retryable(failure.code()) ||
        acquisition_metrics.attempts_started >= config.retry.maximum_attempts) {
      return fail(std::move(failure));
    }
    acquisition_failure = std::move(failure);
    next_attempt_not_before = saturating_add(now, next_backoff);
    if (config.execution_deadline.has_value() &&
        *next_attempt_not_before > *config.execution_deadline) {
      next_attempt_not_before = config.execution_deadline;
    }
    if (next_backoff < config.retry.maximum_backoff) {
      const auto current = next_backoff.count();
      const auto maximum = config.retry.maximum_backoff.count();
      next_backoff = current > maximum / 2
                         ? config.retry.maximum_backoff
                         : std::min(next_backoff * 2, config.retry.maximum_backoff);
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status start_attempt(const TimePoint now) {
    if (expired(now))
      return expire();
    const std::size_t attempt_index = acquisition_metrics.attempts_started;
    const network::Ipv4Endpoint endpoint =
        config.route.endpoints[attempt_index % config.route.endpoints.size()];
    ++acquisition_metrics.attempts_started;
    if (attempt_index > 0U)
      ++acquisition_metrics.retries_started;
    next_attempt_not_before.reset();
    const common::ByteView frozen{request_bytes};
    const auto magic = frozen.first(
        distributed_vector_grouped_aggregate_shuffle_job_control_format::kRequestMagic.size());
    auto request =
        std::ranges::equal(
            magic,
            distributed_vector_grouped_aggregate_shuffle_job_control_v2_format::kRequestMagic)
            ? decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v2_exact(
                  frozen, config.carrier_limits.request)
        : std::ranges::equal(
              magic,
              distributed_vector_grouped_aggregate_shuffle_job_control_v3_format::kRequestMagic)
            ? decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v3_exact(
                  frozen, config.carrier_limits.request)
        : std::ranges::equal(
              magic,
              distributed_vector_grouped_aggregate_shuffle_job_control_v4_format::kRequestMagic)
            ? decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v4_exact(
                  frozen, config.carrier_limits.request)
            : decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v1_exact(
                  frozen, config.carrier_limits.request);
    if (!request.has_value())
      return schedule_failure(request.error(), now);
    auto started = DistributedVectorGroupedAggregateShuffleJobControlTcpClient::begin(
        {.remote_endpoint = endpoint,
         .tls_context = config.route.tls_context,
         .carrier = {.authenticator = config.authenticator,
                     .node_authorizer = config.node_authorizer,
                     .peer_ipv4_address = endpoint.address,
                     .request = std::move(*request),
                     .limits = config.carrier_limits},
         .connect_timeout = config.connect_timeout},
        now);
    if (!started.has_value())
      return schedule_failure(started.error(), now);
    client.emplace(std::move(*started));
    acquisition_metrics.active_attempts = 1U;
    return common::Status::ok();
  }

  [[nodiscard]] common::Status publish_or_retry(const TimePoint now) {
    DistributedVectorGroupedAggregateShuffleJobControlTcpClient* active = active_client();
    if (active == nullptr) {
      return fail(status(common::StatusCode::kCorruption,
                         "grouped shuffle reducer-job control lost its active attempt"));
    }
    if (active->state() ==
        DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kFailed) {
      return schedule_failure(active->failure(), now);
    }
    if (active->state() !=
        DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kComplete) {
      return common::Status::ok();
    }
    auto response = active->result();
    if (!response.has_value())
      return schedule_failure(response.error(), now);
    if (config.retry_unavailable_response &&
        response->status_code == common::StatusCode::kUnavailable) {
      return schedule_failure(status(common::StatusCode::kUnavailable,
                                     "grouped shuffle reducer-job control response is not ready"),
                              now);
    }
    acquisition_result.emplace(*response);
    client.reset();
    acquisition_metrics.active_attempts = 0U;
    ++acquisition_metrics.completed_attempts;
    acquisition_state =
        DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kComplete;
    return common::Status::ok();
  }

  DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionConfig config;
  std::vector<std::byte> request_bytes;
  std::optional<DistributedVectorGroupedAggregateShuffleJobControlTcpClient> client;
  std::optional<TimePoint> next_attempt_not_before;
  std::chrono::milliseconds next_backoff;
  DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionMetrics acquisition_metrics;
  DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState acquisition_state{
      DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kRunning};
  std::optional<DistributedVectorGroupedAggregateShuffleJobControlResponse> acquisition_result;
  common::Status acquisition_failure{common::StatusCode::kInternal,
                                     "grouped shuffle reducer-job control has not failed"};
};

DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::
    DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition() noexcept = default;
DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::
    DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::
    ~DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition() = default;
DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::
    DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition(
        DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition&&) noexcept = default;
DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition&
DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::operator=(
    DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition>
DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::create(
    DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionConfig config) {
  const auto maximum_backoff =
      std::chrono::duration_cast<std::chrono::milliseconds>(TimePoint::duration::max());
  if (config.route.node_id == 0U || config.route.node_id != target_node(config.request) ||
      config.route.endpoints.empty() || config.route.endpoints.size() > 1024U ||
      config.route.tls_context == nullptr || config.authenticator == nullptr ||
      config.node_authorizer == nullptr || !valid_timeout(config.connect_timeout) ||
      !valid_timeout(config.carrier_limits.handshake_timeout) ||
      !valid_timeout(config.carrier_limits.exchange_timeout) ||
      config.retry.maximum_attempts == 0U || config.retry.maximum_attempts > 1024U ||
      config.retry.initial_backoff.count() <= 0 ||
      config.retry.maximum_backoff < config.retry.initial_backoff ||
      config.retry.maximum_backoff > maximum_backoff ||
      (config.retry_unavailable_response && !is_seal(config.request))) {
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument,
               "grouped shuffle reducer-job control acquisition configuration is invalid"));
  }
  for (std::size_t index = 0U; index < config.route.endpoints.size(); ++index) {
    const network::Ipv4Endpoint& endpoint = config.route.endpoints[index];
    const std::span<const network::Ipv4Endpoint> prior{config.route.endpoints.data(), index};
    if (endpoint.port == 0U || zero_address(endpoint.address) ||
        std::ranges::find(prior, endpoint) != prior.end()) {
      return common::make_unexpected(
          status(common::StatusCode::kInvalidArgument,
                 "grouped shuffle reducer-job control route is invalid or duplicated"));
    }
  }
  auto encoded = encode_request(config.request);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  if (encoded->bytes().size() > config.carrier_limits.request.maximum_frame_length) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted,
               "grouped shuffle reducer-job control request exceeds configured frame limit"));
  }
  try {
    std::vector<std::byte> request_bytes(encoded->bytes().begin(), encoded->bytes().end());
    return DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition{
        std::make_unique<Impl>(std::move(config), std::move(request_bytes))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "grouped shuffle reducer-job control allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted,
               "grouped shuffle reducer-job control configuration exceeds container limits"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  if (!implementation_) {
    return status(common::StatusCode::kInvalidArgument,
                  "grouped shuffle reducer-job control acquisition is empty");
  }
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX) {
    return status(common::StatusCode::kInvalidArgument,
                  "grouped shuffle reducer-job control poll timeout is invalid");
  }
  Impl& impl = *implementation_;
  if (impl.acquisition_state ==
          DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kFailed ||
      impl.acquisition_state ==
          DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kCancelled) {
    return impl.acquisition_failure;
  }
  if (impl.acquisition_state ==
      DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kComplete) {
    return common::Status::ok();
  }

  auto now = TimePoint::clock::now();
  if (impl.expired(now))
    return impl.expire();
  if (!impl.client.has_value()) {
    if (impl.next_attempt_not_before.has_value() && now < *impl.next_attempt_not_before) {
      auto wait = bounded_wait(maximum_wait, now, *impl.next_attempt_not_before);
      if (impl.config.execution_deadline.has_value())
        wait = bounded_wait(wait, now, *impl.config.execution_deadline);
      if (::poll(nullptr, 0U, static_cast<int>(wait.count())) < 0 && errno != EINTR) {
        return impl.fail(status(common::StatusCode::kIoError,
                                "polling grouped shuffle reducer-job backoff failed"));
      }
      now = TimePoint::clock::now();
      if (impl.expired(now))
        return impl.expire();
      if (now < *impl.next_attempt_not_before)
        return common::Status::ok();
    }
    common::Status started = impl.start_attempt(now);
    if (!started.is_ok() ||
        impl.acquisition_state !=
            DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kRunning) {
      return started;
    }
    if (!impl.client.has_value())
      return common::Status::ok();
  }

  auto* active = impl.active_client();
  if (active == nullptr)
    return common::Status::ok();
  const auto interest = active->interest();
  pollfd descriptor{.fd = active->descriptor(),
                    .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                 (interest.want_write ? POLLOUT : 0))};
  auto wait = maximum_wait;
  if (const auto deadline = active->deadline(); deadline.has_value())
    wait = bounded_wait(wait, now, *deadline);
  if (impl.config.execution_deadline.has_value())
    wait = bounded_wait(wait, now, *impl.config.execution_deadline);
  const int ready = ::poll(&descriptor, 1U, static_cast<int>(wait.count()));
  if (ready < 0 && errno != EINTR) {
    return impl.schedule_failure(
        status(common::StatusCode::kIoError,
               "polling grouped shuffle reducer-job control acquisition failed"),
        TimePoint::clock::now());
  }
  now = TimePoint::clock::now();
  if (impl.expired(now))
    return impl.expire();
  const bool readable = ready > 0 && (descriptor.revents & POLLIN) != 0;
  const bool writable = ready > 0 && (descriptor.revents & POLLOUT) != 0;
  if (ready > 0 && (descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0 && !readable &&
      !writable) {
    return impl.schedule_failure(
        status(common::StatusCode::kUnavailable,
               "grouped shuffle reducer-job control connection became unavailable"),
        now);
  }
  const common::Status progress = active->on_ready(readable, writable, now);
  if (!progress.is_ok() &&
      active->state() !=
          DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kFailed) {
    return impl.fail(progress);
  }
  return impl.publish_or_retry(now);
}

common::Status DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::cancel() {
  if (!implementation_) {
    return status(common::StatusCode::kInvalidArgument,
                  "grouped shuffle reducer-job control acquisition is empty");
  }
  Impl& impl = *implementation_;
  if (impl.acquisition_state ==
          DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kFailed ||
      impl.acquisition_state ==
          DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kCancelled) {
    return impl.acquisition_failure;
  }
  if (impl.acquisition_state ==
      DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kComplete) {
    return status(common::StatusCode::kInvalidArgument,
                  "completed grouped shuffle reducer-job control cannot be cancelled");
  }
  impl.client.reset();
  impl.acquisition_metrics.active_attempts = 0U;
  impl.next_attempt_not_before.reset();
  impl.acquisition_failure =
      status(common::StatusCode::kCancelled, "grouped shuffle reducer-job control was cancelled");
  impl.acquisition_state =
      DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kCancelled;
  return impl.acquisition_failure;
}

DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState
DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::state() const noexcept {
  return implementation_
             ? implementation_->acquisition_state
             : DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kFailed;
}

DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionMetrics
DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::metrics() const noexcept {
  return implementation_
             ? implementation_->acquisition_metrics
             : DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionMetrics{};
}

int DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::descriptor() const noexcept {
  const auto* active = implementation_ ? implementation_->active_client() : nullptr;
  return active != nullptr ? active->descriptor() : -1;
}

DistributedVectorGroupedAggregateShuffleJobControlTlsInterest
DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::interest() const noexcept {
  const auto* active = implementation_ ? implementation_->active_client() : nullptr;
  return active != nullptr ? active->interest()
                           : DistributedVectorGroupedAggregateShuffleJobControlTlsInterest{};
}

std::optional<DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::TimePoint>
DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::wake_deadline() const noexcept {
  if (!implementation_ ||
      implementation_->acquisition_state !=
          DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kRunning) {
    return std::nullopt;
  }
  std::optional<TimePoint> deadline = implementation_->config.execution_deadline;
  if (const auto* active = implementation_->active_client(); active != nullptr) {
    const auto carrier_deadline = active->deadline();
    if (carrier_deadline.has_value() && (!deadline.has_value() || *carrier_deadline < *deadline))
      deadline = carrier_deadline;
    return deadline;
  }
  const auto* next_attempt = optional_pointer(implementation_->next_attempt_not_before);
  if (next_attempt != nullptr && (!deadline.has_value() || *next_attempt < *deadline)) {
    deadline = *next_attempt;
  }
  return deadline;
}

common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponse>
DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::result() const {
  if (!implementation_) {
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument,
               "grouped shuffle reducer-job control acquisition is empty"));
  }
  if (implementation_->acquisition_state ==
          DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kFailed ||
      implementation_->acquisition_state ==
          DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kCancelled) {
    return common::make_unexpected(implementation_->acquisition_failure);
  }
  if (implementation_->acquisition_state !=
      DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisitionState::kComplete) {
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument,
               "grouped shuffle reducer-job control response is unavailable"));
  }
  const auto* result = optional_pointer(implementation_->acquisition_result);
  if (result == nullptr) {
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument,
               "grouped shuffle reducer-job control response is unavailable"));
  }
  return *result;
}

const common::Status&
DistributedVectorGroupedAggregateShuffleJobControlTcpAcquisition::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "grouped shuffle reducer-job control acquisition is empty"};
  return implementation_ ? implementation_->acquisition_failure : empty;
}

} // namespace chronos::cluster
