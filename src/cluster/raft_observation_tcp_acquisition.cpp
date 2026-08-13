#include "chronos/cluster/raft_observation_tcp_acquisition.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <new>
#include <optional>
#include <poll.h>
#include <ranges>
#include <span>
#include <stdexcept>
#include <utility>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status status(common::StatusCode code, const char* message) {
  return {code, message};
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      RaftObservationTcpClient::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] bool zero_address(const std::array<std::uint8_t, 4U>& address) noexcept {
  return std::ranges::all_of(address, [](const std::uint8_t byte) { return byte == 0U; });
}

[[nodiscard]] bool retryable(const common::StatusCode code) noexcept {
  return code == common::StatusCode::kUnavailable || code == common::StatusCode::kIoError ||
         code == common::StatusCode::kResourceExhausted;
}

[[nodiscard]] RaftObservationTcpClient::TimePoint
saturating_add(const RaftObservationTcpClient::TimePoint now,
               const std::chrono::milliseconds delay) noexcept {
  const auto converted =
      std::chrono::duration_cast<RaftObservationTcpClient::TimePoint::duration>(delay);
  return now > RaftObservationTcpClient::TimePoint::max() - converted
             ? RaftObservationTcpClient::TimePoint::max()
             : now + converted;
}

[[nodiscard]] std::chrono::milliseconds
bounded_wait(const std::chrono::milliseconds maximum_wait,
             const RaftObservationTcpClient::TimePoint now,
             const RaftObservationTcpClient::TimePoint deadline) noexcept {
  if (deadline <= now)
    return std::chrono::milliseconds{0};
  return std::min(maximum_wait,
                  std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now));
}

} // namespace

class RaftObservationTcpAcquisition::Impl {
public:
  using TimePoint = RaftObservationTcpClient::TimePoint;

  explicit Impl(RaftObservationTcpAcquisitionConfig configured) noexcept
      : config(std::move(configured)), next_backoff(config.retry.initial_backoff) {}

  [[nodiscard]] common::Status fail(common::Status failure) {
    client.reset();
    acquisition_metrics.active_attempts = 0U;
    acquisition_failure = std::move(failure);
    acquisition_state = RaftObservationTcpAcquisitionState::kFailed;
    next_attempt_not_before.reset();
    return acquisition_failure;
  }

  [[nodiscard]] common::Status schedule_failure(common::Status failure, const TimePoint now) {
    client.reset();
    acquisition_metrics.active_attempts = 0U;
    ++acquisition_metrics.failed_attempts;
    if (!retryable(failure.code()) ||
        acquisition_metrics.attempts_started >= config.retry.maximum_attempts) {
      return fail(std::move(failure));
    }
    acquisition_failure = std::move(failure);
    next_attempt_not_before = saturating_add(now, next_backoff);
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
    const std::size_t attempt_index = acquisition_metrics.attempts_started;
    const network::Ipv4Endpoint endpoint =
        config.route.endpoints[attempt_index % config.route.endpoints.size()];
    ++acquisition_metrics.attempts_started;
    if (attempt_index > 0U)
      ++acquisition_metrics.retries_started;
    next_attempt_not_before.reset();
    auto started =
        RaftObservationTcpClient::begin({.remote_endpoint = endpoint,
                                         .tls_context = config.route.tls_context,
                                         .carrier = {.authenticator = config.authenticator,
                                                     .node_authorizer = config.node_authorizer,
                                                     .peer_ipv4_address = endpoint.address,
                                                     .request = config.request,
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
    if (client->state() == RaftObservationTcpClientState::kFailed)
      return schedule_failure(client->failure(), now);
    if (client->state() != RaftObservationTcpClientState::kComplete)
      return common::Status::ok();
    auto observed = client->result();
    if (!observed.has_value())
      return schedule_failure(observed.error(), now);
    acquisition_result.emplace(std::move(*observed));
    client.reset();
    acquisition_metrics.active_attempts = 0U;
    ++acquisition_metrics.completed_attempts;
    acquisition_state = RaftObservationTcpAcquisitionState::kComplete;
    return common::Status::ok();
  }

  RaftObservationTcpAcquisitionConfig config;
  std::optional<RaftObservationTcpClient> client;
  std::optional<TimePoint> next_attempt_not_before;
  std::chrono::milliseconds next_backoff;
  RaftObservationTcpAcquisitionMetrics acquisition_metrics;
  RaftObservationTcpAcquisitionState acquisition_state{
      RaftObservationTcpAcquisitionState::kRunning};
  std::optional<raft::RaftGroupObservation> acquisition_result;
  common::Status acquisition_failure{common::StatusCode::kInternal,
                                     "Raft observation acquisition has not failed"};
};

RaftObservationTcpAcquisition::RaftObservationTcpAcquisition() noexcept = default;
RaftObservationTcpAcquisition::RaftObservationTcpAcquisition(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftObservationTcpAcquisition::~RaftObservationTcpAcquisition() = default;
RaftObservationTcpAcquisition::RaftObservationTcpAcquisition(
    RaftObservationTcpAcquisition&&) noexcept = default;
RaftObservationTcpAcquisition&
RaftObservationTcpAcquisition::operator=(RaftObservationTcpAcquisition&&) noexcept = default;

common::Result<RaftObservationTcpAcquisition>
RaftObservationTcpAcquisition::create(RaftObservationTcpAcquisitionConfig config) {
  const auto maximum_supported_backoff = std::chrono::duration_cast<std::chrono::milliseconds>(
      RaftObservationTcpClient::TimePoint::duration::max());
  if (config.route.node_id == 0U || config.route.node_id != config.request.target_node_id ||
      config.route.endpoints.empty() || config.route.endpoints.size() > 1024U ||
      config.route.tls_context == nullptr || config.authenticator == nullptr ||
      config.node_authorizer == nullptr || !valid_timeout(config.connect_timeout) ||
      !valid_timeout(config.carrier_limits.handshake_timeout) ||
      !valid_timeout(config.carrier_limits.exchange_timeout) ||
      config.retry.maximum_attempts == 0U || config.retry.maximum_attempts > 1024U ||
      config.retry.initial_backoff.count() <= 0 ||
      config.retry.maximum_backoff < config.retry.initial_backoff ||
      config.retry.maximum_backoff > maximum_supported_backoff) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft observation acquisition configuration is invalid"));
  }
  for (std::size_t index = 0U; index < config.route.endpoints.size(); ++index) {
    const network::Ipv4Endpoint& endpoint = config.route.endpoints[index];
    const std::span<const network::Ipv4Endpoint> prior{config.route.endpoints.data(), index};
    if (endpoint.port == 0U || zero_address(endpoint.address) ||
        std::ranges::find(prior, endpoint) != prior.end()) {
      return common::make_unexpected(
          status(common::StatusCode::kInvalidArgument,
                 "Raft observation route address is invalid or duplicated"));
    }
  }
  auto request = encode_raft_observation_request_v1(config.request);
  if (!request.has_value())
    return common::make_unexpected(request.error());
  auto limits = RaftObservationResponseReader::create(config.carrier_limits.transport);
  if (!limits.has_value())
    return common::make_unexpected(limits.error());
  try {
    return RaftObservationTcpAcquisition{std::make_unique<Impl>(std::move(config))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft observation acquisition allocation failed"));
  }
}

common::Status
RaftObservationTcpAcquisition::poll_once(const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft observation acquisition is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return status(common::StatusCode::kInvalidArgument,
                  "Raft observation acquisition poll timeout is invalid");
  Impl& impl = *implementation_;
  if (impl.acquisition_state == RaftObservationTcpAcquisitionState::kFailed ||
      impl.acquisition_state == RaftObservationTcpAcquisitionState::kCancelled) {
    return impl.acquisition_failure;
  }
  if (impl.acquisition_state == RaftObservationTcpAcquisitionState::kComplete)
    return common::Status::ok();

  auto now = Impl::TimePoint::clock::now();
  if (!impl.client.has_value()) {
    if (impl.next_attempt_not_before.has_value() && now < *impl.next_attempt_not_before) {
      const auto wait = bounded_wait(maximum_wait, now, *impl.next_attempt_not_before);
      if (::poll(nullptr, 0U, static_cast<int>(wait.count())) < 0 && errno != EINTR)
        return impl.fail(
            status(common::StatusCode::kIoError, "polling Raft observation retry backoff failed"));
      now = Impl::TimePoint::clock::now();
      if (now < *impl.next_attempt_not_before)
        return common::Status::ok();
    }
    const common::Status started = impl.start_attempt(now);
    if (!started.is_ok() ||
        impl.acquisition_state != RaftObservationTcpAcquisitionState::kRunning) {
      return started;
    }
    if (!impl.client.has_value())
      return common::Status::ok();
  }

  const auto interest = impl.client->interest();
  pollfd descriptor{.fd = impl.client->descriptor(),
                    .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                 (interest.want_write ? POLLOUT : 0))};
  auto wait = maximum_wait;
  const auto deadline = impl.client->deadline();
  if (deadline.has_value())
    wait = bounded_wait(wait, now, *deadline);
  const int ready = ::poll(&descriptor, 1U, static_cast<int>(wait.count()));
  if (ready < 0 && errno != EINTR) {
    now = Impl::TimePoint::clock::now();
    return impl.schedule_failure(
        status(common::StatusCode::kIoError, "polling Raft observation acquisition failed"), now);
  }
  now = Impl::TimePoint::clock::now();
  const bool readable = ready > 0 && (descriptor.revents & POLLIN) != 0;
  const bool writable = ready > 0 && (descriptor.revents & POLLOUT) != 0;
  if (ready > 0 && (descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0 && !readable &&
      !writable) {
    return impl.schedule_failure(
        status(common::StatusCode::kUnavailable, "Raft observation connection became unavailable"),
        now);
  }
  const common::Status progress = impl.client->on_ready(readable, writable, now);
  if (!progress.is_ok() && impl.client->state() != RaftObservationTcpClientState::kFailed)
    return impl.fail(progress);
  return impl.publish_or_retry(now);
}

common::Status RaftObservationTcpAcquisition::cancel() {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft observation acquisition is empty");
  Impl& impl = *implementation_;
  if (impl.acquisition_state == RaftObservationTcpAcquisitionState::kFailed ||
      impl.acquisition_state == RaftObservationTcpAcquisitionState::kCancelled) {
    return impl.acquisition_failure;
  }
  if (impl.acquisition_state == RaftObservationTcpAcquisitionState::kComplete) {
    return status(common::StatusCode::kInvalidArgument,
                  "completed Raft observation acquisition cannot be cancelled");
  }
  impl.client.reset();
  impl.acquisition_metrics.active_attempts = 0U;
  impl.next_attempt_not_before.reset();
  impl.acquisition_failure =
      status(common::StatusCode::kCancelled, "Raft observation acquisition was cancelled");
  impl.acquisition_state = RaftObservationTcpAcquisitionState::kCancelled;
  return impl.acquisition_failure;
}

RaftObservationTcpAcquisitionState RaftObservationTcpAcquisition::state() const noexcept {
  return implementation_ ? implementation_->acquisition_state
                         : RaftObservationTcpAcquisitionState::kFailed;
}

RaftObservationTcpAcquisitionMetrics RaftObservationTcpAcquisition::metrics() const noexcept {
  return implementation_ ? implementation_->acquisition_metrics
                         : RaftObservationTcpAcquisitionMetrics{};
}

common::Result<raft::RaftGroupObservation> RaftObservationTcpAcquisition::result() const {
  if (!implementation_)
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "Raft observation acquisition is empty"));
  if (implementation_->acquisition_state == RaftObservationTcpAcquisitionState::kFailed ||
      implementation_->acquisition_state == RaftObservationTcpAcquisitionState::kCancelled) {
    return common::make_unexpected(implementation_->acquisition_failure);
  }
  if (implementation_->acquisition_state != RaftObservationTcpAcquisitionState::kComplete ||
      !implementation_->acquisition_result.has_value()) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft observation acquisition result is unavailable"));
  }
  try {
    return *implementation_->acquisition_result;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft observation acquisition result allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft observation acquisition result is too large"));
  }
}

const common::Status& RaftObservationTcpAcquisition::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "Raft observation acquisition is empty"};
  return implementation_ ? implementation_->acquisition_failure : empty;
}

} // namespace chronos::cluster
