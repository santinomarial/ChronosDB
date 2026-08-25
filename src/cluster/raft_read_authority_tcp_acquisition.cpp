#include "chronos/cluster/raft_read_authority_tcp_acquisition.hpp"

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

[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      RaftReadAuthorityTcpClient::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] bool zero_address(const std::array<std::uint8_t, 4U>& address) noexcept {
  return std::ranges::all_of(address, [](const std::uint8_t byte) { return byte == 0U; });
}

[[nodiscard]] bool retryable(const common::StatusCode code) noexcept {
  return code == common::StatusCode::kUnavailable || code == common::StatusCode::kIoError ||
         code == common::StatusCode::kResourceExhausted;
}

[[nodiscard]] RaftReadAuthorityTcpClient::TimePoint
saturating_add(const RaftReadAuthorityTcpClient::TimePoint now,
               const std::chrono::milliseconds delay) noexcept {
  const auto converted =
      std::chrono::duration_cast<RaftReadAuthorityTcpClient::TimePoint::duration>(delay);
  return now > RaftReadAuthorityTcpClient::TimePoint::max() - converted
             ? RaftReadAuthorityTcpClient::TimePoint::max()
             : now + converted;
}

[[nodiscard]] std::chrono::milliseconds
bounded_wait(const std::chrono::milliseconds maximum_wait,
             const RaftReadAuthorityTcpClient::TimePoint now,
             const RaftReadAuthorityTcpClient::TimePoint deadline) noexcept {
  if (deadline <= now)
    return std::chrono::milliseconds{0};
  return std::min(maximum_wait,
                  std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now));
}

} // namespace

class RaftReadAuthorityTcpAcquisition::Impl {
public:
  using TimePoint = RaftReadAuthorityTcpClient::TimePoint;

  explicit Impl(RaftReadAuthorityTcpAcquisitionConfig configured)
      : config(std::move(configured)), next_backoff(config.retry.initial_backoff) {}

  [[nodiscard]] RaftReadAuthorityTcpClient* active_client() noexcept {
    return client.has_value() ? &client.operator*() : nullptr;
  }

  [[nodiscard]] const RaftReadAuthorityTcpClient* active_client() const noexcept {
    return client.has_value() ? &client.operator*() : nullptr;
  }

  [[nodiscard]] common::Status fail(common::Status failure) {
    client.reset();
    acquisition_metrics.active_attempts = 0U;
    acquisition_failure = std::move(failure);
    acquisition_state = RaftReadAuthorityTcpAcquisitionState::kFailed;
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
        RaftReadAuthorityTcpClient::begin({.remote_endpoint = endpoint,
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
    RaftReadAuthorityTcpClient* active = active_client();
    if (active == nullptr) {
      return fail(status(common::StatusCode::kCorruption,
                         "Raft read-authority acquisition lost its active attempt"));
    }
    if (active->state() == RaftReadAuthorityTcpClientState::kFailed)
      return schedule_failure(active->failure(), now);
    if (active->state() != RaftReadAuthorityTcpClientState::kComplete)
      return common::Status::ok();
    auto authority = active->result();
    if (!authority.has_value())
      return schedule_failure(authority.error(), now);
    acquisition_result.emplace(std::move(*authority));
    client.reset();
    acquisition_metrics.active_attempts = 0U;
    ++acquisition_metrics.completed_attempts;
    acquisition_state = RaftReadAuthorityTcpAcquisitionState::kComplete;
    return common::Status::ok();
  }

  RaftReadAuthorityTcpAcquisitionConfig config;
  std::optional<RaftReadAuthorityTcpClient> client;
  std::optional<TimePoint> next_attempt_not_before;
  std::chrono::milliseconds next_backoff;
  RaftReadAuthorityTcpAcquisitionMetrics acquisition_metrics;
  RaftReadAuthorityTcpAcquisitionState acquisition_state{
      RaftReadAuthorityTcpAcquisitionState::kRunning};
  std::optional<RaftReadAuthority> acquisition_result;
  common::Status acquisition_failure{common::StatusCode::kInternal,
                                     "Raft read-authority acquisition has not failed"};
};

RaftReadAuthorityTcpAcquisition::RaftReadAuthorityTcpAcquisition() noexcept = default;
RaftReadAuthorityTcpAcquisition::RaftReadAuthorityTcpAcquisition(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftReadAuthorityTcpAcquisition::~RaftReadAuthorityTcpAcquisition() = default;
RaftReadAuthorityTcpAcquisition::RaftReadAuthorityTcpAcquisition(
    RaftReadAuthorityTcpAcquisition&&) noexcept = default;
RaftReadAuthorityTcpAcquisition&
RaftReadAuthorityTcpAcquisition::operator=(RaftReadAuthorityTcpAcquisition&&) noexcept = default;

common::Result<RaftReadAuthorityTcpAcquisition>
RaftReadAuthorityTcpAcquisition::create(RaftReadAuthorityTcpAcquisitionConfig config) {
  const auto maximum_supported_backoff = std::chrono::duration_cast<std::chrono::milliseconds>(
      RaftReadAuthorityTcpClient::TimePoint::duration::max());
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
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument,
               "Raft read-authority acquisition configuration is invalid"));
  }
  for (std::size_t index = 0U; index < config.route.endpoints.size(); ++index) {
    const network::Ipv4Endpoint& endpoint = config.route.endpoints[index];
    const std::span<const network::Ipv4Endpoint> prior{config.route.endpoints.data(), index};
    if (endpoint.port == 0U || zero_address(endpoint.address) ||
        std::ranges::find(prior, endpoint) != prior.end()) {
      return common::make_unexpected(
          status(common::StatusCode::kInvalidArgument,
                 "Raft read-authority route address is invalid or duplicated"));
    }
  }
  auto request = encode_raft_read_authority_request_v1(config.request);
  if (!request.has_value())
    return common::make_unexpected(request.error());
  auto limits = RaftReadAuthorityResponseReader::create(config.carrier_limits.transport);
  if (!limits.has_value())
    return common::make_unexpected(limits.error());
  try {
    return RaftReadAuthorityTcpAcquisition{std::make_unique<Impl>(std::move(config))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft read-authority acquisition allocation failed"));
  }
}

common::Status
RaftReadAuthorityTcpAcquisition::poll_once(const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft read-authority acquisition is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX) {
    return status(common::StatusCode::kInvalidArgument,
                  "Raft read-authority acquisition poll timeout is invalid");
  }
  Impl& impl = *implementation_;
  if (impl.acquisition_state == RaftReadAuthorityTcpAcquisitionState::kFailed ||
      impl.acquisition_state == RaftReadAuthorityTcpAcquisitionState::kCancelled) {
    return impl.acquisition_failure;
  }
  if (impl.acquisition_state == RaftReadAuthorityTcpAcquisitionState::kComplete)
    return common::Status::ok();

  auto now = Impl::TimePoint::clock::now();
  if (!impl.client.has_value()) {
    if (impl.next_attempt_not_before.has_value() && now < *impl.next_attempt_not_before) {
      const auto wait = bounded_wait(maximum_wait, now, *impl.next_attempt_not_before);
      if (::poll(nullptr, 0U, static_cast<int>(wait.count())) < 0 && errno != EINTR) {
        return impl.fail(status(common::StatusCode::kIoError,
                                "polling Raft read-authority retry backoff failed"));
      }
      now = Impl::TimePoint::clock::now();
      if (now < *impl.next_attempt_not_before)
        return common::Status::ok();
    }
    common::Status started = impl.start_attempt(now);
    if (!started.is_ok() ||
        impl.acquisition_state != RaftReadAuthorityTcpAcquisitionState::kRunning) {
      return started;
    }
    if (!impl.client.has_value())
      return common::Status::ok();
  }

  RaftReadAuthorityTcpClient* active = impl.active_client();
  if (active == nullptr)
    return common::Status::ok();
  const auto interest = active->interest();
  pollfd descriptor{.fd = active->descriptor(),
                    .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                 (interest.want_write ? POLLOUT : 0))};
  auto wait = maximum_wait;
  const auto deadline = active->deadline();
  if (deadline.has_value())
    wait = bounded_wait(wait, now, *deadline);
  const int ready = ::poll(&descriptor, 1U, static_cast<int>(wait.count()));
  if (ready < 0 && errno != EINTR) {
    now = Impl::TimePoint::clock::now();
    return impl.schedule_failure(
        status(common::StatusCode::kIoError, "polling Raft read-authority acquisition failed"),
        now);
  }
  now = Impl::TimePoint::clock::now();
  const bool readable = ready > 0 && (descriptor.revents & POLLIN) != 0;
  const bool writable = ready > 0 && (descriptor.revents & POLLOUT) != 0;
  if (ready > 0 && (descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0 && !readable &&
      !writable) {
    return impl.schedule_failure(status(common::StatusCode::kUnavailable,
                                        "Raft read-authority connection became unavailable"),
                                 now);
  }
  const common::Status progress = active->on_ready(readable, writable, now);
  if (!progress.is_ok() && active->state() != RaftReadAuthorityTcpClientState::kFailed)
    return impl.fail(progress);
  return impl.publish_or_retry(now);
}

common::Status RaftReadAuthorityTcpAcquisition::cancel() {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft read-authority acquisition is empty");
  Impl& impl = *implementation_;
  if (impl.acquisition_state == RaftReadAuthorityTcpAcquisitionState::kFailed ||
      impl.acquisition_state == RaftReadAuthorityTcpAcquisitionState::kCancelled) {
    return impl.acquisition_failure;
  }
  if (impl.acquisition_state == RaftReadAuthorityTcpAcquisitionState::kComplete) {
    return status(common::StatusCode::kInvalidArgument,
                  "completed Raft read-authority acquisition cannot be cancelled");
  }
  impl.client.reset();
  impl.acquisition_metrics.active_attempts = 0U;
  impl.next_attempt_not_before.reset();
  impl.acquisition_failure =
      status(common::StatusCode::kCancelled, "Raft read-authority acquisition was cancelled");
  impl.acquisition_state = RaftReadAuthorityTcpAcquisitionState::kCancelled;
  return impl.acquisition_failure;
}

RaftReadAuthorityTcpAcquisitionState RaftReadAuthorityTcpAcquisition::state() const noexcept {
  return implementation_ ? implementation_->acquisition_state
                         : RaftReadAuthorityTcpAcquisitionState::kFailed;
}

RaftReadAuthorityTcpAcquisitionMetrics RaftReadAuthorityTcpAcquisition::metrics() const noexcept {
  return implementation_ ? implementation_->acquisition_metrics
                         : RaftReadAuthorityTcpAcquisitionMetrics{};
}

int RaftReadAuthorityTcpAcquisition::descriptor() const noexcept {
  const RaftReadAuthorityTcpClient* active =
      implementation_ ? implementation_->active_client() : nullptr;
  return active != nullptr ? active->descriptor() : -1;
}

RaftReadAuthorityTlsInterest RaftReadAuthorityTcpAcquisition::interest() const noexcept {
  const RaftReadAuthorityTcpClient* active =
      implementation_ ? implementation_->active_client() : nullptr;
  return active != nullptr ? active->interest() : RaftReadAuthorityTlsInterest{};
}

std::optional<RaftReadAuthorityTcpClient::TimePoint>
RaftReadAuthorityTcpAcquisition::wake_deadline() const noexcept {
  if (!implementation_ ||
      implementation_->acquisition_state != RaftReadAuthorityTcpAcquisitionState::kRunning) {
    return std::nullopt;
  }
  if (const RaftReadAuthorityTcpClient* active = implementation_->active_client();
      active != nullptr)
    return active->deadline();
  return implementation_->next_attempt_not_before;
}

common::Result<RaftReadAuthority> RaftReadAuthorityTcpAcquisition::result() const {
  if (!implementation_)
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "Raft read-authority acquisition is empty"));
  if (implementation_->acquisition_state == RaftReadAuthorityTcpAcquisitionState::kFailed ||
      implementation_->acquisition_state == RaftReadAuthorityTcpAcquisitionState::kCancelled) {
    return common::make_unexpected(implementation_->acquisition_failure);
  }
  const std::optional<RaftReadAuthority>& acquisition_result = implementation_->acquisition_result;
  if (implementation_->acquisition_state != RaftReadAuthorityTcpAcquisitionState::kComplete ||
      !acquisition_result.has_value()) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft read-authority acquisition result is unavailable"));
  }
  try {
    return acquisition_result.value();
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted,
               "Raft read-authority acquisition result allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft read-authority acquisition result is too large"));
  }
}

const common::Status& RaftReadAuthorityTcpAcquisition::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "Raft read-authority acquisition is empty"};
  return implementation_ ? implementation_->acquisition_failure : empty;
}

} // namespace chronos::cluster
