#include "chronos/network/native_quorum_ingest_tcp_execution.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <memory>
#include <new>
#include <optional>
#include <poll.h>
#include <string>
#include <system_error>
#include <utility>

namespace chronos::network {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}

[[nodiscard]] common::Status poll_error(const int error = errno) {
  return {common::StatusCode::kIoError,
          std::string{"polling native QUORUM_SYNC TCP execution: "} +
              std::error_code{error, std::generic_category()}.message()};
}

template <typename Value>
[[nodiscard]] Value* optional_pointer(std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
}

template <typename Value>
[[nodiscard]] const Value* optional_pointer(const std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
}

[[nodiscard]] std::chrono::milliseconds
bounded_wait(const std::chrono::milliseconds maximum_wait,
             const NativeQuorumIngestTcpExecution::TimePoint now,
             const std::optional<NativeQuorumIngestTcpExecution::TimePoint> deadline) noexcept {
  if (!deadline.has_value() || *deadline <= now)
    return deadline.has_value() ? std::chrono::milliseconds{0} : maximum_wait;
  const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(*deadline - now);
  return std::min(maximum_wait, remaining);
}

} // namespace

class NativeQuorumIngestTcpExecution::Impl {
public:
  Impl(NativeQuorumIngestTcpClient owned_client, const std::optional<TimePoint> deadline) noexcept
      : client(std::move(owned_client)), operation_deadline(deadline) {
    observe_client();
  }

  void observe_client() noexcept {
    const NativeQuorumIngestTcpClient* active = optional_pointer(client);
    if (active == nullptr)
      return;
    execution_metrics.attempts_started = active->attempts_started();
    execution_metrics.redirects_followed =
        execution_metrics.attempts_started == 0U ? 0U : execution_metrics.attempts_started - 1U;
    last_route = active->current_route();
    execution_metrics.active_client =
        execution_state == NativeQuorumIngestTcpExecutionState::kRunning;
  }

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (execution_state == NativeQuorumIngestTcpExecutionState::kRunning) {
      observe_client();
      client.reset();
      execution_metrics.active_client = false;
      execution_failure = std::move(failure);
      execution_state = NativeQuorumIngestTcpExecutionState::kFailed;
    }
    return execution_failure;
  }

  [[nodiscard]] common::Status cancel(common::Status cancellation) {
    if (execution_state == NativeQuorumIngestTcpExecutionState::kComplete)
      return common::Status::ok();
    if (execution_state == NativeQuorumIngestTcpExecutionState::kRunning) {
      observe_client();
      client.reset();
      execution_metrics.active_client = false;
      execution_failure = std::move(cancellation);
      execution_state = NativeQuorumIngestTcpExecutionState::kCancelled;
    }
    return execution_failure;
  }

  [[nodiscard]] std::optional<TimePoint> next_deadline() const noexcept {
    const NativeQuorumIngestTcpClient* active = optional_pointer(client);
    const std::optional<TimePoint> client_deadline =
        active != nullptr ? active->deadline() : std::nullopt;
    if (!operation_deadline.has_value())
      return client_deadline;
    if (!client_deadline.has_value())
      return operation_deadline;
    return std::min(*operation_deadline, *client_deadline);
  }

  std::optional<NativeQuorumIngestTcpClient> client;
  std::optional<TimePoint> operation_deadline;
  NativeQuorumIngestTcpExecutionState execution_state{
      NativeQuorumIngestTcpExecutionState::kRunning};
  NativeQuorumIngestTcpExecutionMetrics execution_metrics;
  NativeLeaderRoute last_route;
  common::Status execution_failure{common::StatusCode::kInternal,
                                   "native QUORUM_SYNC TCP execution has not failed"};
};

NativeQuorumIngestTcpExecution::NativeQuorumIngestTcpExecution(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
NativeQuorumIngestTcpExecution::~NativeQuorumIngestTcpExecution() = default;
NativeQuorumIngestTcpExecution::NativeQuorumIngestTcpExecution(
    NativeQuorumIngestTcpExecution&&) noexcept = default;
NativeQuorumIngestTcpExecution&
NativeQuorumIngestTcpExecution::operator=(NativeQuorumIngestTcpExecution&&) noexcept = default;

common::Result<NativeQuorumIngestTcpExecution>
NativeQuorumIngestTcpExecution::begin(NativeQuorumIngestTcpExecutionConfig config,
                                      std::vector<std::byte> encoded_columnar_append) {
  const TimePoint now = TimePoint::clock::now();
  if (config.operation_deadline.has_value() && *config.operation_deadline <= now) {
    return common::make_unexpected(
        status(common::StatusCode::kCancelled, "native QUORUM_SYNC operation deadline expired"));
  }
  auto client = NativeQuorumIngestTcpClient::begin(std::move(config.client),
                                                   std::move(encoded_columnar_append), now);
  if (!client.has_value())
    return common::make_unexpected(client.error());
  try {
    return NativeQuorumIngestTcpExecution{
        std::make_unique<Impl>(std::move(*client), config.operation_deadline)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "native QUORUM_SYNC TCP execution allocation failed"));
  }
}

common::Status
NativeQuorumIngestTcpExecution::poll_once(const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument,
                  "native QUORUM_SYNC TCP execution is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return status(common::StatusCode::kInvalidArgument,
                  "native QUORUM_SYNC TCP poll timeout is invalid");
  Impl& impl = *implementation_;
  if (impl.execution_state == NativeQuorumIngestTcpExecutionState::kFailed ||
      impl.execution_state == NativeQuorumIngestTcpExecutionState::kCancelled) {
    return impl.execution_failure;
  }
  if (impl.execution_state == NativeQuorumIngestTcpExecutionState::kComplete)
    return common::Status::ok();

  ++impl.execution_metrics.poll_calls;
  auto now = TimePoint::clock::now();
  if (impl.operation_deadline.has_value() && now >= *impl.operation_deadline) {
    return impl.cancel(
        status(common::StatusCode::kCancelled, "native QUORUM_SYNC operation deadline expired"));
  }
  NativeQuorumIngestTcpClient* client = optional_pointer(impl.client);
  if (client == nullptr)
    return impl.fail(
        status(common::StatusCode::kInternal, "native QUORUM_SYNC TCP execution lost its client"));
  const NativeQuorumIngestTcpInterest interest = client->interest();
  short requested{};
  if (interest.want_read)
    requested |= POLLIN;
  if (interest.want_write)
    requested |= POLLOUT;
  if (client->descriptor() < 0 || requested == 0)
    return impl.fail(status(common::StatusCode::kInternal,
                            "native QUORUM_SYNC TCP client has no pollable interest"));
  pollfd descriptor{.fd = client->descriptor(), .events = requested, .revents = 0};
  const auto wait = bounded_wait(maximum_wait, now, impl.next_deadline());
  const int ready = ::poll(&descriptor, 1U, static_cast<int>(wait.count()));
  if (ready < 0) {
    if (errno != EINTR)
      return impl.fail(poll_error());
    descriptor.revents = 0;
  }

  now = TimePoint::clock::now();
  if (impl.operation_deadline.has_value() && now >= *impl.operation_deadline) {
    return impl.cancel(
        status(common::StatusCode::kCancelled, "native QUORUM_SYNC operation deadline expired"));
  }
  if ((descriptor.revents & POLLNVAL) != 0)
    return impl.fail(poll_error(EBADF));
  const bool terminal_readiness = (descriptor.revents & (POLLERR | POLLHUP)) != 0;
  const bool readable =
      (descriptor.revents & POLLIN) != 0 || (terminal_readiness && interest.want_read);
  const bool writable =
      (descriptor.revents & POLLOUT) != 0 || (terminal_readiness && interest.want_write);
  if (readable || writable)
    ++impl.execution_metrics.readiness_events;
  common::Status driven = client->on_ready(readable, writable, now);
  impl.observe_client();
  const NativeQuorumIngestTcpClientState client_state = client->state();
  if (!driven.is_ok() || client_state == NativeQuorumIngestTcpClientState::kFailed) {
    if (driven.is_ok())
      driven = client->failure();
    return impl.fail(std::move(driven));
  }
  if (client_state == NativeQuorumIngestTcpClientState::kComplete) {
    impl.execution_state = NativeQuorumIngestTcpExecutionState::kComplete;
    impl.execution_metrics.active_client = false;
  }
  return common::Status::ok();
}

common::Status NativeQuorumIngestTcpExecution::cancel() {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument,
                  "native QUORUM_SYNC TCP execution is empty");
  return implementation_->cancel(
      status(common::StatusCode::kCancelled, "native QUORUM_SYNC TCP execution was cancelled"));
}

NativeQuorumIngestTcpExecutionState NativeQuorumIngestTcpExecution::state() const noexcept {
  return implementation_ ? implementation_->execution_state
                         : NativeQuorumIngestTcpExecutionState::kFailed;
}

NativeQuorumIngestTcpExecutionMetrics NativeQuorumIngestTcpExecution::metrics() const noexcept {
  return implementation_ ? implementation_->execution_metrics
                         : NativeQuorumIngestTcpExecutionMetrics{};
}

std::optional<NativeQuorumIngestTcpExecution::TimePoint>
NativeQuorumIngestTcpExecution::next_deadline() const noexcept {
  if (!implementation_ ||
      implementation_->execution_state != NativeQuorumIngestTcpExecutionState::kRunning) {
    return std::nullopt;
  }
  return implementation_->next_deadline();
}

NativeLeaderRoute NativeQuorumIngestTcpExecution::current_route() const noexcept {
  return implementation_ ? implementation_->last_route : NativeLeaderRoute{};
}

common::Result<QuorumSyncIngestAcknowledgement> NativeQuorumIngestTcpExecution::result() const {
  if (!implementation_)
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "native QUORUM_SYNC TCP execution is empty"));
  if (implementation_->execution_state == NativeQuorumIngestTcpExecutionState::kFailed ||
      implementation_->execution_state == NativeQuorumIngestTcpExecutionState::kCancelled) {
    return common::make_unexpected(implementation_->execution_failure);
  }
  const NativeQuorumIngestTcpClient* client = optional_pointer(implementation_->client);
  if (implementation_->execution_state != NativeQuorumIngestTcpExecutionState::kComplete ||
      client == nullptr) {
    return common::make_unexpected(
        status(common::StatusCode::kUnavailable, "native QUORUM_SYNC TCP execution is incomplete"));
  }
  return client->result();
}

const common::Status& NativeQuorumIngestTcpExecution::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "native QUORUM_SYNC TCP execution is empty"};
  return implementation_ ? implementation_->execution_failure : empty;
}

} // namespace chronos::network
