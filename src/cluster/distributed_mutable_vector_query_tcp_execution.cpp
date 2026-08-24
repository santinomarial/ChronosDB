#include "chronos/cluster/distributed_mutable_vector_query_tcp_execution.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <map>
#include <memory>
#include <new>
#include <optional>
#include <poll.h>
#include <ranges>
#include <span>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status poll_error(const int error = errno) {
  return {common::StatusCode::kIoError,
          std::string("polling mutable vector query TCP execution: ") +
              std::error_code(error, std::generic_category()).message()};
}

[[nodiscard]] bool retryable_client_failure(const common::StatusCode code) noexcept {
  return code == common::StatusCode::kUnavailable || code == common::StatusCode::kIoError;
}

[[nodiscard]] bool zero_address(const std::array<std::uint8_t, 4>& address) noexcept {
  return std::ranges::all_of(address, [](const std::uint8_t byte) { return byte == 0U; });
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] bool retryable_rebinding_failure(const common::StatusCode code) noexcept {
  return code == common::StatusCode::kUnavailable ||
         code == common::StatusCode::kResourceExhausted || code == common::StatusCode::kIoError;
}

} // namespace

class DistributedMutableVectorQueryTcpExecution::Impl {
public:
  using TimePoint = DistributedMutableVectorQueryExecution::TimePoint;

  struct Slot {
    schema::TabletId tablet_id;
    std::size_t route_index{};
    std::optional<DistributedMutableVectorQueryTcpClient> client;

    [[nodiscard]] DistributedMutableVectorQueryTcpClient* active_client() noexcept {
      if (!client.has_value())
        return nullptr;
      // Guarded by the presence check above.
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
      return std::addressof(*client);
    }

    [[nodiscard]] const DistributedMutableVectorQueryTcpClient* active_client() const noexcept {
      if (!client.has_value())
        return nullptr;
      // Guarded by the presence check above.
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
      return std::addressof(*client);
    }
  };

  Impl(DistributedMutableVectorQueryExecution owned_execution,
       DistributedMutableVectorQueryTcpExecutionConfig configured, std::vector<Slot> owned_slots,
       std::vector<pollfd> descriptors, std::vector<std::size_t> indexes)
      : execution(std::move(owned_execution)), config(std::move(configured)),
        slots(std::move(owned_slots)), poll_descriptors(std::move(descriptors)),
        poll_slot_indexes(std::move(indexes)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (execution_state == DistributedMutableVectorQueryTcpExecutionState::kRunning) {
      for (Slot& slot : slots)
        slot.client.reset();
      execution_metrics.active_attempts = 0U;
      execution_failure = std::move(status);
      execution_state = DistributedMutableVectorQueryTcpExecutionState::kFailed;
    }
    return execution_failure;
  }

  [[nodiscard]] common::Status cancel(common::Status status) {
    if (execution_state == DistributedMutableVectorQueryTcpExecutionState::kComplete)
      return common::Status::ok();
    if (execution_state == DistributedMutableVectorQueryTcpExecutionState::kRunning) {
      for (Slot& slot : slots)
        slot.client.reset();
      execution_metrics.active_attempts = 0U;
      execution_failure = std::move(status);
      execution_state = DistributedMutableVectorQueryTcpExecutionState::kCancelled;
    }
    return execution_failure;
  }

  [[nodiscard]] common::Status record_transport_failure(Slot& slot, const common::StatusCode code,
                                                        const TimePoint now) {
    slot.client.reset();
    --execution_metrics.active_attempts;
    ++execution_metrics.transport_failed_attempts;
    return execution.record_transport_failure(slot.tablet_id, code, now);
  }

  [[nodiscard]] common::Status start_due_attempts(const TimePoint now) {
    for (Slot& slot : slots) {
      if (slot.active_client() != nullptr)
        continue;
      auto sender_state = execution.sender_state(slot.tablet_id);
      if (!sender_state.has_value())
        return sender_state.error();
      if (*sender_state != DistributedQuerySenderState::kReady &&
          *sender_state != DistributedQuerySenderState::kBackoff) {
        continue;
      }
      if (*sender_state == DistributedQuerySenderState::kBackoff) {
        auto deadline = execution.next_attempt_not_before(slot.tablet_id);
        if (!deadline.has_value())
          return deadline.error();
        const TimePoint retry_deadline = deadline->value_or(TimePoint::max());
        if (retry_deadline == TimePoint::max() || now < retry_deadline)
          continue;
      }
      auto attempt = execution.begin_attempt(slot.tablet_id, now);
      if (!attempt.has_value())
        return attempt.error();
      const bool retry = attempt->attempt_number > 1U;
      const DistributedQueryNodeRoute& route = config.routes[slot.route_index];
      if (attempt->target_node_id != route.node_id)
        return invalid("mutable vector query attempt escaped its immutable target route");
      const network::Ipv4Endpoint& endpoint =
          route.endpoints[(attempt->attempt_number - 1U) % route.endpoints.size()];
      auto client = DistributedMutableVectorQueryTcpClient::begin(
          std::move(*attempt),
          {.remote_endpoint = endpoint,
           .tls_context = route.tls_context,
           .carrier = {.authenticator = config.authenticator,
                       .node_authorizer = config.node_authorizer,
                       .peer_ipv4_address = endpoint.address,
                       .limits = config.carrier_limits},
           .connect_timeout = config.connect_timeout},
          now);
      ++execution_metrics.attempts_started;
      if (retry)
        ++execution_metrics.retries_started;
      if (!client.has_value()) {
        if (!retryable_client_failure(client.error().code()))
          return client.error();
        ++execution_metrics.transport_failed_attempts;
        common::Status recorded =
            execution.record_transport_failure(slot.tablet_id, client.error().code(), now);
        if (!recorded.is_ok())
          return recorded;
        continue;
      }
      slot.client.emplace(std::move(*client));
      ++execution_metrics.active_attempts;
    }
    return common::Status::ok();
  }

  [[nodiscard]] std::chrono::milliseconds bounded_wait(std::chrono::milliseconds maximum_wait,
                                                       const TimePoint now) const {
    std::optional<TimePoint> earliest = config.execution_deadline;
    for (const Slot& slot : slots) {
      if (slot.active_client() != nullptr)
        continue;
      const auto sender_state = execution.sender_state(slot.tablet_id);
      if (!sender_state.has_value() || *sender_state != DistributedQuerySenderState::kBackoff)
        continue;
      const auto deadline = execution.next_attempt_not_before(slot.tablet_id);
      if (deadline.has_value()) {
        const TimePoint retry_deadline = deadline->value_or(TimePoint::max());
        if (retry_deadline != TimePoint::max() &&
            retry_deadline < earliest.value_or(TimePoint::max())) {
          earliest = retry_deadline;
        }
      }
    }
    if (!earliest.has_value() || *earliest <= now)
      return earliest.has_value() ? std::chrono::milliseconds{0} : maximum_wait;
    const auto until = std::chrono::duration_cast<std::chrono::milliseconds>(*earliest - now);
    return std::min(maximum_wait, until);
  }

  [[nodiscard]] common::Status publish_if_terminal() {
    bool all_succeeded = true;
    for (const Slot& slot : slots) {
      auto sender_state = execution.sender_state(slot.tablet_id);
      if (!sender_state.has_value())
        return fail(sender_state.error());
      if (*sender_state == DistributedQuerySenderState::kFailed) {
        auto finished = execution.finish();
        return fail(
            finished.has_value()
                ? common::Status{common::StatusCode::kInternal,
                                 "failed mutable vector query unexpectedly produced a result"}
                : finished.error());
      }
      all_succeeded = all_succeeded && *sender_state == DistributedQuerySenderState::kSucceeded;
    }
    if (!all_succeeded)
      return common::Status::ok();
    auto finished = execution.finish();
    if (!finished.has_value())
      return fail(finished.error());
    execution_result = std::move(*finished);
    execution_state = DistributedMutableVectorQueryTcpExecutionState::kComplete;
    return common::Status::ok();
  }

  DistributedMutableVectorQueryExecution execution;
  DistributedMutableVectorQueryTcpExecutionConfig config;
  std::vector<Slot> slots;
  std::vector<pollfd> poll_descriptors;
  std::vector<std::size_t> poll_slot_indexes;
  DistributedMutableVectorQueryTcpExecutionMetrics execution_metrics;
  DistributedMutableVectorQueryTcpExecutionState execution_state{
      DistributedMutableVectorQueryTcpExecutionState::kRunning};
  std::optional<DistributedVectorQueryExecutionResultV2> execution_result;
  common::Status execution_failure{common::StatusCode::kInternal,
                                   "mutable vector query TCP execution has not failed"};
};

DistributedMutableVectorQueryTcpExecution::DistributedMutableVectorQueryTcpExecution() noexcept =
    default;
DistributedMutableVectorQueryTcpExecution::DistributedMutableVectorQueryTcpExecution(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedMutableVectorQueryTcpExecution::~DistributedMutableVectorQueryTcpExecution() = default;
DistributedMutableVectorQueryTcpExecution::DistributedMutableVectorQueryTcpExecution(
    DistributedMutableVectorQueryTcpExecution&&) noexcept = default;
DistributedMutableVectorQueryTcpExecution& DistributedMutableVectorQueryTcpExecution::operator=(
    DistributedMutableVectorQueryTcpExecution&&) noexcept = default;

common::Result<DistributedMutableVectorQueryTcpExecution>
DistributedMutableVectorQueryTcpExecution::create(
    DistributedMutableVectorQueryExecution execution,
    DistributedMutableVectorQueryTcpExecutionConfig config) {
  constexpr std::size_t kMinimumResponseBytes =
      kDistributedVectorQueryResponseV2HeaderSize + kDistributedVectorQueryResponseV2TrailerSize;
  if (config.authenticator == nullptr || config.node_authorizer == nullptr ||
      config.routes.empty() || config.routes.size() > 65'536U ||
      config.maximum_rebindings > 1024U || !valid_timeout(config.connect_timeout) ||
      !valid_timeout(config.carrier_limits.handshake_timeout) ||
      !valid_timeout(config.carrier_limits.exchange_timeout) ||
      config.carrier_limits.maximum_response_frames == 0U ||
      config.carrier_limits.maximum_response_frames >
          query::kMaximumDistributedCoordinatorMessages ||
      config.carrier_limits.maximum_response_bytes < kMinimumResponseBytes ||
      config.carrier_limits.maximum_response_bytes >
          kMaximumDistributedVectorQueryV2ResponseBytes) {
    return common::make_unexpected(
        invalid("mutable vector query TCP execution configuration is invalid"));
  }
  try {
    std::map<raft::NodeId, std::size_t> route_indexes;
    for (std::size_t index = 0U; index < config.routes.size(); ++index) {
      const DistributedQueryNodeRoute& route = config.routes[index];
      if (route.node_id == 0U || route.endpoints.empty() || route.endpoints.size() > 1024U ||
          route.tls_context == nullptr || !route_indexes.emplace(route.node_id, index).second) {
        return common::make_unexpected(
            invalid("mutable vector query TCP execution route is invalid or duplicated"));
      }
      for (std::size_t endpoint_index = 0U; endpoint_index < route.endpoints.size();
           ++endpoint_index) {
        const network::Ipv4Endpoint& endpoint = route.endpoints[endpoint_index];
        const std::span<const network::Ipv4Endpoint> prior{route.endpoints.data(), endpoint_index};
        if (endpoint.port == 0U || zero_address(endpoint.address) ||
            std::ranges::find(prior, endpoint) != prior.end()) {
          return common::make_unexpected(
              invalid("mutable vector query TCP route address is invalid or duplicated"));
        }
      }
    }
    std::vector<Impl::Slot> slots;
    slots.reserve(execution.targets().size());
    for (const DistributedMutableVectorQueryTarget& target : execution.targets()) {
      const auto route = route_indexes.find(target.serving_node);
      if (route == route_indexes.end()) {
        return common::make_unexpected(
            invalid("mutable vector query TCP execution has no route for a target node"));
      }
      slots.push_back({target.tablet_id, route->second, std::nullopt});
    }
    std::vector<pollfd> descriptors(slots.size());
    std::vector<std::size_t> indexes(slots.size());
    return DistributedMutableVectorQueryTcpExecution{
        std::make_unique<Impl>(std::move(execution), std::move(config), std::move(slots),
                               std::move(descriptors), std::move(indexes))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("mutable vector query TCP execution allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("mutable vector query TCP execution exceeds limits"));
  }
}

common::Status
DistributedMutableVectorQueryTcpExecution::poll_once(const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return invalid("mutable vector query TCP execution is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return invalid("mutable vector query TCP execution poll timeout is invalid");
  Impl& impl = *implementation_;
  if (impl.execution_state == DistributedMutableVectorQueryTcpExecutionState::kFailed ||
      impl.execution_state == DistributedMutableVectorQueryTcpExecutionState::kCancelled) {
    return impl.execution_failure;
  }
  if (impl.execution_state == DistributedMutableVectorQueryTcpExecutionState::kComplete)
    return common::Status::ok();

  auto now = std::chrono::steady_clock::now();
  if (impl.config.execution_deadline.has_value() && now >= *impl.config.execution_deadline) {
    return impl.cancel(
        {common::StatusCode::kCancelled, "mutable vector query TCP execution deadline expired"});
  }
  common::Status started = impl.start_due_attempts(now);
  if (!started.is_ok())
    return impl.fail(std::move(started));
  common::Status initial_terminal = impl.publish_if_terminal();
  if (!initial_terminal.is_ok() ||
      impl.execution_state != DistributedMutableVectorQueryTcpExecutionState::kRunning) {
    return initial_terminal;
  }

  std::size_t descriptor_count{};
  for (std::size_t index = 0U; index < impl.slots.size(); ++index) {
    const DistributedMutableVectorQueryTcpClient* client = impl.slots[index].active_client();
    if (client == nullptr)
      continue;
    const auto interest = client->interest();
    short events{};
    if (interest.want_read)
      events |= POLLIN;
    if (interest.want_write)
      events |= POLLOUT;
    impl.poll_descriptors[descriptor_count] = {.fd = client->descriptor(), .events = events};
    impl.poll_slot_indexes[descriptor_count] = index;
    ++descriptor_count;
  }
  const auto wait = impl.bounded_wait(maximum_wait, now);
  const int ready = ::poll(impl.poll_descriptors.data(), static_cast<nfds_t>(descriptor_count),
                           static_cast<int>(wait.count()));
  if (ready < 0 && errno != EINTR)
    return impl.fail(poll_error());

  now = std::chrono::steady_clock::now();
  if (impl.config.execution_deadline.has_value() && now >= *impl.config.execution_deadline) {
    return impl.cancel(
        {common::StatusCode::kCancelled, "mutable vector query TCP execution deadline expired"});
  }
  for (std::size_t descriptor_index = 0U; descriptor_index < descriptor_count; ++descriptor_index) {
    Impl::Slot& slot = impl.slots[impl.poll_slot_indexes[descriptor_index]];
    DistributedMutableVectorQueryTcpClient* client = slot.active_client();
    if (client == nullptr)
      continue;
    const short events = impl.poll_descriptors[descriptor_index].revents;
    if ((events & (POLLERR | POLLHUP | POLLNVAL)) != 0 && (events & (POLLIN | POLLOUT)) == 0) {
      common::Status recorded =
          impl.record_transport_failure(slot, common::StatusCode::kIoError, now);
      if (!recorded.is_ok())
        return impl.fail(std::move(recorded));
      continue;
    }
    common::Status driven = client->on_ready((events & POLLIN) != 0, (events & POLLOUT) != 0, now);
    const auto client_state = client->state();
    if (!driven.is_ok() || client_state == DistributedMutableVectorQueryTcpClientState::kFailed) {
      common::Status client_failure = std::move(driven);
      if (client_failure.is_ok())
        client_failure = client->failure();
      if (!retryable_client_failure(client_failure.code()))
        return impl.fail(std::move(client_failure));
      common::Status recorded = impl.record_transport_failure(slot, client_failure.code(), now);
      if (!recorded.is_ok())
        return impl.fail(std::move(recorded));
      continue;
    }
    if (client_state != DistributedMutableVectorQueryTcpClientState::kComplete)
      continue;
    auto responses = client->responses();
    if (!responses.has_value())
      return impl.fail(responses.error());
    common::Status accepted = impl.execution.accept_responses(slot.tablet_id, *responses, now);
    slot.client.reset();
    --impl.execution_metrics.active_attempts;
    ++impl.execution_metrics.transport_completed_attempts;
    if (!accepted.is_ok())
      return impl.fail(std::move(accepted));
  }
  return impl.publish_if_terminal();
}

common::Status DistributedMutableVectorQueryTcpExecution::cancel() {
  if (!implementation_)
    return invalid("mutable vector query TCP execution is empty");
  return implementation_->cancel(
      {common::StatusCode::kCancelled, "mutable vector query TCP execution was cancelled"});
}

common::Status DistributedMutableVectorQueryTcpExecution::rebind(
    DistributedMutableVectorQueryExecution execution,
    DistributedMutableVectorQueryTcpExecutionConfig config) {
  if (!implementation_)
    return invalid("mutable vector query TCP execution is empty");
  Impl& previous = *implementation_;
  if (previous.execution_state != DistributedMutableVectorQueryTcpExecutionState::kFailed ||
      !retryable_rebinding_failure(previous.execution_failure.code())) {
    return invalid("mutable vector query TCP execution is not eligible for rebinding");
  }
  if (previous.execution_metrics.rebindings_started >= previous.config.maximum_rebindings)
    return exhausted("mutable vector query TCP execution rebinding budget is exhausted");
  if (config.execution_deadline != previous.config.execution_deadline ||
      config.maximum_rebindings != previous.config.maximum_rebindings) {
    return invalid("mutable vector query TCP execution rebinding limits changed");
  }
  if (previous.execution.logical_identity() != execution.logical_identity()) {
    return invalid("mutable vector query TCP replacement changes logical query authority");
  }

  auto replacement =
      DistributedMutableVectorQueryTcpExecution::create(std::move(execution), std::move(config));
  if (!replacement.has_value())
    return replacement.error();
  const DistributedMutableVectorQueryTcpExecutionMetrics prior = previous.execution_metrics;
  replacement->implementation_->execution_metrics.attempts_started += prior.attempts_started;
  replacement->implementation_->execution_metrics.retries_started += prior.retries_started;
  replacement->implementation_->execution_metrics.transport_completed_attempts +=
      prior.transport_completed_attempts;
  replacement->implementation_->execution_metrics.transport_failed_attempts +=
      prior.transport_failed_attempts;
  replacement->implementation_->execution_metrics.rebindings_started =
      prior.rebindings_started + 1U;
  implementation_ = std::move(replacement->implementation_);
  return common::Status::ok();
}

DistributedMutableVectorQueryTcpExecutionState
DistributedMutableVectorQueryTcpExecution::state() const noexcept {
  return implementation_ ? implementation_->execution_state
                         : DistributedMutableVectorQueryTcpExecutionState::kFailed;
}

DistributedMutableVectorQueryTcpExecutionMetrics
DistributedMutableVectorQueryTcpExecution::metrics() const noexcept {
  return implementation_ ? implementation_->execution_metrics
                         : DistributedMutableVectorQueryTcpExecutionMetrics{};
}

const std::optional<DistributedVectorQueryExecutionResultV2>&
DistributedMutableVectorQueryTcpExecution::result() const noexcept {
  static const std::optional<DistributedVectorQueryExecutionResultV2> empty;
  return implementation_ ? implementation_->execution_result : empty;
}

common::Result<DistributedVectorQueryExecutionResultV2>
DistributedMutableVectorQueryTcpExecution::take_result() {
  try {
    if (!implementation_) {
      return common::make_unexpected(invalid("mutable vector query TCP execution is empty"));
    }
    if (implementation_->execution_state !=
            DistributedMutableVectorQueryTcpExecutionState::kComplete ||
        !implementation_->execution_result.has_value()) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kUnavailable,
                         "mutable vector query TCP execution result is unavailable"});
    }
    DistributedVectorQueryExecutionResultV2 result = std::move(*implementation_->execution_result);
    implementation_->execution_result.reset();
    return result;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable vector query result transfer failed"));
  }
}

const common::Status& DistributedMutableVectorQueryTcpExecution::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "mutable vector query TCP execution is empty"};
  return implementation_ ? implementation_->execution_failure : empty;
}

common::Result<std::optional<DistributedQueryLeaderHint>>
DistributedMutableVectorQueryTcpExecution::suggested_leader(
    const schema::TabletId& tablet_id) const {
  if (!implementation_) {
    return common::make_unexpected(invalid("mutable vector query TCP execution is empty"));
  }
  return implementation_->execution.suggested_leader(tablet_id);
}

} // namespace chronos::cluster
