#include "chronos/cluster/distributed_grouped_query_tcp_execution.hpp"

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
#include <stdexcept>
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
          std::string("polling grouped query TCP execution: ") +
              std::error_code(error, std::generic_category()).message()};
}

[[nodiscard]] bool retryable_connect_failure(const common::StatusCode code) noexcept {
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

[[nodiscard]] bool retryable(const common::StatusCode code) noexcept {
  return code == common::StatusCode::kUnavailable ||
         code == common::StatusCode::kResourceExhausted || code == common::StatusCode::kIoError;
}

[[nodiscard]] bool same_logical_fragment(
    const query::DistributedGroupedFloat64FragmentDispatch& previous,
    const query::DistributedGroupedFloat64FragmentDispatch& replacement) noexcept {
  const auto& left = previous.fragment.aggregate;
  const auto& right = replacement.fragment.aggregate;
  return previous.raft_group_id == replacement.raft_group_id &&
         previous.fragment.group_key_input_index == replacement.fragment.group_key_input_index &&
         left.query_id == right.query_id && left.database_id == right.database_id &&
         left.table_id == right.table_id && left.tablet_id == right.tablet_id &&
         left.destination_schema_id == right.destination_schema_id &&
         left.read_policy == right.read_policy &&
         left.destination_column_ordinals == right.destination_column_ordinals &&
         left.aggregate_input_index == right.aggregate_input_index &&
         left.event_time_predicate == right.event_time_predicate;
}

[[nodiscard]] bool compatible_rebinding(
    const query::CompatibleDistributedGroupedFloat64Snapshot& previous,
    const query::CompatibleDistributedGroupedFloat64Snapshot& replacement) noexcept {
  if (previous.snapshot().database_id() != replacement.snapshot().database_id() ||
      replacement.snapshot().generation() < previous.snapshot().generation()) {
    return false;
  }
  const auto old_dispatches = previous.dispatches();
  const auto new_dispatches = replacement.dispatches();
  if (old_dispatches.size() != new_dispatches.size())
    return false;
  for (std::size_t index = 0U; index < old_dispatches.size(); ++index) {
    if (!same_logical_fragment(old_dispatches[index], new_dispatches[index]))
      return false;
  }
  return true;
}

} // namespace

class DistributedGroupedQueryTcpExecution::Impl {
public:
  using TimePoint = DistributedGroupedQueryExecution::TimePoint;

  struct Slot {
    schema::TabletId tablet_id;
    std::size_t route_index{};
    std::optional<DistributedGroupedQueryTcpClient> client;
  };

  Impl(DistributedGroupedQueryExecution owned_execution,
       DistributedGroupedQueryTcpExecutionConfig configured, std::vector<Slot> owned_slots,
       std::vector<pollfd> descriptors, std::vector<std::size_t> indexes) noexcept
      : execution(std::move(owned_execution)), config(std::move(configured)),
        slots(std::move(owned_slots)), poll_descriptors(std::move(descriptors)),
        poll_slot_indexes(std::move(indexes)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (execution_state == DistributedGroupedQueryTcpExecutionState::kRunning) {
      for (Slot& slot : slots)
        slot.client.reset();
      execution_metrics.active_attempts = 0U;
      execution_failure = std::move(status);
      execution_state = DistributedGroupedQueryTcpExecutionState::kFailed;
    }
    return execution_failure;
  }

  [[nodiscard]] common::Status cancel(common::Status status) {
    if (execution_state == DistributedGroupedQueryTcpExecutionState::kComplete)
      return common::Status::ok();
    if (execution_state == DistributedGroupedQueryTcpExecutionState::kRunning) {
      for (Slot& slot : slots)
        slot.client.reset();
      execution_metrics.active_attempts = 0U;
      execution_failure = std::move(status);
      execution_state = DistributedGroupedQueryTcpExecutionState::kCancelled;
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
      if (slot.client.has_value())
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
        if (!deadline->has_value() || now < **deadline)
          continue;
      }
      auto attempt = execution.begin_attempt(slot.tablet_id, now);
      if (!attempt.has_value())
        return attempt.error();
      const bool retry = attempt->attempt_number > 1U;
      const DistributedQueryNodeRoute& route = config.routes[slot.route_index];
      const network::Ipv4Endpoint& endpoint =
          route.endpoints[(attempt->attempt_number - 1U) % route.endpoints.size()];
      auto client = DistributedGroupedQueryTcpClient::begin(
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
        if (!retryable_connect_failure(client.error().code()))
          return client.error();
        ++execution_metrics.transport_failed_attempts;
        const common::Status recorded =
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
      if (slot.client.has_value())
        continue;
      const auto sender_state = execution.sender_state(slot.tablet_id);
      if (!sender_state.has_value() || *sender_state != DistributedQuerySenderState::kBackoff)
        continue;
      const auto deadline = execution.next_attempt_not_before(slot.tablet_id);
      if (deadline.has_value() && deadline->has_value() &&
          (!earliest.has_value() || **deadline < *earliest)) {
        earliest = **deadline;
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
        return fail(finished.has_value()
                        ? common::Status{common::StatusCode::kInternal,
                                         "failed grouped query unexpectedly produced a result"}
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
    execution_state = DistributedGroupedQueryTcpExecutionState::kComplete;
    return common::Status::ok();
  }

  DistributedGroupedQueryExecution execution;
  DistributedGroupedQueryTcpExecutionConfig config;
  std::vector<Slot> slots;
  std::vector<pollfd> poll_descriptors;
  std::vector<std::size_t> poll_slot_indexes;
  DistributedGroupedQueryTcpExecutionMetrics execution_metrics;
  DistributedGroupedQueryTcpExecutionState execution_state{
      DistributedGroupedQueryTcpExecutionState::kRunning};
  std::optional<std::vector<query::GroupedFloat64AggregateResult>> execution_result;
  common::Status execution_failure{common::StatusCode::kInternal,
                                   "grouped query TCP execution has not failed"};
};

DistributedGroupedQueryTcpExecution::DistributedGroupedQueryTcpExecution() noexcept = default;
DistributedGroupedQueryTcpExecution::DistributedGroupedQueryTcpExecution(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedGroupedQueryTcpExecution::~DistributedGroupedQueryTcpExecution() = default;
DistributedGroupedQueryTcpExecution::DistributedGroupedQueryTcpExecution(
    DistributedGroupedQueryTcpExecution&&) noexcept = default;
DistributedGroupedQueryTcpExecution& DistributedGroupedQueryTcpExecution::operator=(
    DistributedGroupedQueryTcpExecution&&) noexcept = default;

common::Result<DistributedGroupedQueryTcpExecution>
DistributedGroupedQueryTcpExecution::create(DistributedGroupedQueryExecution execution,
                                            DistributedGroupedQueryTcpExecutionConfig config) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr ||
      config.routes.empty() || config.routes.size() > 65'536U ||
      config.maximum_rebindings > 1024U || !valid_timeout(config.connect_timeout) ||
      !valid_timeout(config.carrier_limits.handshake_timeout) ||
      !valid_timeout(config.carrier_limits.exchange_timeout) ||
      config.carrier_limits.maximum_response_frames == 0U ||
      config.carrier_limits.maximum_response_frames >
          query::kMaximumDistributedCoordinatorMessages) {
    return common::make_unexpected(invalid("grouped query TCP execution configuration is invalid"));
  }
  try {
    std::map<raft::NodeId, std::size_t> route_indexes;
    for (std::size_t index = 0U; index < config.routes.size(); ++index) {
      const DistributedQueryNodeRoute& route = config.routes[index];
      if (route.node_id == 0U || route.endpoints.empty() || route.endpoints.size() > 1024U ||
          route.tls_context == nullptr || !route_indexes.emplace(route.node_id, index).second) {
        return common::make_unexpected(
            invalid("grouped query TCP execution route is invalid or duplicated"));
      }
      for (std::size_t endpoint_index = 0U; endpoint_index < route.endpoints.size();
           ++endpoint_index) {
        const network::Ipv4Endpoint& endpoint = route.endpoints[endpoint_index];
        const std::span<const network::Ipv4Endpoint> prior{route.endpoints.data(), endpoint_index};
        if (endpoint.port == 0U || zero_address(endpoint.address) ||
            std::ranges::find(prior, endpoint) != prior.end()) {
          return common::make_unexpected(
              invalid("grouped query TCP route address is invalid or duplicated"));
        }
      }
    }
    std::vector<Impl::Slot> slots;
    const auto dispatches = execution.snapshot().dispatches();
    slots.reserve(dispatches.size());
    for (const auto& dispatch : dispatches) {
      const auto& fragment = dispatch.fragment.aggregate;
      const auto route = route_indexes.find(fragment.serving_node);
      if (route == route_indexes.end())
        return common::make_unexpected(
            invalid("grouped query TCP execution has no route for a target node"));
      slots.push_back({fragment.tablet_id, route->second, std::nullopt});
    }
    std::vector<pollfd> descriptors(slots.size());
    std::vector<std::size_t> indexes(slots.size());
    return DistributedGroupedQueryTcpExecution{
        std::make_unique<Impl>(std::move(execution), std::move(config), std::move(slots),
                               std::move(descriptors), std::move(indexes))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped query TCP execution allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("grouped query TCP execution exceeds container limits"));
  }
}

common::Status
DistributedGroupedQueryTcpExecution::poll_once(const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return invalid("grouped query TCP execution is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return invalid("grouped query TCP execution poll timeout is invalid");
  Impl& impl = *implementation_;
  if (impl.execution_state == DistributedGroupedQueryTcpExecutionState::kFailed ||
      impl.execution_state == DistributedGroupedQueryTcpExecutionState::kCancelled) {
    return impl.execution_failure;
  }
  if (impl.execution_state == DistributedGroupedQueryTcpExecutionState::kComplete)
    return common::Status::ok();

  auto now = std::chrono::steady_clock::now();
  if (impl.config.execution_deadline.has_value() && now >= *impl.config.execution_deadline) {
    return impl.cancel(
        {common::StatusCode::kCancelled, "grouped query TCP execution deadline expired"});
  }
  const common::Status started = impl.start_due_attempts(now);
  if (!started.is_ok())
    return impl.fail(started);
  const common::Status initial_terminal = impl.publish_if_terminal();
  if (!initial_terminal.is_ok() ||
      impl.execution_state != DistributedGroupedQueryTcpExecutionState::kRunning) {
    return initial_terminal;
  }

  std::size_t descriptor_count{};
  for (std::size_t index = 0U; index < impl.slots.size(); ++index) {
    const auto& client = impl.slots[index].client;
    if (!client.has_value())
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
        {common::StatusCode::kCancelled, "grouped query TCP execution deadline expired"});
  }
  for (std::size_t descriptor_index = 0U; descriptor_index < descriptor_count; ++descriptor_index) {
    Impl::Slot& slot = impl.slots[impl.poll_slot_indexes[descriptor_index]];
    if (!slot.client.has_value())
      continue;
    const short events = impl.poll_descriptors[descriptor_index].revents;
    if ((events & (POLLERR | POLLHUP | POLLNVAL)) != 0 && (events & (POLLIN | POLLOUT)) == 0) {
      const common::Status recorded =
          impl.record_transport_failure(slot, common::StatusCode::kIoError, now);
      if (!recorded.is_ok())
        return impl.fail(recorded);
      continue;
    }
    const common::Status driven =
        slot.client->on_ready((events & POLLIN) != 0, (events & POLLOUT) != 0, now);
    const auto client_state = slot.client->state();
    if (!driven.is_ok() || client_state == DistributedGroupedQueryTcpClientState::kFailed) {
      const common::StatusCode code =
          driven.is_ok() ? slot.client->failure().code() : driven.code();
      const common::Status recorded = impl.record_transport_failure(slot, code, now);
      if (!recorded.is_ok())
        return impl.fail(recorded);
      continue;
    }
    if (client_state != DistributedGroupedQueryTcpClientState::kComplete)
      continue;
    auto responses = slot.client->responses();
    if (!responses.has_value())
      return impl.fail(responses.error());
    const common::Status accepted =
        impl.execution.accept_responses(slot.tablet_id, *responses, now);
    slot.client.reset();
    --impl.execution_metrics.active_attempts;
    ++impl.execution_metrics.transport_completed_attempts;
    if (!accepted.is_ok())
      return impl.fail(accepted);
  }
  return impl.publish_if_terminal();
}

common::Status DistributedGroupedQueryTcpExecution::cancel() {
  if (!implementation_)
    return invalid("grouped query TCP execution is empty");
  return implementation_->cancel(
      {common::StatusCode::kCancelled, "grouped query TCP execution was cancelled"});
}

common::Status
DistributedGroupedQueryTcpExecution::rebind(DistributedGroupedQueryExecution execution,
                                            DistributedGroupedQueryTcpExecutionConfig config) {
  if (!implementation_)
    return invalid("grouped query TCP execution is empty");
  Impl& previous = *implementation_;
  if (previous.execution_state != DistributedGroupedQueryTcpExecutionState::kFailed ||
      !retryable(previous.execution_failure.code())) {
    return invalid("grouped query TCP execution is not eligible for rebinding");
  }
  if (previous.execution_metrics.rebindings_started >= previous.config.maximum_rebindings)
    return exhausted("grouped query TCP execution rebinding budget is exhausted");
  if (config.execution_deadline != previous.config.execution_deadline ||
      config.maximum_rebindings != previous.config.maximum_rebindings) {
    return invalid("grouped query TCP execution rebinding limits changed");
  }
  if (!compatible_rebinding(previous.execution.snapshot(), execution.snapshot()))
    return invalid("grouped query TCP execution replacement changes logical query authority");

  auto replacement =
      DistributedGroupedQueryTcpExecution::create(std::move(execution), std::move(config));
  if (!replacement.has_value())
    return replacement.error();
  const DistributedGroupedQueryTcpExecutionMetrics prior_metrics = previous.execution_metrics;
  replacement->implementation_->execution_metrics.attempts_started +=
      prior_metrics.attempts_started;
  replacement->implementation_->execution_metrics.retries_started += prior_metrics.retries_started;
  replacement->implementation_->execution_metrics.transport_completed_attempts +=
      prior_metrics.transport_completed_attempts;
  replacement->implementation_->execution_metrics.transport_failed_attempts +=
      prior_metrics.transport_failed_attempts;
  replacement->implementation_->execution_metrics.rebindings_started =
      prior_metrics.rebindings_started + 1U;
  implementation_ = std::move(replacement->implementation_);
  return common::Status::ok();
}

DistributedGroupedQueryTcpExecutionState
DistributedGroupedQueryTcpExecution::state() const noexcept {
  return implementation_ ? implementation_->execution_state
                         : DistributedGroupedQueryTcpExecutionState::kFailed;
}

DistributedGroupedQueryTcpExecutionMetrics
DistributedGroupedQueryTcpExecution::metrics() const noexcept {
  return implementation_ ? implementation_->execution_metrics
                         : DistributedGroupedQueryTcpExecutionMetrics{};
}

common::Result<std::vector<query::GroupedFloat64AggregateResult>>
DistributedGroupedQueryTcpExecution::result() const {
  if (!implementation_)
    return common::make_unexpected(invalid("grouped query TCP execution is empty"));
  if (implementation_->execution_state == DistributedGroupedQueryTcpExecutionState::kFailed ||
      implementation_->execution_state == DistributedGroupedQueryTcpExecutionState::kCancelled) {
    return common::make_unexpected(implementation_->execution_failure);
  }
  if (!implementation_->execution_result.has_value())
    return common::make_unexpected(common::Status{common::StatusCode::kUnavailable,
                                                  "grouped query TCP execution is incomplete"});
  return *implementation_->execution_result;
}

const common::Status& DistributedGroupedQueryTcpExecution::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "grouped query TCP execution is empty"};
  return implementation_ ? implementation_->execution_failure : empty;
}

const query::CompatibleDistributedGroupedFloat64Snapshot&
DistributedGroupedQueryTcpExecution::snapshot() const {
  if (!implementation_)
    throw std::logic_error("grouped query TCP execution is empty");
  return implementation_->execution.snapshot();
}

common::Result<std::optional<DistributedQueryLeaderHint>>
DistributedGroupedQueryTcpExecution::suggested_leader(const schema::TabletId& tablet_id) const {
  if (!implementation_)
    return common::make_unexpected(invalid("grouped query TCP execution is empty"));
  return implementation_->execution.suggested_leader(tablet_id);
}

} // namespace chronos::cluster
