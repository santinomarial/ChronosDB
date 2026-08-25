#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tcp_execution.hpp"

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
          std::string("polling mutable grouped query TCP execution: ") +
              std::error_code(error, std::generic_category()).message()};
}

[[nodiscard]] bool retryable_client_failure(const common::StatusCode code) noexcept {
  return code == common::StatusCode::kUnavailable || code == common::StatusCode::kIoError;
}

[[nodiscard]] bool retryable_rebinding_failure(const common::StatusCode code) noexcept {
  return retryable_client_failure(code) || code == common::StatusCode::kResourceExhausted;
}

[[nodiscard]] bool zero_address(const std::array<std::uint8_t, 4>& address) noexcept {
  return std::ranges::all_of(address, [](const std::uint8_t byte) { return byte == 0U; });
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] bool
equal_key_definitions(const std::span<const query::VectorGroupKeyDefinition> left,
                      const std::span<const query::VectorGroupKeyDefinition> right) noexcept {
  return std::ranges::equal(
      left, right,
      [](const query::VectorGroupKeyDefinition& lhs, const query::VectorGroupKeyDefinition& rhs) {
        return lhs.column_ordinal == rhs.column_ordinal && lhs.type == rhs.type &&
               lhs.nullable == rhs.nullable;
      });
}

[[nodiscard]] bool equal_aggregate_definitions(
    const std::span<const query::VectorAggregateDefinition> left,
    const std::span<const query::VectorAggregateDefinition> right) noexcept {
  return std::ranges::equal(left, right,
                            [](const query::VectorAggregateDefinition& lhs,
                               const query::VectorAggregateDefinition& rhs) { return lhs == rhs; });
}

[[nodiscard]] bool
valid_limits(const DistributedMutableVectorGroupedAggregateQueryTlsLimits& limits) noexcept {
  constexpr std::size_t kMinimumResponseBytes =
      kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize +
      kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize;
  return valid_timeout(limits.handshake_timeout) && valid_timeout(limits.exchange_timeout) &&
         limits.maximum_response_frames > 0U &&
         limits.maximum_response_frames <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups &&
         limits.maximum_response_frames <= limits.payload.maximum_groups &&
         limits.maximum_response_bytes >= kMinimumResponseBytes &&
         limits.maximum_response_bytes <=
             kMaximumDistributedVectorGroupedAggregateQueryV2ResponseBytes &&
         limits.payload.maximum_frame_length >=
             query::distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength &&
         limits.payload.maximum_frame_length <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumFrameLength &&
         limits.payload.maximum_key_payload_bytes > 0U &&
         limits.payload.maximum_key_payload_bytes <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumKeyPayloadBytes &&
         limits.payload.maximum_groups > 0U &&
         limits.payload.maximum_groups <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups &&
         limits.payload.maximum_group_keys > 0U &&
         limits.payload.maximum_group_keys <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroupKeys &&
         limits.payload.maximum_aggregates <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumAggregates &&
         limits.payload.state.maximum_frame_length >=
             query::distributed_vector_aggregate_state_format::kMinimumFrameLength &&
         limits.payload.state.maximum_frame_length <=
             query::distributed_vector_aggregate_state_format::kMaximumFrameLength &&
         limits.payload.state.maximum_variable_extremum_bytes > 0U &&
         limits.payload.state.maximum_variable_extremum_bytes <=
             query::distributed_vector_aggregate_state_format::kMaximumExtremumBytes;
}

} // namespace

class DistributedMutableVectorGroupedAggregateQueryTcpExecution::Impl {
public:
  using TimePoint = DistributedMutableVectorGroupedAggregateQueryExecution::TimePoint;

  struct Slot {
    schema::TabletId tablet_id;
    std::size_t route_index{};
    std::optional<DistributedMutableVectorGroupedAggregateQueryTcpClient> client;

    [[nodiscard]] DistributedMutableVectorGroupedAggregateQueryTcpClient* active_client() noexcept {
      if (!client.has_value())
        return nullptr;
      // Guarded by the presence check above.
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
      return std::addressof(*client);
    }

    [[nodiscard]] const DistributedMutableVectorGroupedAggregateQueryTcpClient*
    active_client() const noexcept {
      if (!client.has_value())
        return nullptr;
      // Guarded by the presence check above.
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
      return std::addressof(*client);
    }
  };

  Impl(DistributedMutableVectorGroupedAggregateQueryExecution owned_execution,
       DistributedMutableVectorGroupedAggregateQueryTcpExecutionConfig configured,
       std::vector<Slot> owned_slots, std::vector<pollfd> descriptors,
       std::vector<std::size_t> indexes)
      : execution(std::move(owned_execution)), config(std::move(configured)),
        slots(std::move(owned_slots)), poll_descriptors(std::move(descriptors)),
        poll_slot_indexes(std::move(indexes)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (execution_state ==
        DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kRunning) {
      for (Slot& slot : slots)
        slot.client.reset();
      execution_metrics.active_attempts = 0U;
      execution_failure = std::move(status);
      execution_state = DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kFailed;
    }
    return execution_failure;
  }

  [[nodiscard]] common::Status cancel(common::Status status) {
    if (execution_state ==
        DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kComplete)
      return common::Status::ok();
    if (execution_state ==
        DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kRunning) {
      for (Slot& slot : slots)
        slot.client.reset();
      execution_metrics.active_attempts = 0U;
      execution_failure = std::move(status);
      execution_state = DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kCancelled;
    }
    return execution_failure;
  }

  [[nodiscard]] common::Status record_failure(Slot& slot, const common::StatusCode code,
                                              const TimePoint now) {
    slot.client.reset();
    --execution_metrics.active_attempts;
    ++execution_metrics.transport_failed_attempts;
    return execution.record_transport_failure(slot.tablet_id, code, now);
  }

  [[nodiscard]] common::Status start_due_attempts(const TimePoint now) {
    try {
      for (Slot& slot : slots) {
        if (slot.active_client() != nullptr)
          continue;
        auto sender_state = execution.sender_state(slot.tablet_id);
        if (!sender_state.has_value())
          return sender_state.error();
        if (*sender_state != DistributedQuerySenderState::kReady &&
            *sender_state != DistributedQuerySenderState::kBackoff)
          continue;
        if (*sender_state == DistributedQuerySenderState::kBackoff) {
          auto due = execution.next_attempt_not_before(slot.tablet_id);
          if (!due.has_value())
            return due.error();
          if (!due->has_value() || now < **due)
            continue;
        }
        auto attempt = execution.begin_attempt(slot.tablet_id, now);
        if (!attempt.has_value())
          return attempt.error();
        const bool retry = attempt->attempt_number > 1U;
        const DistributedQueryNodeRoute& route = config.routes[slot.route_index];
        if (attempt->target_node_id != route.node_id)
          return invalid("mutable grouped attempt escaped its immutable target route");
        const network::Ipv4Endpoint& endpoint =
            route.endpoints[(attempt->attempt_number - 1U) % route.endpoints.size()];
        std::vector<query::VectorGroupKeyDefinition> keys(execution.key_definitions().begin(),
                                                          execution.key_definitions().end());
        std::vector<query::VectorAggregateDefinition> aggregates(
            execution.aggregate_definitions().begin(), execution.aggregate_definitions().end());
        auto client = DistributedMutableVectorGroupedAggregateQueryTcpClient::begin(
            std::move(*attempt), std::move(keys), std::move(aggregates),
            execution.decode_resources(),
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
    } catch (const std::bad_alloc&) {
      return exhausted("mutable grouped TCP attempt allocation failed");
    } catch (const std::length_error&) {
      return exhausted("mutable grouped TCP attempt exceeds limits");
    }
  }

  [[nodiscard]] common::Status publish_if_terminal() {
    bool all_succeeded = true;
    for (const Slot& slot : slots) {
      auto sender_state = execution.sender_state(slot.tablet_id);
      if (!sender_state.has_value())
        return fail(sender_state.error());
      if (*sender_state == DistributedQuerySenderState::kFailed) {
        common::Status finished = execution.finish();
        return fail(
            finished.is_ok()
                ? common::Status{common::StatusCode::kInternal,
                                 "failed mutable grouped query unexpectedly finished successfully"}
                : std::move(finished));
      }
      all_succeeded = all_succeeded && *sender_state == DistributedQuerySenderState::kSucceeded;
    }
    if (!all_succeeded)
      return common::Status::ok();
    common::Status finished = execution.finish();
    if (!finished.is_ok())
      return fail(std::move(finished));
    execution_state = DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kComplete;
    return common::Status::ok();
  }

  DistributedMutableVectorGroupedAggregateQueryExecution execution;
  DistributedMutableVectorGroupedAggregateQueryTcpExecutionConfig config;
  std::vector<Slot> slots;
  std::vector<pollfd> poll_descriptors;
  std::vector<std::size_t> poll_slot_indexes;
  DistributedMutableVectorGroupedAggregateQueryTcpExecutionMetrics execution_metrics;
  DistributedMutableVectorGroupedAggregateQueryTcpExecutionState execution_state{
      DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kRunning};
  common::Status execution_failure{common::StatusCode::kInternal,
                                   "mutable grouped TCP execution has not failed"};
};

DistributedMutableVectorGroupedAggregateQueryTcpExecution::
    DistributedMutableVectorGroupedAggregateQueryTcpExecution() noexcept = default;
DistributedMutableVectorGroupedAggregateQueryTcpExecution::
    DistributedMutableVectorGroupedAggregateQueryTcpExecution(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedMutableVectorGroupedAggregateQueryTcpExecution::
    ~DistributedMutableVectorGroupedAggregateQueryTcpExecution() = default;
DistributedMutableVectorGroupedAggregateQueryTcpExecution::
    DistributedMutableVectorGroupedAggregateQueryTcpExecution(
        DistributedMutableVectorGroupedAggregateQueryTcpExecution&&) noexcept = default;
DistributedMutableVectorGroupedAggregateQueryTcpExecution&
DistributedMutableVectorGroupedAggregateQueryTcpExecution::operator=(
    DistributedMutableVectorGroupedAggregateQueryTcpExecution&&) noexcept = default;

common::Result<DistributedMutableVectorGroupedAggregateQueryTcpExecution>
DistributedMutableVectorGroupedAggregateQueryTcpExecution::create(
    DistributedMutableVectorGroupedAggregateQueryExecution execution,
    DistributedMutableVectorGroupedAggregateQueryTcpExecutionConfig config) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr ||
      config.routes.empty() || config.routes.size() > 65'536U ||
      config.maximum_rebindings > 1024U || !valid_timeout(config.connect_timeout) ||
      !valid_limits(config.carrier_limits) ||
      execution.key_definitions().size() > config.carrier_limits.payload.maximum_group_keys ||
      execution.aggregate_definitions().size() > config.carrier_limits.payload.maximum_aggregates) {
    return common::make_unexpected(
        invalid("mutable grouped TCP execution configuration is invalid"));
  }
  try {
    std::map<raft::NodeId, std::size_t> route_indexes;
    for (std::size_t index = 0U; index < config.routes.size(); ++index) {
      const DistributedQueryNodeRoute& route = config.routes[index];
      if (route.node_id == 0U || route.endpoints.empty() || route.endpoints.size() > 1024U ||
          route.tls_context == nullptr || !route_indexes.emplace(route.node_id, index).second) {
        return common::make_unexpected(
            invalid("mutable grouped TCP execution route is invalid or duplicated"));
      }
      for (std::size_t endpoint_index = 0U; endpoint_index < route.endpoints.size();
           ++endpoint_index) {
        const network::Ipv4Endpoint& endpoint = route.endpoints[endpoint_index];
        const std::span<const network::Ipv4Endpoint> prior{route.endpoints.data(), endpoint_index};
        if (endpoint.port == 0U || zero_address(endpoint.address) ||
            std::ranges::find(prior, endpoint) != prior.end()) {
          return common::make_unexpected(
              invalid("mutable grouped TCP route address is invalid or duplicated"));
        }
      }
    }
    std::vector<Impl::Slot> slots;
    slots.reserve(execution.targets().size());
    for (const auto& target : execution.targets()) {
      const auto route = route_indexes.find(target.serving_node);
      if (route == route_indexes.end())
        return common::make_unexpected(
            invalid("mutable grouped TCP execution has no route for a target node"));
      slots.push_back({target.tablet_id, route->second, std::nullopt});
    }
    std::vector<pollfd> descriptors(slots.size());
    std::vector<std::size_t> indexes(slots.size());
    return DistributedMutableVectorGroupedAggregateQueryTcpExecution{
        std::make_unique<Impl>(std::move(execution), std::move(config), std::move(slots),
                               std::move(descriptors), std::move(indexes))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable grouped TCP execution allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("mutable grouped TCP execution exceeds limits"));
  }
}

common::Status DistributedMutableVectorGroupedAggregateQueryTcpExecution::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return invalid("mutable grouped TCP execution is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return invalid("mutable grouped TCP poll timeout is invalid");
  Impl& impl = *implementation_;
  if (impl.execution_state ==
          DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kFailed ||
      impl.execution_state ==
          DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kCancelled)
    return impl.execution_failure;
  if (impl.execution_state ==
      DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kComplete)
    return common::Status::ok();
  auto now = std::chrono::steady_clock::now();
  if (impl.config.execution_deadline.has_value() && now >= *impl.config.execution_deadline)
    return impl.cancel(
        {common::StatusCode::kCancelled, "mutable grouped TCP execution deadline expired"});
  common::Status started = impl.start_due_attempts(now);
  if (!started.is_ok())
    return impl.fail(std::move(started));
  common::Status initial_terminal = impl.publish_if_terminal();
  if (!initial_terminal.is_ok() ||
      impl.execution_state ==
          DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kComplete)
    return initial_terminal;

  std::size_t count{};
  for (std::size_t index = 0U; index < impl.slots.size(); ++index) {
    const auto* client = impl.slots[index].active_client();
    if (client == nullptr)
      continue;
    const auto interest = client->interest();
    impl.poll_descriptors[count] = {.fd = client->descriptor(),
                                    .events =
                                        static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                           (interest.want_write ? POLLOUT : 0)),
                                    .revents = 0};
    impl.poll_slot_indexes[count++] = index;
  }
  std::chrono::milliseconds wait = maximum_wait;
  if (impl.config.execution_deadline.has_value()) {
    if (*impl.config.execution_deadline <= now)
      wait = std::chrono::milliseconds{0};
    else
      wait = std::min(wait, std::chrono::duration_cast<std::chrono::milliseconds>(
                                *impl.config.execution_deadline - now));
  }
  for (const auto& slot : impl.slots) {
    if (slot.active_client() != nullptr)
      continue;
    auto due = impl.execution.next_attempt_not_before(slot.tablet_id);
    if (due.has_value() && due->has_value()) {
      wait =
          **due <= now
              ? std::chrono::milliseconds{0}
              : std::min(wait, std::chrono::duration_cast<std::chrono::milliseconds>(**due - now));
    }
  }
  const int ready = ::poll(impl.poll_descriptors.data(), static_cast<nfds_t>(count),
                           static_cast<int>(wait.count()));
  if (ready < 0 && errno != EINTR)
    return impl.fail(poll_error());
  now = std::chrono::steady_clock::now();
  if (impl.config.execution_deadline.has_value() && now >= *impl.config.execution_deadline)
    return impl.cancel(
        {common::StatusCode::kCancelled, "mutable grouped TCP execution deadline expired"});
  for (std::size_t descriptor = 0U; descriptor < count; ++descriptor) {
    Impl::Slot& slot = impl.slots[impl.poll_slot_indexes[descriptor]];
    auto* client = slot.active_client();
    if (client == nullptr)
      continue;
    const short events = impl.poll_descriptors[descriptor].revents;
    if ((events & (POLLERR | POLLHUP | POLLNVAL)) != 0 && (events & (POLLIN | POLLOUT)) == 0) {
      common::Status recorded = impl.record_failure(slot, common::StatusCode::kIoError, now);
      if (!recorded.is_ok())
        return impl.fail(std::move(recorded));
      continue;
    }
    common::Status driven = client->on_ready((events & POLLIN) != 0, (events & POLLOUT) != 0, now);
    const auto state = client->state();
    if (!driven.is_ok() ||
        state == DistributedMutableVectorGroupedAggregateQueryTcpClientState::kFailed) {
      if (driven.is_ok())
        driven = client->failure();
      if (!retryable_client_failure(driven.code()))
        return impl.fail(std::move(driven));
      common::Status recorded = impl.record_failure(slot, driven.code(), now);
      if (!recorded.is_ok())
        return impl.fail(std::move(recorded));
      continue;
    }
    if (state != DistributedMutableVectorGroupedAggregateQueryTcpClientState::kComplete)
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

common::Status DistributedMutableVectorGroupedAggregateQueryTcpExecution::cancel() {
  if (!implementation_)
    return invalid("mutable grouped TCP execution is empty");
  return implementation_->cancel(
      {common::StatusCode::kCancelled, "mutable grouped TCP execution was cancelled"});
}

common::Status DistributedMutableVectorGroupedAggregateQueryTcpExecution::rebind(
    DistributedMutableVectorGroupedAggregateQueryExecution execution,
    DistributedMutableVectorGroupedAggregateQueryTcpExecutionConfig config) {
  if (!implementation_)
    return invalid("mutable grouped TCP execution is empty");
  Impl& previous = *implementation_;
  if (previous.execution_state !=
          DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kFailed ||
      !retryable_rebinding_failure(previous.execution_failure.code())) {
    return invalid("mutable grouped TCP execution is not eligible for rebinding");
  }
  if (previous.execution_metrics.rebindings_started >= previous.config.maximum_rebindings)
    return exhausted("mutable grouped TCP execution rebinding budget is exhausted");
  if (config.execution_deadline != previous.config.execution_deadline ||
      config.maximum_rebindings != previous.config.maximum_rebindings ||
      previous.execution.logical_identity() != execution.logical_identity() ||
      !equal_key_definitions(previous.execution.key_definitions(), execution.key_definitions()) ||
      !equal_aggregate_definitions(previous.execution.aggregate_definitions(),
                                   execution.aggregate_definitions())) {
    return invalid("mutable grouped TCP replacement changes logical query authority or limits");
  }
  auto replacement = create(std::move(execution), std::move(config));
  if (!replacement.has_value())
    return replacement.error();
  const auto prior = previous.execution_metrics;
  auto& metrics = replacement->implementation_->execution_metrics;
  metrics.attempts_started += prior.attempts_started;
  metrics.retries_started += prior.retries_started;
  metrics.transport_completed_attempts += prior.transport_completed_attempts;
  metrics.transport_failed_attempts += prior.transport_failed_attempts;
  metrics.rebindings_started = prior.rebindings_started + 1U;
  implementation_ = std::move(replacement->implementation_);
  return common::Status::ok();
}

DistributedMutableVectorGroupedAggregateQueryTcpExecutionState
DistributedMutableVectorGroupedAggregateQueryTcpExecution::state() const noexcept {
  return implementation_ ? implementation_->execution_state
                         : DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kFailed;
}

DistributedMutableVectorGroupedAggregateQueryTcpExecutionMetrics
DistributedMutableVectorGroupedAggregateQueryTcpExecution::metrics() const noexcept {
  return implementation_ ? implementation_->execution_metrics
                         : DistributedMutableVectorGroupedAggregateQueryTcpExecutionMetrics{};
}

common::Result<query::PhysicalOperatorStep>
DistributedMutableVectorGroupedAggregateQueryTcpExecution::next() {
  if (!implementation_ ||
      implementation_->execution_state !=
          DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kComplete) {
    return common::make_unexpected(invalid("mutable grouped TCP execution output is unavailable"));
  }
  return implementation_->execution.next();
}

std::span<const query::VectorGroupKeyDefinition>
DistributedMutableVectorGroupedAggregateQueryTcpExecution::key_definitions() const noexcept {
  return implementation_ ? implementation_->execution.key_definitions()
                         : std::span<const query::VectorGroupKeyDefinition>{};
}

std::span<const query::VectorAggregateDefinition>
DistributedMutableVectorGroupedAggregateQueryTcpExecution::aggregate_definitions() const noexcept {
  return implementation_ ? implementation_->execution.aggregate_definitions()
                         : std::span<const query::VectorAggregateDefinition>{};
}

const query::DistributedVectorPlanIntent&
DistributedMutableVectorGroupedAggregateQueryTcpExecution::plan() const {
  if (!implementation_)
    throw std::logic_error("mutable grouped TCP execution is empty");
  return implementation_->execution.plan();
}

const query::DistributedVectorResultSchema&
DistributedMutableVectorGroupedAggregateQueryTcpExecution::result_schema() const {
  if (!implementation_)
    throw std::logic_error("mutable grouped TCP execution is empty");
  return implementation_->execution.result_schema();
}

std::optional<query::QueryResourceContext>
DistributedMutableVectorGroupedAggregateQueryTcpExecution::output_resources() const noexcept {
  return implementation_ ? implementation_->execution.output_resources() : std::nullopt;
}

const common::Status&
DistributedMutableVectorGroupedAggregateQueryTcpExecution::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "mutable grouped TCP execution is empty"};
  return implementation_ ? implementation_->execution_failure : empty;
}

common::Result<std::optional<DistributedQueryLeaderHint>>
DistributedMutableVectorGroupedAggregateQueryTcpExecution::suggested_leader(
    const schema::TabletId& tablet_id) const {
  if (!implementation_)
    return common::make_unexpected(invalid("mutable grouped TCP execution is empty"));
  return implementation_->execution.suggested_leader(tablet_id);
}

} // namespace chronos::cluster
