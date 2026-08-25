#include "chronos/cluster/distributed_vector_grouped_aggregate_query_tcp_execution_v2.hpp"

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
          std::string("polling vector grouped aggregate query v2 TCP execution: ") +
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

[[nodiscard]] bool
valid_carrier_limits(const DistributedVectorGroupedAggregateQueryTlsLimitsV2& limits) noexcept {
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

[[nodiscard]] bool valid_finalization_authority(
    const DistributedVectorGroupedAggregateQueryExecutionV2& execution,
    const DistributedVectorGroupedAggregateFinalizationLimitsV2& limits,
    const query::DistributedVectorGroupedAggregateCoordinatorProjection* const
        projection) noexcept {
  const auto& raw_schema = execution.snapshot().result_schema();
  const auto dispatches = execution.snapshot().dispatches();
  if (dispatches.empty())
    return false;
  const auto& plan = dispatches.front().plan;
  const query::DistributedVectorResultSchema& schema =
      projection == nullptr ? raw_schema : projection->result_schema;
  const std::span<const query::DistributedVectorOrderKey> order_keys =
      projection == nullptr
          ? std::span<const query::DistributedVectorOrderKey>{plan.order_keys}
          : std::span<const query::DistributedVectorOrderKey>{projection->order_keys};
  if (schema.columns.size() > limits.output_batch.maximum_columns ||
      order_keys.size() > limits.sort.maximum_keys ||
      (projection != nullptr &&
       (!plan.order_keys.empty() || plan.limit.has_value() || projection->outputs.empty() ||
        projection->outputs.size() != schema.columns.size())) ||
      ((!order_keys.empty() || projection != nullptr) &&
       (schema.columns.size() > limits.sort.output_limits.maximum_columns ||
        limits.sort.output_limits.maximum_rows > limits.output_batch.maximum_rows))) {
    return false;
  }
  return std::ranges::all_of(schema.columns, [&](const auto& column) {
    return column.name.size() <= limits.output_batch.maximum_column_name_bytes;
  });
}

} // namespace

class DistributedVectorGroupedAggregateQueryTcpExecutionV2::Impl {
public:
  using TimePoint = DistributedVectorGroupedAggregateQuerySenderV2::TimePoint;

  struct Slot {
    schema::TabletId tablet_id;
    std::size_t route_index{};
    DistributedVectorGroupedAggregateQuerySenderV2 sender;
    std::optional<DistributedVectorGroupedAggregateQueryTcpClientV2> client;
    bool delivered{};

    [[nodiscard]] DistributedVectorGroupedAggregateQueryTcpClientV2* active_client() noexcept {
      if (!client.has_value())
        return nullptr;
      // Guarded by the presence check above.
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
      return std::addressof(*client);
    }

    [[nodiscard]] const DistributedVectorGroupedAggregateQueryTcpClientV2*
    active_client() const noexcept {
      if (!client.has_value())
        return nullptr;
      // Guarded by the presence check above.
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
      return std::addressof(*client);
    }
  };

  Impl(DistributedVectorGroupedAggregateQueryExecutionV2 owned_execution,
       DistributedVectorGroupedAggregateQueryTcpExecutionConfigV2 configured,
       std::vector<Slot> owned_slots, std::vector<pollfd> descriptors,
       std::vector<std::size_t> indexes)
      : execution(std::move(owned_execution)), config(std::move(configured)),
        slots(std::move(owned_slots)), poll_descriptors(std::move(descriptors)),
        poll_slot_indexes(std::move(indexes)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (execution_state == DistributedVectorGroupedAggregateQueryTcpExecutionStateV2::kRunning) {
      for (Slot& slot : slots)
        slot.client.reset();
      execution_metrics.active_attempts = 0U;
      execution_failure = std::move(status);
      execution_state = DistributedVectorGroupedAggregateQueryTcpExecutionStateV2::kFailed;
    }
    return execution_failure;
  }

  [[nodiscard]] common::Status cancel(common::Status status) {
    if (execution_state == DistributedVectorGroupedAggregateQueryTcpExecutionStateV2::kComplete)
      return common::Status::ok();
    if (execution_state == DistributedVectorGroupedAggregateQueryTcpExecutionStateV2::kRunning) {
      for (Slot& slot : slots)
        slot.client.reset();
      execution_metrics.active_attempts = 0U;
      execution_failure = std::move(status);
      execution_state = DistributedVectorGroupedAggregateQueryTcpExecutionStateV2::kCancelled;
    }
    return execution_failure;
  }

  [[nodiscard]] common::Status record_transport_failure(Slot& slot, const common::StatusCode code,
                                                        const TimePoint now) {
    slot.client.reset();
    --execution_metrics.active_attempts;
    ++execution_metrics.transport_failed_attempts;
    return slot.sender.record_transport_failure(code, now);
  }

  [[nodiscard]] common::Status start_due_attempts(const TimePoint now) {
    try {
      for (Slot& slot : slots) {
        if (slot.active_client() != nullptr)
          continue;
        const auto sender_state = slot.sender.state();
        if (sender_state != DistributedQuerySenderState::kReady &&
            sender_state != DistributedQuerySenderState::kBackoff) {
          continue;
        }
        if (sender_state == DistributedQuerySenderState::kBackoff) {
          const TimePoint retry_deadline =
              slot.sender.next_attempt_not_before().value_or(TimePoint::max());
          if (retry_deadline == TimePoint::max() || now < retry_deadline)
            continue;
        }
        auto attempt = slot.sender.begin_attempt(now);
        if (!attempt.has_value())
          return attempt.error();
        const bool retry = attempt->attempt_number > 1U;
        const DistributedQueryNodeRoute& route = config.routes[slot.route_index];
        const network::Ipv4Endpoint& endpoint =
            route.endpoints[(attempt->attempt_number - 1U) % route.endpoints.size()];
        std::vector<query::VectorGroupKeyDefinition> keys(slot.sender.keys().begin(),
                                                          slot.sender.keys().end());
        std::vector<query::VectorAggregateDefinition> aggregates(slot.sender.aggregates().begin(),
                                                                 slot.sender.aggregates().end());
        auto client = DistributedVectorGroupedAggregateQueryTcpClientV2::begin(
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
          if (!retryable_connect_failure(client.error().code()))
            return std::move(client).error();
          ++execution_metrics.transport_failed_attempts;
          const common::Status recorded =
              slot.sender.record_transport_failure(client.error().code(), now);
          if (!recorded.is_ok())
            return recorded;
          continue;
        }
        slot.client.emplace(std::move(*client));
        ++execution_metrics.active_attempts;
      }
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return exhausted("vector grouped aggregate query v2 TCP attempt allocation failed");
    } catch (const std::length_error&) {
      return exhausted("vector grouped aggregate query v2 TCP attempt exceeds limits");
    }
  }

  [[nodiscard]] std::chrono::milliseconds bounded_wait(std::chrono::milliseconds maximum_wait,
                                                       const TimePoint now) const {
    std::optional<TimePoint> earliest = config.execution_deadline;
    for (const Slot& slot : slots) {
      if (slot.active_client() != nullptr)
        continue;
      if (slot.sender.state() != DistributedQuerySenderState::kBackoff)
        continue;
      const TimePoint retry_deadline =
          slot.sender.next_attempt_not_before().value_or(TimePoint::max());
      if (retry_deadline != TimePoint::max() &&
          retry_deadline < earliest.value_or(TimePoint::max())) {
        earliest = retry_deadline;
      }
    }
    if (!earliest.has_value() || *earliest <= now)
      return earliest.has_value() ? std::chrono::milliseconds{0} : maximum_wait;
    const auto until = std::chrono::duration_cast<std::chrono::milliseconds>(*earliest - now);
    return std::min(maximum_wait, until);
  }

  [[nodiscard]] common::Status publish_if_terminal() {
    bool all_succeeded = true;
    for (Slot& slot : slots) {
      const auto sender_state = slot.sender.state();
      if (sender_state == DistributedQuerySenderState::kFailed) {
        return fail({slot.sender.last_status_code().value_or(common::StatusCode::kUnavailable),
                     "vector grouped aggregate query v2 sender failed"});
      }
      if (sender_state == DistributedQuerySenderState::kSucceeded && !slot.delivered) {
        const auto& frames = slot.sender.result();
        if (!frames.has_value())
          return fail({common::StatusCode::kInternal,
                       "successful vector grouped aggregate sender has no result"});
        const common::Status accepted = execution.accept_worker_frames(slot.tablet_id, *frames);
        if (!accepted.is_ok())
          return fail(accepted);
        slot.delivered = true;
      }
      all_succeeded = all_succeeded && sender_state == DistributedQuerySenderState::kSucceeded;
    }
    if (!all_succeeded)
      return common::Status::ok();
    const common::Status finished = execution.finish();
    if (!finished.is_ok())
      return fail(finished);
    auto finalized =
        config.coordinator_projection.has_value()
            ? finalize_distributed_vector_grouped_aggregate_with_projection_v2(
                  execution, *config.coordinator_projection, config.finalization_limits)
            : finalize_distributed_vector_grouped_aggregate_v2(execution,
                                                               config.finalization_limits);
    if (!finalized.has_value())
      return fail(finalized.error());
    execution_result.emplace(std::move(*finalized));
    execution_state = DistributedVectorGroupedAggregateQueryTcpExecutionStateV2::kComplete;
    return common::Status::ok();
  }

  DistributedVectorGroupedAggregateQueryExecutionV2 execution;
  DistributedVectorGroupedAggregateQueryTcpExecutionConfigV2 config;
  std::vector<Slot> slots;
  std::vector<pollfd> poll_descriptors;
  std::vector<std::size_t> poll_slot_indexes;
  DistributedVectorGroupedAggregateQueryTcpExecutionMetricsV2 execution_metrics;
  std::optional<DistributedVectorRowsFinalizedResultV2> execution_result;
  DistributedVectorGroupedAggregateQueryTcpExecutionStateV2 execution_state{
      DistributedVectorGroupedAggregateQueryTcpExecutionStateV2::kRunning};
  common::Status execution_failure{
      common::StatusCode::kInternal,
      "vector grouped aggregate query v2 TCP execution has not failed"};
};

DistributedVectorGroupedAggregateQueryTcpExecutionV2::
    DistributedVectorGroupedAggregateQueryTcpExecutionV2() noexcept = default;
DistributedVectorGroupedAggregateQueryTcpExecutionV2::
    DistributedVectorGroupedAggregateQueryTcpExecutionV2(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateQueryTcpExecutionV2::
    ~DistributedVectorGroupedAggregateQueryTcpExecutionV2() = default;
DistributedVectorGroupedAggregateQueryTcpExecutionV2::
    DistributedVectorGroupedAggregateQueryTcpExecutionV2(
        DistributedVectorGroupedAggregateQueryTcpExecutionV2&&) noexcept = default;
DistributedVectorGroupedAggregateQueryTcpExecutionV2&
DistributedVectorGroupedAggregateQueryTcpExecutionV2::operator=(
    DistributedVectorGroupedAggregateQueryTcpExecutionV2&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateQueryTcpExecutionV2>
DistributedVectorGroupedAggregateQueryTcpExecutionV2::create(
    DistributedVectorGroupedAggregateQueryExecutionV2 execution,
    DistributedVectorGroupedAggregateQueryTcpExecutionConfigV2 config) {
  if (config.source_node_id == 0U || config.authenticator == nullptr ||
      config.node_authorizer == nullptr || config.routes.empty() ||
      config.routes.size() > 65'536U || !valid_timeout(config.connect_timeout) ||
      !valid_carrier_limits(config.carrier_limits) ||
      !validate_distributed_vector_grouped_aggregate_finalization_limits_v2(
           config.finalization_limits)
           .is_ok() ||
      !valid_finalization_authority(execution, config.finalization_limits,
                                    config.coordinator_projection.has_value()
                                        ? std::addressof(*config.coordinator_projection)
                                        : nullptr) ||
      execution.key_definitions().size() > config.carrier_limits.payload.maximum_group_keys ||
      execution.aggregate_definitions().size() > config.carrier_limits.payload.maximum_aggregates) {
    return common::make_unexpected(
        invalid("vector grouped aggregate query v2 TCP execution configuration is invalid"));
  }
  try {
    std::map<raft::NodeId, std::size_t> route_indexes;
    for (std::size_t index = 0U; index < config.routes.size(); ++index) {
      const DistributedQueryNodeRoute& route = config.routes[index];
      if (route.node_id == 0U || route.endpoints.empty() || route.endpoints.size() > 1024U ||
          route.tls_context == nullptr || !route_indexes.emplace(route.node_id, index).second) {
        return common::make_unexpected(invalid(
            "vector grouped aggregate query v2 TCP execution route is invalid or duplicated"));
      }
      for (std::size_t endpoint_index = 0U; endpoint_index < route.endpoints.size();
           ++endpoint_index) {
        const network::Ipv4Endpoint& endpoint = route.endpoints[endpoint_index];
        const std::span<const network::Ipv4Endpoint> prior{route.endpoints.data(), endpoint_index};
        if (endpoint.port == 0U || zero_address(endpoint.address) ||
            std::ranges::find(prior, endpoint) != prior.end()) {
          return common::make_unexpected(invalid(
              "vector grouped aggregate query v2 TCP route address is invalid or duplicated"));
        }
      }
    }
    std::vector<Impl::Slot> slots;
    const auto dispatches = execution.snapshot().dispatches();
    slots.reserve(dispatches.size());
    for (const query::DistributedVectorFragmentDispatch& dispatch : dispatches) {
      const auto route = route_indexes.find(dispatch.serving_node);
      if (route == route_indexes.end()) {
        return common::make_unexpected(invalid(
            "vector grouped aggregate query v2 TCP execution has no route for a target node"));
      }
      std::vector<query::VectorGroupKeyDefinition> keys(execution.key_definitions().begin(),
                                                        execution.key_definitions().end());
      std::vector<query::VectorAggregateDefinition> aggregates(
          execution.aggregate_definitions().begin(), execution.aggregate_definitions().end());
      auto sender = DistributedVectorGroupedAggregateQuerySenderV2::create(
          config.source_node_id,
          query::DistributedVectorFragmentDispatchV2{dispatch,
                                                     execution.snapshot().result_schema()},
          std::move(keys), std::move(aggregates), execution.decode_resources(),
          config.sender_limits);
      if (!sender.has_value())
        return common::make_unexpected(sender.error());
      slots.push_back({dispatch.tablet_id, route->second, std::move(*sender), std::nullopt, false});
    }
    std::vector<pollfd> descriptors(slots.size());
    std::vector<std::size_t> indexes(slots.size());
    return DistributedVectorGroupedAggregateQueryTcpExecutionV2{
        std::make_unique<Impl>(std::move(execution), std::move(config), std::move(slots),
                               std::move(descriptors), std::move(indexes))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("vector grouped aggregate query v2 TCP execution allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("vector grouped aggregate query v2 TCP execution exceeds limits"));
  }
}

common::Status DistributedVectorGroupedAggregateQueryTcpExecutionV2::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return invalid("vector grouped aggregate query v2 TCP execution is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return invalid("vector grouped aggregate query v2 TCP execution poll timeout is invalid");
  Impl& impl = *implementation_;
  if (impl.execution_state == DistributedVectorGroupedAggregateQueryTcpExecutionStateV2::kFailed ||
      impl.execution_state ==
          DistributedVectorGroupedAggregateQueryTcpExecutionStateV2::kCancelled) {
    return impl.execution_failure;
  }
  if (impl.execution_state == DistributedVectorGroupedAggregateQueryTcpExecutionStateV2::kComplete)
    return common::Status::ok();

  auto now = std::chrono::steady_clock::now();
  if (impl.config.execution_deadline.has_value() && now >= *impl.config.execution_deadline) {
    return impl.cancel({common::StatusCode::kCancelled,
                        "vector grouped aggregate query v2 TCP execution deadline expired"});
  }
  const common::Status started = impl.start_due_attempts(now);
  if (!started.is_ok())
    return impl.fail(started);
  common::Status initial_terminal = impl.publish_if_terminal();
  if (!initial_terminal.is_ok() ||
      impl.execution_state != DistributedVectorGroupedAggregateQueryTcpExecutionStateV2::kRunning) {
    return initial_terminal;
  }

  std::size_t descriptor_count{};
  for (std::size_t index = 0U; index < impl.slots.size(); ++index) {
    const DistributedVectorGroupedAggregateQueryTcpClientV2* client =
        impl.slots[index].active_client();
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
    return impl.cancel({common::StatusCode::kCancelled,
                        "vector grouped aggregate query v2 TCP execution deadline expired"});
  }
  for (std::size_t descriptor_index = 0U; descriptor_index < descriptor_count; ++descriptor_index) {
    Impl::Slot& slot = impl.slots[impl.poll_slot_indexes[descriptor_index]];
    DistributedVectorGroupedAggregateQueryTcpClientV2* client = slot.active_client();
    if (client == nullptr)
      continue;
    const short events = impl.poll_descriptors[descriptor_index].revents;
    if ((events & (POLLERR | POLLHUP | POLLNVAL)) != 0 && (events & (POLLIN | POLLOUT)) == 0) {
      const common::Status recorded =
          impl.record_transport_failure(slot, common::StatusCode::kIoError, now);
      if (!recorded.is_ok())
        return impl.fail(recorded);
      continue;
    }
    common::Status driven = client->on_ready((events & POLLIN) != 0, (events & POLLOUT) != 0, now);
    const auto client_state = client->state();
    if (!driven.is_ok() ||
        client_state == DistributedVectorGroupedAggregateQueryTcpClientStateV2::kFailed) {
      common::Status client_failure = std::move(driven);
      if (client_failure.is_ok())
        client_failure = client->failure();
      if (client_failure.code() == common::StatusCode::kResourceExhausted)
        return impl.fail(std::move(client_failure));
      const common::Status recorded =
          impl.record_transport_failure(slot, client_failure.code(), now);
      if (!recorded.is_ok())
        return impl.fail(recorded);
      continue;
    }
    if (client_state != DistributedVectorGroupedAggregateQueryTcpClientStateV2::kComplete)
      continue;
    auto responses = client->responses();
    if (!responses.has_value())
      return impl.fail(responses.error());
    const common::Status accepted = slot.sender.accept_responses(*responses, now);
    slot.client.reset();
    --impl.execution_metrics.active_attempts;
    ++impl.execution_metrics.transport_completed_attempts;
    if (!accepted.is_ok())
      return impl.fail(accepted);
  }
  return impl.publish_if_terminal();
}

common::Status DistributedVectorGroupedAggregateQueryTcpExecutionV2::cancel() {
  if (!implementation_)
    return invalid("vector grouped aggregate query v2 TCP execution is empty");
  return implementation_->cancel({common::StatusCode::kCancelled,
                                  "vector grouped aggregate query v2 TCP execution was cancelled"});
}

DistributedVectorGroupedAggregateQueryTcpExecutionStateV2
DistributedVectorGroupedAggregateQueryTcpExecutionV2::state() const noexcept {
  return implementation_ ? implementation_->execution_state
                         : DistributedVectorGroupedAggregateQueryTcpExecutionStateV2::kFailed;
}

DistributedVectorGroupedAggregateQueryTcpExecutionMetricsV2
DistributedVectorGroupedAggregateQueryTcpExecutionV2::metrics() const noexcept {
  return implementation_ ? implementation_->execution_metrics
                         : DistributedVectorGroupedAggregateQueryTcpExecutionMetricsV2{};
}

const std::optional<DistributedVectorRowsFinalizedResultV2>&
DistributedVectorGroupedAggregateQueryTcpExecutionV2::result() const noexcept {
  static const std::optional<DistributedVectorRowsFinalizedResultV2> empty;
  return implementation_ ? implementation_->execution_result : empty;
}

const common::Status&
DistributedVectorGroupedAggregateQueryTcpExecutionV2::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "vector grouped aggregate query v2 TCP execution is empty"};
  return implementation_ ? implementation_->execution_failure : empty;
}

const query::CompatibleDistributedVectorSnapshotV2&
DistributedVectorGroupedAggregateQueryTcpExecutionV2::snapshot() const {
  if (!implementation_)
    throw std::logic_error("vector grouped aggregate query v2 TCP execution is empty");
  return implementation_->execution.snapshot();
}

common::Result<std::optional<DistributedQueryLeaderHint>>
DistributedVectorGroupedAggregateQueryTcpExecutionV2::suggested_leader(
    const schema::TabletId& tablet_id) const {
  if (!implementation_)
    return common::make_unexpected(
        invalid("vector grouped aggregate query v2 TCP execution is empty"));
  for (const Impl::Slot& slot : implementation_->slots) {
    if (slot.tablet_id == tablet_id)
      return slot.sender.suggested_leader();
  }
  return common::make_unexpected(
      invalid("vector grouped aggregate query v2 TCP execution tablet is unplanned"));
}

} // namespace chronos::cluster
