#include "chronos/cluster/distributed_query_tcp_execution.hpp"

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
#include <set>
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

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status poll_error(const int error = errno) {
  return {common::StatusCode::kIoError,
          std::string("polling distributed query TCP execution: ") +
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

[[nodiscard]] bool retryable(const common::StatusCode code) noexcept {
  return code == common::StatusCode::kUnavailable ||
         code == common::StatusCode::kResourceExhausted || code == common::StatusCode::kIoError;
}

[[nodiscard]] bool
same_logical_fragment(const query::DistributedAggregateFragmentDispatch& previous,
                      const query::DistributedAggregateFragmentDispatch& replacement) noexcept {
  const auto& left = previous.fragment;
  const auto& right = replacement.fragment;
  return previous.raft_group_id == replacement.raft_group_id && left.query_id == right.query_id &&
         left.database_id == right.database_id && left.table_id == right.table_id &&
         left.tablet_id == right.tablet_id &&
         left.destination_schema_id == right.destination_schema_id &&
         left.read_policy == right.read_policy &&
         left.destination_column_ordinals == right.destination_column_ordinals &&
         left.aggregate_input_index == right.aggregate_input_index &&
         left.event_time_predicate == right.event_time_predicate;
}

[[nodiscard]] bool
compatible_rebinding(const query::CompatibleDistributedAggregateSnapshot& previous,
                     const query::CompatibleDistributedAggregateSnapshot& replacement) noexcept {
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

namespace {

[[nodiscard]] raft::NodeId
selected_serving_node(const query::DistributedAggregateFragmentDispatch& dispatch) noexcept {
  return dispatch.fragment.serving_node;
}

[[nodiscard]] raft::NodeId
selected_serving_node(const query::DistributedVectorFragmentDispatch& dispatch) noexcept {
  return dispatch.serving_node;
}

template <typename Dispatch>
common::Result<std::vector<DistributedQueryNodeRoute>> resolve_distributed_query_node_routes_impl(
    const raft::MetadataCatalogSnapshot& catalog, const std::span<const Dispatch> dispatches,
    const std::span<const DistributedQueryNodeTlsContext> tls_contexts,
    const DistributedQueryRouteResolutionLimits limits) {
  if (limits.maximum_routes == 0U || limits.maximum_routes > 65'536U ||
      limits.maximum_endpoint_bytes == 0U ||
      limits.maximum_endpoint_bytes > raft::MetadataLimits{}.maximum_endpoint_bytes ||
      limits.maximum_addresses_per_route == 0U || limits.maximum_addresses_per_route > 1024U) {
    return common::make_unexpected(
        invalid("distributed query route resolution limits are invalid"));
  }
  if (catalog.applied_index == 0U ||
      catalog.cluster_nodes.size() > raft::MetadataLimits{}.maximum_nodes ||
      !std::ranges::is_sorted(catalog.cluster_nodes, {}, &raft::ClusterNodeMetadata::node_id) ||
      std::ranges::adjacent_find(catalog.cluster_nodes, {}, &raft::ClusterNodeMetadata::node_id) !=
          catalog.cluster_nodes.end() ||
      std::ranges::any_of(catalog.cluster_nodes,
                          [](const auto& node) { return node.node_id == 0U; })) {
    return common::make_unexpected(
        corruption("distributed query node metadata is not a canonical committed snapshot"));
  }
  if (dispatches.empty() || dispatches.size() > query::DistributedPlanLimits{}.maximum_fragments ||
      tls_contexts.empty() ||
      !std::ranges::is_sorted(tls_contexts, {}, &DistributedQueryNodeTlsContext::node_id) ||
      std::ranges::adjacent_find(tls_contexts, {}, &DistributedQueryNodeTlsContext::node_id) !=
          tls_contexts.end() ||
      std::ranges::any_of(tls_contexts, [](const auto& tls) {
        return tls.node_id == 0U || tls.tls_context == nullptr;
      })) {
    return common::make_unexpected(
        invalid("distributed query dispatch or TLS route authority is invalid"));
  }
  try {
    std::set<raft::NodeId> targets;
    for (const auto& dispatch : dispatches) {
      const raft::NodeId target = selected_serving_node(dispatch);
      if (target == 0U)
        return common::make_unexpected(invalid("distributed query dispatch has no serving node"));
      targets.insert(target);
    }
    if (targets.size() > limits.maximum_routes)
      return common::make_unexpected(exhausted("distributed query route limit is exhausted"));

    std::vector<DistributedQueryNodeRoute> routes;
    routes.reserve(targets.size());
    for (const raft::NodeId target : targets) {
      const auto node = std::ranges::lower_bound(catalog.cluster_nodes, target, {},
                                                 &raft::ClusterNodeMetadata::node_id);
      const auto tls = std::ranges::lower_bound(tls_contexts, target, {},
                                                &DistributedQueryNodeTlsContext::node_id);
      if (node == catalog.cluster_nodes.end() || node->node_id != target ||
          node->endpoint.empty() || node->endpoint.size() > limits.maximum_endpoint_bytes) {
        return common::make_unexpected(
            unavailable("distributed query serving node has no bounded committed endpoint"));
      }
      if (tls == tls_contexts.end() || tls->node_id != target || tls->tls_context == nullptr) {
        return common::make_unexpected(
            unavailable("distributed query serving node has no TLS route context"));
      }
      auto endpoints = network::resolve_ipv4_endpoints(
          node->endpoint,
          {.maximum_addresses = limits.maximum_addresses_per_route,
           .maximum_hostname_bytes = std::min<std::size_t>(limits.maximum_endpoint_bytes, 253U)});
      if (!endpoints.has_value() &&
          endpoints.error().code() == common::StatusCode::kInvalidArgument) {
        return common::make_unexpected(unavailable(
            "distributed query serving node endpoint is not a supported IPv4 or DNS route"));
      }
      if (!endpoints.has_value())
        return common::make_unexpected(endpoints.error());
      routes.push_back({target, std::move(*endpoints), tls->tls_context});
    }
    return routes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("distributed query route resolution allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("distributed query route resolution exceeds container limits"));
  }
}

} // namespace

common::Result<std::vector<DistributedQueryNodeRoute>> resolve_distributed_query_node_routes(
    const raft::MetadataCatalogSnapshot& catalog,
    const std::span<const query::DistributedAggregateFragmentDispatch> dispatches,
    const std::span<const DistributedQueryNodeTlsContext> tls_contexts,
    const DistributedQueryRouteResolutionLimits limits) {
  return resolve_distributed_query_node_routes_impl<query::DistributedAggregateFragmentDispatch>(
      catalog, dispatches, tls_contexts, limits);
}

common::Result<std::vector<DistributedQueryNodeRoute>> resolve_distributed_query_node_routes(
    const raft::MetadataCatalogSnapshot& catalog,
    const std::span<const query::DistributedVectorFragmentDispatch> dispatches,
    const std::span<const DistributedQueryNodeTlsContext> tls_contexts,
    const DistributedQueryRouteResolutionLimits limits) {
  return resolve_distributed_query_node_routes_impl<query::DistributedVectorFragmentDispatch>(
      catalog, dispatches, tls_contexts, limits);
}

class DistributedQueryTcpExecution::Impl {
public:
  using TimePoint = DistributedQueryExecution::TimePoint;

  struct Slot {
    schema::TabletId tablet_id;
    std::size_t route_index{};
    std::optional<DistributedQueryTcpClient> client;
  };

  Impl(DistributedQueryExecution owned_execution, DistributedQueryTcpExecutionConfig configured,
       std::vector<Slot> owned_slots, std::vector<pollfd> descriptors,
       std::vector<std::size_t> indexes)
      : execution(std::move(owned_execution)), config(std::move(configured)),
        slots(std::move(owned_slots)), poll_descriptors(std::move(descriptors)),
        poll_slot_indexes(std::move(indexes)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (execution_state == DistributedQueryTcpExecutionState::kRunning) {
      for (Slot& slot : slots)
        slot.client.reset();
      execution_metrics.active_attempts = 0U;
      execution_failure = std::move(status);
      execution_state = DistributedQueryTcpExecutionState::kFailed;
    }
    return execution_failure;
  }

  [[nodiscard]] common::Status cancel(common::Status status) {
    if (execution_state == DistributedQueryTcpExecutionState::kComplete)
      return common::Status::ok();
    if (execution_state == DistributedQueryTcpExecutionState::kRunning) {
      for (Slot& slot : slots)
        slot.client.reset();
      execution_metrics.active_attempts = 0U;
      execution_failure = std::move(status);
      execution_state = DistributedQueryTcpExecutionState::kCancelled;
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
        const TimePoint retry_deadline = deadline->value_or(TimePoint::max());
        if (retry_deadline == TimePoint::max() || now < retry_deadline)
          continue;
      }
      auto attempt = execution.begin_attempt(slot.tablet_id, now);
      if (!attempt.has_value())
        return attempt.error();
      const bool retry = attempt->attempt_number > 1U;
      const DistributedQueryNodeRoute& route = config.routes[slot.route_index];
      const network::Ipv4Endpoint& endpoint =
          route.endpoints[(attempt->attempt_number - 1U) % route.endpoints.size()];
      auto client =
          DistributedQueryTcpClient::begin(std::move(*attempt),
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
        return fail(finished.has_value()
                        ? common::Status{common::StatusCode::kInternal,
                                         "failed distributed query unexpectedly produced a result"}
                        : finished.error());
      }
      all_succeeded = all_succeeded && *sender_state == DistributedQuerySenderState::kSucceeded;
    }
    if (!all_succeeded)
      return common::Status::ok();
    auto finished = execution.finish();
    if (!finished.has_value())
      return fail(finished.error());
    execution_result = *finished;
    execution_state = DistributedQueryTcpExecutionState::kComplete;
    return common::Status::ok();
  }

  DistributedQueryExecution execution;
  DistributedQueryTcpExecutionConfig config;
  std::vector<Slot> slots;
  std::vector<pollfd> poll_descriptors;
  std::vector<std::size_t> poll_slot_indexes;
  DistributedQueryTcpExecutionMetrics execution_metrics;
  DistributedQueryTcpExecutionState execution_state{DistributedQueryTcpExecutionState::kRunning};
  std::optional<query::MergeableAggregateState> execution_result;
  common::Status execution_failure{common::StatusCode::kInternal,
                                   "distributed query TCP execution has not failed"};
};

DistributedQueryTcpExecution::DistributedQueryTcpExecution() noexcept = default;
DistributedQueryTcpExecution::DistributedQueryTcpExecution(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedQueryTcpExecution::~DistributedQueryTcpExecution() = default;
DistributedQueryTcpExecution::DistributedQueryTcpExecution(
    DistributedQueryTcpExecution&&) noexcept = default;
DistributedQueryTcpExecution&
DistributedQueryTcpExecution::operator=(DistributedQueryTcpExecution&&) noexcept = default;

common::Result<DistributedQueryTcpExecution>
DistributedQueryTcpExecution::create(DistributedQueryExecution execution,
                                     DistributedQueryTcpExecutionConfig config) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr ||
      config.routes.empty() || config.routes.size() > 65'536U ||
      config.maximum_rebindings > 1024U || !valid_timeout(config.connect_timeout) ||
      !valid_timeout(config.carrier_limits.handshake_timeout) ||
      !valid_timeout(config.carrier_limits.exchange_timeout)) {
    return common::make_unexpected(
        invalid("distributed query TCP execution configuration is invalid"));
  }
  try {
    std::map<raft::NodeId, std::size_t> route_indexes;
    for (std::size_t index = 0U; index < config.routes.size(); ++index) {
      const DistributedQueryNodeRoute& route = config.routes[index];
      if (route.node_id == 0U || route.endpoints.empty() || route.endpoints.size() > 1024U ||
          route.tls_context == nullptr || !route_indexes.emplace(route.node_id, index).second) {
        return common::make_unexpected(
            invalid("distributed query TCP execution route is invalid or duplicated"));
      }
      for (std::size_t endpoint_index = 0U; endpoint_index < route.endpoints.size();
           ++endpoint_index) {
        const network::Ipv4Endpoint& endpoint = route.endpoints[endpoint_index];
        const std::span<const network::Ipv4Endpoint> prior_endpoints{route.endpoints.data(),
                                                                     endpoint_index};
        if (endpoint.port == 0U || zero_address(endpoint.address) ||
            std::ranges::find(prior_endpoints, endpoint) != prior_endpoints.end()) {
          return common::make_unexpected(
              invalid("distributed query TCP route address is invalid or duplicated"));
        }
      }
    }
    std::vector<Impl::Slot> slots;
    const auto dispatches = execution.snapshot().dispatches();
    slots.reserve(dispatches.size());
    for (const auto& dispatch : dispatches) {
      const auto route = route_indexes.find(dispatch.fragment.serving_node);
      if (route == route_indexes.end())
        return common::make_unexpected(
            invalid("distributed query TCP execution has no route for a target node"));
      slots.push_back({dispatch.fragment.tablet_id, route->second, std::nullopt});
    }
    std::vector<pollfd> descriptors(slots.size());
    std::vector<std::size_t> indexes(slots.size());
    return DistributedQueryTcpExecution{
        std::make_unique<Impl>(std::move(execution), std::move(config), std::move(slots),
                               std::move(descriptors), std::move(indexes))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed query TCP execution allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("distributed query TCP execution exceeds container limits"));
  }
}

common::Status
DistributedQueryTcpExecution::poll_once(const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return invalid("distributed query TCP execution is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return invalid("distributed query TCP execution poll timeout is invalid");
  Impl& impl = *implementation_;
  if (impl.execution_state == DistributedQueryTcpExecutionState::kFailed ||
      impl.execution_state == DistributedQueryTcpExecutionState::kCancelled) {
    return impl.execution_failure;
  }
  if (impl.execution_state == DistributedQueryTcpExecutionState::kComplete)
    return common::Status::ok();

  auto now = std::chrono::steady_clock::now();
  if (impl.config.execution_deadline.has_value() && now >= *impl.config.execution_deadline) {
    return impl.cancel(
        {common::StatusCode::kCancelled, "distributed query TCP execution deadline expired"});
  }
  const common::Status started = impl.start_due_attempts(now);
  if (!started.is_ok())
    return impl.fail(started);
  common::Status initial_terminal = impl.publish_if_terminal();
  if (!initial_terminal.is_ok() ||
      impl.execution_state != DistributedQueryTcpExecutionState::kRunning) {
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
        {common::StatusCode::kCancelled, "distributed query TCP execution deadline expired"});
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
    common::Status driven =
        slot.client->on_ready((events & POLLIN) != 0, (events & POLLOUT) != 0, now);
    const auto client_state = slot.client->state();
    if (!driven.is_ok() || client_state == DistributedQueryTcpClientState::kFailed) {
      common::Status client_failure = std::move(driven);
      if (client_failure.is_ok())
        client_failure = slot.client->failure();
      if (!retryable_client_failure(client_failure.code()))
        return impl.fail(std::move(client_failure));
      const common::Status recorded =
          impl.record_transport_failure(slot, client_failure.code(), now);
      if (!recorded.is_ok())
        return impl.fail(recorded);
      continue;
    }
    if (client_state != DistributedQueryTcpClientState::kComplete)
      continue;
    auto response = slot.client->response_bytes();
    if (!response.has_value())
      return impl.fail(response.error());
    const common::Status accepted = impl.execution.accept_response(slot.tablet_id, *response, now);
    slot.client.reset();
    --impl.execution_metrics.active_attempts;
    ++impl.execution_metrics.transport_completed_attempts;
    if (!accepted.is_ok())
      return impl.fail(accepted);
  }
  return impl.publish_if_terminal();
}

common::Status DistributedQueryTcpExecution::cancel() {
  if (!implementation_)
    return invalid("distributed query TCP execution is empty");
  return implementation_->cancel(
      {common::StatusCode::kCancelled, "distributed query TCP execution was cancelled"});
}

common::Status DistributedQueryTcpExecution::rebind(DistributedQueryExecution execution,
                                                    DistributedQueryTcpExecutionConfig config) {
  if (!implementation_)
    return invalid("distributed query TCP execution is empty");
  Impl& previous = *implementation_;
  if (previous.execution_state != DistributedQueryTcpExecutionState::kFailed ||
      !retryable(previous.execution_failure.code())) {
    return invalid("distributed query TCP execution is not eligible for rebinding");
  }
  if (previous.execution_metrics.rebindings_started >= previous.config.maximum_rebindings)
    return exhausted("distributed query TCP execution rebinding budget is exhausted");
  if (config.execution_deadline != previous.config.execution_deadline ||
      config.maximum_rebindings != previous.config.maximum_rebindings) {
    return invalid("distributed query TCP execution rebinding limits changed");
  }
  if (!compatible_rebinding(previous.execution.snapshot(), execution.snapshot()))
    return invalid("distributed query TCP execution replacement changes logical query authority");

  auto replacement = DistributedQueryTcpExecution::create(std::move(execution), std::move(config));
  if (!replacement.has_value())
    return replacement.error();
  const DistributedQueryTcpExecutionMetrics prior_metrics = previous.execution_metrics;
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

DistributedQueryTcpExecutionState DistributedQueryTcpExecution::state() const noexcept {
  return implementation_ ? implementation_->execution_state
                         : DistributedQueryTcpExecutionState::kFailed;
}

DistributedQueryTcpExecutionMetrics DistributedQueryTcpExecution::metrics() const noexcept {
  return implementation_ ? implementation_->execution_metrics
                         : DistributedQueryTcpExecutionMetrics{};
}

common::Result<query::MergeableAggregateState> DistributedQueryTcpExecution::result() const {
  if (!implementation_)
    return common::make_unexpected(invalid("distributed query TCP execution is empty"));
  if (implementation_->execution_state == DistributedQueryTcpExecutionState::kFailed ||
      implementation_->execution_state == DistributedQueryTcpExecutionState::kCancelled) {
    return common::make_unexpected(implementation_->execution_failure);
  }
  if (!implementation_->execution_result.has_value())
    return common::make_unexpected(common::Status{common::StatusCode::kUnavailable,
                                                  "distributed query TCP execution is incomplete"});
  return implementation_->execution_result.value_or(query::MergeableAggregateState{});
}

const common::Status& DistributedQueryTcpExecution::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "distributed query TCP execution is empty"};
  return implementation_ ? implementation_->execution_failure : empty;
}

const query::CompatibleDistributedAggregateSnapshot&
DistributedQueryTcpExecution::snapshot() const {
  if (!implementation_)
    throw std::logic_error("distributed query TCP execution is empty");
  return implementation_->execution.snapshot();
}

common::Result<std::optional<DistributedQueryLeaderHint>>
DistributedQueryTcpExecution::suggested_leader(const schema::TabletId& tablet_id) const {
  if (!implementation_)
    return common::make_unexpected(invalid("distributed query TCP execution is empty"));
  return implementation_->execution.suggested_leader(tablet_id);
}

} // namespace chronos::cluster
