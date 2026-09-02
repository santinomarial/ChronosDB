#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tcp_execution.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <new>
#include <optional>
#include <poll.h>
#include <ranges>
#include <set>
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
          std::string("polling grouped shuffle TCP execution: ") +
              std::error_code(error, std::generic_category()).message()};
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
valid_carrier_limits(const DistributedVectorGroupedAggregateShuffleTlsLimits& limits) noexcept {
  return valid_timeout(limits.handshake_timeout) && valid_timeout(limits.exchange_timeout) &&
         validate_distributed_vector_grouped_aggregate_shuffle_stream_limits(limits.stream);
}

} // namespace

class DistributedVectorGroupedAggregateShuffleTcpExecution::Impl {
public:
  using TimePoint = DistributedVectorGroupedAggregateShuffleRetry::TimePoint;

  struct Slot {
    std::size_t route_index{};
    DistributedVectorGroupedAggregateShuffleRetry retry;
    std::optional<DistributedVectorGroupedAggregateShuffleTcpClient> client{std::nullopt};

    [[nodiscard]] DistributedVectorGroupedAggregateShuffleTcpClient* active_client() noexcept {
      return client.has_value() ? std::addressof(*client) : nullptr;
    }

    [[nodiscard]] const DistributedVectorGroupedAggregateShuffleTcpClient*
    active_client() const noexcept {
      return client.has_value() ? std::addressof(*client) : nullptr;
    }
  };

  Impl(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
       DistributedVectorGroupedAggregateShuffleTcpExecutionConfig config, std::vector<Slot> slots,
       std::vector<pollfd> poll_descriptors, std::vector<std::size_t> poll_slot_indexes)
      : authority_(authority), config_(std::move(config)), slots_(std::move(slots)),
        poll_descriptors_(std::move(poll_descriptors)),
        poll_slot_indexes_(std::move(poll_slot_indexes)) {
    metrics_.total_edges = slots_.size();
  }

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ == DistributedVectorGroupedAggregateShuffleTcpExecutionState::kRunning) {
      for (Slot& slot : slots_)
        slot.client.reset();
      metrics_.active_attempts = 0U;
      failure_ = std::move(status);
      state_ = DistributedVectorGroupedAggregateShuffleTcpExecutionState::kFailed;
    }
    return failure_;
  }

  [[nodiscard]] common::Status cancel(common::Status status) {
    if (state_ == DistributedVectorGroupedAggregateShuffleTcpExecutionState::kComplete)
      return common::Status::ok();
    if (state_ == DistributedVectorGroupedAggregateShuffleTcpExecutionState::kRunning) {
      for (Slot& slot : slots_)
        slot.client.reset();
      metrics_.active_attempts = 0U;
      failure_ = std::move(status);
      state_ = DistributedVectorGroupedAggregateShuffleTcpExecutionState::kCancelled;
    }
    return failure_;
  }

  [[nodiscard]] common::Status record_failure(Slot& slot, const common::StatusCode code,
                                              const TimePoint now) {
    slot.client.reset();
    --metrics_.active_attempts;
    ++metrics_.transport_failed_attempts;
    return slot.retry.record_attempt_failure(code, now);
  }

  [[nodiscard]] common::Status start_due_attempts(const TimePoint now) {
    for (Slot& slot : slots_) {
      if (slot.active_client() != nullptr)
        continue;
      const auto retry_state = slot.retry.state();
      if (retry_state != DistributedVectorGroupedAggregateShuffleRetryState::kReady &&
          retry_state != DistributedVectorGroupedAggregateShuffleRetryState::kBackoff) {
        continue;
      }
      if (retry_state == DistributedVectorGroupedAggregateShuffleRetryState::kBackoff &&
          now < slot.retry.next_attempt_not_before().value_or(TimePoint::max())) {
        continue;
      }
      auto attempt = slot.retry.begin_attempt(now);
      if (!attempt.has_value())
        return attempt.error();
      const bool is_retry = attempt->attempt_number > 1U;
      const DistributedQueryNodeRoute& route = config_.routes[slot.route_index];
      const network::Ipv4Endpoint& endpoint =
          route.endpoints[(attempt->attempt_number - 1U) % route.endpoints.size()];
      auto client = DistributedVectorGroupedAggregateShuffleTcpClient::begin(
          std::move(*attempt), authority_.get(),
          {.remote_endpoint = endpoint,
           .tls_context = route.tls_context,
           .carrier = {.authenticator = config_.authenticator,
                       .node_authorizer = config_.node_authorizer,
                       .peer_ipv4_address = endpoint.address,
                       .limits = config_.carrier_limits},
           .connect_timeout = config_.connect_timeout},
          now);
      ++metrics_.attempts_started;
      if (is_retry)
        ++metrics_.retries_started;
      if (!client.has_value()) {
        ++metrics_.transport_failed_attempts;
        const common::Status recorded =
            slot.retry.record_attempt_failure(client.error().code(), now);
        if (!recorded.is_ok())
          return recorded;
        continue;
      }
      slot.client.emplace(std::move(*client));
      ++metrics_.active_attempts;
    }
    return common::Status::ok();
  }

  [[nodiscard]] std::chrono::milliseconds bounded_wait(const std::chrono::milliseconds maximum_wait,
                                                       const TimePoint now) const {
    std::optional<TimePoint> earliest = config_.execution_deadline;
    for (const Slot& slot : slots_) {
      const auto* client = slot.active_client();
      if (client != nullptr) {
        if (client->deadline() < earliest.value_or(TimePoint::max()))
          earliest = client->deadline();
        continue;
      }
      if (slot.retry.state() != DistributedVectorGroupedAggregateShuffleRetryState::kBackoff) {
        continue;
      }
      const auto due = slot.retry.next_attempt_not_before().value_or(TimePoint::max());
      if (due < earliest.value_or(TimePoint::max()))
        earliest = due;
    }
    if (!earliest.has_value())
      return maximum_wait;
    if (*earliest <= now)
      return std::chrono::milliseconds{0};
    return std::min(maximum_wait,
                    std::chrono::duration_cast<std::chrono::milliseconds>(*earliest - now));
  }

  [[nodiscard]] common::Status publish_if_terminal() {
    bool complete = true;
    std::size_t succeeded{};
    for (const Slot& slot : slots_) {
      const auto retry_state = slot.retry.state();
      if (retry_state == DistributedVectorGroupedAggregateShuffleRetryState::kFailed) {
        return fail({slot.retry.last_status_code().value_or(common::StatusCode::kUnavailable),
                     "grouped shuffle remote edge exhausted its retry policy"});
      }
      if (retry_state == DistributedVectorGroupedAggregateShuffleRetryState::kSucceeded)
        ++succeeded;
      complete =
          complete && retry_state == DistributedVectorGroupedAggregateShuffleRetryState::kSucceeded;
    }
    metrics_.succeeded_edges = succeeded;
    if (complete)
      state_ = DistributedVectorGroupedAggregateShuffleTcpExecutionState::kComplete;
    return common::Status::ok();
  }

  std::reference_wrapper<const DistributedVectorGroupedAggregateShuffleAuthority> authority_;
  DistributedVectorGroupedAggregateShuffleTcpExecutionConfig config_;
  std::vector<Slot> slots_;
  std::vector<pollfd> poll_descriptors_;
  std::vector<std::size_t> poll_slot_indexes_;
  DistributedVectorGroupedAggregateShuffleTcpExecutionMetrics metrics_;
  DistributedVectorGroupedAggregateShuffleTcpExecutionState state_{
      DistributedVectorGroupedAggregateShuffleTcpExecutionState::kRunning};
  common::Status failure_{common::StatusCode::kInternal,
                          "grouped shuffle TCP execution has not failed"};
};

DistributedVectorGroupedAggregateShuffleTcpExecution::
    DistributedVectorGroupedAggregateShuffleTcpExecution() noexcept = default;
DistributedVectorGroupedAggregateShuffleTcpExecution::
    DistributedVectorGroupedAggregateShuffleTcpExecution(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateShuffleTcpExecution::
    ~DistributedVectorGroupedAggregateShuffleTcpExecution() = default;
DistributedVectorGroupedAggregateShuffleTcpExecution::
    DistributedVectorGroupedAggregateShuffleTcpExecution(
        DistributedVectorGroupedAggregateShuffleTcpExecution&&) noexcept = default;
DistributedVectorGroupedAggregateShuffleTcpExecution&
DistributedVectorGroupedAggregateShuffleTcpExecution::operator=(
    DistributedVectorGroupedAggregateShuffleTcpExecution&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateShuffleTcpExecution>
DistributedVectorGroupedAggregateShuffleTcpExecution::create(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    std::vector<DistributedVectorGroupedAggregateShuffleRetry> retries,
    DistributedVectorGroupedAggregateShuffleTcpExecutionConfig config) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr || retries.empty() ||
      retries.size() > kMaximumDistributedVectorGroupedAggregateShuffleRemoteEdges ||
      config.routes.empty() ||
      config.routes.size() > kMaximumDistributedVectorGroupedAggregateShuffleRemoteEdges ||
      !valid_timeout(config.connect_timeout) || !valid_carrier_limits(config.carrier_limits)) {
    return common::make_unexpected(
        invalid("grouped shuffle TCP execution configuration is invalid"));
  }
  try {
    std::map<raft::NodeId, std::size_t> route_indexes;
    for (std::size_t index = 0U; index < config.routes.size(); ++index) {
      const auto& route = config.routes[index];
      if (route.node_id == 0U || route.endpoints.empty() || route.endpoints.size() > 1024U ||
          route.tls_context == nullptr || !route_indexes.emplace(route.node_id, index).second) {
        return common::make_unexpected(
            invalid("grouped shuffle TCP execution route is invalid or duplicated"));
      }
      for (std::size_t endpoint_index = 0U; endpoint_index < route.endpoints.size();
           ++endpoint_index) {
        const auto& endpoint = route.endpoints[endpoint_index];
        const std::span<const network::Ipv4Endpoint> prior{route.endpoints.data(), endpoint_index};
        if (endpoint.port == 0U || zero_address(endpoint.address) ||
            std::ranges::find(prior, endpoint) != prior.end()) {
          return common::make_unexpected(
              invalid("grouped shuffle TCP execution route address is invalid or duplicated"));
        }
      }
    }

    using EdgeIdentity = std::pair<schema::TabletId, std::uint32_t>;
    std::set<EdgeIdentity> edges;
    std::vector<Impl::Slot> slots;
    slots.reserve(retries.size());
    for (auto& retry : retries) {
      const auto& edge = retry.edge();
      if (std::addressof(retry.authority()) != std::addressof(authority) ||
          edge.source_node_id == edge.target_node_id || !authority.validate_edge(edge).is_ok() ||
          !edges.emplace(edge.tablet_id, edge.partition_id).second) {
        return common::make_unexpected(
            invalid("grouped shuffle TCP retry authority or edge is invalid"));
      }
      const auto route = route_indexes.find(edge.target_node_id);
      if (route == route_indexes.end()) {
        return common::make_unexpected(
            invalid("grouped shuffle TCP execution has no route for a target node"));
      }
      slots.push_back({.route_index = route->second, .retry = std::move(retry)});
    }
    std::vector<pollfd> descriptors(slots.size());
    std::vector<std::size_t> indexes(slots.size());
    return DistributedVectorGroupedAggregateShuffleTcpExecution{
        std::make_unique<Impl>(authority, std::move(config), std::move(slots),
                               std::move(descriptors), std::move(indexes))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle TCP execution allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle TCP execution exceeds limits"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleTcpExecution::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return invalid("grouped shuffle TCP execution is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return invalid("grouped shuffle TCP execution poll timeout is invalid");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleTcpExecutionState::kFailed ||
      impl.state_ == DistributedVectorGroupedAggregateShuffleTcpExecutionState::kCancelled) {
    return impl.failure_;
  }
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleTcpExecutionState::kComplete)
    return common::Status::ok();

  auto now = std::chrono::steady_clock::now();
  if (impl.config_.execution_deadline.has_value() && now >= *impl.config_.execution_deadline) {
    return impl.cancel(
        {common::StatusCode::kCancelled, "grouped shuffle TCP execution deadline expired"});
  }
  const common::Status started = impl.start_due_attempts(now);
  if (!started.is_ok()) {
    return started.code() == common::StatusCode::kResourceExhausted ? started : impl.fail(started);
  }
  common::Status terminal = impl.publish_if_terminal();
  if (!terminal.is_ok() ||
      impl.state_ != DistributedVectorGroupedAggregateShuffleTcpExecutionState::kRunning) {
    return terminal;
  }

  std::size_t descriptor_count{};
  for (std::size_t index = 0U; index < impl.slots_.size(); ++index) {
    const auto* client = impl.slots_[index].active_client();
    if (client == nullptr)
      continue;
    const auto interest = client->interest();
    short events{};
    if (interest.want_read)
      events |= POLLIN;
    if (interest.want_write)
      events |= POLLOUT;
    impl.poll_descriptors_[descriptor_count] = {
        .fd = client->descriptor(), .events = events, .revents = 0};
    impl.poll_slot_indexes_[descriptor_count] = index;
    ++descriptor_count;
  }
  const auto wait = impl.bounded_wait(maximum_wait, now);
  const int ready = ::poll(impl.poll_descriptors_.data(), static_cast<nfds_t>(descriptor_count),
                           static_cast<int>(wait.count()));
  if (ready < 0 && errno != EINTR)
    return impl.fail(poll_error());

  now = std::chrono::steady_clock::now();
  if (impl.config_.execution_deadline.has_value() && now >= *impl.config_.execution_deadline) {
    return impl.cancel(
        {common::StatusCode::kCancelled, "grouped shuffle TCP execution deadline expired"});
  }
  for (std::size_t descriptor_index = 0U; descriptor_index < descriptor_count; ++descriptor_index) {
    Impl::Slot& slot = impl.slots_[impl.poll_slot_indexes_[descriptor_index]];
    auto* client = slot.active_client();
    if (client == nullptr)
      continue;
    const short events = impl.poll_descriptors_[descriptor_index].revents;
    if ((events & (POLLERR | POLLHUP | POLLNVAL)) != 0 && (events & (POLLIN | POLLOUT)) == 0) {
      const common::Status recorded = impl.record_failure(slot, common::StatusCode::kIoError, now);
      if (!recorded.is_ok())
        return impl.fail(recorded);
      continue;
    }
    common::Status driven = client->on_ready((events & POLLIN) != 0, (events & POLLOUT) != 0, now);
    const auto client_state = client->state();
    if (!driven.is_ok() ||
        client_state == DistributedVectorGroupedAggregateShuffleTcpClientState::kFailed) {
      const common::StatusCode failure_code =
          driven.is_ok() ? client->failure().code() : driven.code();
      const common::Status recorded = impl.record_failure(slot, failure_code, now);
      if (!recorded.is_ok())
        return impl.fail(recorded);
      continue;
    }
    if (client_state != DistributedVectorGroupedAggregateShuffleTcpClientState::kComplete)
      continue;
    slot.client.reset();
    --impl.metrics_.active_attempts;
    ++impl.metrics_.transport_completed_attempts;
    const common::Status acknowledged = slot.retry.record_acknowledged();
    if (!acknowledged.is_ok())
      return impl.fail(acknowledged);
  }
  return impl.publish_if_terminal();
}

common::Status DistributedVectorGroupedAggregateShuffleTcpExecution::cancel() {
  if (!implementation_)
    return invalid("grouped shuffle TCP execution is empty");
  return implementation_->cancel(
      {common::StatusCode::kCancelled, "grouped shuffle TCP execution was cancelled"});
}

DistributedVectorGroupedAggregateShuffleTcpExecutionState
DistributedVectorGroupedAggregateShuffleTcpExecution::state() const noexcept {
  return implementation_ ? implementation_->state_
                         : DistributedVectorGroupedAggregateShuffleTcpExecutionState::kFailed;
}

DistributedVectorGroupedAggregateShuffleTcpExecutionMetrics
DistributedVectorGroupedAggregateShuffleTcpExecution::metrics() const noexcept {
  return implementation_ ? implementation_->metrics_
                         : DistributedVectorGroupedAggregateShuffleTcpExecutionMetrics{};
}

const common::Status&
DistributedVectorGroupedAggregateShuffleTcpExecution::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "grouped shuffle TCP execution is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::cluster
