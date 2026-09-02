#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tcp_execution.hpp"

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
          std::string("polling grouped shuffle result TCP execution: ") +
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

[[nodiscard]] bool valid_carrier_limits(
    const DistributedVectorGroupedAggregateShuffleResultTlsLimits& limits) noexcept {
  return valid_timeout(limits.handshake_timeout) && valid_timeout(limits.exchange_timeout) &&
         validate_distributed_vector_grouped_aggregate_shuffle_result_stream_limits(limits.stream);
}

} // namespace

class DistributedVectorGroupedAggregateShuffleResultTcpExecution::Impl {
public:
  using TimePoint = DistributedVectorGroupedAggregateShuffleResultRetry::TimePoint;

  struct Slot {
    std::size_t route_index{};
    DistributedVectorGroupedAggregateShuffleResultRetry retry;
    std::optional<DistributedVectorGroupedAggregateShuffleResultTcpClient> client{std::nullopt};

    [[nodiscard]] DistributedVectorGroupedAggregateShuffleResultTcpClient*
    active_client() noexcept {
      return client.has_value() ? std::addressof(*client) : nullptr;
    }

    [[nodiscard]] const DistributedVectorGroupedAggregateShuffleResultTcpClient*
    active_client() const noexcept {
      return client.has_value() ? std::addressof(*client) : nullptr;
    }
  };

  Impl(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
       const query::DistributedVectorResultSchema& result_schema,
       DistributedVectorGroupedAggregateShuffleResultTcpExecutionConfig config,
       std::vector<Slot> slots, std::vector<pollfd> poll_descriptors,
       std::vector<std::size_t> poll_slot_indexes)
      : authority_(authority), result_schema_(result_schema), config_(std::move(config)),
        slots_(std::move(slots)), poll_descriptors_(std::move(poll_descriptors)),
        poll_slot_indexes_(std::move(poll_slot_indexes)) {
    metrics_.total_partitions = slots_.size();
  }

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ == DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kRunning) {
      for (Slot& slot : slots_)
        slot.client.reset();
      metrics_.active_attempts = 0U;
      failure_ = std::move(status);
      state_ = DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kFailed;
    }
    return failure_;
  }

  [[nodiscard]] common::Status cancel(common::Status status) {
    if (state_ == DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kComplete)
      return common::Status::ok();
    if (state_ == DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kRunning) {
      for (Slot& slot : slots_)
        slot.client.reset();
      metrics_.active_attempts = 0U;
      failure_ = std::move(status);
      state_ = DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kCancelled;
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
      if (retry_state != DistributedVectorGroupedAggregateShuffleResultRetryState::kReady &&
          retry_state != DistributedVectorGroupedAggregateShuffleResultRetryState::kBackoff) {
        continue;
      }
      if (retry_state == DistributedVectorGroupedAggregateShuffleResultRetryState::kBackoff &&
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
      auto client = DistributedVectorGroupedAggregateShuffleResultTcpClient::begin(
          std::move(*attempt), authority_.get(), result_schema_.get(),
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
      if (slot.retry.state() != DistributedVectorGroupedAggregateShuffleResultRetryState::kBackoff)
        continue;
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
      if (retry_state == DistributedVectorGroupedAggregateShuffleResultRetryState::kFailed) {
        return fail({slot.retry.last_status_code().value_or(common::StatusCode::kUnavailable),
                     "grouped shuffle result partition exhausted its retry policy"});
      }
      if (retry_state == DistributedVectorGroupedAggregateShuffleResultRetryState::kSucceeded)
        ++succeeded;
      complete =
          complete &&
          retry_state == DistributedVectorGroupedAggregateShuffleResultRetryState::kSucceeded;
    }
    metrics_.succeeded_partitions = succeeded;
    if (complete)
      state_ = DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kComplete;
    return common::Status::ok();
  }

  std::reference_wrapper<const DistributedVectorGroupedAggregateShuffleAuthority> authority_;
  std::reference_wrapper<const query::DistributedVectorResultSchema> result_schema_;
  DistributedVectorGroupedAggregateShuffleResultTcpExecutionConfig config_;
  std::vector<Slot> slots_;
  std::vector<pollfd> poll_descriptors_;
  std::vector<std::size_t> poll_slot_indexes_;
  DistributedVectorGroupedAggregateShuffleResultTcpExecutionMetrics metrics_;
  DistributedVectorGroupedAggregateShuffleResultTcpExecutionState state_{
      DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kRunning};
  common::Status failure_{common::StatusCode::kInternal,
                          "grouped shuffle result TCP execution has not failed"};
};

DistributedVectorGroupedAggregateShuffleResultTcpExecution::
    DistributedVectorGroupedAggregateShuffleResultTcpExecution() noexcept = default;
DistributedVectorGroupedAggregateShuffleResultTcpExecution::
    DistributedVectorGroupedAggregateShuffleResultTcpExecution(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateShuffleResultTcpExecution::
    ~DistributedVectorGroupedAggregateShuffleResultTcpExecution() = default;
DistributedVectorGroupedAggregateShuffleResultTcpExecution::
    DistributedVectorGroupedAggregateShuffleResultTcpExecution(
        DistributedVectorGroupedAggregateShuffleResultTcpExecution&&) noexcept = default;
DistributedVectorGroupedAggregateShuffleResultTcpExecution&
DistributedVectorGroupedAggregateShuffleResultTcpExecution::operator=(
    DistributedVectorGroupedAggregateShuffleResultTcpExecution&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateShuffleResultTcpExecution>
DistributedVectorGroupedAggregateShuffleResultTcpExecution::create(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema,
    std::vector<DistributedVectorGroupedAggregateShuffleResultRetry> retries,
    DistributedVectorGroupedAggregateShuffleResultTcpExecutionConfig config) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr || retries.empty() ||
      retries.size() > kMaximumDistributedVectorGroupedAggregateShuffleResultRemotePartitions ||
      config.routes.empty() ||
      config.routes.size() >
          kMaximumDistributedVectorGroupedAggregateShuffleResultRemotePartitions ||
      !valid_timeout(config.connect_timeout) || !valid_carrier_limits(config.carrier_limits)) {
    return common::make_unexpected(
        invalid("grouped shuffle result TCP execution configuration is invalid"));
  }
  try {
    std::map<raft::NodeId, std::size_t> route_indexes;
    for (std::size_t index = 0U; index < config.routes.size(); ++index) {
      const auto& route = config.routes[index];
      if (route.node_id == 0U || route.endpoints.empty() || route.endpoints.size() > 1024U ||
          route.tls_context == nullptr || !route_indexes.emplace(route.node_id, index).second) {
        return common::make_unexpected(
            invalid("grouped shuffle result TCP execution route is invalid or duplicated"));
      }
      for (std::size_t endpoint_index = 0U; endpoint_index < route.endpoints.size();
           ++endpoint_index) {
        const auto& endpoint = route.endpoints[endpoint_index];
        const std::span<const network::Ipv4Endpoint> prior{route.endpoints.data(), endpoint_index};
        if (endpoint.port == 0U || zero_address(endpoint.address) ||
            std::ranges::find(prior, endpoint) != prior.end()) {
          return common::make_unexpected(
              invalid("grouped shuffle result TCP route address is invalid or duplicated"));
        }
      }
    }

    const raft::NodeId coordinator_node_id = retries.front().target_node_id();
    std::set<std::uint32_t> partitions;
    std::vector<Impl::Slot> slots;
    slots.reserve(retries.size());
    for (auto& retry : retries) {
      const auto source = authority.destination_node(retry.partition_id());
      if (retry.authority() != std::addressof(authority) ||
          retry.result_schema() != std::addressof(result_schema) || !source.has_value() ||
          retry.source_node_id() != *source || retry.target_node_id() != coordinator_node_id ||
          retry.source_node_id() == coordinator_node_id ||
          !partitions.emplace(retry.partition_id()).second) {
        return common::make_unexpected(
            invalid("grouped shuffle result TCP retry authority or partition is invalid"));
      }
      const auto route = route_indexes.find(coordinator_node_id);
      if (route == route_indexes.end()) {
        return common::make_unexpected(
            invalid("grouped shuffle result TCP execution has no coordinator route"));
      }
      slots.push_back({.route_index = route->second, .retry = std::move(retry)});
    }
    std::vector<pollfd> descriptors(slots.size());
    std::vector<std::size_t> indexes(slots.size());
    return DistributedVectorGroupedAggregateShuffleResultTcpExecution{
        std::make_unique<Impl>(authority, result_schema, std::move(config), std::move(slots),
                               std::move(descriptors), std::move(indexes))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped shuffle result TCP execution allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("grouped shuffle result TCP execution exceeds limits"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleResultTcpExecution::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return invalid("grouped shuffle result TCP execution is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return invalid("grouped shuffle result TCP execution poll timeout is invalid");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kFailed ||
      impl.state_ == DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kCancelled) {
    return impl.failure_;
  }
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kComplete)
    return common::Status::ok();

  auto now = std::chrono::steady_clock::now();
  if (impl.config_.execution_deadline.has_value() && now >= *impl.config_.execution_deadline) {
    return impl.cancel(
        {common::StatusCode::kCancelled, "grouped shuffle result TCP execution deadline expired"});
  }
  const common::Status started = impl.start_due_attempts(now);
  if (!started.is_ok())
    return started.code() == common::StatusCode::kResourceExhausted ? started : impl.fail(started);
  common::Status terminal = impl.publish_if_terminal();
  if (!terminal.is_ok() ||
      impl.state_ != DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kRunning) {
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
        {common::StatusCode::kCancelled, "grouped shuffle result TCP execution deadline expired"});
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
        client_state == DistributedVectorGroupedAggregateShuffleResultTcpClientState::kFailed) {
      const common::StatusCode failure_code =
          driven.is_ok() ? client->failure().code() : driven.code();
      const common::Status recorded = impl.record_failure(slot, failure_code, now);
      if (!recorded.is_ok())
        return impl.fail(recorded);
      continue;
    }
    if (client_state != DistributedVectorGroupedAggregateShuffleResultTcpClientState::kComplete)
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

common::Status DistributedVectorGroupedAggregateShuffleResultTcpExecution::cancel() {
  if (!implementation_)
    return invalid("grouped shuffle result TCP execution is empty");
  return implementation_->cancel(
      {common::StatusCode::kCancelled, "grouped shuffle result TCP execution was cancelled"});
}

DistributedVectorGroupedAggregateShuffleResultTcpExecutionState
DistributedVectorGroupedAggregateShuffleResultTcpExecution::state() const noexcept {
  return implementation_ ? implementation_->state_
                         : DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kFailed;
}

DistributedVectorGroupedAggregateShuffleResultTcpExecutionMetrics
DistributedVectorGroupedAggregateShuffleResultTcpExecution::metrics() const noexcept {
  return implementation_ ? implementation_->metrics_
                         : DistributedVectorGroupedAggregateShuffleResultTcpExecutionMetrics{};
}

const common::Status&
DistributedVectorGroupedAggregateShuffleResultTcpExecution::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "grouped shuffle result TCP execution is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::cluster
