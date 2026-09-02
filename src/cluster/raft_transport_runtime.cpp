#include "chronos/cluster/raft_transport_runtime.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <limits>
#include <new>
#include <optional>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}
[[nodiscard]] common::Status poll_error(const int error = errno) {
  return {common::StatusCode::kIoError,
          std::string("polling Raft transport runtime: ") +
              std::error_code(error, std::generic_category()).message()};
}
void increment(std::uint64_t& value) noexcept {
  if (value != std::numeric_limits<std::uint64_t>::max())
    ++value;
}
[[nodiscard]] bool routing_backpressure(const common::StatusCode code) noexcept {
  return code == common::StatusCode::kNotFound || code == common::StatusCode::kUnavailable ||
         code == common::StatusCode::kResourceExhausted;
}

} // namespace

class RaftTransportRuntime::Impl {
public:
  enum class PollKind : std::uint8_t { kDurable = 1, kListener = 2, kInbound = 3, kOutbound = 4 };
  struct PollOwner {
    PollKind kind{PollKind::kDurable};
    std::uint64_t identity{};
  };
  struct PendingResult {
    RaftTransportRuntimeResult value;
    bool routed{};
  };
  struct PendingApplication {
    raft::GroupId group_id;
    raft::AsyncDurableRaftCompletion completion;
  };

  Impl(raft::AsyncDurableMultiRaftRuntime* durable, raft::RaftTimerDriver timers,
       RaftTransportTcpServer server, RaftTransportPeerManager peers,
       const RaftTransportRuntimeLimits configured,
       std::vector<std::optional<PendingResult>> result_storage,
       std::vector<std::optional<PendingApplication>> application_storage,
       std::vector<pollfd> descriptor_storage, std::vector<PollOwner> owner_storage) noexcept
      : durable_runtime(durable), timer_driver(std::move(timers)), inbound(std::move(server)),
        outbound(std::move(peers)), limits(configured), results(std::move(result_storage)),
        applications(std::move(application_storage)), descriptors(std::move(descriptor_storage)),
        owners(std::move(owner_storage)) {}

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (failure_status.is_ok())
      failure_status = std::move(failure);
    runtime_metrics.failed = true;
    return failure_status;
  }

  [[nodiscard]] common::Status push(RaftTransportRuntimeResult result) {
    if (result.submission_sequence == 0U || result.submission_sequence <= last_submission_sequence)
      return fail(status(common::StatusCode::kCorruption,
                         "Raft transport completion FIFO identity is invalid"));
    if (results[result_tail].has_value())
      return fail(status(common::StatusCode::kCorruption,
                         "Raft transport result ring ownership is inconsistent"));
    const std::uint64_t submission_sequence = result.submission_sequence;
    results[result_tail].emplace(PendingResult{std::move(result), false});
    result_tail = (result_tail + 1U) % results.size();
    ++result_count;
    runtime_metrics.pending_results = result_count;
    last_submission_sequence = submission_sequence;
    return common::Status::ok();
  }

  [[nodiscard]] std::optional<std::size_t> free_application_slot() const noexcept {
    for (std::size_t index = 0U; index < applications.size(); ++index) {
      if (!applications[index].has_value())
        return index;
    }
    return std::nullopt;
  }

  [[nodiscard]] common::Result<std::uint64_t> submit_application(raft::DurableRaftRequest request) {
    if (!failure_status.is_ok())
      return common::make_unexpected(failure_status);
    if (request.group_id.is_nil() ||
        std::holds_alternative<raft::ObserveGroupOperation>(request.operation) ||
        std::holds_alternative<raft::StartElectionOperation>(request.operation) ||
        std::holds_alternative<raft::ReceiveOperation>(request.operation) ||
        std::holds_alternative<raft::HeartbeatOperation>(request.operation)) {
      return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                            "Raft application transition request is invalid"));
    }
    const std::optional<std::size_t> slot = free_application_slot();
    if (!slot.has_value())
      return common::make_unexpected(
          status(common::StatusCode::kResourceExhausted, "Raft application request bound is full"));
    const raft::GroupId group_id = request.group_id;
    try {
      std::vector<raft::DurableRaftRequest> batch;
      batch.reserve(2U);
      batch.push_back(std::move(request));
      batch.emplace_back(group_id, raft::ObserveGroupOperation{});
      auto completion = durable_runtime->try_submit(std::move(batch));
      if (!completion.has_value())
        return common::make_unexpected(completion.error());
      const std::uint64_t sequence = completion->submission_sequence();
      applications[*slot].emplace(PendingApplication{group_id, std::move(*completion)});
      ++application_count;
      runtime_metrics.pending_application_requests = application_count;
      return sequence;
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                            "Raft application request allocation failed"));
    }
  }

  [[nodiscard]] std::optional<std::size_t> next_application() const noexcept {
    std::optional<std::size_t> next;
    std::uint64_t next_sequence = std::numeric_limits<std::uint64_t>::max();
    for (std::size_t index = 0U; index < applications.size(); ++index) {
      const PendingApplication* pending =
          applications[index]
              .transform([](const PendingApplication& value) { return &value; })
              .value_or(nullptr);
      if (pending == nullptr)
        continue;
      const std::uint64_t sequence = pending->completion.submission_sequence();
      if (sequence < next_sequence) {
        next = index;
        next_sequence = sequence;
      }
    }
    return next;
  }

  [[nodiscard]] common::Result<RaftTransportRuntimeResult>
  take_application(const std::size_t index) {
    if (index >= applications.size())
      return common::make_unexpected(
          status(common::StatusCode::kCorruption, "Raft application slot identity is invalid"));
    PendingApplication* pending = applications[index]
                                      .transform([](PendingApplication& value) { return &value; })
                                      .value_or(nullptr);
    if (pending == nullptr)
      return common::make_unexpected(status(common::StatusCode::kCorruption,
                                            "Raft application slot ownership is inconsistent"));
    const std::uint64_t sequence = pending->completion.submission_sequence();
    auto batch = pending->completion.wait();
    if (!batch.has_value())
      return common::make_unexpected(batch.error());
    raft::RaftGroupObservation* observation =
        batch->size() == 2U
            ? (*batch)[1]
                  .observation.transform([](raft::RaftGroupObservation& value) { return &value; })
                  .value_or(nullptr)
            : nullptr;
    if (batch->size() != 2U || !(*batch)[1].status.is_ok() || (*batch)[1].transition.has_value() ||
        observation == nullptr || observation->group_id != pending->group_id) {
      return common::make_unexpected(
          status(common::StatusCode::kCorruption,
                 "Raft application batch lacks its ordered group observation"));
    }
    raft::RaftGroupObservation owned_observation = std::move(*observation);
    const raft::GroupId group_id = pending->group_id;
    raft::DurableRaftResult result = std::move((*batch)[0]);
    applications[index].reset();
    --application_count;
    runtime_metrics.pending_application_requests = application_count;
    return RaftTransportRuntimeResult{sequence,
                                      RaftTransportRuntimeResultOrigin::kApplication,
                                      group_id,
                                      std::nullopt,
                                      std::nullopt,
                                      std::move(result),
                                      std::move(owned_observation)};
  }

  [[nodiscard]] common::Status intake(const TimePoint now) {
    for (std::size_t admitted = 0U;
         admitted < limits.maximum_results_per_poll && result_count != results.size(); ++admitted) {
      const auto inbound_sequence = inbound.next_outstanding_sequence();
      const auto timer_sequence = timer_driver.next_outstanding_sequence();
      const std::optional<std::size_t> application = next_application();
      const std::size_t application_index = application.value_or(applications.size());
      const PendingApplication* pending_application =
          application_index < applications.size()
              ? applications[application_index]
                    .transform([](const PendingApplication& value) { return &value; })
                    .value_or(nullptr)
              : nullptr;
      const std::optional<std::uint64_t> application_sequence =
          pending_application != nullptr
              ? std::optional<std::uint64_t>{pending_application->completion.submission_sequence()}
              : std::nullopt;
      if (!inbound_sequence.has_value() && !timer_sequence.has_value() &&
          !application_sequence.has_value())
        break;
      if (inbound_sequence.has_value() &&
          (!timer_sequence.has_value() || *inbound_sequence < *timer_sequence) &&
          (!application_sequence.has_value() || *inbound_sequence < *application_sequence)) {
        if (inbound.next_completed_sequence() != inbound_sequence)
          break;
        auto completed = inbound.take_completed();
        if (!completed.has_value())
          return fail(completed.error());
        RaftTransportCompletedReceive* received_value =
            completed->transform([](RaftTransportCompletedReceive& value) { return &value; })
                .value_or(nullptr);
        if (received_value == nullptr ||
            received_value->submission_sequence != inbound_sequence.value_or(0U))
          return fail(status(common::StatusCode::kCorruption,
                             "Raft inbound completion changed during ordered pickup"));
        RaftTransportCompletedReceive received = std::move(*received_value);
        if (received.observation.has_value()) {
          const common::Status activity = timer_driver.note_activity(*received.observation, now);
          if (!activity.is_ok())
            return fail(activity);
        }
        increment(runtime_metrics.inbound_results);
        common::Status stored =
            push({received.submission_sequence, RaftTransportRuntimeResultOrigin::kInbound,
                  received.group_id, received.source_node_id, std::nullopt,
                  std::move(received.result), std::move(received.observation)});
        if (!stored.is_ok())
          return stored;
      } else if (timer_sequence.has_value() &&
                 (!application_sequence.has_value() || *timer_sequence < *application_sequence)) {
        if (timer_driver.next_completed_sequence() != timer_sequence)
          break;
        auto completed = timer_driver.take_completed();
        if (!completed.has_value())
          return fail(completed.error());
        if (completed->submission_sequence != *timer_sequence)
          return fail(status(common::StatusCode::kCorruption,
                             "Raft timer completion changed during ordered pickup"));
        increment(runtime_metrics.timer_results);
        common::Status stored =
            push({completed->submission_sequence, RaftTransportRuntimeResultOrigin::kTimer,
                  completed->action.group_id, std::nullopt, completed->action,
                  std::move(completed->result), std::move(completed->observation)});
        if (!stored.is_ok())
          return stored;
      } else {
        if (application_index >= applications.size() || !application_sequence.has_value())
          return fail(status(common::StatusCode::kCorruption,
                             "Raft application completion selection is inconsistent"));
        if (pending_application == nullptr || !pending_application->completion.is_ready())
          break;
        auto completed = take_application(application_index);
        if (!completed.has_value())
          return fail(completed.error());
        if (completed->submission_sequence != application_sequence.value_or(0U))
          return fail(status(common::StatusCode::kCorruption,
                             "Raft application completion changed during ordered pickup"));
        increment(runtime_metrics.application_results);
        common::Status stored = push(std::move(*completed));
        if (!stored.is_ok())
          return stored;
      }
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status route_pending(const TimePoint now) {
    for (std::size_t offset = 0U; offset < result_count; ++offset) {
      PendingResult* pending = results[(result_head + offset) % results.size()]
                                   .transform([](PendingResult& value) { return &value; })
                                   .value_or(nullptr);
      if (pending == nullptr)
        return fail(status(common::StatusCode::kCorruption,
                           "Raft transport result ring ownership is inconsistent"));
      if (pending->routed)
        continue;
      if (!pending->value.result.status.is_ok()) {
        pending->routed = true;
        continue;
      }
      if (!pending->value.result.transition.has_value())
        return fail(status(common::StatusCode::kCorruption,
                           "successful Raft runtime result lacks its transition"));
      const common::Status routed =
          outbound.route_result(pending->value.group_id, pending->value.result, now);
      if (routed.is_ok()) {
        pending->routed = true;
        increment(runtime_metrics.routed_results);
        continue;
      }
      if (routing_backpressure(routed.code())) {
        increment(runtime_metrics.routing_backpressure);
        return common::Status::ok();
      }
      return fail(routed);
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status progress(const TimePoint now) {
    common::Status progressed = outbound.drive(now);
    if (!progressed.is_ok())
      return fail(progressed);
    progressed = inbound.drive(now);
    if (!progressed.is_ok())
      return fail(progressed);
    progressed = timer_driver.drive(now);
    if (!progressed.is_ok())
      return fail(progressed);
    progressed = intake(now);
    if (!progressed.is_ok())
      return progressed;
    return route_pending(now);
  }

  [[nodiscard]] common::Status append_descriptor(const pollfd descriptor, const PollOwner owner) {
    if (descriptor.fd < 0)
      return fail(
          status(common::StatusCode::kCorruption, "Raft transport poll descriptor is invalid"));
    if (descriptors.size() == limits.maximum_poll_descriptors)
      return fail(status(common::StatusCode::kResourceExhausted,
                         "Raft transport poll descriptor bound is full"));
    descriptors.push_back(descriptor);
    owners.push_back(owner);
    return common::Status::ok();
  }

  [[nodiscard]] common::Status prepare_poll() {
    descriptors.clear();
    owners.clear();
    common::Status appended = append_descriptor(
        {.fd = durable_runtime->completion_descriptor(), .events = POLLIN, .revents = 0},
        {PollKind::kDurable, 0U});
    if (!appended.is_ok())
      return appended;
    appended =
        append_descriptor({.fd = inbound.listener_descriptor(), .events = POLLIN, .revents = 0},
                          {PollKind::kListener, 0U});
    if (!appended.is_ok())
      return appended;
    auto inbound_interests = inbound.interests();
    if (!inbound_interests.has_value())
      return fail(inbound_interests.error());
    for (const RaftTransportTcpServerInterest& interest : *inbound_interests) {
      short events{};
      if (interest.want_read)
        events |= POLLIN;
      if (interest.want_write)
        events |= POLLOUT;
      appended = append_descriptor({.fd = interest.descriptor, .events = events, .revents = 0},
                                   {PollKind::kInbound, interest.connection_id});
      if (!appended.is_ok())
        return appended;
    }
    auto outbound_interests = outbound.interests();
    if (!outbound_interests.has_value())
      return fail(outbound_interests.error());
    for (const RaftTransportPeerInterest& interest : *outbound_interests) {
      short events{};
      if (interest.want_read)
        events |= POLLIN;
      if (interest.want_write)
        events |= POLLOUT;
      appended = append_descriptor({.fd = interest.descriptor, .events = events, .revents = 0},
                                   {PollKind::kOutbound, interest.peer_node_id});
      if (!appended.is_ok())
        return appended;
    }
    return common::Status::ok();
  }

  [[nodiscard]] std::optional<TimePoint> next_deadline() const noexcept {
    std::optional<TimePoint> next = timer_driver.next_deadline();
    const auto inbound_deadline = inbound.next_deadline();
    if (inbound_deadline.has_value() && (!next.has_value() || *inbound_deadline < *next))
      next = inbound_deadline;
    const auto outbound_deadline = outbound.next_deadline();
    if (outbound_deadline.has_value() && (!next.has_value() || *outbound_deadline < *next))
      next = outbound_deadline;
    return next;
  }

  [[nodiscard]] int poll_timeout(const std::chrono::milliseconds maximum_wait,
                                 const TimePoint now) const noexcept {
    const PendingResult* pending =
        result_count != 0U ? results[result_head]
                                 .transform([](const PendingResult& value) { return &value; })
                                 .value_or(nullptr)
                           : nullptr;
    if (pending != nullptr && pending->routed)
      return 0;
    auto wait = maximum_wait;
    const auto deadline = next_deadline();
    if (deadline.has_value()) {
      if (*deadline <= now)
        return 0;
      const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(*deadline - now);
      wait = std::min(wait, remaining);
    }
    return static_cast<int>(wait.count());
  }

  [[nodiscard]] common::Status dispatch_events(const TimePoint now) {
    constexpr short terminal = POLLERR | POLLHUP | POLLNVAL;
    for (std::size_t index = 0U; index < descriptors.size(); ++index) {
      const short events = descriptors[index].revents;
      if (events == 0)
        continue;
      const bool readable = (events & POLLIN) != 0;
      const bool writable = (events & POLLOUT) != 0;
      const bool closed = (events & terminal) != 0;
      const PollOwner owner = owners[index];
      if (owner.kind == PollKind::kDurable) {
        if (closed)
          return fail(
              status(common::StatusCode::kIoError, "Raft durable completion descriptor failed"));
        common::Status drained = durable_runtime->drain_completion_notifications();
        if (!drained.is_ok())
          return fail(drained);
        increment(runtime_metrics.durable_wakeups);
      } else if (owner.kind == PollKind::kListener) {
        if (closed)
          return fail(status(common::StatusCode::kIoError, "Raft listener descriptor failed"));
        if (readable) {
          common::Status accepted = inbound.accept_ready(now);
          if (!accepted.is_ok())
            return fail(accepted);
        }
      } else if (owner.kind == PollKind::kInbound) {
        if (readable || writable) {
          common::Status progressed = inbound.on_ready(owner.identity, readable, writable, now);
          if (!progressed.is_ok() && progressed.code() != common::StatusCode::kNotFound)
            return fail(progressed);
        }
        if (closed) {
          common::Status removed = inbound.on_transport_closed(owner.identity);
          if (!removed.is_ok() && removed.code() != common::StatusCode::kNotFound)
            return fail(removed);
        }
      } else {
        if (readable || writable) {
          common::Status progressed = outbound.on_ready(owner.identity, readable, writable, now);
          if (!progressed.is_ok())
            return fail(progressed);
        }
        if (closed) {
          common::Status removed = outbound.on_transport_closed(owner.identity, now);
          if (!removed.is_ok())
            return fail(removed);
        }
      }
    }
    return common::Status::ok();
  }

  raft::AsyncDurableMultiRaftRuntime* durable_runtime{};
  raft::RaftTimerDriver timer_driver;
  RaftTransportTcpServer inbound;
  RaftTransportPeerManager outbound;
  RaftTransportRuntimeLimits limits;
  std::vector<std::optional<PendingResult>> results;
  std::vector<std::optional<PendingApplication>> applications;
  std::vector<pollfd> descriptors;
  std::vector<PollOwner> owners;
  std::size_t result_head{};
  std::size_t result_tail{};
  std::size_t result_count{};
  std::size_t application_count{};
  std::uint64_t last_submission_sequence{};
  RaftTransportRuntimeMetrics runtime_metrics;
  common::Status failure_status;
};

RaftTransportRuntime::RaftTransportRuntime(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftTransportRuntime::~RaftTransportRuntime() = default;
RaftTransportRuntime::RaftTransportRuntime(RaftTransportRuntime&&) noexcept = default;
RaftTransportRuntime& RaftTransportRuntime::operator=(RaftTransportRuntime&&) noexcept = default;

common::Result<RaftTransportRuntime>
RaftTransportRuntime::create(raft::AsyncDurableMultiRaftRuntime* durable_runtime,
                             raft::RaftTimerDriver&& timer_driver, RaftTransportTcpServer&& inbound,
                             RaftTransportPeerManager&& outbound,
                             const RaftTransportRuntimeLimits limits) {
  if (durable_runtime == nullptr || durable_runtime->completion_descriptor() < 0 ||
      !inbound.is_running() || limits.maximum_pending_results == 0U ||
      limits.maximum_pending_application_requests == 0U || limits.maximum_results_per_poll == 0U ||
      limits.maximum_poll_descriptors < 3U || limits.maximum_poll_descriptors > 65'536U)
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft transport runtime configuration is invalid"));
  try {
    std::vector<std::optional<Impl::PendingResult>> results(limits.maximum_pending_results);
    std::vector<std::optional<Impl::PendingApplication>> applications(
        limits.maximum_pending_application_requests);
    std::vector<pollfd> descriptors;
    descriptors.reserve(limits.maximum_poll_descriptors);
    std::vector<Impl::PollOwner> owners;
    owners.reserve(limits.maximum_poll_descriptors);
    return RaftTransportRuntime{std::make_unique<Impl>(
        durable_runtime, std::move(timer_driver), std::move(inbound), std::move(outbound), limits,
        std::move(results), std::move(applications), std::move(descriptors), std::move(owners))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted, "Raft transport runtime allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft transport runtime bounds exceed container limits"));
  }
}

common::Status RaftTransportRuntime::add_group(const raft::RaftGroupObservation& observation,
                                               const TimePoint now) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft transport runtime is empty");
  return implementation_->timer_driver.add_group(observation, now);
}

common::Status RaftTransportRuntime::remove_group(const raft::GroupId& group_id) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft transport runtime is empty");
  return implementation_->timer_driver.remove_group(group_id);
}

common::Result<std::uint64_t>
RaftTransportRuntime::try_submit_application(raft::DurableRaftRequest request) {
  if (!implementation_)
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "Raft transport runtime is empty"));
  return implementation_->submit_application(std::move(request));
}

common::Status RaftTransportRuntime::poll_once(const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft transport runtime is empty");
  Impl& impl = *implementation_;
  if (!impl.failure_status.is_ok())
    return impl.failure_status;
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return status(common::StatusCode::kInvalidArgument, "Raft transport poll timeout is invalid");
  const TimePoint before = std::chrono::steady_clock::now();
  common::Status progressed = impl.progress(before);
  if (!progressed.is_ok())
    return progressed;
  common::Status prepared = impl.prepare_poll();
  if (!prepared.is_ok())
    return prepared;
  increment(impl.runtime_metrics.polls);
  const int ready = ::poll(impl.descriptors.data(), static_cast<nfds_t>(impl.descriptors.size()),
                           impl.poll_timeout(maximum_wait, before));
  if (ready < 0 && errno != EINTR)
    return impl.fail(poll_error());
  const TimePoint now = std::chrono::steady_clock::now();
  if (ready > 0) {
    common::Status dispatched = impl.dispatch_events(now);
    if (!dispatched.is_ok())
      return dispatched;
  }
  return impl.progress(now);
}

common::Result<RaftTransportRuntimeResult> RaftTransportRuntime::take_completed() {
  if (!implementation_)
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "Raft transport runtime is empty"));
  Impl& impl = *implementation_;
  if (impl.result_count == 0U) {
    if (!impl.failure_status.is_ok())
      return common::make_unexpected(impl.failure_status);
    return common::make_unexpected(
        status(common::StatusCode::kUnavailable, "Raft transport result is not ready"));
  }
  Impl::PendingResult* pending = impl.results[impl.result_head]
                                     .transform([](Impl::PendingResult& value) { return &value; })
                                     .value_or(nullptr);
  if (pending == nullptr) {
    return common::make_unexpected(impl.fail(status(
        common::StatusCode::kCorruption, "Raft transport result ring ownership is inconsistent")));
  }
  if (!pending->routed) {
    if (!impl.failure_status.is_ok())
      return common::make_unexpected(impl.failure_status);
    return common::make_unexpected(
        status(common::StatusCode::kUnavailable, "Raft transport result is not ready"));
  }
  RaftTransportRuntimeResult result = std::move(pending->value);
  impl.results[impl.result_head].reset();
  impl.result_head = (impl.result_head + 1U) % impl.results.size();
  --impl.result_count;
  impl.runtime_metrics.pending_results = impl.result_count;
  increment(impl.runtime_metrics.completed_results);
  return result;
}

network::Ipv4Endpoint RaftTransportRuntime::bound_endpoint() const noexcept {
  return implementation_ ? implementation_->inbound.bound_endpoint() : network::Ipv4Endpoint{};
}
RaftTransportRuntimeMetrics RaftTransportRuntime::metrics() const noexcept {
  return implementation_ ? implementation_->runtime_metrics : RaftTransportRuntimeMetrics{};
}
bool RaftTransportRuntime::failed() const noexcept {
  return !implementation_ || !implementation_->failure_status.is_ok();
}
const common::Status& RaftTransportRuntime::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "Raft transport runtime is empty"};
  return implementation_ ? implementation_->failure_status : empty;
}

} // namespace chronos::cluster
