#include "chronos/raft/runtime_timer_driver.hpp"

#include <new>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

[[nodiscard]] common::Status make_status(const common::StatusCode code, const char* message) {
  return {code, message};
}

} // namespace

class RaftTimerDriver::Impl {
public:
  struct Pending {
    RaftTimerAction action;
    AsyncDurableRaftCompletion completion;
  };

  Impl(RaftTimerDriverConfig config, RaftTimerRuntime timers,
       std::vector<std::optional<Pending>> pending,
       std::vector<std::optional<RaftTimerCompletedAction>> completed) noexcept
      : config_(config), timers_(std::move(timers)), pending_(std::move(pending)),
        completed_(std::move(completed)) {}

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (failure_.is_ok())
      failure_ = std::move(failure);
    return failure_;
  }

  [[nodiscard]] common::Result<TimePoint> election_deadline(const RaftGroupObservation& observation,
                                                            const TimePoint now) const {
    if (observation.role == Role::kLeader)
      return TimePoint::max();
    common::Result<TimePoint> deadline = common::make_unexpected(
        make_status(common::StatusCode::kInternal, "Raft election deadline source did not return"));
    try {
      deadline = config_.election_deadlines->next_election_deadline(observation.group_id,
                                                                    observation.current_term, now);
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(make_status(common::StatusCode::kResourceExhausted,
                                                 "Raft election deadline allocation failed"));
    } catch (const std::exception&) {
      return common::make_unexpected(make_status(
          common::StatusCode::kInternal, "Raft election deadline source threw an exception"));
    }
    if (!deadline.has_value())
      return common::make_unexpected(deadline.error());
    if (*deadline <= now)
      return common::make_unexpected(make_status(common::StatusCode::kInvalidArgument,
                                                 "Raft election source returned a stale deadline"));
    return *deadline;
  }

  [[nodiscard]] std::size_t free_pending() const noexcept {
    return pending_.size() - pending_count_;
  }

  [[nodiscard]] std::optional<std::size_t> free_pending_slot() const noexcept {
    for (std::size_t index = 0U; index < pending_.size(); ++index)
      if (!pending_[index].has_value())
        return index;
    return std::nullopt;
  }

  [[nodiscard]] common::Status collect_ready(const TimePoint now) {
    while (completed_count_ != completed_.size()) {
      std::optional<std::size_t> next;
      std::uint64_t next_sequence{};
      for (std::size_t index = 0U; index < pending_.size(); ++index) {
        const std::optional<Pending>& slot = pending_[index];
        if (!slot.has_value())
          continue;
        const Pending& candidate = slot.value();
        if (!candidate.completion.is_ready())
          continue;
        const std::uint64_t candidate_sequence = candidate.completion.submission_sequence();
        if (!next.has_value() || candidate_sequence < next_sequence) {
          next = index;
          next_sequence = candidate_sequence;
        }
      }
      if (!next.has_value())
        break;
      std::optional<Pending>& pending_slot = pending_[next.value()];
      if (!pending_slot.has_value()) {
        return fail(make_status(common::StatusCode::kCorruption,
                                "Raft timer pending selection is inconsistent"));
      }
      Pending& pending = pending_slot.value();
      auto results = pending.completion.wait();
      if (!results.has_value())
        return fail(results.error());
      std::vector<DurableRaftResult>& batch = results.value();
      if (batch.size() != 2U || !batch[1].status.is_ok() || !batch[1].observation.has_value() ||
          batch[1].transition.has_value()) {
        return fail(make_status(common::StatusCode::kCorruption,
                                "Raft timer batch lacks its ordered group observation"));
      }
      RaftGroupObservation observation = std::move(batch[1].observation).value();
      auto deadline = election_deadline(observation, now);
      if (!deadline.has_value())
        return fail(deadline.error());
      const common::Status rearmed =
          timers_.complete(pending.action, observation, now, deadline.value());
      if (!rearmed.is_ok() && rearmed.code() != common::StatusCode::kInvalidArgument)
        return fail(rearmed);
      std::optional<RaftTimerCompletedAction>& completed_slot = completed_[completed_tail_];
      if (completed_slot.has_value()) {
        return fail(make_status(common::StatusCode::kCorruption,
                                "Raft timer completed accounting is inconsistent"));
      }
      completed_slot.emplace(RaftTimerCompletedAction{pending.completion.submission_sequence(),
                                                      pending.action, std::move(batch[0]),
                                                      std::move(observation)});
      completed_tail_ = (completed_tail_ + 1U) % completed_.size();
      ++completed_count_;
      pending_slot.reset();
      --pending_count_;
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status admit_due(const TimePoint now) {
    auto actions = timers_.poll(now);
    if (!actions.has_value())
      return fail(actions.error());
    for (const RaftTimerAction& action : *actions) {
      if (free_pending() == 0U) {
        const common::Status released = timers_.reject_admission(action);
        if (!released.is_ok())
          return fail(released);
        continue;
      }
      std::vector<DurableRaftRequest> requests;
      try {
        requests.reserve(2U);
        requests.push_back(action.request());
        requests.emplace_back(action.group_id, ObserveGroupOperation{});
      } catch (const std::bad_alloc&) {
        const common::Status released = timers_.reject_admission(action);
        if (!released.is_ok())
          return fail(released);
        return make_status(common::StatusCode::kResourceExhausted,
                           "Raft timer request allocation failed");
      }
      auto completion = config_.runtime->try_submit(std::move(requests));
      if (!completion.has_value()) {
        const common::Status released = timers_.reject_admission(action);
        if (!released.is_ok())
          return fail(released);
        if (completion.error().code() == common::StatusCode::kResourceExhausted)
          continue;
        return fail(completion.error());
      }
      const std::optional<std::size_t> slot = free_pending_slot();
      if (!slot.has_value())
        return fail(make_status(common::StatusCode::kCorruption,
                                "Raft timer pending accounting is inconsistent"));
      pending_[*slot].emplace(Pending{action, std::move(*completion)});
      ++pending_count_;
    }
    return common::Status::ok();
  }

  RaftTimerDriverConfig config_;
  RaftTimerRuntime timers_;
  std::vector<std::optional<Pending>> pending_;
  std::vector<std::optional<RaftTimerCompletedAction>> completed_;
  std::size_t pending_count_{};
  std::size_t completed_head_{};
  std::size_t completed_tail_{};
  std::size_t completed_count_{};
  common::Status failure_;
};

RaftTimerDriver::RaftTimerDriver(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftTimerDriver::~RaftTimerDriver() = default;
RaftTimerDriver::RaftTimerDriver(RaftTimerDriver&&) noexcept = default;
RaftTimerDriver& RaftTimerDriver::operator=(RaftTimerDriver&&) noexcept = default;

common::Result<RaftTimerDriver> RaftTimerDriver::create(const RaftTimerDriverConfig config) {
  if (config.runtime == nullptr || config.election_deadlines == nullptr ||
      config.limits.maximum_inflight_actions == 0U || config.limits.maximum_completed_actions == 0U)
    return common::make_unexpected(make_status(common::StatusCode::kInvalidArgument,
                                               "Raft timer driver configuration is invalid"));
  auto timers = RaftTimerRuntime::create(config.limits.timers);
  if (!timers.has_value())
    return common::make_unexpected(timers.error());
  try {
    std::vector<std::optional<Impl::Pending>> pending(config.limits.maximum_inflight_actions);
    std::vector<std::optional<RaftTimerCompletedAction>> completed(
        config.limits.maximum_completed_actions);
    return RaftTimerDriver{std::make_unique<Impl>(config, std::move(*timers), std::move(pending),
                                                  std::move(completed))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        make_status(common::StatusCode::kResourceExhausted, "Raft timer driver allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(make_status(common::StatusCode::kResourceExhausted,
                                               "Raft timer driver bounds exceed container limits"));
  }
}

common::Status RaftTimerDriver::add_group(const RaftGroupObservation& observation,
                                          const TimePoint now) {
  if (!implementation_)
    return make_status(common::StatusCode::kInvalidArgument, "Raft timer driver is empty");
  if (!implementation_->failure_.is_ok())
    return implementation_->failure_;
  auto deadline = implementation_->election_deadline(observation, now);
  if (!deadline.has_value())
    return deadline.error();
  return implementation_->timers_.add_group(observation, now, *deadline);
}

common::Status RaftTimerDriver::remove_group(const GroupId& group_id) {
  if (!implementation_)
    return make_status(common::StatusCode::kInvalidArgument, "Raft timer driver is empty");
  if (!implementation_->failure_.is_ok())
    return implementation_->failure_;
  for (const std::optional<Impl::Pending>& pending : implementation_->pending_) {
    if (pending.has_value() && pending->action.group_id == group_id)
      return make_status(common::StatusCode::kUnavailable,
                         "Raft timer group still has an in-flight action");
  }
  return implementation_->timers_.remove_group(group_id);
}

common::Status RaftTimerDriver::note_activity(const RaftGroupObservation& observation,
                                              const TimePoint now) {
  if (!implementation_)
    return make_status(common::StatusCode::kInvalidArgument, "Raft timer driver is empty");
  if (!implementation_->failure_.is_ok())
    return implementation_->failure_;
  auto deadline = implementation_->election_deadline(observation, now);
  if (!deadline.has_value())
    return deadline.error();
  return implementation_->timers_.note_activity(observation, now, *deadline);
}

common::Status RaftTimerDriver::drive(const TimePoint now) {
  if (!implementation_)
    return make_status(common::StatusCode::kInvalidArgument, "Raft timer driver is empty");
  if (!implementation_->failure_.is_ok())
    return implementation_->failure_;
  common::Status collected = implementation_->collect_ready(now);
  if (!collected.is_ok())
    return collected;
  return implementation_->admit_due(now);
}

common::Result<RaftTimerCompletedAction> RaftTimerDriver::take_completed() {
  if (!implementation_)
    return common::make_unexpected(
        make_status(common::StatusCode::kInvalidArgument, "Raft timer driver is empty"));
  if (implementation_->completed_count_ == 0U)
    return common::make_unexpected(
        make_status(common::StatusCode::kUnavailable, "Raft timer result is not ready"));
  std::optional<RaftTimerCompletedAction>& completed =
      implementation_->completed_[implementation_->completed_head_];
  if (!completed.has_value()) {
    return common::make_unexpected(implementation_->fail(make_status(
        common::StatusCode::kCorruption, "Raft timer completed accounting is inconsistent")));
  }
  RaftTimerCompletedAction result = std::move(completed.value());
  completed.reset();
  implementation_->completed_head_ =
      (implementation_->completed_head_ + 1U) % implementation_->completed_.size();
  --implementation_->completed_count_;
  return result;
}

std::optional<std::uint64_t> RaftTimerDriver::next_completed_sequence() const noexcept {
  if (!implementation_ || implementation_->completed_count_ == 0U)
    return std::nullopt;
  const std::optional<RaftTimerCompletedAction>& completed =
      implementation_->completed_[implementation_->completed_head_];
  return completed.has_value() ? std::optional<std::uint64_t>{completed.value().submission_sequence}
                               : std::nullopt;
}

std::optional<RaftTimerDriver::TimePoint> RaftTimerDriver::next_deadline() const noexcept {
  return implementation_ ? implementation_->timers_.next_deadline() : std::nullopt;
}

std::size_t RaftTimerDriver::inflight_actions() const noexcept {
  return implementation_ ? implementation_->pending_count_ : 0U;
}
std::size_t RaftTimerDriver::completed_actions() const noexcept {
  return implementation_ ? implementation_->completed_count_ : 0U;
}
bool RaftTimerDriver::failed() const noexcept {
  return !implementation_ || !implementation_->failure_.is_ok();
}
const common::Status& RaftTimerDriver::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "Raft timer driver is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::raft
