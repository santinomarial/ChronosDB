#include "chronos/raft/runtime_timer.hpp"

#include <limits>
#include <new>
#include <stdexcept>
#include <utility>

namespace chronos::raft {
namespace {
[[nodiscard]] common::Status status(common::StatusCode code, const char* message) {
  return {code, message};
}
[[nodiscard]] bool valid_observation(const RaftGroupObservation& value) noexcept {
  return !value.group_id.is_nil() && value.node_id != 0U;
}
[[nodiscard]] RaftTimerRuntime::TimePoint
deadline_after(const RaftTimerRuntime::TimePoint now,
               const std::chrono::milliseconds interval) noexcept {
  const auto duration = std::chrono::duration_cast<RaftTimerRuntime::TimePoint::duration>(interval);
  return now > RaftTimerRuntime::TimePoint::max() - duration ? RaftTimerRuntime::TimePoint::max()
                                                             : now + duration;
}
} // namespace

class RaftTimerRuntime::Impl {
public:
  struct Timer {
    GroupId group_id;
    NodeId node_id{};
    Role role{Role::kFollower};
    Term term{};
    TimePoint deadline{};
    std::uint64_t generation{1U};
    bool in_flight{};
  };
  explicit Impl(RaftTimerLimits limits) : limits_(limits) {}
  [[nodiscard]] Timer* find(const GroupId& id) noexcept {
    for (Timer& timer : groups_)
      if (timer.group_id == id)
        return &timer;
    return nullptr;
  }
  [[nodiscard]] common::Status arm(Timer& timer, const RaftGroupObservation& observation,
                                   TimePoint now, TimePoint election_deadline, bool advance) {
    if (!valid_observation(observation) ||
        (observation.role != Role::kLeader && election_deadline <= now))
      return status(common::StatusCode::kInvalidArgument, "Raft timer observation is invalid");
    if (advance && timer.generation == std::numeric_limits<std::uint64_t>::max())
      return status(common::StatusCode::kResourceExhausted, "Raft timer generation is exhausted");
    timer.group_id = observation.group_id;
    timer.node_id = observation.node_id;
    timer.role = observation.role;
    timer.term = observation.current_term;
    timer.deadline = timer.role == Role::kLeader ? deadline_after(now, limits_.heartbeat_interval)
                                                 : election_deadline;
    if (advance)
      ++timer.generation;
    timer.in_flight = false;
    return common::Status::ok();
  }
  RaftTimerLimits limits_;
  std::vector<Timer> groups_;
};

DurableRaftRequest RaftTimerAction::request() const {
  return kind == RaftTimerActionKind::kHeartbeat
             ? DurableRaftRequest{group_id, HeartbeatOperation{}}
             : DurableRaftRequest{group_id, StartElectionOperation{}};
}
RaftTimerRuntime::RaftTimerRuntime(std::unique_ptr<Impl> impl) noexcept
    : implementation_(std::move(impl)) {}
RaftTimerRuntime::~RaftTimerRuntime() = default;
RaftTimerRuntime::RaftTimerRuntime(RaftTimerRuntime&&) noexcept = default;
RaftTimerRuntime& RaftTimerRuntime::operator=(RaftTimerRuntime&&) noexcept = default;

common::Result<RaftTimerRuntime> RaftTimerRuntime::create(const RaftTimerLimits limits) {
  if (limits.maximum_groups == 0U || limits.maximum_actions_per_poll == 0U ||
      limits.heartbeat_interval.count() <= 0)
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "Raft timer limits are invalid"));
  try {
    auto impl = std::make_unique<Impl>(limits);
    impl->groups_.reserve(limits.maximum_groups);
    return RaftTimerRuntime{std::move(impl)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted, "Raft timer allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft timer group bound exceeds container limits"));
  }
}

common::Status RaftTimerRuntime::add_group(const RaftGroupObservation& observation, TimePoint now,
                                           TimePoint election_deadline) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft timer runtime is empty");
  if (implementation_->find(observation.group_id) != nullptr)
    return status(common::StatusCode::kAlreadyExists, "Raft timer group already exists");
  if (implementation_->groups_.size() == implementation_->limits_.maximum_groups)
    return status(common::StatusCode::kResourceExhausted, "Raft timer group bound is full");
  Impl::Timer timer;
  common::Status armed = implementation_->arm(timer, observation, now, election_deadline, false);
  if (!armed.is_ok())
    return armed;
  implementation_->groups_.push_back(std::move(timer));
  return common::Status::ok();
}
common::Status RaftTimerRuntime::remove_group(const GroupId& group_id) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft timer runtime is empty");
  for (auto it = implementation_->groups_.begin(); it != implementation_->groups_.end(); ++it) {
    if (it->group_id == group_id) {
      implementation_->groups_.erase(it);
      return common::Status::ok();
    }
  }
  return status(common::StatusCode::kNotFound, "Raft timer group does not exist");
}
common::Status RaftTimerRuntime::note_activity(const RaftGroupObservation& observation,
                                               TimePoint now, TimePoint election_deadline) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft timer runtime is empty");
  Impl::Timer* timer = implementation_->find(observation.group_id);
  return timer == nullptr ? status(common::StatusCode::kNotFound, "Raft timer group does not exist")
                          : implementation_->arm(*timer, observation, now, election_deadline, true);
}
common::Result<std::vector<RaftTimerAction>> RaftTimerRuntime::poll(TimePoint now) {
  if (!implementation_)
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "Raft timer runtime is empty"));
  try {
    std::vector<RaftTimerAction> actions;
    actions.reserve(implementation_->limits_.maximum_actions_per_poll);
    for (Impl::Timer& timer : implementation_->groups_) {
      if (actions.size() == implementation_->limits_.maximum_actions_per_poll)
        break;
      if (timer.in_flight || now < timer.deadline)
        continue;
      timer.in_flight = true;
      actions.push_back({timer.group_id, timer.generation,
                         timer.role == Role::kLeader ? RaftTimerActionKind::kHeartbeat
                                                     : RaftTimerActionKind::kStartElection});
    }
    return actions;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted, "Raft timer poll allocation failed"));
  }
}
common::Status RaftTimerRuntime::reject_admission(const RaftTimerAction& action) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft timer runtime is empty");
  Impl::Timer* timer = implementation_->find(action.group_id);
  if (timer == nullptr)
    return status(common::StatusCode::kNotFound, "Raft timer group does not exist");
  if (!timer->in_flight || timer->generation != action.generation)
    return status(common::StatusCode::kInvalidArgument, "Raft timer action is stale");
  timer->in_flight = false;
  return common::Status::ok();
}
common::Status RaftTimerRuntime::complete(const RaftTimerAction& action,
                                          const RaftGroupObservation& observation, TimePoint now,
                                          TimePoint election_deadline) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft timer runtime is empty");
  Impl::Timer* timer = implementation_->find(action.group_id);
  if (timer == nullptr)
    return status(common::StatusCode::kNotFound, "Raft timer group does not exist");
  if (!timer->in_flight || timer->generation != action.generation ||
      observation.group_id != action.group_id)
    return status(common::StatusCode::kInvalidArgument, "Raft timer completion is stale");
  return implementation_->arm(*timer, observation, now, election_deadline, true);
}
std::optional<RaftTimerRuntime::TimePoint> RaftTimerRuntime::next_deadline() const noexcept {
  std::optional<TimePoint> next;
  if (!implementation_)
    return next;
  for (const Impl::Timer& timer : implementation_->groups_)
    if (!timer.in_flight && (!next.has_value() || timer.deadline < *next))
      next = timer.deadline;
  return next;
}
std::size_t RaftTimerRuntime::group_count() const noexcept {
  return implementation_ ? implementation_->groups_.size() : 0U;
}
} // namespace chronos::raft
