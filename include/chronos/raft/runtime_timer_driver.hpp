#ifndef CHRONOS_RAFT_RUNTIME_TIMER_DRIVER_HPP_
#define CHRONOS_RAFT_RUNTIME_TIMER_DRIVER_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/async_durable_runtime.hpp"
#include "chronos/raft/runtime_timer.hpp"

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>

namespace chronos::raft {

// Embedding-owned election randomization policy. Implementations must return a strictly future
// monotonic deadline and provide their own synchronization if shared elsewhere.
class RaftElectionDeadlineSource {
public:
  virtual ~RaftElectionDeadlineSource() = default;
  [[nodiscard]] virtual common::Result<RaftTimerRuntime::TimePoint>
  next_election_deadline(const GroupId& group_id, Term current_term,
                         RaftTimerRuntime::TimePoint now) = 0;
};

struct RaftTimerDriverLimits {
  std::size_t maximum_inflight_actions{1024U};
  std::size_t maximum_completed_actions{1024U};
  RaftTimerLimits timers;
};

struct RaftTimerDriverConfig {
  AsyncDurableMultiRaftRuntime* runtime{};
  RaftElectionDeadlineSource* election_deadlines{};
  RaftTimerDriverLimits limits;
};

struct RaftTimerCompletedAction {
  RaftTimerAction action;
  DurableRaftResult result;
  RaftGroupObservation observation;
};

// Single-event-loop composition of generation-tagged deadlines and the asynchronous durable owner.
// Each admitted timer action is followed by ObserveGroupOperation in the same FIFO batch. Completed
// transitions remain owned here until take_completed(), preserving outbound/snapshot/read results.
class RaftTimerDriver {
public:
  using TimePoint = RaftTimerRuntime::TimePoint;

  RaftTimerDriver() = delete;
  ~RaftTimerDriver();
  RaftTimerDriver(const RaftTimerDriver&) = delete;
  RaftTimerDriver& operator=(const RaftTimerDriver&) = delete;
  RaftTimerDriver(RaftTimerDriver&&) noexcept;
  RaftTimerDriver& operator=(RaftTimerDriver&&) noexcept;

  [[nodiscard]] static common::Result<RaftTimerDriver> create(RaftTimerDriverConfig config);
  [[nodiscard]] common::Status add_group(const RaftGroupObservation& observation, TimePoint now);
  [[nodiscard]] common::Status remove_group(const GroupId& group_id);
  [[nodiscard]] common::Status note_activity(const RaftGroupObservation& observation,
                                             TimePoint now);
  [[nodiscard]] common::Status drive(TimePoint now);
  [[nodiscard]] common::Result<RaftTimerCompletedAction> take_completed();
  [[nodiscard]] std::optional<TimePoint> next_deadline() const noexcept;
  [[nodiscard]] std::size_t inflight_actions() const noexcept;
  [[nodiscard]] std::size_t completed_actions() const noexcept;
  [[nodiscard]] bool failed() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit RaftTimerDriver(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_RUNTIME_TIMER_DRIVER_HPP_
