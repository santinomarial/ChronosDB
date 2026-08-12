#ifndef CHRONOS_RAFT_RUNTIME_TIMER_HPP_
#define CHRONOS_RAFT_RUNTIME_TIMER_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/durable_runtime.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::raft {

struct RaftTimerLimits {
  std::size_t maximum_groups{4096U};
  std::size_t maximum_actions_per_poll{1024U};
  std::chrono::milliseconds heartbeat_interval{100};
};
enum class RaftTimerActionKind : std::uint8_t { kStartElection = 1, kHeartbeat = 2 };
struct RaftTimerAction {
  GroupId group_id;
  std::uint64_t generation{};
  RaftTimerActionKind kind{RaftTimerActionKind::kStartElection};
  [[nodiscard]] DurableRaftRequest request() const;
};

// Single-thread-affine bounded deadline scheduler. The embedding supplies randomized election
// deadlines. Generation-tagged actions reject stale completion after newer observed activity.
class RaftTimerRuntime {
public:
  using TimePoint = std::chrono::steady_clock::time_point;
  RaftTimerRuntime() = delete;
  ~RaftTimerRuntime();
  RaftTimerRuntime(const RaftTimerRuntime&) = delete;
  RaftTimerRuntime& operator=(const RaftTimerRuntime&) = delete;
  RaftTimerRuntime(RaftTimerRuntime&&) noexcept;
  RaftTimerRuntime& operator=(RaftTimerRuntime&&) noexcept;
  [[nodiscard]] static common::Result<RaftTimerRuntime> create(RaftTimerLimits limits = {});
  [[nodiscard]] common::Status add_group(const RaftGroupObservation& observation, TimePoint now,
                                         TimePoint election_deadline);
  [[nodiscard]] common::Status remove_group(const GroupId& group_id);
  [[nodiscard]] common::Status note_activity(const RaftGroupObservation& observation, TimePoint now,
                                             TimePoint election_deadline);
  [[nodiscard]] common::Result<std::vector<RaftTimerAction>> poll(TimePoint now);
  [[nodiscard]] common::Status reject_admission(const RaftTimerAction& action);
  [[nodiscard]] common::Status complete(const RaftTimerAction& action,
                                        const RaftGroupObservation& observation, TimePoint now,
                                        TimePoint election_deadline);
  [[nodiscard]] std::size_t group_count() const noexcept;

private:
  class Impl;
  explicit RaftTimerRuntime(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};
} // namespace chronos::raft
#endif // CHRONOS_RAFT_RUNTIME_TIMER_HPP_
