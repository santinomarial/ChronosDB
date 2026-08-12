#include "chronos/raft/runtime_timer.hpp"

#include <chrono>
#include <gtest/gtest.h>
#include <variant>

namespace chronos::raft {
namespace {

[[nodiscard]] GroupId group(const std::byte seed = std::byte{1U}) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return GroupId{bytes};
}
[[nodiscard]] RaftGroupObservation observation(const Role role, const Term term = 1U) {
  return {.group_id = group(), .node_id = 2U, .role = role, .current_term = term};
}

TEST(RaftTimerRuntimeTest, EmitsRetriesAndRearmsElectionAndHeartbeatActions) {
  using namespace std::chrono_literals;
  const auto start = RaftTimerRuntime::TimePoint{};
  auto timers = RaftTimerRuntime::create(
      {.maximum_groups = 2U, .maximum_actions_per_poll = 1U, .heartbeat_interval = 5ms});
  ASSERT_TRUE(timers.has_value()) << timers.error().to_string();
  ASSERT_TRUE(timers->add_group(observation(Role::kFollower), start, start + 10ms).is_ok());
  ASSERT_TRUE(timers->next_deadline().has_value());
  EXPECT_EQ(*timers->next_deadline(), start + 10ms);
  EXPECT_TRUE(timers->poll(start + 9ms)->empty());
  auto due = timers->poll(start + 10ms);
  ASSERT_EQ(due->size(), 1U);
  EXPECT_FALSE(timers->next_deadline().has_value());
  EXPECT_EQ(due->front().kind, RaftTimerActionKind::kStartElection);
  EXPECT_TRUE(std::holds_alternative<StartElectionOperation>(due->front().request().operation));
  EXPECT_TRUE(timers->poll(start + 10ms)->empty());
  ASSERT_TRUE(timers->reject_admission(due->front()).is_ok());
  auto retry = timers->poll(start + 10ms);
  ASSERT_EQ(retry->size(), 1U);
  EXPECT_EQ(retry->front().generation, due->front().generation);
  ASSERT_TRUE(
      timers->complete(retry->front(), observation(Role::kLeader, 2U), start + 10ms, start + 20ms)
          .is_ok());
  ASSERT_TRUE(timers->next_deadline().has_value());
  EXPECT_EQ(*timers->next_deadline(), start + 15ms);
  EXPECT_TRUE(timers->poll(start + 14ms)->empty());
  auto heartbeat = timers->poll(start + 15ms);
  ASSERT_EQ(heartbeat->size(), 1U);
  EXPECT_EQ(heartbeat->front().kind, RaftTimerActionKind::kHeartbeat);
  EXPECT_TRUE(std::holds_alternative<HeartbeatOperation>(heartbeat->front().request().operation));
}

TEST(RaftTimerRuntimeTest, NewerActivityInvalidatesStaleCompletionAndBoundsGroups) {
  using namespace std::chrono_literals;
  const auto start = RaftTimerRuntime::TimePoint{};
  auto timers = RaftTimerRuntime::create(
      {.maximum_groups = 1U, .maximum_actions_per_poll = 1U, .heartbeat_interval = 5ms});
  ASSERT_TRUE(timers.has_value());
  ASSERT_TRUE(timers->add_group(observation(Role::kFollower), start, start + 1ms).is_ok());
  const auto action = timers->poll(start + 1ms)->front();
  ASSERT_TRUE(
      timers->note_activity(observation(Role::kFollower, 2U), start + 2ms, start + 20ms).is_ok());
  EXPECT_EQ(
      timers->complete(action, observation(Role::kCandidate, 2U), start + 3ms, start + 30ms).code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(timers->poll(start + 19ms)->empty());
  EXPECT_EQ(timers
                ->add_group({.group_id = group(std::byte{2U}),
                             .node_id = 2U,
                             .role = Role::kFollower,
                             .current_term = 1U},
                            start, start + 10ms)
                .code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(timers->note_activity(observation(Role::kFollower), start, start).code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::raft
